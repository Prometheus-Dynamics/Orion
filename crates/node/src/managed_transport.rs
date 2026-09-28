#[cfg(feature = "transport-http")]
use crate::app::GracefulTaskHandle;
use crate::{ManagedNodeTransportSurface, NodeApp, NodeError};
#[cfg(feature = "transport-http")]
use orion::transport::http::HttpControlHandler;
#[cfg(feature = "transport-quic")]
use orion::transport::quic::{QuicEndpoint, QuicFrameHandler};
#[cfg(feature = "transport-tcp")]
use orion::transport::tcp::TcpFrameHandler;
#[cfg(any(feature = "transport-tcp", feature = "transport-quic"))]
use orion_data_plane::RemoteBinding;
#[cfg(any(feature = "transport-tcp", feature = "transport-quic"))]
use std::collections::BTreeMap;
use std::{net::SocketAddr, sync::Arc};
#[cfg(feature = "transport-http")]
use tokio::sync::oneshot;

#[cfg(feature = "transport-http")]
mod http;
#[cfg(feature = "transport-quic")]
mod quic;
#[cfg(feature = "transport-tcp")]
mod tcp;

#[cfg(all(
    feature = "transport-http",
    any(test, feature = "transport-tcp", feature = "transport-quic")
))]
use http::start_http_surface;
#[cfg(feature = "transport-http")]
use http::start_http_surface_with_shutdown;
#[cfg(feature = "transport-quic")]
use quic::start_quic_surface;
#[cfg(all(feature = "transport-http", feature = "transport-quic"))]
use quic::start_quic_surface_with_shutdown;
#[cfg(feature = "transport-tcp")]
use tcp::start_tcp_surface;
#[cfg(all(feature = "transport-http", feature = "transport-tcp"))]
use tcp::start_tcp_surface_with_shutdown;

#[derive(Clone)]
pub(crate) enum ManagedSurfaceLaunchRequest {
    #[cfg(feature = "transport-http")]
    PeerHttpControl {
        addr: SocketAddr,
        handler: Arc<dyn HttpControlHandler>,
    },
    #[cfg(feature = "transport-http")]
    HttpProbe {
        addr: SocketAddr,
        handler: Arc<dyn HttpControlHandler>,
    },
    #[cfg(feature = "transport-tcp")]
    PeerTcpData {
        addr: SocketAddr,
        handler: Arc<dyn TcpFrameHandler>,
    },
    #[cfg(feature = "transport-quic")]
    PeerQuicData {
        addr: SocketAddr,
        handler: Arc<dyn QuicFrameHandler>,
        server_name: Option<String>,
    },
}

impl ManagedSurfaceLaunchRequest {
    pub fn surface(&self) -> ManagedNodeTransportSurface {
        match self {
            #[cfg(feature = "transport-http")]
            Self::PeerHttpControl { .. } => ManagedNodeTransportSurface::PeerHttpControl,
            #[cfg(feature = "transport-http")]
            Self::HttpProbe { .. } => ManagedNodeTransportSurface::HttpProbe,
            #[cfg(feature = "transport-tcp")]
            Self::PeerTcpData { .. } => ManagedNodeTransportSurface::PeerTcpData,
            #[cfg(feature = "transport-quic")]
            Self::PeerQuicData { .. } => ManagedNodeTransportSurface::PeerQuicData,
        }
    }

    #[cfg(any(
        all(test, feature = "transport-http"),
        feature = "transport-tcp",
        feature = "transport-quic"
    ))]
    pub async fn start(
        self,
        app: NodeApp,
    ) -> Result<
        (
            ManagedTransportBinding,
            tokio::task::JoinHandle<Result<(), NodeError>>,
        ),
        NodeError,
    > {
        let surface = self.surface();
        let started = match self {
            #[cfg(feature = "transport-http")]
            Self::PeerHttpControl { addr, handler } => {
                start_http_surface(app.clone(), surface, addr, handler, false).await?
            }
            #[cfg(feature = "transport-http")]
            Self::HttpProbe { addr, handler } => {
                start_http_surface(app.clone(), surface, addr, handler, true).await?
            }
            #[cfg(feature = "transport-tcp")]
            Self::PeerTcpData { addr, handler } => {
                start_tcp_surface(app.clone(), surface, addr, handler).await?
            }
            #[cfg(feature = "transport-quic")]
            Self::PeerQuicData {
                addr,
                handler,
                server_name,
            } => start_quic_surface(app.clone(), surface, addr, handler, server_name).await?,
        };
        Ok(started)
    }

    #[cfg(feature = "transport-http")]
    pub async fn start_with_shutdown(
        self,
        app: NodeApp,
    ) -> Result<(ManagedTransportBinding, GracefulTaskHandle<NodeError>), NodeError> {
        let surface = self.surface();
        let started = match self {
            Self::PeerHttpControl { addr, handler } => {
                start_http_surface_with_shutdown(app.clone(), surface, addr, handler, false).await?
            }
            Self::HttpProbe { addr, handler } => {
                start_http_surface_with_shutdown(app.clone(), surface, addr, handler, true).await?
            }
            #[cfg(feature = "transport-tcp")]
            Self::PeerTcpData { addr, handler } => {
                start_tcp_surface_with_shutdown(app.clone(), surface, addr, handler).await?
            }
            #[cfg(feature = "transport-quic")]
            Self::PeerQuicData {
                addr,
                handler,
                server_name,
            } => {
                start_quic_surface_with_shutdown(app.clone(), surface, addr, handler, server_name)
                    .await?
            }
        };
        Ok(started)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ManagedTransportBinding {
    #[cfg(any(feature = "transport-http", feature = "transport-tcp"))]
    Socket(SocketAddr),
    #[cfg(feature = "transport-quic")]
    Quic(QuicEndpoint),
}

#[cfg(any(
    all(test, feature = "transport-http"),
    feature = "transport-tcp",
    feature = "transport-quic"
))]
fn spawn_managed_task<F>(future: F) -> tokio::task::JoinHandle<Result<(), NodeError>>
where
    F: std::future::Future<Output = Result<(), NodeError>> + Send + 'static,
{
    tokio::spawn(future)
}

#[cfg(feature = "transport-http")]
fn new_shutdown_handle<F>(
    future: impl FnOnce(oneshot::Receiver<()>) -> F,
) -> GracefulTaskHandle<NodeError>
where
    F: std::future::Future<Output = Result<(), NodeError>> + Send + 'static,
{
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let handle = tokio::spawn(future(shutdown_rx));
    GracefulTaskHandle::new(shutdown_tx, handle)
}

#[cfg(any(
    feature = "transport-tcp",
    all(feature = "transport-http", any(test, feature = "transport-quic"))
))]
fn launch_socket_surface<F>(
    addr: SocketAddr,
    future: F,
) -> (
    ManagedTransportBinding,
    tokio::task::JoinHandle<Result<(), NodeError>>,
)
where
    F: std::future::Future<Output = Result<(), NodeError>> + Send + 'static,
{
    (
        ManagedTransportBinding::Socket(addr),
        spawn_managed_task(future),
    )
}

#[cfg(feature = "transport-http")]
fn launch_socket_surface_with_shutdown<F>(
    addr: SocketAddr,
    future: impl FnOnce(oneshot::Receiver<()>) -> F,
) -> (ManagedTransportBinding, GracefulTaskHandle<NodeError>)
where
    F: std::future::Future<Output = Result<(), NodeError>> + Send + 'static,
{
    (
        ManagedTransportBinding::Socket(addr),
        new_shutdown_handle(future),
    )
}

#[cfg(any(feature = "transport-tcp", feature = "transport-quic"))]
fn data_endpoint(
    transport: &str,
    local: &str,
    remote: &str,
    peer_node_id: &str,
    binding: &RemoteBinding,
) -> crate::app::CommunicationEndpointRuntime {
    let (resource_id, binding_kind) = match binding {
        RemoteBinding::Channel(binding) => (binding.resource_id.to_string(), "channel"),
        RemoteBinding::Proxy(binding) => (binding.resource_id.to_string(), "proxy"),
    };
    let mut endpoint = crate::app::CommunicationEndpointRuntime::new(
        format!("{transport}/data-plane/{peer_node_id}/{resource_id}"),
        transport,
        "data_plane",
    );
    endpoint.local = Some(local.to_owned());
    endpoint.remote = Some(remote.to_owned());
    endpoint.labels = BTreeMap::from([
        ("peer_node_id".to_owned(), peer_node_id.to_owned()),
        ("resource_id".to_owned(), resource_id),
        ("binding".to_owned(), binding_kind.to_owned()),
    ]);
    endpoint
}
