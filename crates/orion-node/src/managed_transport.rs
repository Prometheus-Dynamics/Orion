use crate::{
    ManagedNodeTransportSurface, ManagedServerTransportSecurity, NodeApp, NodeError,
    app::GracefulTaskHandle,
};
use orion::transport::http::{HttpControlHandler, HttpServer};
#[cfg(feature = "transport-quic")]
use orion::transport::quic::{QuicEndpoint, QuicFrameHandler, QuicFrameServer};
#[cfg(feature = "transport-tcp")]
use orion::transport::tcp::{TcpFrameHandler, TcpFrameServer};
use std::{net::SocketAddr, sync::Arc};
use tokio::sync::oneshot;

#[derive(Clone)]
pub(crate) enum ManagedSurfaceLaunchRequest {
    PeerHttpControl {
        addr: SocketAddr,
        handler: Arc<dyn HttpControlHandler>,
    },
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
            Self::PeerHttpControl { .. } => ManagedNodeTransportSurface::PeerHttpControl,
            Self::HttpProbe { .. } => ManagedNodeTransportSurface::HttpProbe,
            #[cfg(feature = "transport-tcp")]
            Self::PeerTcpData { .. } => ManagedNodeTransportSurface::PeerTcpData,
            #[cfg(feature = "transport-quic")]
            Self::PeerQuicData { .. } => ManagedNodeTransportSurface::PeerQuicData,
        }
    }

    #[cfg(any(test, feature = "transport-tcp", feature = "transport-quic"))]
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
            Self::PeerHttpControl { addr, handler } => {
                start_http_surface(app.clone(), surface, addr, handler, false).await?
            }
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
    Socket(SocketAddr),
    #[cfg(feature = "transport-quic")]
    Quic(QuicEndpoint),
}

#[cfg(any(test, feature = "transport-tcp", feature = "transport-quic"))]
fn spawn_managed_task<F>(future: F) -> tokio::task::JoinHandle<Result<(), NodeError>>
where
    F: std::future::Future<Output = Result<(), NodeError>> + Send + 'static,
{
    tokio::spawn(future)
}

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

#[cfg(any(test, feature = "transport-tcp", feature = "transport-quic"))]
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

#[cfg(feature = "transport-quic")]
fn launch_quic_surface<F>(
    endpoint: QuicEndpoint,
    future: F,
) -> (
    ManagedTransportBinding,
    tokio::task::JoinHandle<Result<(), NodeError>>,
)
where
    F: std::future::Future<Output = Result<(), NodeError>> + Send + 'static,
{
    (
        ManagedTransportBinding::Quic(endpoint),
        spawn_managed_task(future),
    )
}

#[cfg(feature = "transport-quic")]
fn launch_quic_surface_with_shutdown<F>(
    endpoint: QuicEndpoint,
    future: impl FnOnce(oneshot::Receiver<()>) -> F,
) -> (ManagedTransportBinding, GracefulTaskHandle<NodeError>)
where
    F: std::future::Future<Output = Result<(), NodeError>> + Send + 'static,
{
    (
        ManagedTransportBinding::Quic(endpoint),
        new_shutdown_handle(future),
    )
}

fn resolve_http_tls(
    app: &NodeApp,
    surface: ManagedNodeTransportSurface,
) -> Result<Option<orion::transport::http::HttpServerTlsConfig>, NodeError> {
    match app.managed_surface_server_transport_security(surface)? {
        Some(ManagedServerTransportSecurity::Http(tls)) => Ok(Some(tls)),
        #[cfg(any(feature = "transport-tcp", feature = "transport-quic"))]
        Some(_) => Err(NodeError::Storage(
            "managed HTTP adapter resolved non-HTTP transport security".into(),
        )),
        None => Ok(None),
    }
}

#[cfg(feature = "transport-tcp")]
fn resolve_tcp_tls(
    app: &NodeApp,
    surface: ManagedNodeTransportSurface,
) -> Result<Option<orion::transport::tcp::TcpServerTlsConfig>, NodeError> {
    match app.managed_surface_server_transport_security(surface)? {
        Some(ManagedServerTransportSecurity::Tcp(tls)) => Ok(Some(tls)),
        Some(_) => Err(NodeError::Storage(
            "managed TCP adapter resolved non-TCP transport security".into(),
        )),
        None => Ok(None),
    }
}

#[cfg(feature = "transport-quic")]
fn resolve_quic_tls(
    app: &NodeApp,
    surface: ManagedNodeTransportSurface,
) -> Result<Option<orion::transport::quic::QuicServerTlsConfig>, NodeError> {
    match app.managed_surface_server_transport_security(surface)? {
        Some(ManagedServerTransportSecurity::Quic(tls)) => Ok(Some(tls)),
        Some(_) => Err(NodeError::Storage(
            "managed QUIC adapter resolved non-QUIC transport security".into(),
        )),
        None => Ok(None),
    }
}

#[cfg(feature = "transport-quic")]
fn peer_quic_endpoint(
    surface: ManagedNodeTransportSurface,
    addr: SocketAddr,
    server_name: Option<String>,
) -> Result<QuicEndpoint, NodeError> {
    match surface {
        ManagedNodeTransportSurface::PeerQuicData => {
            let mut endpoint = QuicEndpoint::new(addr.ip().to_string(), addr.port());
            if let Some(server_name) = server_name {
                endpoint = endpoint.with_server_name(server_name);
            }
            Ok(endpoint)
        }
        _ => Err(NodeError::Storage(
            "non-QUIC managed surface cannot be started with the QUIC transport adapter".into(),
        )),
    }
}

#[cfg(any(test, feature = "transport-tcp", feature = "transport-quic"))]
async fn start_http_surface(
    app: NodeApp,
    surface: ManagedNodeTransportSurface,
    addr: SocketAddr,
    handler: Arc<dyn HttpControlHandler>,
    probe: bool,
) -> Result<
    (
        ManagedTransportBinding,
        tokio::task::JoinHandle<Result<(), NodeError>>,
    ),
    NodeError,
> {
    let (addr, server, listener) = if probe {
        HttpServer::bind_probe(addr, handler).await?
    } else {
        HttpServer::bind(addr, handler).await?
    };
    let tls = resolve_http_tls(&app, surface)?;
    Ok(launch_socket_surface(addr, async move {
        let result = match tls {
            Some(tls) => server.serve_tls(listener, tls).await,
            None => server.serve(listener).await,
        };
        result.map_err(NodeError::from)
    }))
}

async fn start_http_surface_with_shutdown(
    app: NodeApp,
    surface: ManagedNodeTransportSurface,
    addr: SocketAddr,
    handler: Arc<dyn HttpControlHandler>,
    probe: bool,
) -> Result<(ManagedTransportBinding, GracefulTaskHandle<NodeError>), NodeError> {
    let (addr, server, listener) = if probe {
        HttpServer::bind_probe(addr, handler).await?
    } else {
        HttpServer::bind(addr, handler).await?
    };
    let tls = resolve_http_tls(&app, surface)?;
    Ok(launch_socket_surface_with_shutdown(
        addr,
        move |shutdown_rx| async move {
            let result = match tls {
                Some(tls) => {
                    server
                        .serve_tls_with_shutdown(listener, tls, async {
                            let _ = shutdown_rx.await;
                        })
                        .await
                }
                None => {
                    server
                        .serve_with_shutdown(listener, async {
                            let _ = shutdown_rx.await;
                        })
                        .await
                }
            };
            result.map_err(NodeError::from)
        },
    ))
}

#[cfg(feature = "transport-tcp")]
async fn start_tcp_surface(
    app: NodeApp,
    surface: ManagedNodeTransportSurface,
    addr: SocketAddr,
    handler: Arc<dyn TcpFrameHandler>,
) -> Result<
    (
        ManagedTransportBinding,
        tokio::task::JoinHandle<Result<(), NodeError>>,
    ),
    NodeError,
> {
    let (addr, server, listener) = match surface {
        ManagedNodeTransportSurface::PeerTcpData => TcpFrameServer::bind(addr, handler).await?,
        _ => {
            return Err(NodeError::Storage(
                "non-TCP managed surface cannot be started with the TCP transport adapter".into(),
            ));
        }
    };
    let tls = resolve_tcp_tls(&app, surface)?;
    Ok(launch_socket_surface(addr, async move {
        let result = match tls {
            Some(tls) => server.serve_tls(listener, tls).await,
            None => server.serve(listener).await,
        };
        result.map_err(NodeError::from)
    }))
}

#[cfg(feature = "transport-tcp")]
async fn start_tcp_surface_with_shutdown(
    app: NodeApp,
    surface: ManagedNodeTransportSurface,
    addr: SocketAddr,
    handler: Arc<dyn TcpFrameHandler>,
) -> Result<(ManagedTransportBinding, GracefulTaskHandle<NodeError>), NodeError> {
    let (addr, server, listener) = match surface {
        ManagedNodeTransportSurface::PeerTcpData => TcpFrameServer::bind(addr, handler).await?,
        _ => {
            return Err(NodeError::Storage(
                "non-TCP managed surface cannot be started with the TCP transport adapter".into(),
            ));
        }
    };
    let tls = resolve_tcp_tls(&app, surface)?;
    Ok(launch_socket_surface_with_shutdown(
        addr,
        move |shutdown_rx| async move {
            let result = match tls {
                Some(tls) => {
                    server
                        .serve_tls_with_shutdown(listener, tls, async {
                            let _ = shutdown_rx.await;
                        })
                        .await
                }
                None => {
                    server
                        .serve_with_shutdown(listener, async {
                            let _ = shutdown_rx.await;
                        })
                        .await
                }
            };
            result.map_err(NodeError::from)
        },
    ))
}

#[cfg(feature = "transport-quic")]
async fn start_quic_surface(
    app: NodeApp,
    surface: ManagedNodeTransportSurface,
    addr: SocketAddr,
    handler: Arc<dyn QuicFrameHandler>,
    server_name: Option<String>,
) -> Result<
    (
        ManagedTransportBinding,
        tokio::task::JoinHandle<Result<(), NodeError>>,
    ),
    NodeError,
> {
    let endpoint = peer_quic_endpoint(surface, addr, server_name)?;
    let tls = resolve_quic_tls(&app, surface)?;

    let (server, local_endpoint) = match tls {
        Some(tls) => QuicFrameServer::bind_secure(endpoint, handler, tls).await?,
        None => QuicFrameServer::bind(endpoint, handler).await?,
    };
    Ok(launch_quic_surface(local_endpoint, async move {
        server.serve().await.map_err(NodeError::from)
    }))
}

#[cfg(feature = "transport-quic")]
async fn start_quic_surface_with_shutdown(
    app: NodeApp,
    surface: ManagedNodeTransportSurface,
    addr: SocketAddr,
    handler: Arc<dyn QuicFrameHandler>,
    server_name: Option<String>,
) -> Result<(ManagedTransportBinding, GracefulTaskHandle<NodeError>), NodeError> {
    let endpoint = peer_quic_endpoint(surface, addr, server_name)?;
    let tls = resolve_quic_tls(&app, surface)?;

    let (server, local_endpoint) = match tls {
        Some(tls) => QuicFrameServer::bind_secure(endpoint, handler, tls).await?,
        None => QuicFrameServer::bind(endpoint, handler).await?,
    };
    Ok(launch_quic_surface_with_shutdown(
        local_endpoint,
        move |shutdown_rx| async move {
            server
                .serve_with_shutdown(async {
                    let _ = shutdown_rx.await;
                })
                .await
                .map_err(NodeError::from)
        },
    ))
}
