#[cfg(feature = "transport-http")]
use super::new_shutdown_handle;
use super::{ManagedTransportBinding, data_endpoint, spawn_managed_task};
#[cfg(feature = "transport-http")]
use crate::app::GracefulTaskHandle;
use crate::{ManagedNodeTransportSurface, ManagedServerTransportSecurity, NodeApp, NodeError};
use orion::transport::quic::{
    QuicEndpoint, QuicFrame, QuicFrameClient, QuicFrameHandler, QuicFrameServer, QuicTransportError,
};
use orion_transport_quic::QuicCodec;
use std::time::Instant;
use std::{net::SocketAddr, sync::Arc};
#[cfg(feature = "transport-http")]
use tokio::sync::oneshot;

impl NodeApp {
    pub async fn send_quic_data_frame_metered(
        &self,
        client: &QuicFrameClient,
        frame: QuicFrame,
    ) -> Result<QuicFrame, QuicTransportError> {
        let started = Instant::now();
        let encode_started = Instant::now();
        let bytes_sent = QuicCodec
            .encode_frame(&frame)
            .map(|bytes| bytes.len().min(u64::MAX as usize) as u64)
            .unwrap_or(0);
        let encode_duration = encode_started.elapsed();
        let endpoint = quic_outbound_data_endpoint(&frame);
        match client.send(frame).await {
            Ok(response) => {
                let decode_started = Instant::now();
                let bytes_received = QuicCodec
                    .encode_frame(&response)
                    .map(|bytes| bytes.len().min(u64::MAX as usize) as u64)
                    .unwrap_or(0);
                let decode_duration = decode_started.elapsed();
                self.record_communication_endpoint_exchange_with_stages(
                    endpoint,
                    bytes_received,
                    bytes_sent,
                    started.elapsed(),
                    crate::app::CommunicationStageDurations {
                        encode: Some(encode_duration),
                        decode: Some(decode_duration),
                        socket_write: Some(started.elapsed()),
                        ..Default::default()
                    },
                );
                Ok(response)
            }
            Err(err) => {
                self.record_communication_endpoint_failure_kind(
                    endpoint,
                    Some(started.elapsed()),
                    crate::app::classify_quic_communication_failure(&err),
                    err.to_string(),
                );
                Err(err)
            }
        }
    }
}

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

#[cfg(feature = "transport-http")]
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

fn resolve_quic_tls(
    app: &NodeApp,
    surface: ManagedNodeTransportSurface,
) -> Result<Option<orion::transport::quic::QuicServerTlsConfig>, NodeError> {
    match app.managed_surface_server_transport_security(surface)? {
        Some(ManagedServerTransportSecurity::Quic(tls)) => Ok(Some(tls)),
        #[cfg(any(feature = "transport-http", feature = "transport-tcp"))]
        Some(_) => Err(NodeError::Storage(
            "managed QUIC adapter resolved non-QUIC transport security".into(),
        )),
        None => Ok(None),
    }
}

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
        #[cfg(any(feature = "transport-http", feature = "transport-tcp"))]
        _ => Err(NodeError::Storage(
            "non-QUIC managed surface cannot be started with the QUIC transport adapter".into(),
        )),
    }
}

pub(super) async fn start_quic_surface(
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
    let handler: Arc<dyn QuicFrameHandler> = Arc::new(MeteredQuicFrameHandler {
        app: app.clone(),
        inner: handler,
    });

    let (server, local_endpoint) = match tls {
        Some(tls) => QuicFrameServer::bind_secure(endpoint, handler, tls).await?,
        None => {
            return Err(NodeError::Storage(
                "managed QUIC surface requires explicit TLS configuration".into(),
            ));
        }
    };
    let server = server
        .with_max_payload_bytes(app.config.runtime_tuning.transport_max_payload_bytes)
        .with_io_timeout(app.config.runtime_tuning.transport_io_timeout)
        .with_max_connections(
            app.config
                .runtime_tuning
                .transport_max_concurrent_connections,
        );
    Ok(launch_quic_surface(local_endpoint, async move {
        server.serve().await.map_err(NodeError::from)
    }))
}

#[cfg(feature = "transport-http")]
pub(super) async fn start_quic_surface_with_shutdown(
    app: NodeApp,
    surface: ManagedNodeTransportSurface,
    addr: SocketAddr,
    handler: Arc<dyn QuicFrameHandler>,
    server_name: Option<String>,
) -> Result<(ManagedTransportBinding, GracefulTaskHandle<NodeError>), NodeError> {
    let endpoint = peer_quic_endpoint(surface, addr, server_name)?;
    let tls = resolve_quic_tls(&app, surface)?;
    let handler: Arc<dyn QuicFrameHandler> = Arc::new(MeteredQuicFrameHandler {
        app: app.clone(),
        inner: handler,
    });

    let (server, local_endpoint) = match tls {
        Some(tls) => QuicFrameServer::bind_secure(endpoint, handler, tls).await?,
        None => {
            return Err(NodeError::Storage(
                "managed QUIC surface requires explicit TLS configuration".into(),
            ));
        }
    };
    let server = server
        .with_max_payload_bytes(app.config.runtime_tuning.transport_max_payload_bytes)
        .with_io_timeout(app.config.runtime_tuning.transport_io_timeout)
        .with_max_connections(
            app.config
                .runtime_tuning
                .transport_max_concurrent_connections,
        );
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

struct MeteredQuicFrameHandler {
    app: NodeApp,
    inner: Arc<dyn QuicFrameHandler>,
}

impl QuicFrameHandler for MeteredQuicFrameHandler {
    fn handle_frame(&self, frame: QuicFrame) -> Result<QuicFrame, QuicTransportError> {
        let started = Instant::now();
        let decode_started = Instant::now();
        let bytes_received = QuicCodec
            .encode_frame(&frame)
            .map(|bytes| bytes.len().min(u64::MAX as usize) as u64)
            .unwrap_or(0);
        let decode_duration = decode_started.elapsed();
        let endpoint = quic_inbound_data_endpoint(&frame);
        let response = self.inner.handle_frame(frame);
        match response {
            Ok(response) => {
                let encode_started = Instant::now();
                let bytes_sent = QuicCodec
                    .encode_frame(&response)
                    .map(|bytes| bytes.len().min(u64::MAX as usize) as u64)
                    .unwrap_or(0);
                let encode_duration = encode_started.elapsed();
                self.app.record_communication_endpoint_exchange_with_stages(
                    endpoint,
                    bytes_received,
                    bytes_sent,
                    started.elapsed(),
                    crate::app::CommunicationStageDurations {
                        encode: Some(encode_duration),
                        decode: Some(decode_duration),
                        socket_read: Some(started.elapsed()),
                        ..Default::default()
                    },
                );
                Ok(response)
            }
            Err(err) => {
                self.app.record_communication_endpoint_failure_kind(
                    endpoint,
                    Some(started.elapsed()),
                    crate::app::classify_quic_communication_failure(&err),
                    err.to_string(),
                );
                Err(err)
            }
        }
    }
}

fn quic_inbound_data_endpoint(frame: &QuicFrame) -> crate::app::CommunicationEndpointRuntime {
    data_endpoint(
        "quic",
        &format!("{}:{}", frame.destination.host, frame.destination.port),
        &format!("{}:{}", frame.source.host, frame.source.port),
        frame.link.remote_node_id.as_ref(),
        &frame.binding,
    )
}

fn quic_outbound_data_endpoint(frame: &QuicFrame) -> crate::app::CommunicationEndpointRuntime {
    data_endpoint(
        "quic",
        &format!("{}:{}", frame.source.host, frame.source.port),
        &format!("{}:{}", frame.destination.host, frame.destination.port),
        frame.link.remote_node_id.as_ref(),
        &frame.binding,
    )
}
