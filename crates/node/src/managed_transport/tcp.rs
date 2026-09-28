use super::{
    ManagedTransportBinding, data_endpoint, launch_socket_surface,
    launch_socket_surface_with_shutdown,
};
use crate::{
    ManagedNodeTransportSurface, ManagedServerTransportSecurity, NodeApp, NodeError,
    app::GracefulTaskHandle,
};
use orion::transport::tcp::{
    TcpFrame, TcpFrameClient, TcpFrameHandler, TcpFrameServer, TcpTransportError,
};
use orion_transport_tcp::TcpCodec;
use std::time::Instant;
use std::{net::SocketAddr, sync::Arc};

impl NodeApp {
    #[cfg(feature = "transport-tcp")]
    pub async fn send_tcp_data_frame_metered(
        &self,
        client: &TcpFrameClient,
        frame: TcpFrame,
    ) -> Result<TcpFrame, TcpTransportError> {
        let started = Instant::now();
        let encode_started = Instant::now();
        let bytes_sent = TcpCodec
            .encode_frame(&frame)
            .map(|bytes| bytes.len().min(u64::MAX as usize) as u64)
            .unwrap_or(0);
        let encode_duration = encode_started.elapsed();
        let endpoint = tcp_outbound_data_endpoint(&frame);
        match client.send(frame).await {
            Ok(response) => {
                let decode_started = Instant::now();
                let bytes_received = TcpCodec
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
                    crate::app::classify_tcp_communication_failure(&err),
                    err.to_string(),
                );
                Err(err)
            }
        }
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

#[cfg(feature = "transport-tcp")]
pub(super) async fn start_tcp_surface(
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
    let handler: Arc<dyn TcpFrameHandler> = Arc::new(MeteredTcpFrameHandler {
        app: app.clone(),
        inner: handler,
    });
    let (addr, server, listener) = match surface {
        ManagedNodeTransportSurface::PeerTcpData => TcpFrameServer::bind(addr, handler).await?,
        _ => {
            return Err(NodeError::Storage(
                "non-TCP managed surface cannot be started with the TCP transport adapter".into(),
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
pub(super) async fn start_tcp_surface_with_shutdown(
    app: NodeApp,
    surface: ManagedNodeTransportSurface,
    addr: SocketAddr,
    handler: Arc<dyn TcpFrameHandler>,
) -> Result<(ManagedTransportBinding, GracefulTaskHandle<NodeError>), NodeError> {
    let handler: Arc<dyn TcpFrameHandler> = Arc::new(MeteredTcpFrameHandler {
        app: app.clone(),
        inner: handler,
    });
    let (addr, server, listener) = match surface {
        ManagedNodeTransportSurface::PeerTcpData => TcpFrameServer::bind(addr, handler).await?,
        _ => {
            return Err(NodeError::Storage(
                "non-TCP managed surface cannot be started with the TCP transport adapter".into(),
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

#[cfg(feature = "transport-tcp")]
struct MeteredTcpFrameHandler {
    app: NodeApp,
    inner: Arc<dyn TcpFrameHandler>,
}

#[cfg(feature = "transport-tcp")]
impl TcpFrameHandler for MeteredTcpFrameHandler {
    fn handle_frame(&self, frame: TcpFrame) -> Result<TcpFrame, TcpTransportError> {
        let started = Instant::now();
        let decode_started = Instant::now();
        let bytes_received = TcpCodec
            .encode_frame(&frame)
            .map(|bytes| bytes.len().min(u64::MAX as usize) as u64)
            .unwrap_or(0);
        let decode_duration = decode_started.elapsed();
        let endpoint = tcp_inbound_data_endpoint(&frame);
        let response = self.inner.handle_frame(frame);
        match response {
            Ok(response) => {
                let encode_started = Instant::now();
                let bytes_sent = TcpCodec
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
                    crate::app::classify_tcp_communication_failure(&err),
                    err.to_string(),
                );
                Err(err)
            }
        }
    }
}

#[cfg(feature = "transport-tcp")]
fn tcp_inbound_data_endpoint(frame: &TcpFrame) -> crate::app::CommunicationEndpointRuntime {
    data_endpoint(
        "tcp",
        &format!("{}:{}", frame.destination.host, frame.destination.port),
        &format!("{}:{}", frame.source.host, frame.source.port),
        frame.link.remote_node_id.as_ref(),
        &frame.binding,
    )
}

#[cfg(feature = "transport-tcp")]
fn tcp_outbound_data_endpoint(frame: &TcpFrame) -> crate::app::CommunicationEndpointRuntime {
    data_endpoint(
        "tcp",
        &format!("{}:{}", frame.source.host, frame.source.port),
        &format!("{}:{}", frame.destination.host, frame.destination.port),
        frame.link.remote_node_id.as_ref(),
        &frame.binding,
    )
}
