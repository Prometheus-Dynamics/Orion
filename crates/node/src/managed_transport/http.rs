#[cfg(any(test, feature = "transport-tcp", feature = "transport-quic"))]
use super::launch_socket_surface;
use super::{ManagedTransportBinding, launch_socket_surface_with_shutdown};
use crate::{
    ManagedNodeTransportSurface, ManagedServerTransportSecurity, NodeApp, NodeError,
    app::GracefulTaskHandle,
};
use orion::transport::http::{HttpControlHandler, HttpServer};
use std::{net::SocketAddr, sync::Arc};

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

#[cfg(any(test, feature = "transport-tcp", feature = "transport-quic"))]
pub(super) async fn start_http_surface(
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
    let server = server
        .with_max_body_bytes(app.config.runtime_tuning.transport_max_payload_bytes)
        .with_io_timeout(app.config.runtime_tuning.transport_io_timeout)
        .with_max_connections(
            app.config
                .runtime_tuning
                .transport_max_concurrent_connections,
        );
    let tls = resolve_http_tls(&app, surface)?;
    Ok(launch_socket_surface(addr, async move {
        let result = match tls {
            Some(tls) => server.serve_tls(listener, tls).await,
            None => server.serve(listener).await,
        };
        result.map_err(NodeError::from)
    }))
}

pub(super) async fn start_http_surface_with_shutdown(
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
    let server = server
        .with_max_body_bytes(app.config.runtime_tuning.transport_max_payload_bytes)
        .with_io_timeout(app.config.runtime_tuning.transport_io_timeout)
        .with_max_connections(
            app.config
                .runtime_tuning
                .transport_max_concurrent_connections,
        );
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
