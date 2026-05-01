use orion_node::{NodeApp, NodeProcessConfig};
use std::io;
use tracing::info;
use tracing::warn;
use tracing_subscriber::{EnvFilter, fmt};

fn init_tracing() {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let _ = fmt()
        .with_env_filter(env_filter)
        .with_writer(io::stderr)
        .try_init();
}

fn log_shutdown_error(
    node_id: &orion_node::NodeId,
    component: &'static str,
    error: &dyn std::fmt::Display,
) {
    warn!(node = %node_id, component, error = %error, "graceful shutdown reported an error");
}

#[tokio::main]
async fn main() -> Result<(), orion_node::NodeError> {
    init_tracing();
    let process = NodeProcessConfig::try_from_env()?;
    let config = process.node.clone();
    info!(
        node = %config.node_id,
        http_bind_addr = %config.http_bind_addr,
        peers = config.peers.len(),
        peer_authentication = ?config.peer_authentication,
        peer_sync_execution = ?config.peer_sync_execution,
        state_dir = ?config.state_dir,
        reconcile_interval_ms = config.reconcile_interval.as_millis(),
        ipc_stream_heartbeat_interval_ms = config.ipc_stream_heartbeat_interval.as_millis(),
        ipc_stream_heartbeat_timeout_ms = config.ipc_stream_heartbeat_timeout.as_millis(),
        audit_log_path = ?process.audit_log_path,
        http_probe_addr = ?process.http_probe_addr,
        auto_http_tls = process.auto_http_tls,
        has_http_tls_files = process.http_tls_cert_path.is_some() || process.http_tls_key_path.is_some(),
        "starting orion-node"
    );
    let mut app_builder = NodeApp::builder().config(config.clone());
    if let (Some(cert_path), Some(key_path)) = (
        process.http_tls_cert_path.as_ref(),
        process.http_tls_key_path.as_ref(),
    ) {
        app_builder = app_builder.with_http_tls_files(cert_path, key_path);
    } else if process.auto_http_tls {
        app_builder = app_builder.with_auto_http_tls(true);
    }
    if let Some(path) = process.audit_log_path.as_ref() {
        app_builder = app_builder.with_audit_log_path(path);
    }
    let app = app_builder.try_build()?;
    let reconcile_loop = app.spawn_reconcile_loop(config.reconcile_interval);
    let peer_sync_loop = (!config.peers.is_empty()).then(|| {
        app.spawn_peer_sync_loop_with_execution(
            config.reconcile_interval,
            config.peer_sync_execution,
        )
    });
    let (ipc_socket, ipc_server) = app
        .start_ipc_server_graceful(&config.ipc_socket_path)
        .await?;
    let (ipc_stream_socket, ipc_stream_server) = app
        .start_ipc_stream_server_graceful(&process.ipc_stream_socket_path)
        .await?;
    let (http_addr, http_server) = app
        .start_http_server_graceful(config.http_bind_addr)
        .await?;
    let probe_server = if let Some(probe_addr) = process.http_probe_addr {
        Some(app.start_http_probe_server_graceful(probe_addr).await?)
    } else {
        None
    };
    let snapshot = app.snapshot();
    let http_scheme = if app.http_tls_cert_path().is_some() {
        "https"
    } else {
        "http"
    };
    let http_probe = probe_server
        .as_ref()
        .map(|(addr, _)| addr.to_string())
        .unwrap_or_else(|| "-".to_owned());
    let http_tls_cert = app
        .http_tls_cert_path()
        .map(|path| path.display().to_string())
        .unwrap_or_else(|| "-".to_owned());
    println!(
        "orion-node: initialized node={} http={}://{} http_probe={} http_tls_cert={} ipc={} ipc_stream={} peers={} desired_rev={} observed_rev={} applied_rev={}",
        snapshot.node_id,
        http_scheme,
        http_addr,
        http_probe,
        http_tls_cert,
        ipc_socket.display(),
        ipc_stream_socket.display(),
        snapshot.registered_peers,
        snapshot.desired_revision,
        snapshot.observed_revision,
        snapshot.applied_revision
    );
    info!(
        node = %snapshot.node_id,
        http_scheme,
        http_addr = %http_addr,
        http_probe = %http_probe,
        http_tls_cert = %http_tls_cert,
        ipc_socket = %ipc_socket.display(),
        ipc_stream_socket = %ipc_stream_socket.display(),
        peers = snapshot.registered_peers,
        desired_revision = %snapshot.desired_revision,
        observed_revision = %snapshot.observed_revision,
        applied_revision = %snapshot.applied_revision,
        peer_sync_execution = ?config.peer_sync_execution,
        peer_authentication = ?config.peer_authentication,
        state_dir = ?config.state_dir,
        audit_log_path = ?process.audit_log_path,
        "orion-node initialized"
    );

    if let Some(shutdown_after_init) = process.shutdown_after_init {
        info!(
            node = %snapshot.node_id,
            shutdown_after_init_ms = shutdown_after_init.as_millis(),
            "scheduled automatic shutdown after initialization"
        );
        tokio::time::sleep(shutdown_after_init).await;
        info!(node = %snapshot.node_id, "shutting down orion-node after initialization delay");
        reconcile_loop.shutdown().await;
        if let Some(peer_sync_loop) = peer_sync_loop {
            peer_sync_loop.shutdown().await;
        }
        if let Err(err) = ipc_server.shutdown().await {
            log_shutdown_error(&snapshot.node_id, "ipc", &err);
        }
        if let Err(err) = ipc_stream_server.shutdown().await {
            log_shutdown_error(&snapshot.node_id, "ipc_stream", &err);
        }
        if let Err(err) = http_server.shutdown().await {
            log_shutdown_error(&snapshot.node_id, "http", &err);
        }
        if let Some((_, probe_server)) = probe_server
            && let Err(err) = probe_server.shutdown().await
        {
            log_shutdown_error(&snapshot.node_id, "http_probe", &err);
        }
        return Ok(());
    }

    tokio::signal::ctrl_c()
        .await
        .map_err(|err| orion_node::NodeError::StartupSignalListener {
            message: err.to_string(),
        })?;
    info!(node = %snapshot.node_id, "received shutdown signal");

    reconcile_loop.shutdown().await;
    if let Some(peer_sync_loop) = peer_sync_loop {
        peer_sync_loop.shutdown().await;
    }
    if let Err(err) = ipc_server.shutdown().await {
        log_shutdown_error(&snapshot.node_id, "ipc", &err);
    }
    if let Err(err) = ipc_stream_server.shutdown().await {
        log_shutdown_error(&snapshot.node_id, "ipc_stream", &err);
    }
    if let Err(err) = http_server.shutdown().await {
        log_shutdown_error(&snapshot.node_id, "http", &err);
    }
    if let Some((_, probe_server)) = probe_server
        && let Err(err) = probe_server.shutdown().await
    {
        log_shutdown_error(&snapshot.node_id, "http_probe", &err);
    }
    info!(node = %snapshot.node_id, "orion-node shutdown complete");
    Ok(())
}
