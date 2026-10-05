use orion_node::{NodeApp, NodeProcessConfig};
use std::io;
use tracing::info;
use tracing::warn;
use tracing_subscriber::{EnvFilter, fmt};

// Opt-in global allocators, set only for the binary so library users keep control. `alloc-jemalloc`
// wins when both features are enabled (for example `cargo clippy --all-features`); enabling only
// one of them is the supported configuration. See docs/node-env.md.
#[cfg(feature = "alloc-jemalloc")]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[cfg(all(feature = "alloc-mimalloc", not(feature = "alloc-jemalloc")))]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

/// Compiled-in jemalloc options tuned for small appliances: two arenas instead of four per core,
/// one background thread that purges freed pages after about a second (so memory returns even when
/// the node is idle), no lazy `MADV_FREE` stage that keeps pages counted in RSS, and no transparent
/// huge pages for jemalloc's own mappings. tikv-jemallocator prefixes jemalloc symbols, so the
/// options symbol is `_rjem_malloc_conf` and runtime overrides go in `_RJEM_MALLOC_CONF`.
#[cfg(feature = "alloc-jemalloc")]
#[used]
#[unsafe(export_name = "_rjem_malloc_conf")]
static JEMALLOC_CONF: &[u8; 105] = b"narenas:2,background_thread:true,max_background_threads:1,\
dirty_decay_ms:1000,muzzy_decay_ms:0,thp:never\0";

fn init_tracing() {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let _ = fmt()
        .with_env_filter(env_filter)
        .with_writer(io::stderr)
        .try_init();
}

/// Stand-in for the HTTP server handles in builds without the `transport-http` feature, so the
/// startup/shutdown sequence below stays identical across feature sets.
#[cfg(not(feature = "transport-http"))]
struct DisabledHttpServer;

#[cfg(not(feature = "transport-http"))]
impl DisabledHttpServer {
    async fn shutdown(self) -> Result<(), std::convert::Infallible> {
        Ok(())
    }
}

/// Stand-in for the `orion+tcp` peer listener in builds without the `peer-tcp` feature.
#[cfg(not(feature = "peer-tcp"))]
struct DisabledPeerTcpServer;

#[cfg(not(feature = "peer-tcp"))]
impl DisabledPeerTcpServer {
    async fn shutdown(self) -> Result<(), std::convert::Infallible> {
        Ok(())
    }
}

/// Stand-in for peer discovery in builds without the `discovery-mdns` feature.
#[cfg(not(feature = "discovery-mdns"))]
struct DisabledDiscovery;

#[cfg(not(feature = "discovery-mdns"))]
impl DisabledDiscovery {
    async fn shutdown(self) {}
}

/// Starts mDNS discovery when `ORION_NODE_DISCOVERY=mdns`, advertising the `orion+tcp` listener
/// (and the HTTP listener when it is reachable from other hosts).
#[cfg(feature = "discovery-mdns")]
fn start_discovery(
    app: &NodeApp,
    process: &NodeProcessConfig,
    peer_tcp_addr: Option<std::net::SocketAddr>,
    http_addr: Option<std::net::SocketAddr>,
) -> Result<Option<orion_node::discovery::DiscoveryHandle>, orion_node::NodeError> {
    let (Some(config), Some(peer_tcp_addr)) = (process.discovery.clone(), peer_tcp_addr) else {
        return Ok(None);
    };
    let addresses = if peer_tcp_addr.ip().is_unspecified() {
        Vec::new()
    } else {
        vec![peer_tcp_addr.ip()]
    };
    let endpoints = orion_node::discovery::AdvertisedEndpoints {
        peer_tcp_port: Some(peer_tcp_addr.port()),
        http_port: http_addr
            .filter(|addr| !addr.ip().is_loopback())
            .map(|addr| addr.port()),
        https: app.http_tls_cert_path().is_some(),
        addresses,
    };
    let backend = orion_node::discovery::MdnsDiscoveryBackend::new(config.interfaces.clone());
    app.start_discovery(config, Box::new(backend), endpoints)
        .map(Some)
}

/// Stand-in for the link gateway in builds without the `link-gateway` feature.
#[cfg(not(feature = "link-gateway"))]
struct DisabledLinkGateway;

#[cfg(not(feature = "link-gateway"))]
impl DisabledLinkGateway {
    async fn shutdown(self) {}
}

fn log_shutdown_error(
    node_id: &orion_node::NodeId,
    component: &'static str,
    error: &dyn std::fmt::Display,
) {
    warn!(node = %node_id, component, error = %error, "graceful shutdown reported an error");
}

/// Waits for the configured post-init delay (`ORION_NODE_SHUTDOWN_AFTER_INIT_MS`) or, without one,
/// for SIGINT or (on Unix) SIGTERM, the signal systemd and container runtimes stop services with.
async fn wait_for_shutdown(
    node_id: &orion_node::NodeId,
    shutdown_after_init: Option<std::time::Duration>,
) -> Result<(), orion_node::NodeError> {
    if let Some(shutdown_after_init) = shutdown_after_init {
        info!(
            node = %node_id,
            shutdown_after_init_ms = shutdown_after_init.as_millis(),
            "scheduled automatic shutdown after initialization"
        );
        tokio::time::sleep(shutdown_after_init).await;
        info!(node = %node_id, "shutting down orion-node after initialization delay");
        return Ok(());
    }
    let signal_error = |err: io::Error| orion_node::NodeError::StartupSignalListener {
        message: err.to_string(),
    };
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .map_err(signal_error)?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result.map_err(signal_error)?,
            _ = terminate.recv() => {}
        }
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c().await.map_err(signal_error)?;
    info!(node = %node_id, "received shutdown signal");
    Ok(())
}

fn main() -> Result<(), orion_node::NodeError> {
    init_tracing();
    let process = NodeProcessConfig::try_from_env()?;
    let runtime = process.runtime_threads.build_runtime().map_err(|err| {
        orion_node::NodeError::Config(format!("failed to build tokio runtime: {err}"))
    })?;
    runtime.block_on(run(process))
}

async fn run(process: NodeProcessConfig) -> Result<(), orion_node::NodeError> {
    let config = process.node.clone();
    info!(
        node = %config.node_id,
        http_enabled = process.http_enabled,
        http_bind_addr = %config.http_bind_addr,
        runtime_worker_threads = ?process.runtime_threads.worker_threads,
        runtime_max_blocking_threads = ?process.runtime_threads.max_blocking_threads,
        peers = config.peers.len(),
        peer_authentication = ?config.peer_authentication,
        peer_sync_execution = ?config.peer_sync_execution,
        state_dir = ?config.state_dir,
        reconcile_interval_ms = config.reconcile_interval.as_millis(),
        reconcile_backstop_ms = config.runtime_tuning.reconcile_backstop_interval.as_millis(),
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
    // Peers enrolled through discovery are trusted peers like ORION_NODE_PEERS entries.
    #[cfg(feature = "discovery-mdns")]
    let dynamic_peers = app.restore_discovered_enrollments()? > 0 || process.discovery.is_some();
    #[cfg(not(feature = "discovery-mdns"))]
    let dynamic_peers = false;
    let reconcile_loop = app.spawn_reconcile_loop(config.reconcile_interval);
    let clock_facts_loop = app.spawn_clock_facts_loop();
    let host_facts_loop = app.spawn_host_facts_loop();
    let peer_sync_loop = (!config.peers.is_empty() || dynamic_peers).then(|| {
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
    #[cfg(feature = "link-gateway")]
    let link_gateway = app.start_link_gateway(process.links.clone())?;
    #[cfg(feature = "link-gateway")]
    let links_summary = process
        .links
        .iter()
        .map(|link| link.name.as_str())
        .collect::<Vec<_>>()
        .join(",");
    #[cfg(not(feature = "link-gateway"))]
    let link_gateway = DisabledLinkGateway;
    #[cfg(not(feature = "link-gateway"))]
    let links_summary = String::new();
    let links_summary = if links_summary.is_empty() {
        "-".to_owned()
    } else {
        links_summary
    };
    #[cfg(not(feature = "transport-http"))]
    let (http_server, probe_server) = (
        None::<(std::net::SocketAddr, DisabledHttpServer)>,
        None::<(std::net::SocketAddr, DisabledHttpServer)>,
    );
    #[cfg(feature = "transport-http")]
    let http_server = if process.http_enabled {
        Some(
            app.start_http_server_graceful(config.http_bind_addr)
                .await?,
        )
    } else {
        None
    };
    #[cfg(feature = "peer-tcp")]
    let peer_tcp_server = match process.peer_tcp_addr {
        Some(addr) => Some(app.start_peer_tcp_server(addr).await?),
        None => None,
    };
    #[cfg(not(feature = "peer-tcp"))]
    let peer_tcp_server = None::<(std::net::SocketAddr, DisabledPeerTcpServer)>;
    #[cfg(feature = "discovery-mdns")]
    let discovery = start_discovery(
        &app,
        &process,
        peer_tcp_server.as_ref().map(|(addr, _)| *addr),
        http_server.as_ref().map(|(addr, _)| *addr),
    )?;
    #[cfg(not(feature = "discovery-mdns"))]
    let discovery = None::<DisabledDiscovery>;
    #[cfg(feature = "transport-http")]
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
    let http_addr = http_server
        .as_ref()
        .map(|(addr, _)| format!("{http_scheme}://{addr}"))
        .unwrap_or_else(|| "off".to_owned());
    let http_probe = probe_server
        .as_ref()
        .map(|(addr, _)| addr.to_string())
        .unwrap_or_else(|| "-".to_owned());
    let peer_tcp = peer_tcp_server
        .as_ref()
        .map(|(addr, _)| format!("orion+tcp://{addr}"))
        .unwrap_or_else(|| "-".to_owned());
    let http_tls_cert = app
        .http_tls_cert_path()
        .map(|path| path.display().to_string())
        .unwrap_or_else(|| "-".to_owned());
    println!(
        "orion-node: initialized node={} http={} http_probe={} peer_tcp={} http_tls_cert={} ipc={} ipc_stream={} links={} peers={} desired_rev={} observed_rev={} applied_rev={}",
        snapshot.node_id,
        http_addr,
        http_probe,
        peer_tcp,
        http_tls_cert,
        ipc_socket.display(),
        ipc_stream_socket.display(),
        links_summary,
        snapshot.registered_peers,
        snapshot.desired_revision,
        snapshot.observed_revision,
        snapshot.applied_revision
    );
    info!(
        node = %snapshot.node_id,
        http_addr = %http_addr,
        http_probe = %http_probe,
        peer_tcp = %peer_tcp,
        http_tls_cert = %http_tls_cert,
        ipc_socket = %ipc_socket.display(),
        ipc_stream_socket = %ipc_stream_socket.display(),
        links = %links_summary,
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

    // Every listener is up: tell systemd (Type=notify) and start petting its watchdog.
    #[cfg(all(feature = "systemd-notify", unix))]
    let (notifier, watchdog) = {
        let notifier = orion_node::systemd::Notifier::from_env().map(std::sync::Arc::new);
        let watchdog = notifier.as_ref().and_then(|notifier| {
            notifier.ready(&format!(
                "serving node={} ipc={} peer_tcp={} http={} links={} peers={}",
                snapshot.node_id,
                ipc_socket.display(),
                peer_tcp,
                http_addr,
                links_summary,
                snapshot.registered_peers,
            ));
            let interval = orion_node::systemd::watchdog_interval_from_env()?;
            info!(
                node = %snapshot.node_id,
                watchdog_interval_ms = interval.as_millis(),
                "systemd watchdog enabled"
            );
            Some(orion_node::systemd::spawn_watchdog(
                notifier.clone(),
                interval,
                std::sync::Arc::new(app.clone()),
            ))
        });
        (notifier, watchdog)
    };

    wait_for_shutdown(&snapshot.node_id, process.shutdown_after_init).await?;

    #[cfg(all(feature = "systemd-notify", unix))]
    {
        if let Some(notifier) = notifier {
            notifier.stopping("shutting down");
        }
        if let Some(watchdog) = watchdog {
            watchdog.shutdown().await;
        }
    }
    if let Some(discovery) = discovery {
        discovery.shutdown().await;
    }
    link_gateway.shutdown().await;
    reconcile_loop.shutdown().await;
    clock_facts_loop.shutdown().await;
    if let Some(host_facts_loop) = host_facts_loop {
        host_facts_loop.shutdown().await;
    }
    if let Some(peer_sync_loop) = peer_sync_loop {
        peer_sync_loop.shutdown().await;
    }
    if let Err(err) = ipc_server.shutdown().await {
        log_shutdown_error(&snapshot.node_id, "ipc", &err);
    }
    if let Err(err) = ipc_stream_server.shutdown().await {
        log_shutdown_error(&snapshot.node_id, "ipc_stream", &err);
    }
    if let Some((_, http_server)) = http_server
        && let Err(err) = http_server.shutdown().await
    {
        log_shutdown_error(&snapshot.node_id, "http", &err);
    }
    if let Some((_, probe_server)) = probe_server
        && let Err(err) = probe_server.shutdown().await
    {
        log_shutdown_error(&snapshot.node_id, "http_probe", &err);
    }
    if let Some((_, peer_tcp_server)) = peer_tcp_server
        && let Err(err) = peer_tcp_server.shutdown().await
    {
        log_shutdown_error(&snapshot.node_id, "peer_tcp", &err);
    }
    info!(node = %snapshot.node_id, "orion-node shutdown complete");
    Ok(())
}
