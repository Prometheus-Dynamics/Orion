use super::*;

#[test]
fn audit_log_records_transport_security_and_trust_lifecycle_events() {
    let state_dir = temp_state_dir("audit-log");
    let audit_log_path = state_dir.join("audit").join("events.jsonl");
    let app = NodeApp::builder()
        .config(NodeConfig {
            node_id: NodeId::new("node.audit"),
            http_bind_addr: "127.0.0.1:0".parse().expect("socket address should parse"),
            ipc_socket_path: NodeConfig::default_ipc_socket_path_for("node.audit"),
            reconcile_interval: Duration::from_millis(10),
            state_dir: Some(state_dir.clone()),
            peers: Vec::new(),
            peer_authentication: PeerAuthenticationMode::Optional,
            peer_sync_execution: NodeConfig::try_peer_sync_execution_from_env()
                .expect("peer sync execution defaults should parse"),
            ipc_stream_heartbeat_interval: NodeConfig::default_ipc_stream_heartbeat_interval(),
            ipc_stream_heartbeat_timeout: NodeConfig::default_ipc_stream_heartbeat_timeout(),
            runtime_tuning: NodeConfig::try_runtime_tuning_from_env()
                .expect("runtime tuning defaults should parse"),
        })
        .with_audit_log_path(&audit_log_path)
        .with_auto_http_tls(true)
        .try_build()
        .expect("node app should build");

    app.record_http_transport_error(&HttpTransportError::Tls(
        "tls handshake failed: unknown ca".into(),
    ));
    app.enroll_peer(
        PeerConfig::new("node.remote", "https://127.0.0.1:9555").with_trusted_public_key_hex(
            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        ),
    )
    .expect("peer should enroll");
    app.revoke_peer_identity(&NodeId::new("node.remote"))
        .expect("peer revoke should succeed");
    app.rotate_http_tls_identity()
        .expect("auto http tls rotation should succeed");

    let contents = fs::read_to_string(&audit_log_path).expect("audit log should be readable");
    assert!(contents.contains("\"kind\":\"transport_security_failure\""));
    assert!(contents.contains("\"kind\":\"peer_enrolled\""));
    assert!(contents.contains("\"kind\":\"peer_revoked\""));
    assert!(contents.contains("\"kind\":\"http_tls_rotated\""));

    let _ = fs::remove_dir_all(state_dir);
}

#[tokio::test]
async fn graceful_reconcile_loop_shutdown_does_not_wait_for_full_interval() {
    let app = NodeApp::builder()
        .config(NodeConfig {
            node_id: NodeId::new("node.shutdown.loop"),
            http_bind_addr: "127.0.0.1:0".parse().expect("socket address should parse"),
            ipc_socket_path: NodeConfig::default_ipc_socket_path_for("node.shutdown.loop"),
            reconcile_interval: Duration::from_millis(10),
            state_dir: None,
            peers: Vec::new(),
            peer_authentication: PeerAuthenticationMode::Optional,
            peer_sync_execution: NodeConfig::try_peer_sync_execution_from_env()
                .expect("peer sync execution defaults should parse"),
            ipc_stream_heartbeat_interval: NodeConfig::default_ipc_stream_heartbeat_interval(),
            ipc_stream_heartbeat_timeout: NodeConfig::default_ipc_stream_heartbeat_timeout(),
            runtime_tuning: NodeConfig::try_runtime_tuning_from_env()
                .expect("runtime tuning defaults should parse"),
        })
        .try_build()
        .expect("node app should build");

    let handle = app.spawn_reconcile_loop(Duration::from_secs(60));
    tokio::time::timeout(Duration::from_millis(200), handle.shutdown())
        .await
        .expect("reconcile loop shutdown should not wait for full interval");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn graceful_reconcile_loop_shutdown_completes_during_mutation_load() {
    let app = NodeApp::builder()
        .config(NodeConfig {
            node_id: NodeId::new("node.shutdown.concurrent"),
            http_bind_addr: "127.0.0.1:0".parse().expect("socket address should parse"),
            ipc_socket_path: NodeConfig::default_ipc_socket_path_for("node.shutdown.concurrent"),
            reconcile_interval: Duration::from_millis(10),
            state_dir: None,
            peers: Vec::new(),
            peer_authentication: PeerAuthenticationMode::Optional,
            peer_sync_execution: NodeConfig::try_peer_sync_execution_from_env()
                .expect("peer sync execution defaults should parse"),
            ipc_stream_heartbeat_interval: NodeConfig::default_ipc_stream_heartbeat_interval(),
            ipc_stream_heartbeat_timeout: NodeConfig::default_ipc_stream_heartbeat_timeout(),
            runtime_tuning: NodeConfig::try_runtime_tuning_from_env()
                .expect("runtime tuning defaults should parse"),
        })
        .try_build()
        .expect("node app should build");

    let handle = app.spawn_reconcile_loop(Duration::from_secs(60));
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mutator_app = app.clone();
    let stop_mutator = stop.clone();
    let mutator = tokio::spawn(async move {
        let mut index = 0_u32;
        while !stop_mutator.load(std::sync::atomic::Ordering::Relaxed) {
            let mut desired = mutator_app.state_snapshot().state.desired;
            desired.put_node(
                orion::control_plane::NodeRecord::builder(format!("node-shutdown-{index}")).build(),
            );
            mutator_app.replace_desired(desired);
            index = index.saturating_add(1);
            tokio::task::yield_now().await;
        }
    });

    tokio::time::sleep(Duration::from_millis(50)).await;
    tokio::time::timeout(Duration::from_millis(300), handle.shutdown())
        .await
        .expect("reconcile loop shutdown should complete under mutation load");
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    tokio::time::timeout(Duration::from_secs(1), mutator)
        .await
        .expect("mutation task should stop promptly")
        .expect("mutation task should join");
}

#[tokio::test]
async fn graceful_ipc_server_shutdown_removes_socket_file() {
    let state_dir = temp_state_dir("graceful-ipc-shutdown");
    let socket_path = state_dir.join("control.sock");
    let app = NodeApp::builder()
        .config(NodeConfig {
            node_id: NodeId::new("node.shutdown.ipc"),
            http_bind_addr: "127.0.0.1:0".parse().expect("socket address should parse"),
            ipc_socket_path: socket_path.clone(),
            reconcile_interval: Duration::from_millis(10),
            state_dir: Some(state_dir.clone()),
            peers: Vec::new(),
            peer_authentication: PeerAuthenticationMode::Optional,
            peer_sync_execution: NodeConfig::try_peer_sync_execution_from_env()
                .expect("peer sync execution defaults should parse"),
            ipc_stream_heartbeat_interval: NodeConfig::default_ipc_stream_heartbeat_interval(),
            ipc_stream_heartbeat_timeout: NodeConfig::default_ipc_stream_heartbeat_timeout(),
            runtime_tuning: NodeConfig::try_runtime_tuning_from_env()
                .expect("runtime tuning defaults should parse"),
        })
        .try_build()
        .expect("node app should build");

    let (bound_path, server) = app
        .start_ipc_server_graceful(&socket_path)
        .await
        .expect("graceful ipc server should start");
    assert!(
        bound_path.exists(),
        "ipc socket should exist while server is running"
    );

    server
        .shutdown()
        .await
        .expect("graceful ipc server shutdown should succeed");
    assert!(
        !bound_path.exists(),
        "ipc socket should be removed during graceful shutdown"
    );

    let _ = fs::remove_dir_all(state_dir);
}

#[tokio::test]
async fn graceful_ipc_stream_server_shutdown_removes_socket_file() {
    let state_dir = temp_state_dir("graceful-ipc-stream-shutdown");
    let socket_path = state_dir.join("control-stream.sock");
    let app = NodeApp::builder()
        .config(NodeConfig {
            node_id: NodeId::new("node.shutdown.ipc-stream"),
            http_bind_addr: "127.0.0.1:0".parse().expect("socket address should parse"),
            ipc_socket_path: NodeConfig::default_ipc_socket_path_for("node.shutdown.ipc-stream"),
            reconcile_interval: Duration::from_millis(10),
            state_dir: Some(state_dir.clone()),
            peers: Vec::new(),
            peer_authentication: PeerAuthenticationMode::Optional,
            peer_sync_execution: NodeConfig::try_peer_sync_execution_from_env()
                .expect("peer sync execution defaults should parse"),
            ipc_stream_heartbeat_interval: NodeConfig::default_ipc_stream_heartbeat_interval(),
            ipc_stream_heartbeat_timeout: NodeConfig::default_ipc_stream_heartbeat_timeout(),
            runtime_tuning: NodeConfig::try_runtime_tuning_from_env()
                .expect("runtime tuning defaults should parse"),
        })
        .try_build()
        .expect("node app should build");

    let (bound_path, server) = app
        .start_ipc_stream_server_graceful(&socket_path)
        .await
        .expect("graceful ipc stream server should start");
    assert!(
        bound_path.exists(),
        "ipc stream socket should exist while server is running"
    );

    server
        .shutdown()
        .await
        .expect("graceful ipc stream server shutdown should succeed");
    assert!(
        !bound_path.exists(),
        "ipc stream socket should be removed during graceful shutdown"
    );

    let _ = fs::remove_dir_all(state_dir);
}
