use super::*;

#[tokio::test]
async fn readiness_stays_ready_after_startup_when_a_peer_is_degraded() {
    let ipc_path = temp_socket_path("readiness-degraded");
    let stream_path = temp_stream_socket_path("readiness-degraded");
    let app = NodeApp::builder()
        .config(NodeConfig {
            node_id: NodeId::new("node-a"),
            http_bind_addr: "127.0.0.1:0".parse().expect("socket address should parse"),
            ipc_socket_path: ipc_path.clone(),
            reconcile_interval: Duration::from_millis(10),
            state_dir: None,
            peers: vec![PeerConfig::new("node-b", "http://127.0.0.1:9201")],
            peer_authentication: crate::PeerAuthenticationMode::Optional,
            peer_sync_execution: NodeConfig::try_peer_sync_execution_from_env()
                .expect("peer sync execution defaults should parse"),
            ipc_stream_heartbeat_interval: NodeConfig::default_ipc_stream_heartbeat_interval(),
            ipc_stream_heartbeat_timeout: NodeConfig::default_ipc_stream_heartbeat_timeout(),
            runtime_tuning: NodeConfig::try_runtime_tuning_from_env()
                .expect("runtime tuning defaults should parse"),
        })
        .try_build()
        .expect("node app should build");

    let (_http_addr, http_handle) = app
        .start_http_server("127.0.0.1:0".parse().expect("socket address should parse"))
        .await
        .expect("http server should start");
    let (_ipc_path, ipc_handle) = app
        .start_ipc_server(&ipc_path)
        .await
        .expect("ipc server should start");
    let (_stream_path, stream_handle) = app
        .start_ipc_stream_server(&stream_path)
        .await
        .expect("ipc stream server should start");
    app.replay_state().expect("startup replay should complete");
    app.set_peer_sync_status(&NodeId::new("node-b"), PeerSyncStatus::BackingOff)
        .expect("peer sync status should update");

    let readiness = app.readiness_snapshot();
    let health = app.health_snapshot();

    assert!(readiness.ready);
    assert!(readiness.initial_sync_complete);
    assert_eq!(
        readiness.status,
        orion::control_plane::NodeReadinessStatus::Ready
    );
    assert_eq!(readiness.degraded_peer_count, 1);
    assert!(
        !readiness
            .reasons
            .iter()
            .any(|reason| reason.contains("degraded"))
    );

    assert_eq!(health.degraded_peer_count, 1);
    assert_eq!(
        health.status,
        orion::control_plane::NodeHealthStatus::Degraded
    );
    assert!(
        health
            .reasons
            .iter()
            .any(|reason| reason.contains("peers degraded"))
    );

    http_handle.abort();
    ipc_handle.abort();
    stream_handle.abort();
    let _ = fs::remove_file(ipc_path);
    let _ = fs::remove_file(stream_path);
}

#[tokio::test]
async fn readiness_stays_not_ready_while_initial_peer_sync_is_pending() {
    let ipc_path = temp_socket_path("readiness-pending");
    let stream_path = temp_stream_socket_path("readiness-pending");
    let app = NodeApp::builder()
        .config(NodeConfig {
            node_id: NodeId::new("node-a"),
            http_bind_addr: "127.0.0.1:0".parse().expect("socket address should parse"),
            ipc_socket_path: ipc_path.clone(),
            reconcile_interval: Duration::from_millis(10),
            state_dir: None,
            peers: vec![PeerConfig::new("node-b", "http://127.0.0.1:9202")],
            peer_authentication: crate::PeerAuthenticationMode::Optional,
            peer_sync_execution: NodeConfig::try_peer_sync_execution_from_env()
                .expect("peer sync execution defaults should parse"),
            ipc_stream_heartbeat_interval: NodeConfig::default_ipc_stream_heartbeat_interval(),
            ipc_stream_heartbeat_timeout: NodeConfig::default_ipc_stream_heartbeat_timeout(),
            runtime_tuning: NodeConfig::try_runtime_tuning_from_env()
                .expect("runtime tuning defaults should parse"),
        })
        .try_build()
        .expect("node app should build");

    let (_http_addr, http_handle) = app
        .start_http_server("127.0.0.1:0".parse().expect("socket address should parse"))
        .await
        .expect("http server should start");
    let (_ipc_path, ipc_handle) = app
        .start_ipc_server(&ipc_path)
        .await
        .expect("ipc server should start");
    let (_stream_path, stream_handle) = app
        .start_ipc_stream_server(&stream_path)
        .await
        .expect("ipc stream server should start");
    app.replay_state().expect("startup replay should complete");

    let readiness = app.readiness_snapshot();
    let health = app.health_snapshot();

    assert!(!readiness.ready);
    assert!(!readiness.initial_sync_complete);
    assert_eq!(
        readiness.status,
        orion::control_plane::NodeReadinessStatus::NotReady
    );
    assert!(
        readiness
            .reasons
            .iter()
            .any(|reason| reason == "initial peer sync still pending")
    );
    assert_eq!(
        health.status,
        orion::control_plane::NodeHealthStatus::Healthy
    );

    http_handle.abort();
    ipc_handle.abort();
    stream_handle.abort();
    let _ = fs::remove_file(ipc_path);
    let _ = fs::remove_file(stream_path);
}
