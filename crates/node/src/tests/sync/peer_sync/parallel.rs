use super::*;

#[tokio::test]
async fn node_sync_all_peers_uses_configured_execution_and_reports_mixed_results() {
    let node_a = NodeApp::builder()
        .config(test_node_config_with_auth(
            "node-a",
            "node-test",
            crate::PeerAuthenticationMode::Optional,
        ))
        .try_build()
        .expect("node app should build");
    let (addr_a, server_a) = node_a
        .start_http_server("127.0.0.1:0".parse().expect("socket address should parse"))
        .await
        .expect("node A server should start");

    let node = NodeApp::builder()
        .config(NodeConfig {
            node_id: NodeId::new("node-c"),
            http_bind_addr: "127.0.0.1:0".parse().expect("socket address should parse"),
            ipc_socket_path: NodeConfig::default_ipc_socket_path_for("node-test"),
            reconcile_interval: std::time::Duration::from_millis(50),
            state_dir: None,
            peers: vec![
                PeerConfig::new(
                    "node-a",
                    orion_core::PeerBaseUrl::new(format!("http://{}", addr_a)),
                ),
                PeerConfig::new("node-b", "http://127.0.0.1:65534"),
            ],
            peer_authentication: crate::PeerAuthenticationMode::Optional,
            peer_sync_execution: PeerSyncExecution::Parallel { max_in_flight: 2 },
            ipc_stream_heartbeat_interval: NodeConfig::default_ipc_stream_heartbeat_interval(),
            ipc_stream_heartbeat_timeout: NodeConfig::default_ipc_stream_heartbeat_timeout(),
            runtime_tuning: NodeConfig::try_runtime_tuning_from_env()
                .expect("runtime tuning defaults should parse"),
        })
        .try_build()
        .expect("node app should build");

    let results = node.sync_all_peers().await;
    assert_eq!(results.len(), 2);
    assert!(
        results
            .iter()
            .any(|(node_id, result)| node_id == &NodeId::new("node-a") && result.is_ok())
    );
    assert!(
        results
            .iter()
            .any(|(node_id, result)| node_id == &NodeId::new("node-b") && result.is_err())
    );

    server_a.abort();
}

#[tokio::test]
async fn node_sync_all_peers_parallel_reports_mixed_success_and_failure() {
    let node_a = NodeApp::builder()
        .config(test_node_config_with_auth(
            "node-a",
            "node-test",
            crate::PeerAuthenticationMode::Optional,
        ))
        .try_build()
        .expect("node app should build");
    let (addr_a, server_a) = node_a
        .start_http_server("127.0.0.1:0".parse().expect("socket address should parse"))
        .await
        .expect("node A server should start");

    let node = NodeApp::builder()
        .config(NodeConfig {
            node_id: NodeId::new("node-main"),
            http_bind_addr: "127.0.0.1:0".parse().expect("socket address should parse"),
            ipc_socket_path: NodeConfig::default_ipc_socket_path_for("node-test"),
            reconcile_interval: std::time::Duration::from_millis(50),
            state_dir: None,
            peers: vec![
                PeerConfig::new(
                    "node-a",
                    orion_core::PeerBaseUrl::new(format!("http://{}", addr_a)),
                ),
                PeerConfig::new("node-b", "http://127.0.0.1:65534"),
            ],
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

    let results = node
        .sync_all_peers_with_execution(PeerSyncExecution::Parallel { max_in_flight: 2 })
        .await;

    assert_eq!(results.len(), 2);
    assert!(
        results
            .iter()
            .any(|(peer_id, result)| { peer_id == &NodeId::new("node-a") && result.is_ok() })
    );
    assert!(
        results
            .iter()
            .any(|(peer_id, result)| { peer_id == &NodeId::new("node-b") && result.is_err() })
    );

    server_a.abort();
}

#[test]
fn node_parallel_peer_sync_uses_adaptive_in_flight_cap() {
    assert_eq!(NodeApp::effective_parallel_in_flight_for_test(4, 0), 0);
    assert_eq!(NodeApp::effective_parallel_in_flight_for_test(4, 1), 1);
    assert_eq!(NodeApp::effective_parallel_in_flight_for_test(4, 2), 2);
    assert_eq!(NodeApp::effective_parallel_in_flight_for_test(4, 4), 4);
    assert_eq!(NodeApp::effective_parallel_in_flight_for_test(4, 6), 3);
    assert_eq!(NodeApp::effective_parallel_in_flight_for_test(2, 6), 2);
    assert_eq!(NodeApp::effective_parallel_in_flight_for_test(1, 6), 1);
}

#[test]
fn node_parallel_peer_sync_only_staggers_large_peer_sets() {
    assert_eq!(NodeApp::parallel_spawn_stagger_ms_for_test(0, 2), 0);
    assert_eq!(NodeApp::parallel_spawn_stagger_ms_for_test(1, 4), 0);
    assert_eq!(NodeApp::parallel_spawn_stagger_ms_for_test(1, 6), 5);
    assert_eq!(NodeApp::parallel_spawn_stagger_ms_for_test(3, 6), 15);
}
