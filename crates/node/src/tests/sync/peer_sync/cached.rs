use super::*;

#[tokio::test]
async fn node_sync_peer_noops_when_revisions_already_match() {
    let node_a = NodeApp::builder()
        .config(test_node_config_with_auth(
            "node-a",
            "node-test",
            crate::PeerAuthenticationMode::Optional,
        ))
        .try_build()
        .expect("node app should build");

    let mut desired = node_a.state_snapshot().state.desired;
    desired.put_artifact(orion::control_plane::ArtifactRecord::builder("artifact.pose").build());
    node_a.replace_desired(desired);

    let (addr_a, server_a) = node_a
        .start_http_server("127.0.0.1:0".parse().expect("socket address should parse"))
        .await
        .expect("node A server should start");

    let node_b = NodeApp::builder()
        .config(NodeConfig {
            node_id: NodeId::new("node-b"),
            http_bind_addr: "127.0.0.1:0".parse().expect("socket address should parse"),
            ipc_socket_path: NodeConfig::default_ipc_socket_path_for("node-test"),
            reconcile_interval: std::time::Duration::from_millis(50),
            state_dir: None,
            peers: vec![PeerConfig::new(
                "node-a",
                orion_core::PeerBaseUrl::new(format!("http://{}", addr_a)),
            )],
            peer_authentication: crate::PeerAuthenticationMode::Optional,
            peer_sync_execution: NodeConfig::try_peer_sync_execution_from_env()
                .expect("peer sync execution defaults should parse"),
            ipc_stream_heartbeat_interval: NodeConfig::default_ipc_stream_heartbeat_interval(),
            ipc_stream_heartbeat_timeout: NodeConfig::default_ipc_stream_heartbeat_timeout(),
            runtime_tuning: NodeConfig::try_runtime_tuning_from_env()
                .expect("runtime tuning defaults should parse"),
        })
        .desired(node_a.state_snapshot().state.desired.clone())
        .try_build()
        .expect("node app should build");

    let before = node_b.state_snapshot();
    node_b
        .sync_peer(&NodeId::new("node-a"))
        .await
        .expect("sync should succeed");
    let after = node_b.state_snapshot();

    assert_eq!(before.state.desired, after.state.desired);
    let peer = node_b
        .peer_states()
        .into_iter()
        .find(|peer| peer.node_id == NodeId::new("node-a"))
        .expect("peer state should exist");
    assert_eq!(peer.sync_status, PeerSyncStatus::Synced);

    server_a.abort();
}

#[tokio::test]
async fn node_sync_peer_remote_snapshot_preserves_local_registrations() {
    #[derive(Clone)]
    struct NodeBProvider;

    impl ProviderIntegration for NodeBProvider {
        fn provider_record(&self) -> ProviderRecord {
            ProviderRecord::builder("provider.local.b", "node-b")
                .resource_type(ResourceType::new("imu.sample"))
                .build()
        }

        fn snapshot(&self) -> ProviderSnapshot {
            ProviderSnapshot {
                provider: self.provider_record(),
                resources: vec![
                    ResourceRecord::builder("resource.imu-b-1", "imu.sample", "provider.local.b")
                        .health(HealthState::Healthy)
                        .availability(AvailabilityState::Available)
                        .lease_state(LeaseState::Unleased)
                        .build(),
                ],
            }
        }
    }

    #[derive(Clone)]
    struct NodeBExecutor;

    impl ExecutorIntegration for NodeBExecutor {
        fn executor_record(&self) -> ExecutorRecord {
            ExecutorRecord::builder("executor.local.b", "node-b")
                .runtime_type(RuntimeType::new("graph.exec.v1"))
                .build()
        }

        fn snapshot(&self) -> ExecutorSnapshot {
            ExecutorSnapshot {
                executor: self.executor_record(),
                workloads: Vec::new(),
                resources: Vec::new(),
            }
        }
    }

    let node_a = NodeApp::builder()
        .config(NodeConfig {
            node_id: NodeId::new("node-a"),
            http_bind_addr: "127.0.0.1:0".parse().expect("socket address should parse"),
            ipc_socket_path: NodeConfig::default_ipc_socket_path_for("node-test"),
            reconcile_interval: std::time::Duration::from_millis(50),
            state_dir: None,
            peers: vec![PeerConfig::new("node-b", "http://127.0.0.1:1")],
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

    let mut desired = node_a.state_snapshot().state.desired;
    desired.put_artifact(orion::control_plane::ArtifactRecord::builder("artifact.pose").build());
    node_a.replace_desired(desired);

    let (addr_a, server_a) = node_a
        .start_http_server("127.0.0.1:0".parse().expect("socket address should parse"))
        .await
        .expect("node A server should start");

    let app = NodeApp::builder()
        .config(NodeConfig {
            node_id: NodeId::new("node-b"),
            http_bind_addr: "127.0.0.1:0".parse().expect("socket address should parse"),
            ipc_socket_path: NodeConfig::default_ipc_socket_path_for("node-test"),
            reconcile_interval: std::time::Duration::from_millis(50),
            state_dir: None,
            peers: vec![PeerConfig::new(
                "node-a",
                orion_core::PeerBaseUrl::new(format!("http://{}", addr_a)),
            )],
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
    app.register_provider(NodeBProvider)
        .expect("provider registration should succeed");
    app.register_executor(NodeBExecutor)
        .expect("executor registration should succeed");

    app.sync_peer(&NodeId::new("node-a"))
        .await
        .expect("sync should succeed");

    let snapshot = app.state_snapshot();
    assert!(
        snapshot
            .state
            .desired
            .providers
            .contains_key(&ProviderId::new("provider.local.b"))
    );
    assert!(
        snapshot
            .state
            .desired
            .executors
            .contains_key(&ExecutorId::new("executor.local.b"))
    );

    server_a.abort();
}
