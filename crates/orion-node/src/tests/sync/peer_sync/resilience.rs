use super::*;

#[tokio::test]
async fn node_sync_peer_resolves_equal_revision_conflict_deterministically() {
    let node_b = NodeApp::builder()
        .config(NodeConfig {
            node_id: NodeId::new("node-b"),
            http_bind_addr: "127.0.0.1:0".parse().expect("socket address should parse"),
            ipc_socket_path: NodeConfig::default_ipc_socket_path_for("node-test"),
            reconcile_interval: std::time::Duration::from_millis(50),
            state_dir: None,
            peers: vec![PeerConfig::new("node-a", "http://127.0.0.1:1")],
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

    let mut desired_b = node_b.state_snapshot().state.desired;
    desired_b
        .put_artifact(orion::control_plane::ArtifactRecord::builder("artifact.conflict.b").build());
    desired_b.put_workload(
        WorkloadRecord::builder(
            WorkloadId::new("workload.conflict"),
            RuntimeType::new("graph.exec.v1"),
            ArtifactId::new("artifact.conflict.b"),
        )
        .desired_state(DesiredState::Stopped)
        .assigned_to(NodeId::new("node-b"))
        .build(),
    );
    node_b.replace_desired(desired_b.clone());

    let (addr_b, server_b) = node_b
        .start_http_server("127.0.0.1:0".parse().expect("socket address should parse"))
        .await
        .expect("node B server should start");

    let node_a = NodeApp::builder()
        .config(NodeConfig {
            node_id: NodeId::new("node-a"),
            http_bind_addr: "127.0.0.1:0".parse().expect("socket address should parse"),
            ipc_socket_path: NodeConfig::default_ipc_socket_path_for("node-test"),
            reconcile_interval: std::time::Duration::from_millis(50),
            state_dir: None,
            peers: vec![PeerConfig::new(
                "node-b",
                orion_core::PeerBaseUrl::new(format!("http://{}", addr_b)),
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

    let mut desired_a = node_a.state_snapshot().state.desired;
    desired_a
        .put_artifact(orion::control_plane::ArtifactRecord::builder("artifact.conflict.a").build());
    desired_a.put_workload(
        WorkloadRecord::builder(
            WorkloadId::new("workload.conflict"),
            RuntimeType::new("graph.exec.v1"),
            ArtifactId::new("artifact.conflict.a"),
        )
        .desired_state(DesiredState::Stopped)
        .assigned_to(NodeId::new("node-a"))
        .build(),
    );
    node_a.replace_desired(desired_a.clone());

    let mut expected = orion::control_plane::DesiredClusterState {
        revision: orion::Revision::new(3),
        ..Default::default()
    };
    expected.artifacts.insert(
        ArtifactId::new("artifact.conflict.a"),
        orion::control_plane::ArtifactRecord::builder("artifact.conflict.a").build(),
    );
    expected.artifacts.insert(
        ArtifactId::new("artifact.conflict.b"),
        orion::control_plane::ArtifactRecord::builder("artifact.conflict.b").build(),
    );
    expected.workloads.insert(
        WorkloadId::new("workload.conflict"),
        WorkloadRecord::builder(
            WorkloadId::new("workload.conflict"),
            RuntimeType::new("graph.exec.v1"),
            ArtifactId::new("artifact.conflict.b"),
        )
        .desired_state(DesiredState::Stopped)
        .assigned_to(NodeId::new("node-b"))
        .build(),
    );

    node_a
        .sync_peer(&NodeId::new("node-b"))
        .await
        .expect("equal revision peer sync should succeed");

    assert_eq!(node_a.state_snapshot().state.desired, expected);
    assert_eq!(node_b.state_snapshot().state.desired, expected);

    server_b.abort();
}

#[tokio::test]
async fn node_sync_peer_backs_off_after_unreachable_peer_error() {
    let node = NodeApp::builder()
        .config(NodeConfig {
            node_id: NodeId::new("node-a"),
            http_bind_addr: "127.0.0.1:0".parse().expect("socket address should parse"),
            ipc_socket_path: NodeConfig::default_ipc_socket_path_for("node-test"),
            reconcile_interval: std::time::Duration::from_millis(50),
            state_dir: None,
            peers: vec![PeerConfig::new("node-b", "http://127.0.0.1:65534")],
            peer_authentication: crate::PeerAuthenticationMode::Optional,
            peer_sync_execution: NodeConfig::try_peer_sync_execution_from_env()
                .expect("peer sync execution defaults should parse"),
            ipc_stream_heartbeat_interval: NodeConfig::default_ipc_stream_heartbeat_interval(),
            ipc_stream_heartbeat_timeout: NodeConfig::default_ipc_stream_heartbeat_timeout(),
            runtime_tuning: NodeConfig::try_runtime_tuning_from_env()
                .expect("runtime tuning defaults should parse"),
        })
        .with_peer_sync_backoff_base(Duration::from_secs(5))
        .with_peer_sync_backoff_max(Duration::from_secs(5))
        .with_peer_sync_backoff_jitter_ms(0)
        .try_build()
        .expect("node app should build");

    let err = node
        .sync_peer(&NodeId::new("node-b"))
        .await
        .expect_err("sync should fail for unreachable peer");
    assert!(matches!(err, NodeError::HttpTransport(_)));

    let peer = node
        .peer_states()
        .into_iter()
        .find(|peer| peer.node_id == NodeId::new("node-b"))
        .expect("peer state should exist");
    assert_eq!(peer.sync_status, PeerSyncStatus::BackingOff);
    assert!(peer.last_error.is_some());
    assert_eq!(peer.consecutive_failures, 1);
    assert!(peer.next_sync_allowed_at_ms.is_some());

    node.sync_peer(&NodeId::new("node-b"))
        .await
        .expect("sync should skip while peer is backing off");

    let peer = node
        .peer_states()
        .into_iter()
        .find(|peer| peer.node_id == NodeId::new("node-b"))
        .expect("peer state should exist");
    assert_eq!(peer.sync_status, PeerSyncStatus::BackingOff);
    assert_eq!(peer.consecutive_failures, 1);
}

#[tokio::test]
async fn node_sync_peer_propagates_workload_tombstone_over_existing_record() {
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
    let node_b = NodeApp::builder()
        .config(NodeConfig {
            node_id: NodeId::new("node-b"),
            http_bind_addr: "127.0.0.1:0".parse().expect("socket address should parse"),
            ipc_socket_path: NodeConfig::default_ipc_socket_path_for("node-test"),
            reconcile_interval: std::time::Duration::from_millis(50),
            state_dir: None,
            peers: vec![PeerConfig::new("node-b", "http://127.0.0.1:1")],
            peer_authentication: crate::PeerAuthenticationMode::Disabled,
            peer_sync_execution: NodeConfig::try_peer_sync_execution_from_env()
                .expect("peer sync execution defaults should parse"),
            ipc_stream_heartbeat_interval: NodeConfig::default_ipc_stream_heartbeat_interval(),
            ipc_stream_heartbeat_timeout: NodeConfig::default_ipc_stream_heartbeat_timeout(),
            runtime_tuning: NodeConfig::try_runtime_tuning_from_env()
                .expect("runtime tuning defaults should parse"),
        })
        .try_build()
        .expect("node app should build");

    let workload = WorkloadRecord::builder(
        WorkloadId::new("workload.delete"),
        RuntimeType::new("graph.exec.v1"),
        ArtifactId::new("artifact.delete"),
    )
    .desired_state(DesiredState::Stopped)
    .assigned_to(NodeId::new("node-a"))
    .build();

    let mut desired_a = node_a.state_snapshot().state.desired;
    desired_a.put_workload(workload.clone());
    node_a.replace_desired(desired_a);

    let mut desired_b = node_b.state_snapshot().state.desired;
    desired_b.put_workload(workload);
    node_b.replace_desired(desired_b);

    let mut desired_a = node_a.state_snapshot().state.desired;
    desired_a.remove_workload(&WorkloadId::new("workload.delete"));
    node_a.replace_desired(desired_a);

    let (addr_a, server_a) = node_a
        .start_http_server("127.0.0.1:0".parse().expect("socket address should parse"))
        .await
        .expect("node A server should start");

    node_b
        .register_peer(PeerConfig::new(
            "node-a",
            orion_core::PeerBaseUrl::new(format!("http://{}", addr_a)),
        ))
        .expect("peer registration should succeed");
    node_b
        .sync_peer(&NodeId::new("node-a"))
        .await
        .expect("peer sync should succeed");

    let desired_b = node_b.state_snapshot().state.desired;
    assert!(
        !desired_b
            .workloads
            .contains_key(&WorkloadId::new("workload.delete"))
    );
    assert_eq!(
        desired_b
            .workload_tombstones
            .get(&WorkloadId::new("workload.delete")),
        Some(&Revision::new(2))
    );

    server_a.abort();
}

#[tokio::test]
async fn node_sync_peer_higher_revision_readd_overrides_tombstone() {
    let node_a = NodeApp::builder()
        .config(test_node_config_with_auth(
            "node-a",
            "node-test",
            crate::PeerAuthenticationMode::Disabled,
        ))
        .try_build()
        .expect("node app should build");
    let node_b = NodeApp::builder()
        .config(test_node_config_with_auth(
            "node-b",
            "node-test",
            crate::PeerAuthenticationMode::Optional,
        ))
        .try_build()
        .expect("node app should build");

    let workload_id = WorkloadId::new("workload.readd");
    let mut desired_a = node_a.state_snapshot().state.desired;
    desired_a.put_workload(
        WorkloadRecord::builder(
            workload_id.clone(),
            RuntimeType::new("graph.exec.v1"),
            ArtifactId::new("artifact.readd"),
        )
        .desired_state(DesiredState::Stopped)
        .assigned_to(NodeId::new("node-a"))
        .build(),
    );
    desired_a.remove_workload(&workload_id);
    node_a.replace_desired(desired_a);

    let mut desired_b = node_b.state_snapshot().state.desired;
    desired_b.put_workload(
        WorkloadRecord::builder(
            workload_id.clone(),
            RuntimeType::new("graph.exec.v1"),
            ArtifactId::new("artifact.readd.v2"),
        )
        .desired_state(DesiredState::Stopped)
        .assigned_to(NodeId::new("node-b"))
        .build(),
    );
    desired_b.bump_revision();
    node_b.replace_desired(desired_b);

    let (addr_b, server_b) = node_b
        .start_http_server("127.0.0.1:0".parse().expect("socket address should parse"))
        .await
        .expect("node B server should start");

    node_a
        .register_peer(PeerConfig::new(
            "node-b",
            orion_core::PeerBaseUrl::new(format!("http://{}", addr_b)),
        ))
        .expect("peer registration should succeed");
    node_a
        .sync_peer(&NodeId::new("node-b"))
        .await
        .expect("peer sync should succeed");

    let desired_a = node_a.state_snapshot().state.desired;
    assert_eq!(
        desired_a
            .workloads
            .get(&workload_id)
            .expect("re-added workload should win")
            .assigned_node_id
            .as_ref(),
        Some(&NodeId::new("node-b"))
    );
    assert!(!desired_a.workload_tombstones.contains_key(&workload_id));

    server_b.abort();
}
