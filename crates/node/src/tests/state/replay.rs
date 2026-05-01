use super::*;

#[test]
fn repeated_replay_cycles_under_churn_converge_to_latest_state() {
    let state_dir = temp_state_dir("replay-churn");

    for cycle in 0..6 {
        let app = NodeApp::builder()
            .config(NodeConfig {
                node_id: NodeId::new("node-a"),
                http_bind_addr: "127.0.0.1:0".parse().expect("socket address should parse"),
                ipc_socket_path: NodeConfig::default_ipc_socket_path_for("node-test"),
                reconcile_interval: Duration::from_millis(10),
                state_dir: Some(state_dir.clone()),
                peers: Vec::new(),
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

        let artifact_id = format!("artifact.churn.{cycle}");
        app.put_artifact_content(
            &orion::control_plane::ArtifactRecord::builder(orion::ArtifactId::new(
                artifact_id.clone(),
            ))
            .build(),
            &[cycle as u8],
        )
        .expect("artifact should persist");

        let workload_id = format!("workload.churn.{cycle}");
        let mut desired = app.state_snapshot().state.desired;
        desired.put_workload(
            WorkloadRecord::builder(
                WorkloadId::new(&workload_id),
                RuntimeType::new("graph.exec.v1"),
                ArtifactId::new(&artifact_id),
            )
            .desired_state(DesiredState::Stopped)
            .assigned_to(NodeId::new("node-a"))
            .build(),
        );

        if cycle > 0 {
            desired.remove_workload(&WorkloadId::new(format!("workload.churn.{}", cycle - 1)));
        }

        app.replace_desired(desired);
        app.persist_state().expect("state should persist");
    }

    let replayed = NodeApp::builder()
        .config(NodeConfig {
            node_id: NodeId::new("node-a"),
            http_bind_addr: "127.0.0.1:0".parse().expect("socket address should parse"),
            ipc_socket_path: NodeConfig::default_ipc_socket_path_for("node-test"),
            reconcile_interval: Duration::from_millis(10),
            state_dir: Some(state_dir.clone()),
            peers: Vec::new(),
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

    let snapshot = replayed.state_snapshot();
    assert!(
        snapshot
            .state
            .desired
            .workloads
            .contains_key(&WorkloadId::new("workload.churn.5"))
    );
    assert!(
        !snapshot
            .state
            .desired
            .workloads
            .contains_key(&WorkloadId::new("workload.churn.4"))
    );

    let _ = fs::remove_dir_all(state_dir);
}

#[test]
fn replay_recovers_from_valid_snapshot_with_corrupt_history() {
    let state_dir = temp_state_dir("snapshot-valid-history-corrupt");

    let app = NodeApp::builder()
        .config(NodeConfig {
            node_id: NodeId::new("node-a"),
            http_bind_addr: "127.0.0.1:0".parse().expect("socket address should parse"),
            ipc_socket_path: NodeConfig::default_ipc_socket_path_for("node-test"),
            reconcile_interval: Duration::from_millis(10),
            state_dir: Some(state_dir.clone()),
            peers: Vec::new(),
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

    app.put_artifact_content(
        &orion::control_plane::ArtifactRecord::builder("artifact.snapshot-valid").build(),
        &[1, 2, 3],
    )
    .expect("artifact should persist");
    let mut desired = app.state_snapshot().state.desired;
    desired.put_workload(
        WorkloadRecord::builder(
            WorkloadId::new("workload.snapshot-valid"),
            RuntimeType::new("graph.exec.v1"),
            ArtifactId::new("artifact.snapshot-valid"),
        )
        .desired_state(DesiredState::Stopped)
        .assigned_to(NodeId::new("node-a"))
        .build(),
    );
    app.replace_desired(desired);
    app.persist_state().expect("state should persist");
    drop(app);

    fs::write(state_dir.join("mutation-history.rkyv"), b"not-rkyv")
        .expect("corrupt history should be written");

    let replayed = NodeApp::builder()
        .config(NodeConfig {
            node_id: NodeId::new("node-a"),
            http_bind_addr: "127.0.0.1:0".parse().expect("socket address should parse"),
            ipc_socket_path: NodeConfig::default_ipc_socket_path_for("node-test"),
            reconcile_interval: Duration::from_millis(10),
            state_dir: Some(state_dir.clone()),
            peers: Vec::new(),
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

    assert!(
        replayed
            .replay_state()
            .expect("snapshot-based replay should succeed")
    );
    let snapshot = replayed.state_snapshot();
    assert!(
        snapshot
            .state
            .desired
            .workloads
            .contains_key(&WorkloadId::new("workload.snapshot-valid"))
    );

    let _ = fs::remove_dir_all(state_dir);
}

#[test]
fn replay_recovers_from_history_when_snapshot_is_missing() {
    let state_dir = temp_state_dir("history-only-replay");

    let app = NodeApp::builder()
        .config(NodeConfig {
            node_id: NodeId::new("node-a"),
            http_bind_addr: "127.0.0.1:0".parse().expect("socket address should parse"),
            ipc_socket_path: NodeConfig::default_ipc_socket_path_for("node-test"),
            reconcile_interval: Duration::from_millis(10),
            state_dir: Some(state_dir.clone()),
            peers: Vec::new(),
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

    app.put_artifact_content(
        &orion::control_plane::ArtifactRecord::builder("artifact.history-only").build(),
        &[4, 5, 6],
    )
    .expect("artifact should persist");
    let mut desired = app.state_snapshot().state.desired;
    desired.put_workload(
        WorkloadRecord::builder(
            WorkloadId::new("workload.history-only"),
            RuntimeType::new("graph.exec.v1"),
            ArtifactId::new("artifact.history-only"),
        )
        .desired_state(DesiredState::Stopped)
        .assigned_to(NodeId::new("node-a"))
        .build(),
    );
    app.replace_desired(desired);
    app.persist_state().expect("state should persist");
    drop(app);

    let storage = NodeStorage::new(state_dir.clone());
    assert!(storage.snapshot_manifest_path().exists());
    assert!(storage.snapshot_desired_path().exists());
    assert!(storage.snapshot_observed_path().exists());
    assert!(storage.snapshot_applied_path().exists());

    fs::remove_file(state_dir.join("snapshot-manifest.rkyv"))
        .expect("snapshot manifest should be removed");

    let replayed = NodeApp::builder()
        .config(NodeConfig {
            node_id: NodeId::new("node-a"),
            http_bind_addr: "127.0.0.1:0".parse().expect("socket address should parse"),
            ipc_socket_path: NodeConfig::default_ipc_socket_path_for("node-test"),
            reconcile_interval: Duration::from_millis(10),
            state_dir: Some(state_dir.clone()),
            peers: Vec::new(),
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

    assert!(
        replayed
            .replay_state()
            .expect("history-based replay should succeed")
    );
    let snapshot = replayed.state_snapshot();
    assert!(
        snapshot
            .state
            .desired
            .artifacts
            .contains_key(&ArtifactId::new("artifact.history-only"))
    );
    assert!(
        snapshot
            .state
            .desired
            .workloads
            .contains_key(&WorkloadId::new("workload.history-only"))
    );

    let _ = fs::remove_dir_all(state_dir);
}

#[test]
fn replay_rejects_partial_sectioned_snapshot_loss() {
    let state_dir = temp_state_dir("snapshot-partial-loss");

    let app = NodeApp::builder()
        .config(NodeConfig {
            node_id: NodeId::new("node-a"),
            http_bind_addr: "127.0.0.1:0".parse().expect("socket address should parse"),
            ipc_socket_path: NodeConfig::default_ipc_socket_path_for("node-test"),
            reconcile_interval: Duration::from_millis(10),
            state_dir: Some(state_dir.clone()),
            peers: Vec::new(),
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

    app.put_artifact_content(
        &orion::control_plane::ArtifactRecord::builder("artifact.partial-loss").build(),
        &[7, 8, 9],
    )
    .expect("artifact should persist");
    app.persist_state().expect("state should persist");
    drop(app);

    let storage = NodeStorage::new(state_dir.clone());
    fs::remove_file(storage.snapshot_desired_path())
        .expect("desired snapshot section should be removed");
    fs::remove_file(storage.mutation_history_path()).expect("mutation history should be removed");
    let _ = fs::remove_file(storage.mutation_history_baseline_path());

    let replayed = NodeApp::builder()
        .config(NodeConfig {
            node_id: NodeId::new("node-a"),
            http_bind_addr: "127.0.0.1:0".parse().expect("socket address should parse"),
            ipc_socket_path: NodeConfig::default_ipc_socket_path_for("node-test"),
            reconcile_interval: Duration::from_millis(10),
            state_dir: Some(state_dir.clone()),
            peers: Vec::new(),
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

    let err = replayed
        .replay_state()
        .expect_err("partial sectioned snapshot loss should fail");
    assert!(matches!(err, NodeError::Storage(_)));

    let _ = fs::remove_dir_all(state_dir);
}
