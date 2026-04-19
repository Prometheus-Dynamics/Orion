use super::*;

#[test]
fn replay_recovers_from_current_snapshot_with_corrupt_baseline() {
    let state_dir = temp_state_dir("current-snapshot-corrupt-baseline");
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
        .with_snapshot_rewrite_cadence(1)
        .with_max_mutation_history_batches(2)
        .try_build()
        .expect("node app should build");

    app.put_artifact_content(
        &orion::control_plane::ArtifactRecord::builder("artifact.current-snapshot").build(),
        &[3, 2, 1],
    )
    .expect("artifact should persist");
    for cycle in 0..4 {
        let mut desired = app.state_snapshot().state.desired;
        desired.put_workload(
            WorkloadRecord::builder(
                WorkloadId::new(format!("workload.current-snapshot.{cycle}")),
                RuntimeType::new("graph.exec.v1"),
                ArtifactId::new("artifact.current-snapshot"),
            )
            .desired_state(DesiredState::Stopped)
            .assigned_to(NodeId::new("node-a"))
            .build(),
        );
        app.replace_desired(desired);
        app.persist_state().expect("state should persist");
    }
    drop(app);

    fs::write(
        state_dir.join("mutation-history-baseline.rkyv"),
        b"not-rkyv",
    )
    .expect("corrupt baseline should be written");

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
        .with_snapshot_rewrite_cadence(1)
        .with_max_mutation_history_batches(2)
        .with_startup_replay(false)
        .try_build()
        .expect("node app should build");

    assert!(
        replayed
            .replay_state()
            .expect("current snapshot replay should succeed")
    );
    let snapshot = replayed.state_snapshot();
    assert!(
        snapshot
            .state
            .desired
            .workloads
            .contains_key(&WorkloadId::new("workload.current-snapshot.3"))
    );

    let _ = fs::remove_dir_all(state_dir);
}

#[test]
fn replay_recovers_from_current_snapshot_with_baseline_present_and_corrupt_history() {
    let state_dir = temp_state_dir("current-snapshot-corrupt-history");
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
        .with_snapshot_rewrite_cadence(1)
        .with_max_mutation_history_batches(2)
        .try_build()
        .expect("node app should build");

    app.put_artifact_content(
        &orion::control_plane::ArtifactRecord::builder("artifact.current-history").build(),
        &[6, 6, 6],
    )
    .expect("artifact should persist");
    for cycle in 0..5 {
        let mut desired = app.state_snapshot().state.desired;
        desired.put_workload(
            WorkloadRecord::builder(
                WorkloadId::new(format!("workload.current-history.{cycle}")),
                RuntimeType::new("graph.exec.v1"),
                ArtifactId::new("artifact.current-history"),
            )
            .desired_state(DesiredState::Stopped)
            .assigned_to(NodeId::new("node-a"))
            .build(),
        );
        app.replace_desired(desired);
        app.persist_state().expect("state should persist");
    }
    drop(app);

    assert!(
        state_dir.join("mutation-history-baseline.rkyv").exists(),
        "compaction should leave a baseline on disk"
    );
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
        .with_snapshot_rewrite_cadence(1)
        .with_max_mutation_history_batches(2)
        .with_startup_replay(false)
        .try_build()
        .expect("node app should build");

    assert!(
        replayed
            .replay_state()
            .expect("current snapshot replay should succeed")
    );
    let snapshot = replayed.state_snapshot();
    assert!(
        snapshot
            .state
            .desired
            .workloads
            .contains_key(&WorkloadId::new("workload.current-history.4"))
    );

    let _ = fs::remove_dir_all(state_dir);
}

#[test]
fn repeated_compaction_cycles_over_long_churn_remain_replayable_without_snapshot_manifest() {
    let state_dir = temp_state_dir("long-churn-compaction");

    for cycle in 0..24 {
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
            .with_snapshot_rewrite_cadence(100)
            .with_max_mutation_history_batches(2)
            .try_build()
            .expect("node app should build");

        let artifact_id = format!("artifact.long-churn.{cycle}");
        app.put_artifact_content(
            &orion::control_plane::ArtifactRecord::builder(artifact_id.as_str()).build(),
            &[cycle as u8],
        )
        .expect("artifact should persist");

        let mut desired = app.state_snapshot().state.desired;
        desired.put_workload(
            WorkloadRecord::builder(
                WorkloadId::new(format!("workload.long-churn.{cycle}")),
                RuntimeType::new("graph.exec.v1"),
                ArtifactId::new(&artifact_id),
            )
            .desired_state(DesiredState::Stopped)
            .assigned_to(NodeId::new("node-a"))
            .build(),
        );
        if cycle > 0 {
            desired.remove_workload(&WorkloadId::new(format!(
                "workload.long-churn.{}",
                cycle - 1
            )));
        }
        app.replace_desired(desired);
        app.persist_state().expect("state should persist");
    }

    let storage = NodeStorage::new(state_dir.clone());
    let manifest = storage
        .load_snapshot_manifest()
        .expect("manifest should load")
        .expect("manifest should exist");
    let baseline = storage
        .load_mutation_history_baseline()
        .expect("baseline should load")
        .expect("baseline should exist");
    assert!(manifest.latest_desired_revision > manifest.desired_snapshot_revision);
    assert!(baseline.revision > manifest.desired_snapshot_revision);

    fs::remove_file(storage.snapshot_manifest_path()).expect("snapshot manifest should be removed");

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
        .with_snapshot_rewrite_cadence(100)
        .with_max_mutation_history_batches(2)
        .with_startup_replay(false)
        .try_build()
        .expect("node app should build");

    assert!(
        replayed
            .replay_state()
            .expect("history-only replay should succeed")
    );
    let snapshot = replayed.state_snapshot();
    assert!(
        snapshot
            .state
            .desired
            .workloads
            .contains_key(&WorkloadId::new("workload.long-churn.23"))
    );
    assert!(
        !snapshot
            .state
            .desired
            .workloads
            .contains_key(&WorkloadId::new("workload.long-churn.22"))
    );

    let _ = fs::remove_dir_all(state_dir);
}
