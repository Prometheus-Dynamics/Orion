use super::*;

#[test]
fn persistence_compacts_mutation_history_to_configured_bound() {
    let state_dir = temp_state_dir("bounded-history");

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
        .with_max_mutation_history_batches(3)
        .try_build()
        .expect("node app should build");

    for cycle in 0..8 {
        let artifact_id = format!("artifact.bound.{cycle}");
        app.put_artifact_content(
            &orion::control_plane::ArtifactRecord::builder(artifact_id.as_str()).build(),
            &[cycle as u8],
        )
        .expect("artifact should persist");

        let mut desired = app.state_snapshot().state.desired;
        desired.put_workload(
            WorkloadRecord::builder(
                WorkloadId::new(format!("workload.bound.{cycle}")),
                RuntimeType::new("graph.exec.v1"),
                ArtifactId::new(&artifact_id),
            )
            .desired_state(DesiredState::Stopped)
            .assigned_to(NodeId::new("node-a"))
            .build(),
        );
        app.replace_desired(desired);
    }

    app.persist_state().expect("state should persist");

    let storage = NodeStorage::new(state_dir.clone());
    let history = storage
        .load_mutation_history()
        .expect("history should load from disk");
    assert_eq!(history.len(), 3);
    assert!(storage.snapshot_manifest_path().exists());
    assert!(storage.snapshot_desired_path().exists());
    assert!(storage.snapshot_observed_path().exists());
    assert!(storage.snapshot_applied_path().exists());
    assert!(
        state_dir.join("mutation-history-baseline.rkyv").exists(),
        "compacted history should persist a replay baseline"
    );

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
        .with_max_mutation_history_batches(3)
        .try_build()
        .expect("node app should build");

    assert!(replayed.replay_state().expect("replay should succeed"));
    let snapshot = replayed.state_snapshot();
    assert!(
        snapshot
            .state
            .desired
            .workloads
            .contains_key(&WorkloadId::new("workload.bound.7"))
    );

    let _ = fs::remove_dir_all(state_dir);
}

#[test]
fn persistence_compacts_mutation_history_to_configured_byte_budget() {
    let state_dir = temp_state_dir("bounded-history-bytes");
    let history_budget = 1_024usize;

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
        .with_max_mutation_history_batches(64)
        .with_max_mutation_history_bytes(history_budget)
        .try_build()
        .expect("node app should build");

    for cycle in 0..24 {
        let artifact_id = format!("artifact.bound.bytes.{cycle}");
        app.put_artifact_content(
            &orion::control_plane::ArtifactRecord::builder(artifact_id.as_str())
                .content_type("application/octet-stream")
                .size_bytes(512)
                .build(),
            &[cycle as u8; 512],
        )
        .expect("artifact should persist");

        let mut desired = app.state_snapshot().state.desired;
        desired.put_workload(
            WorkloadRecord::builder(
                WorkloadId::new(format!("workload.bound.bytes.{cycle}")),
                RuntimeType::new("graph.exec.v1"),
                ArtifactId::new(&artifact_id),
            )
            .desired_state(DesiredState::Stopped)
            .assigned_to(NodeId::new("node-a"))
            .build(),
        );
        app.replace_desired(desired);
    }

    app.persist_state().expect("state should persist");

    let history_path = state_dir.join("mutation-history.rkyv");
    let history_size = fs::metadata(&history_path)
        .expect("history file metadata should exist")
        .len() as usize;
    assert!(
        history_size <= history_budget,
        "history file should stay within byte budget, got {history_size} > {history_budget}"
    );
    assert!(
        state_dir.join("mutation-history-baseline.rkyv").exists(),
        "byte-bounded history should persist a replay baseline"
    );

    let storage = NodeStorage::new(state_dir.clone());
    let history = storage
        .load_mutation_history()
        .expect("history should load from disk");
    assert!(
        !history.is_empty(),
        "compaction should keep a replayable tail"
    );
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
        .with_max_mutation_history_batches(64)
        .with_max_mutation_history_bytes(history_budget)
        .try_build()
        .expect("node app should build");

    assert!(replayed.replay_state().expect("replay should succeed"));
    let snapshot = replayed.state_snapshot();
    assert!(
        snapshot
            .state
            .desired
            .workloads
            .contains_key(&WorkloadId::new("workload.bound.bytes.23"))
    );

    let _ = fs::remove_dir_all(state_dir);
}

#[test]
fn persistence_defers_desired_snapshot_rewrite_until_cadence() {
    let state_dir = temp_state_dir("snapshot-cadence");
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
        .with_snapshot_rewrite_cadence(3)
        .try_build()
        .expect("node app should build");

    app.put_artifact_content(
        &orion::control_plane::ArtifactRecord::builder("artifact.cadence.0").build(),
        &[0],
    )
    .expect("initial artifact should persist");
    let storage = NodeStorage::new(state_dir.clone());
    let manifest = storage
        .load_snapshot_manifest()
        .expect("manifest should load")
        .expect("manifest should exist");
    assert_eq!(manifest.desired_snapshot_revision, Revision::new(1));
    assert_eq!(manifest.latest_desired_revision, Revision::new(1));

    for cycle in 1..=2 {
        let mut desired = app.state_snapshot().state.desired;
        desired.put_workload(
            WorkloadRecord::builder(
                WorkloadId::new(format!("workload.cadence.{cycle}")),
                RuntimeType::new("graph.exec.v1"),
                ArtifactId::new("artifact.cadence.0"),
            )
            .desired_state(DesiredState::Stopped)
            .assigned_to(NodeId::new("node-a"))
            .build(),
        );
        app.replace_desired(desired);
        app.persist_state().expect("state should persist");
    }

    let manifest = storage
        .load_snapshot_manifest()
        .expect("manifest should load")
        .expect("manifest should exist");
    assert_eq!(manifest.desired_snapshot_revision, Revision::new(1));
    assert_eq!(manifest.latest_desired_revision, Revision::new(3));

    let mut desired = app.state_snapshot().state.desired;
    desired.put_workload(
        WorkloadRecord::builder(
            WorkloadId::new("workload.cadence.3"),
            RuntimeType::new("graph.exec.v1"),
            ArtifactId::new("artifact.cadence.0"),
        )
        .desired_state(DesiredState::Stopped)
        .assigned_to(NodeId::new("node-a"))
        .build(),
    );
    app.replace_desired(desired);
    app.persist_state().expect("state should persist");

    let manifest = storage
        .load_snapshot_manifest()
        .expect("manifest should load")
        .expect("manifest should exist");
    assert_eq!(manifest.desired_snapshot_revision, Revision::new(4));
    assert_eq!(manifest.latest_desired_revision, Revision::new(4));

    let _ = fs::remove_dir_all(state_dir);
}

#[test]
fn replay_advances_stale_desired_snapshot_from_history_tail() {
    let state_dir = temp_state_dir("stale-snapshot-history");
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
        .try_build()
        .expect("node app should build");

    app.put_artifact_content(
        &orion::control_plane::ArtifactRecord::builder("artifact.stale-history").build(),
        &[1, 2, 3],
    )
    .expect("artifact should persist");

    for cycle in 0..3 {
        let mut desired = app.state_snapshot().state.desired;
        desired.put_workload(
            WorkloadRecord::builder(
                WorkloadId::new(format!("workload.stale-history.{cycle}")),
                RuntimeType::new("graph.exec.v1"),
                ArtifactId::new("artifact.stale-history"),
            )
            .desired_state(DesiredState::Stopped)
            .assigned_to(NodeId::new("node-a"))
            .build(),
        );
        app.replace_desired(desired);
        app.persist_state().expect("state should persist");
    }
    drop(app);

    let storage = NodeStorage::new(state_dir.clone());
    let manifest = storage
        .load_snapshot_manifest()
        .expect("manifest should load")
        .expect("manifest should exist");
    assert!(manifest.latest_desired_revision > manifest.desired_snapshot_revision);

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
        .with_startup_replay(false)
        .try_build()
        .expect("node app should build");

    assert!(replayed.replay_state().expect("replay should succeed"));
    let snapshot = replayed.state_snapshot();
    assert!(
        snapshot
            .state
            .desired
            .workloads
            .contains_key(&WorkloadId::new("workload.stale-history.2"))
    );

    let _ = fs::remove_dir_all(state_dir);
}

#[test]
fn replay_advances_stale_desired_snapshot_from_compaction_baseline() {
    let state_dir = temp_state_dir("stale-snapshot-baseline");
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
        .with_max_mutation_history_batches(1)
        .try_build()
        .expect("node app should build");

    app.put_artifact_content(
        &orion::control_plane::ArtifactRecord::builder("artifact.stale-baseline").build(),
        &[4, 5, 6],
    )
    .expect("artifact should persist");

    for cycle in 0..16 {
        let mut desired = app.state_snapshot().state.desired;
        desired.put_workload(
            WorkloadRecord::builder(
                WorkloadId::new(format!("workload.stale-baseline.{cycle}")),
                RuntimeType::new("graph.exec.v1"),
                ArtifactId::new("artifact.stale-baseline"),
            )
            .desired_state(DesiredState::Stopped)
            .assigned_to(NodeId::new("node-a"))
            .build(),
        );
        if cycle > 0 {
            desired.remove_workload(&WorkloadId::new(format!(
                "workload.stale-baseline.{}",
                cycle - 1
            )));
        }
        app.replace_desired(desired);
        app.persist_state().expect("state should persist");
    }
    drop(app);

    let storage = NodeStorage::new(state_dir.clone());
    let manifest = storage
        .load_snapshot_manifest()
        .expect("manifest should load")
        .expect("manifest should exist");
    let baseline = storage
        .load_mutation_history_baseline()
        .expect("baseline should load")
        .expect("baseline should exist");
    assert!(
        manifest.latest_desired_revision > manifest.desired_snapshot_revision,
        "latest revision {} should exceed desired snapshot revision {}",
        manifest.latest_desired_revision,
        manifest.desired_snapshot_revision
    );
    assert!(
        baseline.revision > manifest.desired_snapshot_revision,
        "baseline revision {} should exceed desired snapshot revision {}",
        baseline.revision,
        manifest.desired_snapshot_revision
    );

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
        .with_max_mutation_history_batches(1)
        .with_startup_replay(false)
        .try_build()
        .expect("node app should build");

    assert!(replayed.replay_state().expect("replay should succeed"));
    let snapshot = replayed.state_snapshot();
    assert!(
        snapshot
            .state
            .desired
            .workloads
            .contains_key(&WorkloadId::new("workload.stale-baseline.15"))
    );
    assert!(
        !snapshot
            .state
            .desired
            .workloads
            .contains_key(&WorkloadId::new("workload.stale-baseline.14"))
    );

    let _ = fs::remove_dir_all(state_dir);
}

#[test]
fn replay_rejects_stale_desired_snapshot_without_history_tail() {
    let state_dir = temp_state_dir("stale-snapshot-missing-history");
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
        .with_snapshot_rewrite_cadence(10)
        .try_build()
        .expect("node app should build");

    app.put_artifact_content(
        &orion::control_plane::ArtifactRecord::builder("artifact.stale-missing").build(),
        &[7, 8, 9],
    )
    .expect("artifact should persist");
    let mut desired = app.state_snapshot().state.desired;
    desired.put_workload(
        WorkloadRecord::builder(
            WorkloadId::new("workload.stale-missing"),
            RuntimeType::new("graph.exec.v1"),
            ArtifactId::new("artifact.stale-missing"),
        )
        .desired_state(DesiredState::Stopped)
        .assigned_to(NodeId::new("node-a"))
        .build(),
    );
    app.replace_desired(desired);
    app.persist_state().expect("state should persist");
    drop(app);

    fs::remove_file(state_dir.join("mutation-history.rkyv"))
        .expect("mutation history should be removed");

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
        .with_snapshot_rewrite_cadence(10)
        .with_startup_replay(false)
        .try_build()
        .expect("node app should build");

    let err = replayed
        .replay_state()
        .expect_err("stale desired snapshot without history should fail");
    assert!(matches!(err, NodeError::Storage(_)));

    let _ = fs::remove_dir_all(state_dir);
}
