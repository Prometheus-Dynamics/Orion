use super::*;
use orion::control_plane::{MaintenanceAction, MaintenanceCommand, MaintenanceMode};

#[test]
fn provider_registration_flapping_across_restarts_keeps_single_provider_identity() {
    let state_dir = temp_state_dir("provider-flap");

    for _ in 0..5 {
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

        app.register_provider(NamedProvider {
            provider_id: "provider.flap",
            resource_id: "resource.flap",
            resource_type: "imu.sample",
            ownership_mode: orion::control_plane::ResourceOwnershipMode::Exclusive,
            capabilities: Vec::new(),
        })
        .expect("provider registration should succeed after restart");
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
    assert_eq!(snapshot.state.desired.providers.len(), 1);
    assert!(
        snapshot
            .state
            .desired
            .providers
            .contains_key(&ProviderId::new("provider.flap"))
    );

    let _ = fs::remove_dir_all(state_dir);
}

#[test]
fn executor_registration_flapping_across_restarts_keeps_single_executor_identity() {
    let state_dir = temp_state_dir("executor-flap");

    for _ in 0..5 {
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

        app.register_executor(NamedExecutor::new("executor.flap", "graph.exec.v1"))
            .expect("executor registration should succeed after restart");
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
    assert_eq!(snapshot.state.desired.executors.len(), 1);
    assert!(
        snapshot
            .state
            .desired
            .executors
            .contains_key(&ExecutorId::new("executor.flap"))
    );

    let _ = fs::remove_dir_all(state_dir);
}

#[tokio::test]
async fn restart_during_reconcile_recovers_latest_desired_state() {
    let state_dir = temp_state_dir("restart-reconcile");

    let app = NodeApp::builder()
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
    let executor = NamedExecutor::new("executor.reconcile", "graph.exec.v1");
    let command_log = executor.commands.clone();
    app.register_provider(NamedProvider {
        provider_id: "provider.reconcile",
        resource_id: "resource.reconcile",
        resource_type: "imu.sample",
        ownership_mode: orion::control_plane::ResourceOwnershipMode::Exclusive,
        capabilities: Vec::new(),
    })
    .expect("provider registration should succeed");
    app.register_executor(executor)
        .expect("executor registration should succeed");

    app.put_artifact_content(
        &orion::control_plane::ArtifactRecord::builder("artifact.reconcile").build(),
        &[9, 9, 9],
    )
    .expect("artifact should persist");

    let mut desired = app.state_snapshot().state.desired;
    desired.put_workload(
        WorkloadRecord::builder(
            WorkloadId::new("workload.reconcile"),
            RuntimeType::new("graph.exec.v1"),
            ArtifactId::new("artifact.reconcile"),
        )
        .desired_state(DesiredState::Running)
        .assigned_to(NodeId::new("node-a"))
        .require_resource(ResourceType::new("imu.sample"), 1)
        .build(),
    );
    app.replace_desired(desired);

    let reconcile_loop = app.spawn_reconcile_loop(Duration::from_millis(10));
    tokio::time::sleep(Duration::from_millis(40)).await;
    reconcile_loop.stop();
    drop(app);

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
            .contains_key(&WorkloadId::new("workload.reconcile"))
    );
    assert!(
        !command_log
            .lock()
            .expect("reconcile command log should not be poisoned")
            .is_empty()
    );

    let _ = fs::remove_dir_all(state_dir);
}

#[test]
fn maintenance_state_persists_across_restart() {
    let state_dir = temp_state_dir("maintenance-persist");

    let app = NodeApp::builder()
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

    app.update_maintenance(MaintenanceCommand {
        action: MaintenanceAction::Enter,
        allow_runtime_types: vec![RuntimeType::new("helios.updater.v1")],
        allow_workload_ids: vec![WorkloadId::new("workload.updater")],
    })
    .expect("maintenance update should succeed");

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

    let status = replayed.maintenance_status();
    assert_eq!(status.state.mode, MaintenanceMode::Maintenance);
    assert_eq!(
        status.state.allow_runtime_types,
        vec![RuntimeType::new("helios.updater.v1")]
    );
    assert_eq!(
        status.state.allow_workload_ids,
        vec![WorkloadId::new("workload.updater")]
    );

    let _ = fs::remove_dir_all(state_dir);
}

#[tokio::test]
async fn restart_during_mutation_apply_recovers_latest_mutation() {
    let state_dir = temp_state_dir("restart-mutation");

    let app = NodeApp::builder()
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
    app.register_provider(NamedProvider {
        provider_id: "provider.mutation",
        resource_id: "resource.mutation",
        resource_type: "imu.sample",
        ownership_mode: orion::control_plane::ResourceOwnershipMode::Exclusive,
        capabilities: Vec::new(),
    })
    .expect("provider registration should succeed");
    app.register_executor(NamedExecutor::new("executor.mutation", "graph.exec.v1"))
        .expect("executor registration should succeed");

    let (addr, server) = app
        .start_http_server("127.0.0.1:0".parse().expect("socket address should parse"))
        .await
        .expect("HTTP server should start");
    let client = HttpClient::try_new(format!("http://{}", addr)).expect("HTTP client should build");

    let response = client
        .send(
            &app.security
                .wrap_http_payload(HttpRequestPayload::Control(Box::new(
                    orion::control_plane::ControlMessage::Mutations(
                        orion::control_plane::MutationBatch {
                            base_revision: app.snapshot().desired_revision,
                            mutations: vec![
                                orion::control_plane::DesiredStateMutation::PutArtifact(
                                    orion::control_plane::ArtifactRecord::builder(
                                        "artifact.mutation",
                                    )
                                    .build(),
                                ),
                                orion::control_plane::DesiredStateMutation::PutWorkload(
                                    WorkloadRecord::builder(
                                        WorkloadId::new("workload.mutation"),
                                        RuntimeType::new("graph.exec.v1"),
                                        ArtifactId::new("artifact.mutation"),
                                    )
                                    .desired_state(DesiredState::Running)
                                    .assigned_to(NodeId::new("node-a"))
                                    .require_resource(ResourceType::new("imu.sample"), 1)
                                    .build(),
                                ),
                            ],
                        },
                    ),
                )))
                .expect("mutation request should be signed"),
        )
        .await
        .expect("mutation request should succeed");
    assert_eq!(response, HttpResponsePayload::Accepted);

    server.abort();
    drop(app);

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
            .artifacts
            .contains_key(&ArtifactId::new("artifact.mutation"))
    );
    assert!(
        snapshot
            .state
            .desired
            .workloads
            .contains_key(&WorkloadId::new("workload.mutation"))
    );

    let _ = fs::remove_dir_all(state_dir);
}
