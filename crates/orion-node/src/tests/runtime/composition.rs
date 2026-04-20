use super::*;

#[test]
fn node_app_composes_runtime_and_transports() {
    let config = test_node_config("node-a", "node-test");
    let app = NodeApp::try_new(config.clone()).expect("node app should build");

    assert_eq!(app.config, config);
    assert_eq!(app.snapshot().node_id, config.node_id);
}

#[test]
fn node_app_builder_supports_default_transport_composition() {
    let app = NodeApp::builder()
        .config(test_node_config_with_auth(
            "node-b",
            "node-test",
            crate::PeerAuthenticationMode::Disabled,
        ))
        .with_default_transports()
        .try_build()
        .expect("node app should build");

    assert_eq!(app.snapshot().node_id.as_str(), "node-b");
}

#[test]
fn node_app_registration_and_tick_drive_executor_commands() {
    let app = NodeApp::builder()
        .config(test_node_config_with_auth(
            "node-a",
            "node-test",
            crate::PeerAuthenticationMode::Disabled,
        ))
        .try_build()
        .expect("node app should build");

    let executor = TestExecutor::new();
    let command_log = executor.commands.clone();
    app.register_provider(TestProvider)
        .expect("provider registration should succeed");
    app.register_executor(executor)
        .expect("executor registration should succeed");

    let desired = orion::control_plane::DesiredClusterState::default();
    app.replace_desired(desired);
    {
        let mut desired = app.state_snapshot().state.desired;
        desired.put_provider(
            ProviderRecord::builder(ProviderId::new("provider.local"), NodeId::new("node-a"))
                .resource_type(ResourceType::new("imu.sample"))
                .build(),
        );
        desired.put_executor(
            ExecutorRecord::builder(ExecutorId::new("executor.local"), NodeId::new("node-a"))
                .runtime_type(RuntimeType::new("graph.exec.v1"))
                .build(),
        );
        desired.put_workload(
            WorkloadRecord::builder(
                WorkloadId::new("workload.pose"),
                RuntimeType::new("graph.exec.v1"),
                ArtifactId::new("artifact.pose"),
            )
            .desired_state(DesiredState::Running)
            .assigned_to(NodeId::new("node-a"))
            .require_resource(ResourceType::new("imu.sample"), 1)
            .observed_state(WorkloadObservedState::Pending)
            .build(),
        );
        app.replace_desired(desired);
    }

    let report = app.tick().expect("tick should reconcile successfully");

    assert_eq!(report.commands.len(), 1);
    assert_eq!(
        command_log
            .lock()
            .expect("test executor command log lock should not be poisoned")
            .len(),
        1
    );
}

#[test]
fn node_tick_plans_running_workload_from_desired_only_bound_resource() {
    let app = NodeApp::builder()
        .config(test_node_config_with_auth(
            "node-a",
            "node-test",
            crate::PeerAuthenticationMode::Disabled,
        ))
        .try_build()
        .expect("node app should build");

    let executor = TestExecutor::new();
    let command_log = executor.commands.clone();
    app.register_executor(executor)
        .expect("executor registration should succeed");

    let mut desired = DesiredClusterState::default();
    desired.put_provider(
        ProviderRecord::builder("provider.camera", "node-a")
            .resource_type("camera.device")
            .build(),
    );
    desired.put_executor(
        ExecutorRecord::builder("executor.local", "node-a")
            .runtime_type("graph.exec.v1")
            .build(),
    );
    desired.put_resource(
        orion::control_plane::ResourceRecord::builder(
            "resource.camera.raw.front",
            "camera.device",
            "provider.camera",
        )
        .health(orion::control_plane::HealthState::Healthy)
        .availability(orion::control_plane::AvailabilityState::Available)
        .build(),
    );
    desired.put_workload(
        WorkloadRecord::builder("workload.pose", "graph.exec.v1", "artifact.pose")
            .desired_state(DesiredState::Running)
            .assigned_to("node-a")
            .require_resource("camera.device", 1)
            .bind_resource("resource.camera.raw.front", "node-a")
            .build(),
    );
    app.replace_desired(desired);

    let report = app.tick().expect("tick should reconcile successfully");

    assert_eq!(report.commands.len(), 1);
    assert_eq!(
        command_log
            .lock()
            .expect("test executor command log should not be poisoned")
            .len(),
        1
    );
}

#[test]
fn node_tick_allows_multiple_claims_on_shared_read_raw_resource() {
    let app = NodeApp::builder()
        .config(test_node_config_with_auth(
            "node-a",
            "node-test",
            crate::PeerAuthenticationMode::Optional,
        ))
        .try_build()
        .expect("node app should build");

    let executor = TestExecutor::new();
    let command_log = executor.commands.clone();
    app.register_provider(NamedProvider {
        provider_id: "provider.shared",
        resource_id: "resource.shared-1",
        resource_type: "imu.sample",
        ownership_mode: orion::control_plane::ResourceOwnershipMode::SharedRead,
        capabilities: Vec::new(),
    })
    .expect("shared provider registration should succeed");
    app.register_executor(executor)
        .expect("executor registration should succeed");

    let mut desired = app.state_snapshot().state.desired;
    desired.put_workload(
        WorkloadRecord::builder("workload.shared-a", "graph.exec.v1", "artifact.shared-a")
            .desired_state(DesiredState::Running)
            .assigned_to("node-a")
            .require_resource_with_ownership(
                "imu.sample",
                1,
                orion::control_plane::ResourceOwnershipMode::SharedRead,
            )
            .build(),
    );
    desired.put_workload(
        WorkloadRecord::builder("workload.shared-b", "graph.exec.v1", "artifact.shared-b")
            .desired_state(DesiredState::Running)
            .assigned_to("node-a")
            .require_resource_with_ownership(
                "imu.sample",
                1,
                orion::control_plane::ResourceOwnershipMode::SharedRead,
            )
            .build(),
    );
    app.validate_desired_state_for_test(&desired)
        .expect("shared-read claims should validate");
    app.replace_desired(desired);

    let report = app.tick().expect("tick should reconcile successfully");

    assert_eq!(report.commands.len(), 2);
    assert_eq!(
        command_log
            .lock()
            .expect("test executor command log should not be poisoned")
            .len(),
        2
    );
}

#[test]
fn node_tick_allows_consumers_to_bind_executor_published_derived_resources() {
    let app = NodeApp::builder()
        .config(test_node_config_with_auth(
            "node-a",
            "node-test",
            crate::PeerAuthenticationMode::Disabled,
        ))
        .try_build()
        .expect("node app should build");

    let executor = CameraPipelineExecutor::new();
    let command_log = executor.commands.clone();
    app.register_provider(CameraProvider)
        .expect("camera provider registration should succeed");
    app.register_executor(executor)
        .expect("camera executor registration should succeed");

    let mut desired = app.state_snapshot().state.desired;
    desired.put_workload(
        WorkloadRecord::builder(
            "workload.camera-controller",
            "camera.controller.v1",
            "artifact.camera",
        )
        .config(
            WorkloadConfig::of::<CameraControllerConfigV1>()
                .field("width", TypedConfigValue::UInt(1280)),
        )
        .desired_state(DesiredState::Running)
        .assigned_to("node-a")
        .require_resource_with_ownership(
            "camera.device",
            1,
            orion::control_plane::ResourceOwnershipMode::ExclusiveOwnerPublishesDerived,
        )
        .build(),
    );
    desired.put_workload(
        WorkloadRecord::builder(
            "workload.vision-a",
            "vision.consumer.v1",
            "artifact.vision-a",
        )
        .desired_state(DesiredState::Running)
        .assigned_to("node-a")
        .require_resource_with_ownership(
            "camera.frame_stream",
            1,
            orion::control_plane::ResourceOwnershipMode::SharedRead,
        )
        .build(),
    );
    desired.put_workload(
        WorkloadRecord::builder(
            "workload.vision-b",
            "vision.consumer.v1",
            "artifact.vision-b",
        )
        .desired_state(DesiredState::Running)
        .assigned_to("node-a")
        .require_resource_with_ownership(
            "camera.frame_stream",
            1,
            orion::control_plane::ResourceOwnershipMode::SharedRead,
        )
        .build(),
    );
    app.replace_desired(desired);

    let report = app.tick().expect("tick should reconcile successfully");

    assert_eq!(report.commands.len(), 2);
    assert_eq!(
        command_log
            .lock()
            .expect("camera executor command log should not be poisoned")
            .len(),
        2
    );
}

#[test]
fn node_tick_does_not_start_two_controller_workloads_for_one_exclusive_raw_resource() {
    let app = NodeApp::builder()
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

    let executor = CameraControllerExecutor::new();
    let command_log = executor.commands.clone();
    app.register_provider(CameraProvider)
        .expect("camera provider registration should succeed");
    app.register_executor(executor)
        .expect("camera executor registration should succeed");

    let mut desired = app.state_snapshot().state.desired;
    desired.put_workload(
        WorkloadRecord::builder(
            "workload.camera-controller-a",
            "camera.controller.v1",
            "artifact.camera-a",
        )
        .config(
            WorkloadConfig::of::<CameraControllerConfigV1>()
                .field("width", TypedConfigValue::UInt(1280)),
        )
        .desired_state(DesiredState::Running)
        .assigned_to("node-a")
        .require_resource_with_ownership(
            "camera.device",
            1,
            orion::control_plane::ResourceOwnershipMode::ExclusiveOwnerPublishesDerived,
        )
        .build(),
    );
    desired.put_workload(
        WorkloadRecord::builder(
            "workload.camera-controller-b",
            "camera.controller.v1",
            "artifact.camera-b",
        )
        .config(
            WorkloadConfig::of::<CameraControllerConfigV1>()
                .field("width", TypedConfigValue::UInt(640)),
        )
        .desired_state(DesiredState::Running)
        .assigned_to("node-a")
        .require_resource_with_ownership(
            "camera.device",
            1,
            orion::control_plane::ResourceOwnershipMode::ExclusiveOwnerPublishesDerived,
        )
        .build(),
    );
    app.replace_desired(desired);

    let report = app.tick().expect("tick should reconcile successfully");

    assert_eq!(report.commands.len(), 1);
    assert_eq!(
        command_log
            .lock()
            .expect("camera executor command log should not be poisoned")
            .len(),
        1
    );
}
