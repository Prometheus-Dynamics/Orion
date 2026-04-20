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
fn node_validation_rejects_invalid_typed_workload_config_before_assignment() {
    let app = NodeApp::builder()
        .config(test_node_config_with_auth(
            "node-a",
            "node-test",
            crate::PeerAuthenticationMode::Disabled,
        ))
        .try_build()
        .expect("node app should build");
    app.register_executor(ValidatingExecutor)
        .expect("executor registration should succeed");

    let mut desired = DesiredClusterState::default();
    desired.put_executor(
        ExecutorRecord::builder("executor.camera", "node-a")
            .runtime_type("camera.controller.v1")
            .build(),
    );
    desired.put_workload(
        WorkloadRecord::builder("workload.camera", "camera.controller.v1", "artifact.camera")
            .config(WorkloadConfig::new("wrong.schema.v1"))
            .desired_state(DesiredState::Running)
            .assigned_to("node-a")
            .build(),
    );

    let err = app
        .validate_desired_state_for_test(&desired)
        .expect_err("invalid config should be rejected");
    assert!(err.to_string().contains("wrong.schema.v1"));
}

#[test]
fn node_validation_accepts_valid_typed_workload_config() {
    let app = NodeApp::builder()
        .config(test_node_config_with_auth(
            "node-a",
            "node-test",
            crate::PeerAuthenticationMode::Disabled,
        ))
        .try_build()
        .expect("node app should build");
    app.register_executor(ValidatingExecutor)
        .expect("executor registration should succeed");

    let mut desired = DesiredClusterState::default();
    desired.put_executor(
        ExecutorRecord::builder("executor.camera", "node-a")
            .runtime_type("camera.controller.v1")
            .build(),
    );
    desired.put_workload(
        WorkloadRecord::builder("workload.camera", "camera.controller.v1", "artifact.camera")
            .config(
                WorkloadConfig::of::<CameraControllerConfigV1>()
                    .field("width", TypedConfigValue::UInt(1280)),
            )
            .desired_state(DesiredState::Running)
            .assigned_to("node-a")
            .build(),
    );

    app.validate_desired_state_for_test(&desired)
        .expect("valid config should pass validation");
}

#[test]
fn node_validation_rejects_missing_required_resource_capability() {
    let app = NodeApp::builder()
        .config(test_node_config_with_auth(
            "node-a",
            "node-test",
            crate::PeerAuthenticationMode::Disabled,
        ))
        .try_build()
        .expect("node app should build");
    app.register_provider(NamedProvider {
        provider_id: "provider.camera",
        resource_id: "resource.camera.raw.front",
        resource_type: "camera.device",
        ownership_mode: orion::control_plane::ResourceOwnershipMode::ExclusiveOwnerPublishesDerived,
        capabilities: Vec::new(),
    })
    .expect("provider registration should succeed");
    app.register_executor(ValidatingExecutor)
        .expect("executor registration should succeed");

    let mut desired = DesiredClusterState::default();
    desired.put_provider(
        ProviderRecord::builder("provider.camera", "node-a")
            .resource_type("camera.device")
            .build(),
    );
    desired.put_executor(
        ExecutorRecord::builder("executor.camera", "node-a")
            .runtime_type("camera.controller.v1")
            .build(),
    );
    desired.put_workload(
        WorkloadRecord::builder("workload.camera", "camera.controller.v1", "artifact.camera")
            .config(
                WorkloadConfig::of::<CameraControllerConfigV1>()
                    .field("width", TypedConfigValue::UInt(1280)),
            )
            .desired_state(DesiredState::Running)
            .assigned_to("node-a")
            .require_resource_with_ownership_and_capability(
                "camera.device",
                1,
                orion::control_plane::ResourceOwnershipMode::ExclusiveOwnerPublishesDerived,
                CapabilityId::of::<CaptureConfigurable>(),
            )
            .build(),
    );

    let err = app
        .validate_desired_state_for_test(&desired)
        .expect_err("missing capability should be rejected");
    assert!(err.to_string().contains("capture.configurable"));
}

#[test]
fn node_validation_accepts_required_resource_capability() {
    let app = NodeApp::builder()
        .config(test_node_config_with_auth(
            "node-a",
            "node-test",
            crate::PeerAuthenticationMode::Disabled,
        ))
        .try_build()
        .expect("node app should build");
    app.register_provider(NamedProvider {
        provider_id: "provider.camera",
        resource_id: "resource.camera.raw.front",
        resource_type: "camera.device",
        ownership_mode: orion::control_plane::ResourceOwnershipMode::ExclusiveOwnerPublishesDerived,
        capabilities: vec![CapabilityId::of::<CaptureConfigurable>()],
    })
    .expect("provider registration should succeed");
    app.register_executor(ValidatingExecutor)
        .expect("executor registration should succeed");

    let mut desired = DesiredClusterState::default();
    desired.put_provider(
        ProviderRecord::builder("provider.camera", "node-a")
            .resource_type("camera.device")
            .build(),
    );
    desired.put_executor(
        ExecutorRecord::builder("executor.camera", "node-a")
            .runtime_type("camera.controller.v1")
            .build(),
    );
    desired.put_workload(
        WorkloadRecord::builder("workload.camera", "camera.controller.v1", "artifact.camera")
            .config(
                WorkloadConfig::of::<CameraControllerConfigV1>()
                    .field("width", TypedConfigValue::UInt(1280)),
            )
            .desired_state(DesiredState::Running)
            .assigned_to("node-a")
            .require_resource_with_ownership_and_capability(
                "camera.device",
                1,
                orion::control_plane::ResourceOwnershipMode::ExclusiveOwnerPublishesDerived,
                CapabilityId::of::<CaptureConfigurable>(),
            )
            .build(),
    );

    app.validate_desired_state_for_test(&desired)
        .expect("matching capability should validate");
}

#[test]
fn node_validation_accepts_explicit_bound_resource_for_matching_requirement() {
    let app = NodeApp::builder()
        .config(test_node_config_with_auth(
            "node-a",
            "node-test",
            crate::PeerAuthenticationMode::Disabled,
        ))
        .try_build()
        .expect("node app should build");
    app.register_executor(ValidatingExecutor)
        .expect("executor registration should succeed");

    let mut desired = DesiredClusterState::default();
    desired.put_provider(
        ProviderRecord::builder("provider.camera", "node-a")
            .resource_type("camera.device")
            .build(),
    );
    desired.put_executor(
        ExecutorRecord::builder("executor.camera", "node-a")
            .runtime_type("camera.controller.v1")
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
        WorkloadRecord::builder("workload.camera", "camera.controller.v1", "artifact.camera")
            .config(
                WorkloadConfig::of::<CameraControllerConfigV1>()
                    .field("width", TypedConfigValue::UInt(1280)),
            )
            .desired_state(DesiredState::Running)
            .assigned_to("node-a")
            .require_resource("camera.device", 1)
            .bind_resource("resource.camera.raw.front", "node-a")
            .build(),
    );

    app.validate_desired_state_for_test(&desired)
        .expect("matching explicit resource binding should satisfy requirement validation");
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
fn node_validation_rejects_shared_limited_claim_above_limit() {
    let app = NodeApp::builder()
        .config(test_node_config_with_auth(
            "node-a",
            "node-test",
            crate::PeerAuthenticationMode::Disabled,
        ))
        .try_build()
        .expect("node app should build");

    app.register_provider(NamedProvider {
        provider_id: "provider.limited",
        resource_id: "resource.limited-1",
        resource_type: "imu.sample",
        ownership_mode: orion::control_plane::ResourceOwnershipMode::SharedLimited {
            max_consumers: 1,
        },
        capabilities: Vec::new(),
    })
    .expect("limited provider registration should succeed");
    app.register_executor(TestExecutor::new())
        .expect("executor registration should succeed");

    let mut desired = app.state_snapshot().state.desired;
    desired.put_workload(
        WorkloadRecord::builder("workload.limit-a", "graph.exec.v1", "artifact.limit-a")
            .desired_state(DesiredState::Running)
            .assigned_to("node-a")
            .require_resource_with_ownership(
                "imu.sample",
                1,
                orion::control_plane::ResourceOwnershipMode::SharedLimited { max_consumers: 1 },
            )
            .build(),
    );
    desired.put_workload(
        WorkloadRecord::builder("workload.limit-b", "graph.exec.v1", "artifact.limit-b")
            .desired_state(DesiredState::Running)
            .assigned_to("node-a")
            .require_resource_with_ownership(
                "imu.sample",
                1,
                orion::control_plane::ResourceOwnershipMode::SharedLimited { max_consumers: 1 },
            )
            .build(),
    );

    let err = app
        .validate_desired_state_for_test(&desired)
        .expect_err("shared-limited over-claim should be rejected");
    assert!(err.to_string().contains("shared consumer limit 1"));
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

#[tokio::test(flavor = "current_thread")]
async fn async_snapshot_adoption_does_not_block_the_runtime_thread() {
    let state_dir = temp_state_dir("async-snapshot-adoption");
    let app = NodeApp::builder()
        .config(test_node_config_with_state_dir_and_auth(
            "node-a",
            "node-async-snapshot",
            state_dir.clone(),
            crate::PeerAuthenticationMode::Disabled,
        ))
        .try_build()
        .expect("node app should build");

    let mut desired = DesiredClusterState::default();
    desired.put_workload(
        WorkloadRecord::builder("workload.pose", "graph.exec.v1", "artifact.pose")
            .desired_state(DesiredState::Running)
            .assigned_to("node-a")
            .build(),
    );
    let snapshot = orion::control_plane::StateSnapshot {
        state: orion::control_plane::ClusterStateEnvelope::new(
            desired,
            Default::default(),
            Default::default(),
        ),
    };

    set_test_persist_delay(Duration::from_millis(50));
    let tick_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let tick_count_task = tick_count.clone();
    let stop_task = stop.clone();
    let heartbeat = tokio::spawn(async move {
        while !stop_task.load(std::sync::atomic::Ordering::Relaxed) {
            tick_count_task.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            tokio::task::yield_now().await;
        }
    });

    app.adopt_remote_snapshot_async_for_test(snapshot)
        .await
        .expect("async snapshot adoption should succeed");
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    heartbeat.await.expect("heartbeat should join");
    clear_test_persist_delay();

    assert!(
        tick_count.load(std::sync::atomic::Ordering::Relaxed) > 0,
        "background task should keep running while async persistence waits"
    );

    let _ = fs::remove_dir_all(state_dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tick_async_keeps_state_reads_available_while_provider_snapshot_blocks() {
    let app = Arc::new(
        NodeApp::builder()
            .config(test_node_config_with_auth(
                "node-a",
                "tick-async-provider-lock",
                crate::PeerAuthenticationMode::Disabled,
            ))
            .try_build()
            .expect("node app should build"),
    );
    let (provider, started_rx) =
        BlockingProvider::new("provider.blocking", "resource.blocking", "imu.sample");
    app.register_provider(provider.clone())
        .expect("provider registration should succeed");

    let tick_app = Arc::clone(&app);
    let tick = tokio::spawn(async move { tick_app.tick_async().await });

    started_rx
        .recv_timeout(Duration::from_millis(250))
        .expect("provider snapshot should start");

    let snapshot_app = Arc::clone(&app);
    let (snapshot_tx, snapshot_rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let revision = snapshot_app.state_snapshot().state.desired.revision;
        let _ = snapshot_tx.send(revision);
    });

    let revision = snapshot_rx
        .recv_timeout(Duration::from_millis(100))
        .expect("state snapshot should not block while provider snapshot is running");
    assert!(revision > Revision::ZERO);

    provider.release();
    tick.await
        .expect("tick task should join")
        .expect("tick should complete");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tick_async_keeps_state_reads_available_while_executor_snapshot_blocks() {
    let app = Arc::new(
        NodeApp::builder()
            .config(test_node_config_with_auth(
                "node-a",
                "tick-async-executor-lock",
                crate::PeerAuthenticationMode::Disabled,
            ))
            .try_build()
            .expect("node app should build"),
    );
    let (executor, started_rx) = BlockingExecutor::new("executor.blocking", "graph.exec.v1");
    app.register_executor(executor.clone())
        .expect("executor registration should succeed");

    let tick_app = Arc::clone(&app);
    let tick = tokio::spawn(async move { tick_app.tick_async().await });

    started_rx
        .recv_timeout(Duration::from_millis(250))
        .expect("executor snapshot should start");

    let snapshot_app = Arc::clone(&app);
    let (snapshot_tx, snapshot_rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let revision = snapshot_app.state_snapshot().state.desired.revision;
        let _ = snapshot_tx.send(revision);
    });

    let revision = snapshot_rx
        .recv_timeout(Duration::from_millis(100))
        .expect("state snapshot should not block while executor snapshot is running");
    assert!(revision > Revision::ZERO);

    executor.release();
    tick.await
        .expect("tick task should join")
        .expect("tick should complete");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tick_async_makes_progress_under_concurrent_mutation_load() {
    let app = NodeApp::builder()
        .config(test_node_config_with_auth(
            "node-a",
            "tick-async-mutation-load",
            crate::PeerAuthenticationMode::Disabled,
        ))
        .try_build()
        .expect("node app should build");
    app.register_provider(TestProvider)
        .expect("provider registration should succeed");
    app.register_executor(TestExecutor::new())
        .expect("executor registration should succeed");

    let mutator_app = app.clone();
    let mutator = tokio::spawn(async move {
        for index in 0..24_u32 {
            let mut desired = mutator_app.state_snapshot().state.desired;
            desired.put_node(
                orion::control_plane::NodeRecord::builder(format!("node-churn-{index}")).build(),
            );
            desired.put_artifact(
                orion::control_plane::ArtifactRecord::builder(format!("artifact-churn-{index}"))
                    .build(),
            );
            mutator_app.replace_desired(desired);
            tokio::task::yield_now().await;
        }
    });

    let ticker_app = app.clone();
    let ticker = tokio::spawn(async move {
        for _ in 0..24 {
            ticker_app.tick_async().await?;
            tokio::task::yield_now().await;
        }
        Ok::<(), NodeError>(())
    });

    tokio::time::timeout(Duration::from_secs(2), async {
        mutator.await.expect("mutator task should join");
        ticker
            .await
            .expect("ticker task should join")
            .expect("tick loop should succeed");
    })
    .await
    .expect("tick and mutation load should complete without hanging");

    assert!(
        app.state_snapshot().state.desired.revision > Revision::ZERO,
        "concurrent mutation load should advance desired revision"
    );
}
