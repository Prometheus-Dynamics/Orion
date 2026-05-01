use super::*;

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
fn node_validation_rejects_workload_assigned_to_unschedulable_node() {
    let app = NodeApp::builder()
        .config(test_node_config_with_auth(
            "node-a",
            "node-test",
            crate::PeerAuthenticationMode::Disabled,
        ))
        .try_build()
        .expect("node app should build");
    app.register_executor(TestExecutor::new())
        .expect("executor registration should succeed");

    let mut desired = app.state_snapshot().state.desired;
    desired.put_artifact(ArtifactRecord::builder("artifact.unschedulable").build());
    desired.put_node(
        orion::control_plane::NodeRecord::builder("node-b")
            .schedulable(false)
            .health(orion::control_plane::HealthState::Healthy)
            .build(),
    );
    desired.put_workload(
        WorkloadRecord::builder(
            "workload.unschedulable",
            "graph.exec.v1",
            "artifact.unschedulable",
        )
        .desired_state(DesiredState::Running)
        .assigned_to("node-b")
        .build(),
    );

    let err = app
        .validate_desired_state_for_test(&desired)
        .expect_err("unschedulable assigned node should be rejected");
    assert!(err.to_string().contains("unschedulable node node-b"));
}
