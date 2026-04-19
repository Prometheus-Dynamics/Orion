mod error;
mod executor;
mod provider;
mod reconcile;
mod state;

pub use error::RuntimeError;
pub use executor::{
    ExecutorCommand, ExecutorDescriptor, ExecutorIntegration, ExecutorSnapshot, WorkloadPlan,
};
pub use provider::{
    ProviderDescriptor, ProviderIntegration, ProviderSnapshot,
    validate_requirement_against_resource,
};
pub use reconcile::{ReconcileReport, Runtime};
pub use state::{LocalRuntimeStore, RuntimeSnapshot};

#[cfg(test)]
mod tests {
    use super::*;
    use orion_control_plane::{
        AvailabilityState, DesiredClusterState, DesiredState, ExecutorRecord, HealthState,
        LeaseState, ObservedClusterState, ProviderRecord, ResourceOwnershipMode, ResourceRecord,
        RestartPolicy, TypedConfigValue, WorkloadConfig, WorkloadObservedState, WorkloadRecord,
        WorkloadRequirement,
    };
    use orion_core::{
        ArtifactId, CapabilityDef, CapabilityId, ConfigSchemaDef, ConfigSchemaId, ExecutorId,
        NodeId, ProviderId, ResourceId, ResourceType, Revision, RuntimeType, WorkloadId,
    };

    struct CameraControllerConfigV1;
    impl ConfigSchemaDef for CameraControllerConfigV1 {
        const SCHEMA_ID: &'static str = "camera.controller.config.v1";
    }

    struct CaptureConfigurable;
    impl CapabilityDef for CaptureConfigurable {
        const CAPABILITY_ID: &'static str = "capture.configurable";
    }

    fn executor_record() -> ExecutorRecord {
        ExecutorRecord {
            executor_id: ExecutorId::new("executor.local"),
            node_id: NodeId::new("node-a"),
            runtime_types: vec![RuntimeType::new("graph.exec.v1")],
        }
    }

    fn provider_record() -> ProviderRecord {
        ProviderRecord {
            provider_id: ProviderId::new("provider.local"),
            node_id: NodeId::new("node-a"),
            resource_types: vec![ResourceType::new("imu.sample")],
        }
    }

    fn resource_record() -> ResourceRecord {
        ResourceRecord {
            resource_id: ResourceId::new("resource.imu-1"),
            resource_type: ResourceType::new("imu.sample"),
            provider_id: ProviderId::new("provider.local"),
            realized_by_executor_id: None,
            ownership_mode: ResourceOwnershipMode::Exclusive,
            realized_for_workload_id: None,
            source_resource_id: None,
            source_workload_id: None,
            health: HealthState::Healthy,
            availability: AvailabilityState::Available,
            lease_state: LeaseState::Unleased,
            capabilities: Vec::new(),
            labels: Vec::new(),
            endpoints: Vec::new(),
            state: None,
        }
    }

    fn workload_record(desired_state: DesiredState) -> WorkloadRecord {
        WorkloadRecord {
            workload_id: WorkloadId::new("workload.pose"),
            runtime_type: RuntimeType::new("graph.exec.v1"),
            artifact_id: ArtifactId::new("artifact.pose"),
            config: None,
            desired_state,
            observed_state: WorkloadObservedState::Assigned,
            assigned_node_id: Some(NodeId::new("node-a")),
            requirements: vec![WorkloadRequirement {
                resource_type: ResourceType::new("imu.sample"),
                count: 1,
                ownership_mode: None,
                required_capabilities: Vec::new(),
            }],
            resource_bindings: Vec::new(),
            restart_policy: RestartPolicy::OnFailure,
        }
    }

    fn runtime_store() -> LocalRuntimeStore {
        let mut desired = DesiredClusterState::default();
        desired.put_provider(provider_record());
        desired.put_executor(executor_record());

        let mut observed = ObservedClusterState::default();
        observed.put_resource(resource_record());

        let mut store = LocalRuntimeStore::new(NodeId::new("node-a"));
        store.replace_desired(desired);
        store.observed = observed;
        store
    }

    #[test]
    fn plan_workload_builds_local_bindings() {
        let runtime = Runtime::new(NodeId::new("node-a"));
        let executor = executor_record();
        let workload = workload_record(DesiredState::Running);
        let resource = resource_record();
        let plan = runtime
            .plan_workload(&workload, &[&executor], &[&resource])
            .expect("plan should succeed")
            .expect("local running workload should plan");

        assert_eq!(plan.executor_id.as_str(), "executor.local");
        assert_eq!(plan.resource_bindings.len(), 1);
        assert_eq!(plan.resource_bindings[0].node_id.as_str(), "node-a");
    }

    #[test]
    fn reconcile_starts_running_workload_when_not_already_running() {
        let mut store = runtime_store();
        store
            .desired
            .put_workload(workload_record(DesiredState::Running));

        let report = Runtime::new(NodeId::new("node-a"))
            .reconcile(&store)
            .expect("reconcile should succeed");

        assert_eq!(report.commands.len(), 1);
        assert!(matches!(report.commands[0], ExecutorCommand::Start(_)));
    }

    #[test]
    fn reconcile_skips_start_when_running_state_matches_plan() {
        let mut store = runtime_store();
        let mut workload = workload_record(DesiredState::Running);
        workload.observed_state = WorkloadObservedState::Running;
        workload.resource_bindings = vec![orion_control_plane::ResourceBinding {
            resource_id: ResourceId::new("resource.imu-1"),
            node_id: NodeId::new("node-a"),
        }];
        store.desired.put_workload(workload.clone());
        store.observed.put_workload(workload);

        let report = Runtime::new(NodeId::new("node-a"))
            .reconcile(&store)
            .expect("reconcile should succeed");

        assert!(report.commands.is_empty());
    }

    #[test]
    fn reconcile_stops_active_workload_when_desired_state_is_stopped() {
        let mut store = runtime_store();
        let desired = workload_record(DesiredState::Stopped);
        let mut observed = desired.clone();
        observed.observed_state = WorkloadObservedState::Running;
        store.desired.put_workload(desired);
        store.observed.put_workload(observed);

        let report = Runtime::new(NodeId::new("node-a"))
            .reconcile(&store)
            .expect("reconcile should succeed");

        assert_eq!(report.commands.len(), 1);
        assert!(matches!(report.commands[0], ExecutorCommand::Stop { .. }));
    }

    #[test]
    fn reconcile_stops_orphaned_local_workload() {
        let mut store = runtime_store();
        let mut observed = workload_record(DesiredState::Running);
        observed.observed_state = WorkloadObservedState::Running;
        store.observed.put_workload(observed);

        let report = Runtime::new(NodeId::new("node-a"))
            .reconcile(&store)
            .expect("reconcile should succeed");

        assert_eq!(report.commands.len(), 1);
        assert!(matches!(report.commands[0], ExecutorCommand::Stop { .. }));
    }

    #[test]
    fn provider_snapshot_rejects_non_local_provider() {
        let mut store = LocalRuntimeStore::new(NodeId::new("node-a"));
        let err = store
            .apply_provider_snapshot(ProviderSnapshot {
                provider: ProviderRecord {
                    provider_id: ProviderId::new("provider.remote"),
                    node_id: NodeId::new("node-b"),
                    resource_types: vec![ResourceType::new("imu.sample")],
                },
                resources: vec![ResourceRecord {
                    resource_id: ResourceId::new("resource.remote-1"),
                    resource_type: ResourceType::new("imu.sample"),
                    provider_id: ProviderId::new("provider.remote"),
                    realized_by_executor_id: None,
                    ownership_mode: ResourceOwnershipMode::Exclusive,
                    realized_for_workload_id: None,
                    source_resource_id: None,
                    source_workload_id: None,
                    health: HealthState::Healthy,
                    availability: AvailabilityState::Available,
                    lease_state: LeaseState::Unleased,
                    capabilities: Vec::new(),
                    labels: Vec::new(),
                    endpoints: Vec::new(),
                    state: None,
                }],
            })
            .expect_err("remote provider snapshot should be rejected");

        assert!(matches!(err, RuntimeError::ProviderNodeMismatch { .. }));
    }

    #[test]
    fn executor_snapshot_updates_observed_local_workloads() {
        let mut store = runtime_store();
        let mut workload = workload_record(DesiredState::Running);
        workload.observed_state = WorkloadObservedState::Running;

        store
            .apply_executor_snapshot(ExecutorSnapshot {
                executor: executor_record(),
                workloads: vec![workload.clone()],
                resources: Vec::new(),
            })
            .expect("executor snapshot should apply");

        assert_eq!(
            store
                .observed
                .workloads
                .get(&workload.workload_id)
                .expect("observed workload should exist")
                .observed_state,
            WorkloadObservedState::Running
        );
    }

    #[test]
    fn snapshot_reports_local_counts() {
        let mut store = runtime_store();
        store
            .desired
            .put_workload(workload_record(DesiredState::Running));

        let snapshot = store.snapshot();

        assert_eq!(snapshot.local_desired_workloads, 1);
        assert_eq!(snapshot.local_resources, 1);
        assert_eq!(snapshot.local_providers, 1);
        assert_eq!(snapshot.local_executors, 1);
    }

    #[test]
    fn reconcile_does_not_double_allocate_single_resource() {
        let mut store = runtime_store();
        let mut second = workload_record(DesiredState::Running);
        second.workload_id = WorkloadId::new("workload.pose-2");
        store
            .desired
            .put_workload(workload_record(DesiredState::Running));
        store.desired.put_workload(second);

        let report = Runtime::new(NodeId::new("node-a"))
            .reconcile(&store)
            .expect("reconcile should succeed");

        assert_eq!(report.commands.len(), 1);
    }

    #[test]
    fn reconcile_allows_shared_read_resource_for_multiple_workloads() {
        let mut store = runtime_store();
        let mut shared_resource = resource_record();
        shared_resource.resource_id = ResourceId::new("resource.shared-1");
        shared_resource.ownership_mode = ResourceOwnershipMode::SharedRead;
        store.observed.put_resource(shared_resource);

        let mut first = workload_record(DesiredState::Running);
        first.requirements[0].ownership_mode = Some(ResourceOwnershipMode::SharedRead);
        let mut second = workload_record(DesiredState::Running);
        second.workload_id = WorkloadId::new("workload.pose-2");
        second.requirements[0].ownership_mode = Some(ResourceOwnershipMode::SharedRead);
        store.desired.put_workload(first);
        store.desired.put_workload(second);

        let report = Runtime::new(NodeId::new("node-a"))
            .reconcile(&store)
            .expect("reconcile should succeed");

        assert_eq!(report.commands.len(), 2);
    }

    #[test]
    fn reconcile_rejects_shared_limited_resource_above_limit() {
        let mut store = runtime_store();
        let mut limited = resource_record();
        limited.resource_id = ResourceId::new("resource.shared-limited");
        limited.ownership_mode = ResourceOwnershipMode::SharedLimited { max_consumers: 1 };
        store.observed.put_resource(limited);

        let mut first = workload_record(DesiredState::Running);
        first.requirements[0].ownership_mode =
            Some(ResourceOwnershipMode::SharedLimited { max_consumers: 1 });
        let mut second = workload_record(DesiredState::Running);
        second.workload_id = WorkloadId::new("workload.pose-2");
        second.requirements[0].ownership_mode =
            Some(ResourceOwnershipMode::SharedLimited { max_consumers: 1 });
        store.desired.put_workload(first);
        store.desired.put_workload(second);

        let report = Runtime::new(NodeId::new("node-a"))
            .reconcile(&store)
            .expect("reconcile should succeed");

        assert_eq!(report.commands.len(), 1);
    }

    #[test]
    fn plan_workload_rejects_resource_missing_required_capability() {
        let runtime = Runtime::new(NodeId::new("node-a"));
        let executor = executor_record();
        let mut workload = workload_record(DesiredState::Running);
        workload.requirements[0].required_capabilities =
            vec![CapabilityId::of::<CaptureConfigurable>()];
        let resource = resource_record();

        let err = runtime
            .plan_workload(&workload, &[&executor], &[&resource])
            .expect_err("missing capability should reject planning");

        assert!(matches!(err, RuntimeError::UnsupportedResourceType(_)));
    }

    #[test]
    fn plan_workload_accepts_resource_with_required_capability() {
        let runtime = Runtime::new(NodeId::new("node-a"));
        let executor = executor_record();
        let mut workload = workload_record(DesiredState::Running);
        workload.requirements[0].required_capabilities =
            vec![CapabilityId::of::<CaptureConfigurable>()];

        let mut resource = resource_record();
        resource
            .capabilities
            .push(orion_control_plane::ResourceCapability::of::<
                CaptureConfigurable,
            >());

        let plan = runtime
            .plan_workload(&workload, &[&executor], &[&resource])
            .expect("capability-matched workload should plan")
            .expect("local workload should produce a plan");

        assert_eq!(plan.resource_bindings.len(), 1);
    }

    #[test]
    fn executor_snapshot_publishes_derived_resources_into_observed_state() {
        let mut store = runtime_store();
        let mut workload = workload_record(DesiredState::Running);
        workload.workload_id = WorkloadId::new("workload.camera-controller");
        workload.runtime_type = RuntimeType::new("camera.controller.v1");
        workload.observed_state = WorkloadObservedState::Running;

        let derived_resource = ResourceRecord::builder(
            "resource.camera.stream.front",
            "camera.frame_stream",
            "provider.local",
        )
        .realized_by_executor("executor.local")
        .ownership_mode(ResourceOwnershipMode::SharedRead)
        .realized_for_workload(workload.workload_id.clone())
        .source_resource("resource.imu-1")
        .source_workload(workload.workload_id.clone())
        .availability(AvailabilityState::Available)
        .build();

        store
            .apply_executor_snapshot(ExecutorSnapshot {
                executor: executor_record(),
                workloads: vec![workload.clone()],
                resources: vec![derived_resource.clone()],
            })
            .expect("executor snapshot should publish derived resource");

        assert_eq!(
            store
                .observed
                .resources
                .get(&derived_resource.resource_id)
                .expect("derived resource should exist"),
            &derived_resource
        );
    }

    #[test]
    fn executor_snapshot_rejects_invalid_derived_resource_without_provenance() {
        let mut store = runtime_store();
        let mut workload = workload_record(DesiredState::Running);
        workload.workload_id = WorkloadId::new("workload.camera-controller");
        workload.observed_state = WorkloadObservedState::Running;

        let err = store
            .apply_executor_snapshot(ExecutorSnapshot {
                executor: executor_record(),
                workloads: vec![workload],
                resources: vec![
                    ResourceRecord::builder(
                        "resource.camera.stream.front",
                        "camera.frame_stream",
                        "provider.local",
                    )
                    .ownership_mode(ResourceOwnershipMode::SharedRead)
                    .availability(AvailabilityState::Available)
                    .build(),
                ],
            })
            .expect_err("invalid derived resource should be rejected");

        assert!(matches!(err, RuntimeError::InvalidDerivedResource { .. }));
    }

    #[test]
    fn reconcile_allows_consumers_to_bind_executor_published_derived_resource() {
        let mut desired = DesiredClusterState::default();
        desired.put_provider(provider_record());
        desired.put_executor(ExecutorRecord {
            executor_id: ExecutorId::new("executor.local"),
            node_id: NodeId::new("node-a"),
            runtime_types: vec![
                RuntimeType::new("camera.controller.v1"),
                RuntimeType::new("vision.consumer.v1"),
            ],
        });

        let mut observed = ObservedClusterState::default();
        let mut raw = resource_record();
        raw.resource_id = ResourceId::new("resource.camera.raw.front");
        raw.resource_type = ResourceType::new("camera.device");
        raw.ownership_mode = ResourceOwnershipMode::ExclusiveOwnerPublishesDerived;
        observed.put_resource(raw);
        observed.put_resource(
            ResourceRecord::builder(
                "resource.camera.stream.front",
                "camera.frame_stream",
                "provider.local",
            )
            .realized_by_executor("executor.local")
            .ownership_mode(ResourceOwnershipMode::SharedRead)
            .realized_for_workload("workload.camera-controller")
            .source_resource("resource.camera.raw.front")
            .source_workload("workload.camera-controller")
            .availability(AvailabilityState::Available)
            .build(),
        );

        let mut store = LocalRuntimeStore::new(NodeId::new("node-a"));
        store.replace_desired(desired);
        store.observed = observed;

        store.desired.put_workload(
            WorkloadRecord::builder(
                "workload.camera-controller",
                "camera.controller.v1",
                "artifact.camera",
            )
            .desired_state(DesiredState::Running)
            .assigned_to("node-a")
            .require_resource_with_ownership(
                "camera.device",
                1,
                ResourceOwnershipMode::ExclusiveOwnerPublishesDerived,
            )
            .build(),
        );
        store.desired.put_workload(
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
                ResourceOwnershipMode::SharedRead,
            )
            .build(),
        );
        store.desired.put_workload(
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
                ResourceOwnershipMode::SharedRead,
            )
            .build(),
        );

        let report = Runtime::new(NodeId::new("node-a"))
            .reconcile(&store)
            .expect("reconcile should succeed");

        assert_eq!(report.commands.len(), 3);
    }

    #[test]
    fn runtime_error_exposes_config_schema_and_reason() {
        let unsupported = RuntimeError::UnsupportedWorkloadConfigSchema {
            workload_id: WorkloadId::new("workload.camera"),
            runtime_type: RuntimeType::new("camera.controller.v1"),
            schema_id: ConfigSchemaId::new("wrong.schema.v1"),
        };
        let invalid = RuntimeError::InvalidWorkloadConfig {
            workload_id: WorkloadId::new("workload.camera"),
            reason: "missing width".into(),
        };

        assert!(unsupported.to_string().contains("wrong.schema.v1"));
        assert!(invalid.to_string().contains("missing width"));
    }

    #[test]
    fn workload_config_holds_typed_payloads() {
        let config = WorkloadConfig::of::<CameraControllerConfigV1>()
            .field("width", TypedConfigValue::UInt(1280))
            .field("height", TypedConfigValue::UInt(720))
            .field("share", TypedConfigValue::Bool(true));

        assert_eq!(
            config.schema_id,
            ConfigSchemaId::of::<CameraControllerConfigV1>()
        );
        assert_eq!(config.uint("width"), Some(1280));
        assert_eq!(config.uint("height"), Some(720));
        assert_eq!(config.bool("share"), Some(true));
    }

    #[test]
    fn observed_revision_only_advances_after_all_local_components_sync() {
        let mut store = runtime_store();
        assert_eq!(store.observed.revision, Revision::ZERO);

        store
            .apply_provider_snapshot(ProviderSnapshot {
                provider: provider_record(),
                resources: vec![resource_record()],
            })
            .expect("provider snapshot should apply");
        assert_eq!(store.observed.revision, Revision::ZERO);

        let mut workload = workload_record(DesiredState::Running);
        workload.observed_state = WorkloadObservedState::Running;
        store
            .apply_executor_snapshot(ExecutorSnapshot {
                executor: executor_record(),
                workloads: vec![workload],
                resources: Vec::new(),
            })
            .expect("executor snapshot should apply");
        assert_eq!(store.observed.revision, store.desired.revision);
    }

    #[test]
    fn unknown_executor_snapshot_is_rejected() {
        let mut store = runtime_store();
        let err = store
            .apply_executor_snapshot(ExecutorSnapshot {
                executor: ExecutorRecord {
                    executor_id: ExecutorId::new("executor.unknown"),
                    node_id: NodeId::new("node-a"),
                    runtime_types: vec![RuntimeType::new("graph.exec.v1")],
                },
                workloads: Vec::new(),
                resources: Vec::new(),
            })
            .expect_err("unknown executor should be rejected");

        assert!(matches!(err, RuntimeError::UnknownExecutor(_)));
    }
}
