//! Canonical control-plane records, state groupings, and transport-agnostic
//! control messages for Orion.

mod messages;
mod records;
mod state;

pub use messages::{
    AuditLogBackpressureMode, ClientEvent, ClientEventKind, ClientEventPoll, ClientHello,
    ClientRole, ClientSession, ClientSessionMetricsSnapshot, ControlMessage, DesiredStateMutation,
    DesiredStateObjectSelector, DesiredStateSection, DesiredStateSectionFingerprints,
    DesiredStateSummary, ExecutorStateUpdate, ExecutorWorkloadQuery, HttpMutualTlsMode,
    MaintenanceAction, MaintenanceCommand, MaintenanceMode, MaintenanceState, MaintenanceStatus,
    MutationApplyError, MutationBatch, NodeHealthSnapshot, NodeHealthStatus,
    NodeObservabilitySnapshot, NodeReadinessSnapshot, NodeReadinessStatus, ObservabilityEvent,
    ObservabilityEventKind, ObservedStateUpdate, OperationFailureCategory,
    OperationMetricsSnapshot, PeerEnrollment, PeerHello, PeerIdentityUpdate, PeerSyncErrorKind,
    PeerSyncStatus, PeerTrustRecord, PeerTrustSnapshot, PersistenceMetricsSnapshot,
    ProviderLeaseQuery, ProviderStateUpdate, StateSnapshot, StateWatch, SyncDiffRequest,
    SyncRequest, SyncSummaryRequest, TransportMetricsSnapshot,
};
pub use records::{
    AppliedClusterState, ArtifactRecord, ArtifactRecordBuilder, ClusterStateEnvelope,
    DesiredClusterState, ExecutorRecord, ExecutorRecordBuilder, LeaseRecord, LeaseRecordBuilder,
    NodeRecord, NodeRecordBuilder, ObservedClusterState, ProviderRecord, ProviderRecordBuilder,
    ResourceActionResult, ResourceActionStatus, ResourceBinding, ResourceCapability,
    ResourceConfigState, ResourceOwnershipMode, ResourceRecord, ResourceRecordBuilder,
    ResourceState, TypedConfigValue, WorkloadConfig, WorkloadRecord, WorkloadRecordBuilder,
    WorkloadRequirement,
};
pub use state::{
    AvailabilityState, DesiredState, HealthState, LeaseState, RestartPolicy, WorkloadObservedState,
};

#[cfg(test)]
mod tests {
    use super::*;
    use orion_core::{
        ArtifactId, CapabilityDef, CapabilityId, ConfigSchemaDef, ConfigSchemaId, ExecutorId,
        NodeId, ProviderId, ResourceId, ResourceType, ResourceTypeDef, Revision, RuntimeType,
        RuntimeTypeDef, WorkloadId,
    };

    struct GraphExecV1;
    impl RuntimeTypeDef for GraphExecV1 {
        const TYPE: &'static str = "graph.exec.v1";
    }

    struct ImuSampleSource;
    impl ResourceTypeDef for ImuSampleSource {
        const TYPE: &'static str = "imu.sample_source";
    }

    struct CameraControllerConfigV1;
    impl ConfigSchemaDef for CameraControllerConfigV1 {
        const SCHEMA_ID: &'static str = "camera.controller.config.v1";
    }

    struct CaptureConfigurable;
    impl CapabilityDef for CaptureConfigurable {
        const CAPABILITY_ID: &'static str = "capture.configurable";
    }

    #[test]
    fn workload_record_carries_requirements_and_bindings_separately() {
        let record = WorkloadRecord::builder(
            WorkloadId::new("workload.pose"),
            RuntimeType::of::<GraphExecV1>(),
            ArtifactId::new("artifact.pose.v1"),
        )
        .desired_state(DesiredState::Running)
        .observed_state(WorkloadObservedState::Assigned)
        .assigned_to(NodeId::new("node-a"))
        .require::<ImuSampleSource>(1)
        .bind_resource(ResourceId::new("node-a.imu-01"), NodeId::new("node-a"))
        .restart_policy(RestartPolicy::OnFailure)
        .build();

        assert_eq!(record.requirements.len(), 1);
        assert_eq!(record.resource_bindings.len(), 1);
        assert_eq!(
            record.resource_bindings[0].resource_id.as_str(),
            "node-a.imu-01"
        );
    }

    #[test]
    fn workload_config_and_resource_ownership_are_first_class() {
        let record = WorkloadRecord::builder(
            WorkloadId::new("workload.camera"),
            RuntimeType::new("camera.controller.v1"),
            ArtifactId::new("artifact.camera"),
        )
        .config(
            WorkloadConfig::of::<CameraControllerConfigV1>()
                .field("width", TypedConfigValue::UInt(1280))
                .field("height", TypedConfigValue::UInt(720)),
        )
        .require_resource_with_ownership(
            ResourceType::new("camera.device"),
            1,
            ResourceOwnershipMode::ExclusiveOwnerPublishesDerived,
        )
        .build();
        let derived = ResourceRecord::builder(
            ResourceId::new("resource.camera.stream.front"),
            ResourceType::new("camera.frame_stream"),
            ProviderId::new("provider.peripherals"),
        )
        .supports_capability_of::<CaptureConfigurable>()
        .ownership_mode(ResourceOwnershipMode::SharedRead)
        .realized_for_workload(WorkloadId::new("workload.camera"))
        .source_resource(ResourceId::new("resource.camera.raw.front"))
        .source_workload(WorkloadId::new("workload.camera"))
        .build();

        assert_eq!(
            record
                .config
                .as_ref()
                .expect("config should exist")
                .schema_id,
            ConfigSchemaId::of::<CameraControllerConfigV1>()
        );
        assert_eq!(
            record.requirements[0].ownership_mode,
            Some(ResourceOwnershipMode::ExclusiveOwnerPublishesDerived)
        );
        assert_eq!(
            derived.source_workload_id,
            Some(WorkloadId::new("workload.camera"))
        );
        assert_eq!(
            derived.capabilities[0].capability_id,
            CapabilityId::of::<CaptureConfigurable>()
        );
    }

    #[test]
    fn provider_and_executor_records_attach_to_nodes() {
        let node_id = NodeId::new("node-a");
        let provider =
            ProviderRecord::builder(ProviderId::new("provider.peripherals"), node_id.clone())
                .resource_type(ResourceType::new("camera.device"))
                .build();
        let executor = ExecutorRecord::builder(ExecutorId::new("executor.engine"), node_id)
            .supports::<GraphExecV1>()
            .build();

        assert_eq!(provider.provider_id.as_str(), "provider.peripherals");
        assert_eq!(executor.executor_id.as_str(), "executor.engine");
    }

    #[test]
    fn cluster_state_envelope_keeps_desired_observed_and_applied_distinct() {
        let envelope = ClusterStateEnvelope {
            desired: DesiredClusterState {
                revision: Revision::new(3),
                ..DesiredClusterState::default()
            },
            observed: ObservedClusterState {
                revision: Revision::new(2),
                ..ObservedClusterState::default()
            },
            applied: AppliedClusterState {
                revision: Revision::new(2),
            },
        };

        assert_eq!(envelope.desired.revision.get(), 3);
        assert_eq!(envelope.observed.revision.get(), 2);
        assert_eq!(envelope.applied.revision.get(), 2);
    }

    #[test]
    fn control_messages_are_transport_agnostic() {
        let message = ControlMessage::Hello(PeerHello {
            node_id: NodeId::new("node-a"),
            desired_revision: Revision::new(4),
            desired_fingerprint: 42,
            desired_section_fingerprints: DesiredStateSectionFingerprints {
                nodes: 1,
                artifacts: 2,
                workloads: 3,
                resources: 4,
                providers: 5,
                executors: 6,
                leases: 7,
            },
            observed_revision: Revision::new(3),
            applied_revision: Revision::new(3),
            transport_binding_version: None,
            transport_binding_public_key: None,
            transport_tls_cert_pem: None,
            transport_binding_signature: None,
        });

        match message {
            ControlMessage::Hello(hello) => {
                assert_eq!(hello.node_id.as_str(), "node-a");
                assert_eq!(hello.desired_revision.get(), 4);
                assert_eq!(hello.desired_fingerprint, 42);
            }
            ControlMessage::SyncRequest(_)
            | ControlMessage::SyncSummaryRequest(_)
            | ControlMessage::SyncDiffRequest(_)
            | ControlMessage::QueryStateSnapshot
            | ControlMessage::Snapshot(_)
            | ControlMessage::Mutations(_)
            | ControlMessage::ClientHello(_)
            | ControlMessage::ClientWelcome(_)
            | ControlMessage::ProviderState(_)
            | ControlMessage::ExecutorState(_)
            | ControlMessage::QueryExecutorWorkloads(_)
            | ControlMessage::WatchExecutorWorkloads(_)
            | ControlMessage::ExecutorWorkloads(_)
            | ControlMessage::QueryProviderLeases(_)
            | ControlMessage::WatchProviderLeases(_)
            | ControlMessage::ProviderLeases(_)
            | ControlMessage::QueryObservability
            | ControlMessage::Observability(_)
            | ControlMessage::WatchState(_)
            | ControlMessage::PollClientEvents(_)
            | ControlMessage::ClientEvents(_)
            | ControlMessage::Ping
            | ControlMessage::Pong
            | ControlMessage::Accepted
            | ControlMessage::EnrollPeer(_)
            | ControlMessage::QueryPeerTrust
            | ControlMessage::PeerTrust(_)
            | ControlMessage::RevokePeer(_)
            | ControlMessage::ReplacePeerIdentity(_)
            | ControlMessage::RotateHttpTlsIdentity
            | ControlMessage::QueryMaintenance
            | ControlMessage::UpdateMaintenance(_)
            | ControlMessage::MaintenanceStatus(_)
            | ControlMessage::Rejected(_) => {
                panic!("unexpected control message")
            }
        }
    }

    #[test]
    fn full_state_replay_builds_put_mutations_from_current_desired_state() {
        let mut desired = DesiredClusterState::default();
        desired.put_artifact(ArtifactRecord::builder("artifact.pose").build());
        desired.put_workload(
            WorkloadRecord::builder(
                WorkloadId::new("workload.pose"),
                RuntimeType::of::<GraphExecV1>(),
                ArtifactId::new("artifact.pose"),
            )
            .desired_state(DesiredState::Running)
            .build(),
        );

        let batch = MutationBatch::full_state_replay(Revision::ZERO, &desired);
        assert_eq!(batch.base_revision, Revision::ZERO);
        assert_eq!(batch.mutations.len(), 2);
        assert!(matches!(
            &batch.mutations[0],
            DesiredStateMutation::PutArtifact(_)
        ));
        assert!(matches!(
            &batch.mutations[1],
            DesiredStateMutation::PutWorkload(_)
        ));
    }

    #[test]
    fn desired_state_uses_keyed_records_and_advances_revision_on_put_and_remove() {
        let workload_id = WorkloadId::new("workload.pose");
        let mut desired = DesiredClusterState::default();
        desired.put_workload(
            WorkloadRecord::builder(
                workload_id.clone(),
                RuntimeType::of::<GraphExecV1>(),
                ArtifactId::new("artifact.pose.v1"),
            )
            .desired_state(DesiredState::Running)
            .build(),
        );

        assert_eq!(desired.revision.get(), 1);
        assert!(desired.workloads.contains_key(&workload_id));

        desired.remove_workload(&workload_id);

        assert_eq!(desired.revision.get(), 2);
        assert!(!desired.workloads.contains_key(&workload_id));
    }

    #[test]
    fn mutation_batch_applies_transport_agnostic_state_changes() {
        let workload = WorkloadRecord::builder(
            WorkloadId::new("workload.pose"),
            RuntimeType::of::<GraphExecV1>(),
            ArtifactId::new("artifact.pose.v1"),
        )
        .desired_state(DesiredState::Running)
        .build();
        let mut desired = DesiredClusterState::default();
        MutationBatch {
            base_revision: desired.revision,
            mutations: vec![DesiredStateMutation::PutWorkload(workload.clone())],
        }
        .apply_to(&mut desired);

        assert!(desired.workloads.contains_key(&workload.workload_id));
        assert_eq!(desired.revision.get(), 1);
    }

    #[test]
    fn builders_and_typed_type_defs_reduce_record_boilerplate() {
        let resource = ResourceRecord::builder(
            ResourceId::new("node-a.imu-01"),
            ResourceType::of::<ImuSampleSource>(),
            ProviderId::new("provider.peripherals"),
        )
        .supports_capability_of::<CaptureConfigurable>()
        .health(HealthState::Healthy)
        .availability(AvailabilityState::Available)
        .label("imu")
        .endpoint("ipc://imu")
        .build();

        let node = NodeRecord::builder(NodeId::new("node-a"))
            .health(HealthState::Healthy)
            .label("primary")
            .build();

        let artifact = ArtifactRecord::builder(ArtifactId::new("artifact.pose"))
            .content_type("application/orion-workload")
            .size_bytes(128)
            .build();

        assert_eq!(
            resource.resource_type,
            ResourceType::of::<ImuSampleSource>()
        );
        assert_eq!(
            resource.capabilities[0].capability_id,
            CapabilityId::of::<CaptureConfigurable>()
        );
        assert_eq!(node.health, HealthState::Healthy);
        assert_eq!(artifact.size_bytes, Some(128));
    }

    #[test]
    fn workload_requirement_carries_capability_requirements() {
        let workload = WorkloadRecord::builder(
            WorkloadId::new("workload.capture"),
            RuntimeType::new("camera.controller.v1"),
            ArtifactId::new("artifact.camera"),
        )
        .require_resource_with_capability(
            ResourceType::new("camera.device"),
            1,
            CapabilityId::of::<CaptureConfigurable>(),
        )
        .build();

        assert_eq!(
            workload.requirements[0].required_capabilities,
            vec![CapabilityId::of::<CaptureConfigurable>()]
        );
    }

    #[test]
    fn checked_mutation_batch_rejects_wrong_base_revision() {
        let mut desired = DesiredClusterState::default();
        desired.put_node(NodeRecord {
            node_id: NodeId::new("node-a"),
            health: HealthState::Healthy,
            schedulable: true,
            labels: Vec::new(),
        });

        let result = MutationBatch {
            base_revision: Revision::ZERO,
            mutations: Vec::new(),
        }
        .apply_to_checked(&mut desired);

        assert_eq!(
            result,
            Err(MutationApplyError::RevisionMismatch {
                expected: Revision::ZERO,
                found: Revision::new(1),
            })
        );
    }

    #[test]
    fn observed_and_applied_helpers_keep_state_updates_explicit() {
        let mut observed = ObservedClusterState::default();
        observed.set_revision(Revision::new(3));
        observed.put_node(NodeRecord {
            node_id: NodeId::new("node-a"),
            health: HealthState::Healthy,
            schedulable: true,
            labels: Vec::new(),
        });

        let mut applied = AppliedClusterState::default();
        applied.mark_applied(Revision::new(3));

        let envelope = ClusterStateEnvelope::new(
            DesiredClusterState::default(),
            observed.clone(),
            applied.clone(),
        );

        assert_eq!(observed.revision.get(), 3);
        assert_eq!(applied.revision.get(), 3);
        assert_eq!(envelope.observed.revision.get(), 3);
        assert_eq!(envelope.applied.revision.get(), 3);
    }
}
