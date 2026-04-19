use super::*;
use orion_control_plane::{
    AvailabilityState, ClientEventKind, ClientRole, ControlMessage, DesiredState, ExecutorRecord,
    HealthState, LeaseState, MutationBatch, NodeRecord, ProviderRecord, ResourceOwnershipMode,
    ResourceRecord, StateSnapshot, WorkloadObservedState, WorkloadRecord,
};
use orion_core::{
    ArtifactId, CapabilityDef, CapabilityId, ExecutorId, NodeId, ProviderId, ResourceId,
    ResourceType, Revision, RuntimeType, WorkloadId,
};
use orion_transport_ipc::{ControlEnvelope, IpcTransport, LocalAddress, LocalControlTransport};

mod local_defaults;

fn setup_transport() -> IpcTransport {
    let transport = IpcTransport::new();
    assert!(transport.register_control_endpoint(LocalAddress::new("orion")));
    transport
}

struct CaptureConfigurable;
impl CapabilityDef for CaptureConfigurable {
    const CAPABILITY_ID: &'static str = "capture.configurable";
}

#[test]
fn session_registers_local_endpoint_and_sends_control_messages() {
    let transport = setup_transport();
    let session = ClientSession::connect(
        transport.clone(),
        ClientIdentity::new("cli", ClientRole::ControlPlane),
        SessionConfig::new(LocalAddress::new("cli")),
    )
    .expect("session should connect");

    session
        .request_sync(NodeId::new("node-a"), Revision::new(3))
        .expect("sync request should send");

    let envelope = transport
        .recv_control(&LocalAddress::new("orion"))
        .expect("orion should receive request");
    assert_eq!(envelope.source.as_str(), "cli");
    assert!(matches!(envelope.message, ControlMessage::SyncRequest(_)));
}

#[test]
fn control_plane_client_rejects_wrong_role() {
    let transport = setup_transport();
    let session = ClientSession::connect(
        transport,
        ClientIdentity::new("provider-a", ClientRole::Provider),
        SessionConfig::new(LocalAddress::new("provider-a")),
    )
    .expect("session should connect");

    let error = match ControlPlaneClient::new(session) {
        Ok(_) => panic!("role mismatch expected"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        ClientError::RoleMismatch {
            expected: ClientRole::ControlPlane,
            found: ClientRole::Provider,
        }
    ));
}

#[test]
fn provider_client_publishes_provider_and_resource_mutations() {
    let transport = setup_transport();
    let session = ClientSession::connect(
        transport.clone(),
        ClientIdentity::new("provider-a", ClientRole::Provider),
        SessionConfig::new(LocalAddress::new("provider-a")),
    )
    .expect("session should connect");
    let client = ProviderClient::new(session).expect("provider client");
    let provider = ProviderRecord {
        provider_id: ProviderId::new("provider.local"),
        node_id: NodeId::new("node-a"),
        resource_types: vec![ResourceType::new("camera.device")],
    };

    assert!(
        transport.send_control(orion_transport_ipc::ControlEnvelope {
            source: LocalAddress::new("orion"),
            destination: LocalAddress::new("provider-a"),
            message: ControlMessage::Accepted,
        })
    );
    client
        .register_provider(provider.clone())
        .expect("provider registration should send");
    assert!(
        transport.send_control(orion_transport_ipc::ControlEnvelope {
            source: LocalAddress::new("orion"),
            destination: LocalAddress::new("provider-a"),
            message: ControlMessage::Accepted,
        })
    );
    client
        .publish_resources(
            provider,
            vec![ResourceRecord {
                resource_id: ResourceId::new("resource.camera-01"),
                resource_type: ResourceType::new("camera.device"),
                provider_id: ProviderId::new("provider.local"),
                realized_by_executor_id: None,
                ownership_mode: orion_control_plane::ResourceOwnershipMode::Exclusive,
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
        )
        .expect("resource publication should send");

    let provider_message = transport
        .recv_control(&LocalAddress::new("orion"))
        .expect("provider mutation should arrive");
    let resource_message = transport
        .recv_control(&LocalAddress::new("orion"))
        .expect("resource mutation should arrive");

    assert!(matches!(
        provider_message.message,
        ControlMessage::ProviderState(_)
    ));
    assert!(matches!(
        resource_message.message,
        ControlMessage::ProviderState(_)
    ));
}

#[test]
fn provider_client_surfaces_rejected_reason() {
    let transport = setup_transport();
    let session = ClientSession::connect(
        transport.clone(),
        ClientIdentity::new("provider-a", ClientRole::Provider),
        SessionConfig::new(LocalAddress::new("provider-a")),
    )
    .expect("session should connect");
    let client = ProviderClient::new(session).expect("provider client");
    let provider = ProviderRecord {
        provider_id: ProviderId::new("provider.local"),
        node_id: NodeId::new("node-a"),
        resource_types: vec![ResourceType::new("camera.device")],
    };

    assert!(
        transport.send_control(orion_transport_ipc::ControlEnvelope {
            source: LocalAddress::new("orion"),
            destination: LocalAddress::new("provider-a"),
            message: ControlMessage::Rejected("ownership violation".into()),
        })
    );

    let error = client
        .register_provider(provider)
        .expect_err("provider rejection should surface");
    assert!(
        matches!(error, ClientError::Rejected(reason) if reason.contains("ownership violation"))
    );
}

#[test]
fn executor_client_publishes_executor_snapshot() {
    let transport = setup_transport();
    let session = ClientSession::connect(
        transport.clone(),
        ClientIdentity::new("executor-a", ClientRole::Executor),
        SessionConfig::new(LocalAddress::new("executor-a")),
    )
    .expect("session should connect");
    let client = ExecutorClient::new(session).expect("executor client");
    let executor = ExecutorRecord {
        executor_id: ExecutorId::new("executor.local"),
        node_id: NodeId::new("node-a"),
        runtime_types: vec![RuntimeType::new("graph.exec.v1")],
    };

    assert!(
        transport.send_control(orion_transport_ipc::ControlEnvelope {
            source: LocalAddress::new("orion"),
            destination: LocalAddress::new("executor-a"),
            message: ControlMessage::Accepted,
        })
    );
    client
        .publish_snapshot(orion_runtime::ExecutorSnapshot {
            executor,
            workloads: vec![WorkloadRecord {
                workload_id: WorkloadId::new("workload.pose"),
                runtime_type: RuntimeType::new("graph.exec.v1"),
                artifact_id: ArtifactId::new("artifact.pose"),
                config: None,
                desired_state: DesiredState::Running,
                observed_state: WorkloadObservedState::Running,
                assigned_node_id: Some(NodeId::new("node-a")),
                requirements: Vec::new(),
                resource_bindings: Vec::new(),
                restart_policy: orion_control_plane::RestartPolicy::Always,
            }],
            resources: Vec::new(),
        })
        .expect("executor snapshot should send");

    assert!(matches!(
        transport
            .recv_control(&LocalAddress::new("orion"))
            .expect("executor state should arrive")
            .message,
        ControlMessage::ExecutorState(_)
    ));
}

#[test]
fn executor_client_surfaces_rejected_reason() {
    let transport = setup_transport();
    let session = ClientSession::connect(
        transport.clone(),
        ClientIdentity::new("executor-a", ClientRole::Executor),
        SessionConfig::new(LocalAddress::new("executor-a")),
    )
    .expect("session should connect");
    let client = ExecutorClient::new(session).expect("executor client");
    let executor = ExecutorRecord {
        executor_id: ExecutorId::new("executor.local"),
        node_id: NodeId::new("node-a"),
        runtime_types: vec![RuntimeType::new("graph.exec.v1")],
    };

    assert!(
        transport.send_control(orion_transport_ipc::ControlEnvelope {
            source: LocalAddress::new("orion"),
            destination: LocalAddress::new("executor-a"),
            message: ControlMessage::Rejected("unsupported schema".into()),
        })
    );

    let error = client
        .register_executor(executor)
        .expect_err("executor rejection should surface");
    assert!(
        matches!(error, ClientError::Rejected(reason) if reason.contains("unsupported schema"))
    );
}

#[test]
fn control_plane_client_can_publish_snapshot() {
    let transport = setup_transport();
    let session = ClientSession::connect(
        transport.clone(),
        ClientIdentity::new("cli", ClientRole::ControlPlane),
        SessionConfig::new(LocalAddress::new("cli")),
    )
    .expect("session should connect");
    let client = ControlPlaneClient::new(session).expect("control-plane client");

    client
        .publish_snapshot(StateSnapshot {
            state: orion_control_plane::ClusterStateEnvelope {
                desired: Default::default(),
                observed: Default::default(),
                applied: Default::default(),
            },
        })
        .expect("snapshot should send");

    let envelope = transport
        .recv_control(&LocalAddress::new("orion"))
        .expect("snapshot should arrive");
    assert!(matches!(envelope.message, ControlMessage::Snapshot(_)));
}

#[test]
fn control_plane_client_subscribes_and_polls_state_events() {
    let transport = setup_transport();
    let session = ClientSession::connect(
        transport.clone(),
        ClientIdentity::new("cli", ClientRole::ControlPlane),
        SessionConfig::new(LocalAddress::new("cli")),
    )
    .expect("session should connect");
    let client = ControlPlaneClient::new(session).expect("control-plane client");

    assert!(
        transport.send_control(orion_transport_ipc::ControlEnvelope {
            source: LocalAddress::new("orion"),
            destination: LocalAddress::new("cli"),
            message: ControlMessage::Accepted,
        })
    );
    client
        .subscribe_state(Revision::ZERO)
        .expect("watch registration should send");
    let watch_envelope = transport
        .recv_control(&LocalAddress::new("orion"))
        .expect("watch registration should arrive");
    assert!(matches!(
        watch_envelope.message,
        ControlMessage::WatchState(_)
    ));

    let snapshot = StateSnapshot {
        state: orion_control_plane::ClusterStateEnvelope {
            desired: Default::default(),
            observed: Default::default(),
            applied: Default::default(),
        },
    };
    assert!(
        transport.send_control(orion_transport_ipc::ControlEnvelope {
            source: LocalAddress::new("orion"),
            destination: LocalAddress::new("cli"),
            message: ControlMessage::ClientEvents(vec![orion_control_plane::ClientEvent {
                sequence: 1,
                event: ClientEventKind::StateSnapshot(Box::new(snapshot.clone())),
            }]),
        })
    );

    let events = client
        .poll_state_events(0, 8)
        .expect("state event poll should receive response");
    let poll_envelope = transport
        .recv_control(&LocalAddress::new("orion"))
        .expect("state event poll should arrive");
    assert!(matches!(
        poll_envelope.message,
        ControlMessage::PollClientEvents(_)
    ));
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].sequence, 1);
    match &events[0].event {
        ClientEventKind::StateSnapshot(event_snapshot) => {
            assert_eq!(event_snapshot.as_ref(), &snapshot);
        }
        ClientEventKind::ExecutorWorkloads { .. } | ClientEventKind::ProviderLeases { .. } => {
            panic!("unexpected non-control-plane event");
        }
    }
}

#[test]
fn session_receives_messages_for_its_local_address() {
    let transport = setup_transport();
    let session = ClientSession::connect(
        transport.clone(),
        ClientIdentity::new("cli", ClientRole::ControlPlane),
        SessionConfig::new(LocalAddress::new("cli")),
    )
    .expect("session should connect");

    assert!(
        transport.send_control(orion_transport_ipc::ControlEnvelope {
            source: LocalAddress::new("orion"),
            destination: LocalAddress::new("cli"),
            message: ControlMessage::Mutations(MutationBatch {
                base_revision: Revision::ZERO,
                mutations: vec![orion_control_plane::DesiredStateMutation::PutNode(
                    NodeRecord::builder(NodeId::new("node-a")).build(),
                )],
            }),
        })
    );

    let envelope = session
        .recv_control()
        .expect("client should receive message");
    assert_eq!(envelope.source.as_str(), "orion");
}

#[test]
fn provider_app_uses_stored_provider_identity() {
    let transport = setup_transport();
    let session = ClientSession::connect(
        transport.clone(),
        ClientIdentity::new("provider-a", ClientRole::Provider),
        SessionConfig::new(LocalAddress::new("provider-a")),
    )
    .expect("session should connect");
    let client = ProviderClient::new(session).expect("provider client");
    let provider = ProviderRecord {
        provider_id: ProviderId::new("provider.local"),
        node_id: NodeId::new("node-a"),
        resource_types: vec![ResourceType::new("camera.device")],
    };
    let app = ProviderApp::new(client, provider);

    assert!(
        transport.send_control(orion_transport_ipc::ControlEnvelope {
            source: LocalAddress::new("orion"),
            destination: LocalAddress::new("provider-a"),
            message: ControlMessage::Accepted,
        })
    );
    app.register().expect("provider registration should send");

    assert!(
        transport.send_control(orion_transport_ipc::ControlEnvelope {
            source: LocalAddress::new("orion"),
            destination: LocalAddress::new("provider-a"),
            message: ControlMessage::Accepted,
        })
    );
    app.publish_resource(
        ProviderResource::new("resource.camera-01", "camera.device", "provider.local")
            .ownership_mode(ResourceOwnershipMode::ExclusiveOwnerPublishesDerived)
            .supports_capability_of::<CaptureConfigurable>()
            .health(HealthState::Healthy)
            .availability(AvailabilityState::Available)
            .build(),
    )
    .expect("resource publication should send");

    let provider_message = transport
        .recv_control(&LocalAddress::new("orion"))
        .expect("provider mutation should arrive");
    let resource_message = transport
        .recv_control(&LocalAddress::new("orion"))
        .expect("resource mutation should arrive");

    assert!(matches!(
        provider_message.message,
        ControlMessage::ProviderState(_)
    ));
    match resource_message.message {
        ControlMessage::ProviderState(update) => {
            assert_eq!(update.provider.provider_id.as_str(), "provider.local");
            assert_eq!(update.resources.len(), 1);
            assert_eq!(
                update.resources[0].capabilities[0].capability_id,
                CapabilityId::of::<CaptureConfigurable>()
            );
        }
        other => panic!("expected provider state, got {other:?}"),
    }
}

#[test]
fn executor_app_uses_stored_executor_identity() {
    let transport = setup_transport();
    let session = ClientSession::connect(
        transport.clone(),
        ClientIdentity::new("executor-a", ClientRole::Executor),
        SessionConfig::new(LocalAddress::new("executor-a")),
    )
    .expect("session should connect");
    let client = ExecutorClient::new(session).expect("executor client");
    let executor = ExecutorRecord {
        executor_id: ExecutorId::new("executor.local"),
        node_id: NodeId::new("node-a"),
        runtime_types: vec![RuntimeType::new("graph.exec.v1")],
    };
    let app = ExecutorApp::new(client, executor);

    assert!(
        transport.send_control(orion_transport_ipc::ControlEnvelope {
            source: LocalAddress::new("orion"),
            destination: LocalAddress::new("executor-a"),
            message: ControlMessage::Accepted,
        })
    );
    app.register().expect("executor registration should send");

    assert!(
        transport.send_control(orion_transport_ipc::ControlEnvelope {
            source: LocalAddress::new("orion"),
            destination: LocalAddress::new("executor-a"),
            message: ControlMessage::Accepted,
        })
    );
    app.publish_resource(
        DerivedResource::new(
            "resource.camera.stream.front",
            "camera.frame_stream",
            "provider.local",
        )
        .realized_by_executor("executor.local")
        .realized_for_workload("workload.camera")
        .source_resource("resource.camera.raw.front")
        .source_workload("workload.camera")
        .ownership_mode(ResourceOwnershipMode::SharedRead)
        .supports_capability_of::<CaptureConfigurable>()
        .build(),
    )
    .expect("executor resource publication should send");

    let registration = transport
        .recv_control(&LocalAddress::new("orion"))
        .expect("executor registration should arrive");
    let update = transport
        .recv_control(&LocalAddress::new("orion"))
        .expect("executor update should arrive");

    assert!(matches!(
        registration.message,
        ControlMessage::ExecutorState(_)
    ));
    match update.message {
        ControlMessage::ExecutorState(update) => {
            assert_eq!(update.executor.executor_id.as_str(), "executor.local");
            assert_eq!(update.resources.len(), 1);
            assert_eq!(
                update.resources[0].source_workload_id,
                Some(WorkloadId::new("workload.camera"))
            );
        }
        other => panic!("expected executor state, got {other:?}"),
    }
}

#[test]
fn resource_claim_builder_composes_ownership_and_capability() {
    let claim = ResourceClaim::new("camera.device", 1)
        .ownership_mode(ResourceOwnershipMode::ExclusiveOwnerPublishesDerived)
        .requires_capability_of::<CaptureConfigurable>()
        .build();

    assert_eq!(claim.resource_type, ResourceType::new("camera.device"));
    assert_eq!(
        claim.ownership_mode,
        Some(ResourceOwnershipMode::ExclusiveOwnerPublishesDerived)
    );
    assert_eq!(
        claim.required_capabilities,
        vec![CapabilityId::of::<CaptureConfigurable>()]
    );
}
