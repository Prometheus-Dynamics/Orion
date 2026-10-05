//! Canonical wire encodings of representative control-protocol messages.
//!
//! Every message below is encoded with the rkyv archive helpers and compared byte-for-byte with
//! `tests/fixtures/canonical_encodings.txt`, then decoded back and compared with the original.
//!
//! The test only uses APIs that exist without the `std` feature, so it runs in two builds:
//!
//! - `cargo test -p orion-auth --test canonical_encoding` (std, part of `cargo test --workspace`)
//! - `cargo test -p orion-auth --no-default-features --test canonical_encoding` (orion-core,
//!   orion-control-plane, and orion-auth compiled `no_std` + `alloc` on the host; run by
//!   `scripts/check-no-std.sh`)
//!
//! Both builds must match the same fixture, which proves std and no_std builds produce
//! byte-identical archives. Regenerate the fixture only for an intentional wire change (which also
//! requires a `CONTROL_PROTOCOL_VERSION` bump) with
//! `ORION_UPDATE_CANONICAL_ENCODINGS=1 cargo test -p orion-auth --test canonical_encoding`.

use orion_auth::{
    AuthenticatedPeerRequest, NodeTransportBinding, PEER_REQUEST_AUTH_VERSION, PeerRequestAuth,
    PeerRequestPayload, TRANSPORT_BINDING_VERSION, canonical_peer_request_bytes,
    canonical_transport_binding_bytes,
};
use orion_control_plane::{
    AppliedClusterState, ArtifactRecord, ClientHello, ClientRole, ClockSourceKind,
    ClusterStateEnvelope, ControlMessage, DesiredClusterState, DesiredState, DesiredStateMutation,
    DesiredStateSectionFingerprints, ENROLLMENT_PROTOCOL_VERSION, EnrollmentHello, ExecutorRecord,
    HealthState, LeaseRecord, LeaseState, MutationBatch, NodeClockFacts, NodeRecord,
    ObservedClusterState, ObservedStateUpdate, PeerHello, ProviderRecord, ResourceOwnershipMode,
    ResourceRecord, RestartPolicy, StateSnapshot, StatusEntry, StatusQuery, StatusSubject,
    TypedConfigValue, WorkloadConfig, WorkloadObservedState, WorkloadRecord,
};
use orion_core::{
    ArtifactId, CapabilityId, ClientName, ConfigSchemaId, ExecutorId, HlcTimestamp, NodeId,
    PeerBaseUrl, ProviderId, ResourceId, ResourceType, Revision, RuntimeType, WorkloadId,
    decode_from_slice, decode_length_prefixed, encode_length_prefixed, encode_to_vec, hlc_node_tag,
};
use std::{fmt::Write as _, path::PathBuf};

const FIXTURE: &str = "tests/fixtures/canonical_encodings.txt";

fn desired_state() -> DesiredClusterState {
    let mut desired = DesiredClusterState::default();
    desired.put_node(
        NodeRecord::builder(NodeId::new("node-a"))
            .health(HealthState::Healthy)
            .label("mcu")
            .build(),
    );
    desired.put_artifact(
        ArtifactRecord::builder(ArtifactId::new("artifact.pose.v1"))
            .content_type("application/orion-workload")
            .size_bytes(4096)
            .build(),
    );
    desired.put_workload(
        WorkloadRecord::builder(
            WorkloadId::new("workload.pose"),
            RuntimeType::new("graph.exec.v1"),
            ArtifactId::new("artifact.pose.v1"),
        )
        .desired_state(DesiredState::Running)
        .observed_state(WorkloadObservedState::Assigned)
        .assigned_to(NodeId::new("node-a"))
        .config(
            WorkloadConfig::new(ConfigSchemaId::new("pose.config.v1"))
                .field("rate_hz", TypedConfigValue::UInt(200))
                .field("offset", TypedConfigValue::Int(-3))
                .field("enabled", TypedConfigValue::Bool(true))
                .field("frame", TypedConfigValue::String("imu_link".into()))
                .field("calibration", TypedConfigValue::Bytes(vec![1, 2, 3, 255])),
        )
        .require_resource_with_ownership(
            ResourceType::new("imu.sample_source"),
            1,
            ResourceOwnershipMode::Exclusive,
        )
        .bind_resource(ResourceId::new("node-a.imu-01"), NodeId::new("node-a"))
        .restart_policy(RestartPolicy::OnFailure)
        .build(),
    );
    desired.put_resource(
        ResourceRecord::builder(
            ResourceId::new("node-a.imu-01"),
            ResourceType::new("imu.sample_source"),
            ProviderId::new("provider.peripherals"),
        )
        .supports_capability(CapabilityId::new("capture.configurable"))
        .health(HealthState::Healthy)
        .label("imu")
        .endpoint("shm://orion-imu-01")
        .endpoint("styx-frame-lease+unix:///run/helios/cam0.sock")
        .build(),
    );
    desired.put_provider(
        ProviderRecord::builder(
            ProviderId::new("provider.peripherals"),
            NodeId::new("node-a"),
        )
        .resource_type(ResourceType::new("imu.sample_source"))
        .build(),
    );
    desired.put_executor(
        ExecutorRecord::builder(ExecutorId::new("executor.engine"), NodeId::new("node-a"))
            .runtime_type(RuntimeType::new("graph.exec.v1"))
            .build(),
    );
    desired.put_lease(
        LeaseRecord::builder(ResourceId::new("node-a.imu-01"))
            .lease_state(LeaseState::Leased)
            .holder_node(NodeId::new("node-a"))
            .holder_workload(WorkloadId::new("workload.pose"))
            .build(),
    );
    // Per-object versions: a stamped node record and a tombstone (control protocol v3).
    let node = desired.nodes[&NodeId::new("node-a")].clone();
    desired.apply_stamped(
        DesiredStateMutation::PutNode(node),
        HlcTimestamp::new(1_791_000_000_000, 3, hlc_node_tag("node-a")),
    );
    desired.apply_stamped(
        DesiredStateMutation::RemoveArtifact(ArtifactId::new("artifact.retired")),
        HlcTimestamp::new(1_791_000_000_001, 0, hlc_node_tag("node-b")),
    );
    desired
}

fn observed_update() -> ObservedStateUpdate {
    let mut observed = ObservedClusterState::default();
    observed.set_revision(Revision::new(7));
    observed.put_node(
        NodeRecord::builder(NodeId::new("node-a"))
            .health(HealthState::Degraded)
            .clock(
                NodeClockFacts::unknown(1_759_500_000_000)
                    .with_source(ClockSourceKind::Ptp)
                    .with_synchronized(true)
                    .with_offset_ns(-1_500)
                    .with_max_error_ns(16_000_000)
                    .with_estimated_error_ns(2_000)
                    .with_ptp_grandmaster_id("00:1b:19:ff:fe:00:00:01")
                    .with_timebase("TAI"),
            )
            .build(),
    );
    let mut applied = AppliedClusterState::default();
    applied.mark_applied(Revision::new(7));
    ObservedStateUpdate { observed, applied }
}

fn hello() -> ControlMessage {
    ControlMessage::Hello(PeerHello {
        node_id: NodeId::new("node-a"),
        desired_revision: Revision::new(8),
        desired_fingerprint: 0x0123_4567_89ab_cdef,
        desired_section_fingerprints: DesiredStateSectionFingerprints {
            nodes: 1,
            artifacts: 2,
            workloads: 3,
            resources: 4,
            providers: 5,
            executors: 6,
            leases: 7,
        },
        observed_revision: Revision::new(7),
        applied_revision: Revision::new(7),
        transport_binding_version: Some(TRANSPORT_BINDING_VERSION),
        transport_binding_public_key: Some(vec![0x11; 32]),
        transport_tls_cert_pem: Some(b"-----BEGIN CERTIFICATE-----\n...".to_vec()),
        transport_binding_signature: Some(vec![0x22; 64]),
    })
}

fn control_messages() -> Vec<(&'static str, ControlMessage)> {
    let desired = desired_state();
    let snapshot = ControlMessage::Snapshot(StateSnapshot {
        state: ClusterStateEnvelope::new(
            desired.clone(),
            observed_update().observed,
            observed_update().applied,
        ),
    });
    vec![
        ("control.hello", hello()),
        ("control.snapshot", snapshot),
        (
            "control.mutations",
            ControlMessage::Mutations(MutationBatch::full_state_replay(Revision::ZERO, &desired)),
        ),
        (
            "control.client_hello",
            ControlMessage::ClientHello(ClientHello {
                client_name: ClientName::new("mcu.sensor-hub"),
                role: ClientRole::Provider,
            }),
        ),
        ("control.ping", ControlMessage::Ping),
        (
            "control.rejected",
            ControlMessage::Rejected("revision mismatch".into()),
        ),
        (
            "control.publish_status",
            ControlMessage::PublishStatus(vec![
                StatusEntry::new(
                    StatusSubject::Provider(ProviderId::new("provider.camera")),
                    "fps",
                    TypedConfigValue::UInt(30),
                )
                .with_ttl_ms(5_000),
                StatusEntry::new(
                    StatusSubject::Resource(ResourceId::new("resource.camera.front")),
                    "exposure",
                    TypedConfigValue::String("auto".into()),
                ),
            ]),
        ),
        (
            "control.query_status",
            ControlMessage::QueryStatus(
                StatusQuery::subject(StatusSubject::Workload(WorkloadId::new("workload.pose")))
                    .with_key_prefix("latency."),
            ),
        ),
        (
            "control.enrollment_hello",
            ControlMessage::EnrollmentHello(Box::new(EnrollmentHello {
                version: ENROLLMENT_PROTOCOL_VERSION,
                cluster: "lab".into(),
                initiator: NodeId::new("node-a"),
                initiator_public_key: vec![0x11; 32],
                initiator_nonce: vec![0x22; 32],
                initiator_url: Some(PeerBaseUrl::new("orion+tcp://10.0.0.1:9200")),
                responder: NodeId::new("node-b"),
            })),
        ),
    ]
}

fn peer_request() -> AuthenticatedPeerRequest {
    AuthenticatedPeerRequest {
        auth: PeerRequestAuth {
            version: PEER_REQUEST_AUTH_VERSION,
            node_id: NodeId::new("node-a"),
            public_key: vec![0x33; 32],
            nonce: 42,
            signature: vec![0x44; 64],
        },
        payload: PeerRequestPayload::ObservedUpdate(observed_update()),
    }
}

fn transport_binding() -> NodeTransportBinding {
    NodeTransportBinding {
        version: TRANSPORT_BINDING_VERSION,
        node_id: NodeId::new("node-a"),
        public_key: vec![0x55; 32],
        tls_cert_pem: b"-----BEGIN CERTIFICATE-----\n...".to_vec(),
        signature: vec![0x66; 64],
    }
}

/// Encodes every canonical value, checks it decodes back to the original, and returns
/// `(name, bytes)` pairs in fixture order.
fn canonical_encodings() -> Vec<(&'static str, Vec<u8>)> {
    let mut out = Vec::new();

    for (name, message) in control_messages() {
        let bytes = encode_to_vec(&message).expect("control message should encode");
        let decoded: ControlMessage =
            decode_from_slice(&bytes).expect("control message should decode");
        assert_eq!(decoded, message, "{name} should round-trip");
        out.push((name, bytes));
    }

    let framed = encode_length_prefixed(&hello(), |err| err, || "frame too large".to_owned())
        .expect("length-prefixed frame should encode");
    let (decoded, rest): (ControlMessage, _) = decode_length_prefixed(
        &framed,
        |err| err,
        || "incomplete header".to_owned(),
        || "incomplete payload".to_owned(),
        || "frame too large".to_owned(),
    )
    .expect("length-prefixed frame should decode");
    assert_eq!(decoded, hello());
    assert!(rest.is_empty());
    out.push(("frame.length_prefixed_hello", framed));

    let request = peer_request();
    let bytes = encode_to_vec(&request).expect("peer request should encode");
    let decoded: AuthenticatedPeerRequest =
        decode_from_slice(&bytes).expect("peer request should decode");
    assert_eq!(decoded, request);
    out.push(("auth.peer_request", bytes));

    let binding = transport_binding();
    let bytes = encode_to_vec(&binding).expect("transport binding should encode");
    let decoded: NodeTransportBinding =
        decode_from_slice(&bytes).expect("transport binding should decode");
    assert_eq!(decoded, binding);
    out.push(("auth.transport_binding", bytes));

    out.push((
        "auth.canonical_peer_request_bytes",
        canonical_peer_request_bytes(
            PEER_REQUEST_AUTH_VERSION,
            &request.auth.node_id,
            &request.auth.public_key,
            request.auth.nonce,
            &request.payload,
        )
        .expect("canonical peer request bytes should encode"),
    ));
    out.push((
        "auth.canonical_transport_binding_bytes",
        canonical_transport_binding_bytes(
            TRANSPORT_BINDING_VERSION,
            &binding.node_id,
            &binding.public_key,
            &binding.tls_cert_pem,
        )
        .expect("canonical transport binding bytes should encode"),
    ));

    out
}

fn render(encodings: &[(&str, Vec<u8>)]) -> String {
    let mut text = String::from(
        "# Canonical rkyv encodings checked by crates/auth/tests/canonical_encoding.rs.\n\
         # Format: <name> <byte length> <lowercase hex>. Must be identical for std and no_std builds.\n",
    );
    for (name, bytes) in encodings {
        write!(text, "{name} {} ", bytes.len()).expect("writing to a String cannot fail");
        for byte in bytes {
            write!(text, "{byte:02x}").expect("writing to a String cannot fail");
        }
        text.push('\n');
    }
    text
}

#[test]
fn canonical_messages_encode_to_the_recorded_bytes_and_decode_back() {
    let rendered = render(&canonical_encodings());
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(FIXTURE);

    if std::env::var_os("ORION_UPDATE_CANONICAL_ENCODINGS").is_some() {
        std::fs::write(&path, &rendered).expect("fixture should be writable");
        return;
    }

    let expected = std::fs::read_to_string(&path).expect("fixture should be readable");
    if rendered != expected {
        let first_diff = rendered
            .lines()
            .zip(expected.lines())
            .find(|(actual, recorded)| actual != recorded)
            .map(|(actual, _)| actual.split(' ').next().unwrap_or_default().to_owned())
            .unwrap_or_else(|| "<line count>".to_owned());
        panic!(
            "canonical encoding drifted (first differing entry: {first_diff}). The archived wire \
             bytes must be identical across std/no_std builds and releases; an intentional \
             layout change needs a CONTROL_PROTOCOL_VERSION bump and \
             ORION_UPDATE_CANONICAL_ENCODINGS=1 to refresh {FIXTURE}."
        );
    }
}
