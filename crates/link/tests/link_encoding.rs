//! Link message encodings: round trips, robustness, and the wire-format fingerprint.
//!
//! `canonical_messages_encode_to_the_recorded_frames` encodes representative messages as complete
//! frames and compares them byte-for-byte with `tests/fixtures/link_encodings.txt`. Any change to
//! the link wire format (frame layout, kind numbers, body field order or types, postcard itself)
//! makes it fail: bump `LINK_PROTOCOL_VERSION` and regenerate the fixture with
//! `ORION_UPDATE_LINK_ENCODINGS=1 cargo test -p orion-link --features alloc --test link_encoding`.
//! It runs in both the `std` and the `no_std` + `alloc` build of the crate.

#![cfg(feature = "alloc")]

use std::fmt::Write as _;
use std::path::PathBuf;

use orion_control_plane::{
    AvailabilityState, HealthState, LeaseState, ResourceActionResult, ResourceActionStatus,
    ResourceCapability, ResourceConfigState, ResourceOwnershipMode, ResourceState,
    TypedConfigValue,
};
use orion_core::{ExecutorId, NodeId, ProviderId, ResourceId, WorkloadId};
use orion_link::LINK_PROTOCOL_VERSION;
use orion_link::message::{
    Hello, LeaseRecord, Message, MessageError, ProviderRecord, ProviderState, RejectReason,
    ResourceRecord, Roles, Welcome, encode_leases, encode_provider_state, kind,
};

const FIXTURE: &str = "tests/fixtures/link_encodings.txt";

fn provider() -> ProviderRecord {
    ProviderRecord::builder(ProviderId::new("provider.imu-board"), NodeId::new("node-a"))
        .resource_type("imu.sample_source")
        .build()
}

fn resource() -> ResourceRecord {
    ResourceRecord::builder(
        ResourceId::new("imu-board.imu-0"),
        "imu.sample_source",
        ProviderId::new("provider.imu-board"),
    )
    .realized_by_executor(ExecutorId::new("executor.engine"))
    .ownership_mode(ResourceOwnershipMode::SharedLimited { max_consumers: 2 })
    .realized_for_workload(WorkloadId::new("workload.pose"))
    .source_resource(ResourceId::new("imu-board.raw"))
    .source_workload(WorkloadId::new("workload.raw"))
    .health(HealthState::Healthy)
    .availability(AvailabilityState::Available)
    .lease_state(LeaseState::Leased)
    .capability(ResourceCapability::new("capture.configurable").with_detail("200hz"))
    .label("imu")
    .endpoint("can://0x105")
    .state(
        ResourceState::new(1_234)
            .with_action_result(ResourceActionResult {
                action_kind: "calibrate".into(),
                status: ResourceActionStatus::Applied,
                data: Some(TypedConfigValue::Int(-3)),
                error: None,
            })
            .with_config(
                ResourceConfigState::new()
                    .field("rate_hz", TypedConfigValue::UInt(200))
                    .field("frame", TypedConfigValue::String("imu_link".into()))
                    .field("raw", TypedConfigValue::Bytes(vec![0, 1, 255]))
                    .field("enabled", TypedConfigValue::Bool(true)),
            ),
    )
    .build()
}

fn lease() -> LeaseRecord {
    LeaseRecord::builder(ResourceId::new("imu-board.imu-0"))
        .lease_state(LeaseState::Leased)
        .holder_node(NodeId::new("node-a"))
        .holder_workload(WorkloadId::new("workload.pose"))
        .build()
}

fn canonical_messages() -> Vec<(&'static str, Message)> {
    vec![
        (
            "hello",
            Message::Hello(Hello {
                device_name: "imu-board".into(),
                roles: Roles::PROVIDER,
                max_frame: 512,
            }),
        ),
        (
            "welcome",
            Message::Welcome(Welcome {
                node_id: NodeId::new("node-a"),
                session_id: 0x0102_0304,
                heartbeat_ms: 1_000,
                max_frame: 512,
            }),
        ),
        (
            "reject.version_mismatch",
            Message::Reject(RejectReason::VersionMismatch),
        ),
        (
            "reject.unknown_device",
            Message::Reject(RejectReason::UnknownDevice),
        ),
        (
            "reject.unsupported_roles",
            Message::Reject(RejectReason::UnsupportedRoles),
        ),
        (
            "reject.frame_too_small",
            Message::Reject(RejectReason::FrameTooSmall),
        ),
        (
            "reject.no_session",
            Message::Reject(RejectReason::NoSession),
        ),
        (
            "provider_state",
            Message::ProviderState(ProviderState {
                provider: provider(),
                resources: vec![resource()],
            }),
        ),
        ("ack", Message::Ack { seq: 0xBEEF }),
        ("leases", Message::Leases(vec![lease()])),
        ("leases.empty", Message::Leases(Vec::new())),
        ("ping", Message::Ping { now_ms: 1_700_000 }),
        ("pong", Message::Pong { now_ms: 1_700_000 }),
    ]
}

fn encode(message: &Message, seq: u16) -> Vec<u8> {
    let mut buf = vec![0u8; 2048];
    let len = message.encode(seq, &mut buf).expect("message fits");
    buf.truncate(len);
    buf
}

#[test]
fn canonical_messages_encode_to_the_recorded_frames() {
    let mut text = String::from(
        "# Canonical link frames checked by crates/link/tests/link_encoding.rs.\n\
         # Format: <name> <frame length> <lowercase hex of the complete frame>.\n",
    );
    for (index, (name, message)) in canonical_messages().into_iter().enumerate() {
        let frame = encode(&message, 0x0100 + index as u16);
        let (header, decoded) = Message::decode_frame(&frame).expect("decodes");
        assert_eq!(decoded, message, "{name} round-trips");
        assert_eq!(header.kind, message.kind());
        assert_eq!(header.version, LINK_PROTOCOL_VERSION);
        write!(text, "{name} {} ", frame.len()).unwrap();
        for byte in &frame {
            write!(text, "{byte:02x}").unwrap();
        }
        text.push('\n');
    }

    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(FIXTURE);
    if std::env::var_os("ORION_UPDATE_LINK_ENCODINGS").is_some() {
        std::fs::write(&path, &text).expect("fixture should be writable");
        return;
    }
    let expected = std::fs::read_to_string(&path).expect("fixture should be readable");
    if text != expected {
        let first_diff = text
            .lines()
            .zip(expected.lines())
            .find(|(actual, recorded)| actual != recorded)
            .map(|(actual, _)| actual.split(' ').next().unwrap_or_default().to_owned())
            .unwrap_or_else(|| "<line count>".to_owned());
        panic!(
            "link wire encoding drifted (first differing entry: {first_diff}). Devices in the \
             field depend on these bytes: bump LINK_PROTOCOL_VERSION and update the fixture \
             with ORION_UPDATE_LINK_ENCODINGS=1 (see {FIXTURE})."
        );
    }
}

#[test]
fn kind_numbers_are_stable() {
    assert_eq!(
        [
            kind::HELLO,
            kind::WELCOME,
            kind::REJECT,
            kind::PING,
            kind::PONG,
            kind::ACK,
            kind::PROVIDER_STATE,
            kind::LEASES,
            kind::EXECUTOR_STATE,
            kind::WORKLOADS,
            kind::STATUS,
        ],
        [
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x10, 0x11, 0x12, 0x13, 0x14
        ]
    );
}

#[test]
fn borrowed_encoders_match_owned_messages() {
    let mut buf = vec![0u8; 2048];
    let len = encode_provider_state(&provider(), &[resource()], 9, &mut buf).unwrap();
    let owned = Message::ProviderState(ProviderState {
        provider: provider(),
        resources: vec![resource()],
    });
    assert_eq!(&buf[..len], encode(&owned, 9).as_slice());
    let len = encode_leases(&[lease()], 9, &mut buf).unwrap();
    assert_eq!(
        &buf[..len],
        encode(&Message::Leases(vec![lease()]), 9).as_slice()
    );
}

#[test]
fn unknown_kinds_and_reject_codes_decode_without_error() {
    for kind in [
        0x00,
        0x7E,
        0xFF,
        kind::EXECUTOR_STATE,
        kind::WORKLOADS,
        kind::STATUS,
    ] {
        assert_eq!(
            Message::decode_payload(kind, b"anything").unwrap(),
            Message::Unknown(kind)
        );
    }
    assert_eq!(
        Message::decode_payload(kind::REJECT, &[0x99]).unwrap(),
        Message::Reject(RejectReason::Other(0x99))
    );
    assert_eq!(RejectReason::from_code(0x99).code(), 0x99);
    assert!(matches!(
        Message::Unknown(0x7E).encode(1, &mut [0u8; 64]),
        Err(MessageError::Encode)
    ));
}

#[test]
fn small_buffers_and_garbage_bodies_are_errors_not_panics() {
    let message = Message::Leases(vec![lease(); 4]);
    for size in 0..64 {
        let mut buf = vec![0u8; size];
        assert!(message.encode(1, &mut buf).is_err(), "size {size}");
    }
    for kind in [
        kind::HELLO,
        kind::WELCOME,
        kind::PROVIDER_STATE,
        kind::ACK,
        kind::LEASES,
        kind::PING,
    ] {
        assert!(matches!(
            Message::decode_payload(kind, &[]),
            Err(MessageError::Decode { .. })
        ));
        // Truncations of a valid body never panic.
        let frame = encode(&canonical_for(kind), 1);
        let payload = &frame[4..frame.len() - 4];
        for cut in 0..payload.len() {
            let _ = Message::decode_payload(kind, &payload[..cut]);
        }
    }
}

fn canonical_for(kind: u8) -> Message {
    canonical_messages()
        .into_iter()
        .map(|(_, m)| m)
        .find(|m| m.kind() == kind)
        .expect("canonical message for kind")
}

#[test]
fn huge_claimed_sequence_lengths_do_not_preallocate() {
    // A lease list claiming u32::MAX elements, followed by nothing: must fail fast without
    // allocating gigabytes.
    let payload = [0xFF, 0xFF, 0xFF, 0xFF, 0x0F];
    assert!(Message::decode_payload(kind::LEASES, &payload).is_err());
    // Provider state with a valid provider and a huge resource count.
    let frame = encode(
        &Message::ProviderState(ProviderState {
            provider: provider(),
            resources: Vec::new(),
        }),
        1,
    );
    let mut payload = frame[4..frame.len() - 4].to_vec();
    payload.pop(); // the empty-vec length byte
    payload.extend_from_slice(&[0xFF, 0xFF, 0xFF, 0xFF, 0x0F]);
    assert!(Message::decode_payload(kind::PROVIDER_STATE, &payload).is_err());
}

#[test]
fn trailing_payload_bytes_are_ignored() {
    let frame = encode(&Message::Ack { seq: 7 }, 1);
    let mut payload = frame[4..frame.len() - 4].to_vec();
    payload.extend_from_slice(b"future fields");
    assert_eq!(
        Message::decode_payload(kind::ACK, &payload).unwrap(),
        Message::Ack { seq: 7 }
    );
}
