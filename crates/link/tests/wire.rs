//! The minimal device codec (`wire`, feature `device`) against the recorded wire format.
//!
//! Every body a device sends is encoded from borrowed views and compared byte for byte with
//! `tests/fixtures/link_encodings.txt` (recorded from the postcard + serde records), and every
//! body a device receives is decoded from the fixture into views. Runs without `alloc`.

#![cfg(feature = "device")]

use std::collections::BTreeMap;
use std::path::PathBuf;

use orion_link::frame;
use orion_link::wire::{
    self, ActionResultView, ActionStatus, Availability, CapabilityView, ConfigField, Encode,
    Health, HelloView, LeaseState, LeaseView, Leases, Ownership, ProviderStateView, ProviderView,
    Reader, RejectReason, ResourceStateView, ResourceView, Roles, StatusView, Value, WelcomeView,
    WireError, Writer, kind,
};

/// `name -> complete frame`, in fixture order (the fixture's sequence numbers are
/// `0x0100 + index`).
fn fixture() -> BTreeMap<String, (u16, Vec<u8>)> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/link_encodings.txt");
    let text = std::fs::read_to_string(path).expect("fixture");
    text.lines()
        .filter(|line| !line.starts_with('#') && !line.is_empty())
        .enumerate()
        .map(|(index, line)| {
            let mut parts = line.split(' ');
            let name = parts.next().unwrap().to_owned();
            let len: usize = parts.next().unwrap().parse().unwrap();
            let hex = parts.next().unwrap();
            let bytes: Vec<u8> = (0..hex.len())
                .step_by(2)
                .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
                .collect();
            assert_eq!(bytes.len(), len, "{name}");
            (name, (0x0100 + index as u16, bytes))
        })
        .collect()
}

fn frame_of(name: &str) -> (u16, Vec<u8>) {
    fixture()
        .remove(name)
        .unwrap_or_else(|| panic!("fixture entry {name}"))
}

fn payload_of(name: &str) -> Vec<u8> {
    let (_, bytes) = frame_of(name);
    frame::decode(&bytes).unwrap().payload().to_vec()
}

fn encode<B: Encode + ?Sized>(kind: u8, seq: u16, body: &B) -> Vec<u8> {
    let mut buf = vec![0u8; 1024];
    let len = wire::encode_frame(kind, seq, body, &mut buf).expect("fits");
    buf.truncate(len);
    buf
}

const CONFIG: [ConfigField<'static>; 4] = [
    ConfigField {
        key: "enabled",
        value: Value::Bool(true),
    },
    ConfigField {
        key: "frame",
        value: Value::String("imu_link"),
    },
    ConfigField {
        key: "rate_hz",
        value: Value::UInt(200),
    },
    ConfigField {
        key: "raw",
        value: Value::Bytes(&[0, 1, 255]),
    },
];

const STATE: ResourceStateView<'static> = ResourceStateView {
    observed_at_ms: 1_234,
    action_result: Some(ActionResultView {
        action_kind: "calibrate",
        status: ActionStatus::Applied,
        data: Some(Value::Int(-3)),
        error: None,
    }),
    config: Some(&CONFIG),
};

const CAPABILITIES: [CapabilityView<'static>; 1] = [CapabilityView {
    capability_id: "capture.configurable",
    detail: Some("200hz"),
}];

/// The fixture's provider snapshot, as views (lives in flash on a device).
const PROVIDER: ProviderView<'static> =
    ProviderView::new("provider.imu-board", "node-a").with_resource_types(&["imu.sample_source"]);

const RESOURCE: ResourceView<'static> = ResourceView {
    realized_by_executor_id: Some("executor.engine"),
    ownership_mode: Ownership::SharedLimited { max_consumers: 2 },
    realized_for_workload_id: Some("workload.pose"),
    source_resource_id: Some("imu-board.raw"),
    source_workload_id: Some("workload.raw"),
    lease_state: LeaseState::Leased,
    state: Some(&STATE),
    ..ResourceView::new("imu-board.imu-0", "imu.sample_source", "provider.imu-board")
        .with_health(Health::Healthy)
        .with_availability(Availability::Available)
        .with_capabilities(&CAPABILITIES)
        .with_labels(&["imu"])
        .with_endpoints(&["can://0x105"])
};

#[test]
fn device_bodies_encode_to_the_recorded_frames() {
    let (seq, expected) = frame_of("hello");
    let hello = HelloView {
        device_name: "imu-board",
        roles: Roles::PROVIDER,
        max_frame: 512,
    };
    assert_eq!(encode(kind::HELLO, seq, &hello), expected, "hello");

    let (seq, expected) = frame_of("provider_state");
    let state = ProviderStateView {
        provider: &PROVIDER,
        resources: &[RESOURCE],
    };
    assert_eq!(
        encode(kind::PROVIDER_STATE, seq, &state),
        expected,
        "provider_state"
    );

    let (seq, expected) = frame_of("ping");
    assert_eq!(encode(kind::PING, seq, &1_700_000u64), expected, "ping");
    let (seq, expected) = frame_of("pong");
    assert_eq!(encode(kind::PONG, seq, &1_700_000u64), expected, "pong");

    let (seq, expected) = frame_of("status");
    let status = [
        StatusView::new("temperature_mc", Value::Int(41_250)).with_ttl_ms(5_000),
        StatusView::new("mode", Value::String("streaming")),
        StatusView::new("calibrated", Value::Bool(true)),
    ];
    assert_eq!(encode(kind::STATUS, seq, &status[..]), expected, "status");

    // Host-side bodies the device never sends still have a matching encoding.
    let (seq, expected) = frame_of("ack");
    assert_eq!(encode(kind::ACK, seq, &0xBEEFu16), expected, "ack");
    let (seq, expected) = frame_of("reject.no_session");
    assert_eq!(
        encode(kind::REJECT, seq, &RejectReason::NoSession),
        expected,
        "reject"
    );
}

#[test]
fn host_bodies_decode_from_the_recorded_frames() {
    assert_eq!(
        WelcomeView::decode(&payload_of("welcome")).unwrap(),
        WelcomeView {
            node_id: "node-a",
            session_id: 0x0102_0304,
            heartbeat_ms: 1_000,
            max_frame: 512,
        }
    );
    for (name, reason) in [
        ("reject.version_mismatch", RejectReason::VersionMismatch),
        ("reject.unknown_device", RejectReason::UnknownDevice),
        ("reject.unsupported_roles", RejectReason::UnsupportedRoles),
        ("reject.frame_too_small", RejectReason::FrameTooSmall),
        ("reject.no_session", RejectReason::NoSession),
    ] {
        assert_eq!(wire::decode_reject(&payload_of(name)), Ok(reason), "{name}");
    }
    assert_eq!(wire::decode_ack(&payload_of("ack")), Ok(0xBEEF));
    assert_eq!(wire::decode_u64(&payload_of("ping")), Ok(1_700_000));
    assert_eq!(wire::decode_u64(&payload_of("pong")), Ok(1_700_000));

    let payload = payload_of("leases");
    let leases: Vec<LeaseView<'_>> = Leases::decode(&payload).unwrap().collect();
    assert_eq!(
        leases,
        vec![LeaseView {
            resource_id: "imu-board.imu-0",
            lease_state: LeaseState::Leased,
            holder_node_id: Some("node-a"),
            holder_workload_id: Some("workload.pose"),
        }]
    );
    let payload = payload_of("leases.empty");
    let empty = Leases::decode(&payload).unwrap();
    assert_eq!(empty.len(), 0);
    assert_eq!(empty.count(), 0);
}

#[test]
fn malformed_host_bodies_are_errors_not_panics() {
    for name in ["welcome", "ack", "leases", "pong"] {
        let payload = payload_of(name);
        for cut in 0..payload.len() {
            let body = &payload[..cut];
            let failed = match name {
                "welcome" => WelcomeView::decode(body).is_err(),
                "ack" => wire::decode_ack(body).is_err(),
                "leases" => Leases::decode(body).is_err(),
                _ => wire::decode_u64(body).is_err(),
            };
            assert!(failed, "{name} cut at {cut}");
        }
    }
    assert_eq!(wire::decode_reject(&[]), Err(WireError::Decode));
    assert_eq!(wire::decode_reject(&[0x42]), Ok(RejectReason::Other(0x42)));
    // Trailing bytes are ignored (later versions may append fields).
    let mut payload = payload_of("welcome");
    payload.extend_from_slice(b"future fields");
    assert!(WelcomeView::decode(&payload).is_ok());
    // A lease list claiming u32::MAX entries fails fast.
    assert!(Leases::decode(&[0xFF, 0xFF, 0xFF, 0xFF, 0x0F]).is_err());
    // Invalid UTF-8, a bad lease state, a bad option tag, an overlong varint.
    assert!(WelcomeView::decode(&[0x01, 0xFF, 0, 0, 0]).is_err());
    assert!(Leases::decode(&[0x01, 0x01, b'r', 0x07, 0x00, 0x00]).is_err());
    assert!(Leases::decode(&[0x01, 0x01, b'r', 0x00, 0x02, 0x00]).is_err());
    assert!(wire::decode_u64(&[0xFF; 11]).is_err());
    assert!(
        wire::decode_ack(&[0xFF, 0xFF, 0x04]).is_err(),
        "u16 overflow"
    );
    assert_eq!(
        wire::decode_u64(&[0xFF; 9].iter().chain(&[0x01]).copied().collect::<Vec<_>>()),
        Ok(u64::MAX)
    );
    assert!(
        wire::decode_u64(&[0xFF; 9].iter().chain(&[0x02]).copied().collect::<Vec<_>>()).is_err()
    );
}

#[test]
fn varints_and_zigzag_match_postcard() {
    let mut buf = [0u8; 16];
    for (value, bytes) in [
        (0u64, &[0x00][..]),
        (127, &[0x7F]),
        (128, &[0x80, 0x01]),
        (300, &[0xAC, 0x02]),
        (u64::from(u32::MAX), &[0xFF, 0xFF, 0xFF, 0xFF, 0x0F]),
    ] {
        let mut w = Writer::new(&mut buf);
        w.varint(value);
        let len = w.len();
        assert_eq!(&buf[..len], bytes, "{value}");
        assert_eq!(Reader::new(bytes).varint(), Ok(value));
    }
    for (value, zz) in [(0i64, 0u64), (-1, 1), (1, 2), (-3, 5), (i64::MIN, u64::MAX)] {
        let mut w = Writer::new(&mut buf);
        w.zigzag(value);
        let len = w.len();
        let mut expected = Writer::new(&mut [0u8; 0][..]);
        expected.varint(zz);
        assert_eq!(len, expected.len());
    }
}

#[test]
fn writer_counts_past_the_end_and_encode_frame_reports_the_need() {
    let state = ProviderStateView {
        provider: &PROVIDER,
        resources: &[RESOURCE],
    };
    let needed = frame::frame_len(wire::encoded_len(&state));
    assert_eq!(needed, frame_of("provider_state").1.len());
    for size in [0, 7, 8, 64, needed - 1] {
        let mut buf = vec![0u8; size];
        assert_eq!(
            wire::encode_frame(kind::PROVIDER_STATE, 1, &state, &mut buf),
            Err(WireError::BufferTooSmall { needed }),
            "{size}"
        );
    }
    let mut buf = vec![0u8; needed];
    assert_eq!(
        wire::encode_frame(kind::PROVIDER_STATE, 1, &state, &mut buf),
        Ok(needed)
    );
}
