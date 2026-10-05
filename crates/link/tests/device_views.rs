//! The minimal device path (feature `device`, no `alloc`): a device session publishing borrowed
//! views against a scripted host whose frames are built with the `wire` codec. Nothing here needs
//! an allocator on the device side; `session/` covers the same session against the real
//! `HostSession` over streams and CAN.

#![cfg(feature = "device")]

use orion_link::device::{DeviceConfig, DeviceEvent, LinkState, StreamDevice};
use orion_link::wire::{
    self, Encode, Health, LeaseState, LeaseView, ProviderStateView, ProviderView, RejectReason,
    ResourceView, StatusView, Value, Writer, kind,
};
use orion_link::{FrameView, Stream, StreamDecoder, StreamEncoder, frame};

type Device = StreamDevice<128, 128>;

const PROVIDER: ProviderView<'static> =
    ProviderView::new("provider.dev", "unassigned").with_resource_types(&["imu.sample_source"]);
const RESOURCES: [ResourceView<'static>; 1] =
    [
        ResourceView::new("dev.imu-0", "imu.sample_source", "provider.dev")
            .with_health(Health::Healthy)
            .with_labels(&["imu"]),
    ];

/// `Welcome` as the host encodes it.
struct Welcome<'a> {
    node_id: &'a str,
    session_id: u32,
    heartbeat_ms: u32,
    max_frame: u32,
}

impl Encode for Welcome<'_> {
    fn encode(&self, w: &mut Writer<'_>) {
        self.node_id.encode(w);
        self.session_id.encode(w);
        self.heartbeat_ms.encode(w);
        self.max_frame.encode(w);
    }
}

/// A `LeaseRecord` as the host encodes it.
struct Lease<'a>(&'a str, LeaseState, Option<&'a str>, Option<&'a str>);

impl Encode for Lease<'_> {
    fn encode(&self, w: &mut Writer<'_>) {
        self.0.encode(w);
        self.1.encode(w);
        self.2.encode(w);
        self.3.encode(w);
    }
}

/// Collects the frames a device sends (as `(kind, seq, payload)`) and sends host frames.
struct Host {
    decoder: StreamDecoder<512>,
    seq: u16,
}

impl Host {
    fn new() -> Self {
        Self {
            decoder: StreamDecoder::new(),
            seq: 100,
        }
    }

    fn drain(&mut self, device: &mut Device) -> Vec<(u8, u16, Vec<u8>)> {
        let mut out = Vec::new();
        let mut chunk = [0u8; 7];
        loop {
            let n = device.transmit(&mut chunk);
            if n == 0 {
                break;
            }
            self.feed(&chunk[..n], &mut out);
        }
        out
    }

    fn feed(&mut self, bytes: &[u8], out: &mut Vec<(u8, u16, Vec<u8>)>) {
        for &byte in bytes {
            if let Ok(Some(frame)) = self.decoder.push(byte) {
                out.push(parts(&frame));
            }
        }
    }

    fn send<B: Encode + ?Sized>(&mut self, device: &mut Device, kind: u8, body: &B) {
        self.seq = self.seq.wrapping_add(1);
        let mut frame = [0u8; 256];
        let len = wire::encode_frame(kind, self.seq, body, &mut frame).unwrap();
        let bytes: Vec<u8> = StreamEncoder::for_frame(&frame[..len]).collect();
        device.receive(&bytes);
    }

    fn welcome(&mut self, device: &mut Device, node_id: &str, session_id: u32) {
        let welcome = Welcome {
            node_id,
            session_id,
            heartbeat_ms: 1_000,
            max_frame: 512,
        };
        self.send(device, kind::WELCOME, &welcome);
    }
}

fn parts(frame: &FrameView<'_>) -> (u8, u16, Vec<u8>) {
    (frame.kind(), frame.seq(), frame.payload().to_vec())
}

fn events(device: &mut Device) -> Vec<DeviceEvent> {
    core::iter::from_fn(|| device.next_event()).collect()
}

fn payload_of<B: Encode + ?Sized>(body: &B) -> Vec<u8> {
    let mut buf = [0u8; 256];
    let len = wire::encode_frame(kind::PROVIDER_STATE, 0, body, &mut buf).unwrap();
    frame::decode(&buf[..len]).unwrap().payload().to_vec()
}

fn connected() -> (Device, Host) {
    let mut device = Device::new(DeviceConfig::provider("dev"), Stream);
    let mut host = Host::new();
    device.poll(0);
    let frames = host.drain(&mut device);
    assert_eq!(frames[0].0, kind::HELLO);
    // `Hello { "dev", provider, max_frame: 128 }`.
    assert_eq!(frames[0].2, b"\x03dev\x01\x80\x01");
    host.welcome(&mut device, "node-a", 7);
    assert_eq!(
        events(&mut device),
        vec![DeviceEvent::Connected { session_id: 7 }]
    );
    (device, host)
}

#[test]
fn views_are_published_acked_and_leases_reported() {
    let (mut device, mut host) = connected();
    assert_eq!(device.node_id(), Some("node-a"));
    assert_eq!(device.max_frame(), 128);
    device
        .publish_provider_state(&PROVIDER, &RESOURCES)
        .unwrap();
    device.poll(5);
    let frames = host.drain(&mut device);
    let (kind, seq, payload) = &frames[0];
    assert_eq!(*kind, kind::PROVIDER_STATE);
    assert_eq!(
        *payload,
        payload_of(&ProviderStateView {
            provider: &PROVIDER,
            resources: &RESOURCES
        })
    );
    host.send(&mut device, kind::ACK, seq);
    assert_eq!(events(&mut device), vec![DeviceEvent::StateAcked]);
    assert!(!device.state_pending());

    let leases = [
        Lease(
            "dev.imu-0",
            LeaseState::Leased,
            Some("node-a"),
            Some("workload.pose"),
        ),
        Lease("dev.imu-1", LeaseState::Unleased, None, None),
    ];
    host.send(&mut device, kind::LEASES, &leases[..]);
    host.send(&mut device, kind::LEASES, &leases[..]);
    assert_eq!(events(&mut device), vec![DeviceEvent::LeasesChanged]);
    let held: Vec<LeaseView<'_>> = device.leases().collect();
    assert_eq!(held.len(), 2);
    assert_eq!(held[0].resource_id, "dev.imu-0");
    assert_eq!(held[0].holder_workload_id, Some("workload.pose"));
    assert_eq!(held[1].lease_state, LeaseState::Unleased);

    // A host ping is answered with a pong echoing its time, byte for byte.
    host.send(&mut device, kind::PING, &u64::MAX);
    device.poll(10);
    let frames = host.drain(&mut device);
    assert_eq!(frames[0].0, kind::PONG);
    assert_eq!(wire::decode_u64(&frames[0].2), Ok(u64::MAX));

    // Status views go out once, fire-and-forget.
    let status = [StatusView::new("temperature_mc", Value::Int(41_250)).with_ttl_ms(5_000)];
    device.publish_status(&status).unwrap();
    device.poll(20);
    let frames = host.drain(&mut device);
    assert_eq!(frames[0].0, kind::STATUS);
    assert_eq!(frames[0].2, payload_of(&status[..]));
    device.poll(500);
    assert!(host.drain(&mut device).is_empty());

    // Leaving the session forgets the lease set.
    host.send(&mut device, kind::REJECT, &RejectReason::NoSession);
    assert_eq!(events(&mut device), vec![DeviceEvent::Disconnected]);
    assert_eq!(device.leases().len(), 0);
    assert_eq!(device.node_id(), None);
}

#[test]
fn leases_arriving_while_hello_is_in_flight_wait_for_the_next_resend() {
    let mut device = Device::new(DeviceConfig::provider("a-rather-long-device-name"), Stream);
    let mut host = Host::new();
    device.poll(0);
    // Send only part of the Hello, then the host answers (an earlier Hello got through).
    let mut partial = [0u8; 4];
    assert_eq!(device.transmit(&mut partial), 4);
    host.welcome(&mut device, "node-a", 1);
    let leases = [Lease("r", LeaseState::Leased, None, None)];
    host.send(&mut device, kind::LEASES, &leases[..]);
    assert_eq!(
        events(&mut device),
        vec![DeviceEvent::Connected { session_id: 1 }]
    );
    assert_eq!(
        device.leases().len(),
        0,
        "the Hello still occupies the lease buffer"
    );
    // The rest of the Hello goes out intact.
    let mut rest = Vec::new();
    host.feed(&partial, &mut rest);
    let tail = host.drain(&mut device);
    assert_eq!(tail.len(), 1);
    assert_eq!(tail[0].0, kind::HELLO);
    // The host resends leases after every pong; that copy is taken.
    host.send(&mut device, kind::LEASES, &leases[..]);
    assert_eq!(events(&mut device), vec![DeviceEvent::LeasesChanged]);
    assert_eq!(device.leases().len(), 1);
}

#[test]
fn hello_backs_off_and_rejects_are_reported_without_alloc() {
    let mut device = Device::new(DeviceConfig::provider("dev"), Stream);
    let mut host = Host::new();
    let mut hellos = Vec::new();
    for now in (0..=4_000u64).step_by(10) {
        device.poll(now);
        if host.drain(&mut device).iter().any(|f| f.0 == kind::HELLO) {
            hellos.push(now);
        }
    }
    assert_eq!(hellos, vec![0, 250, 750, 1_750, 3_750]);
    host.send(&mut device, kind::REJECT, &RejectReason::UnknownDevice);
    assert_eq!(
        events(&mut device),
        vec![DeviceEvent::Rejected(RejectReason::UnknownDevice)]
    );
    assert_eq!(device.link_state(), LinkState::BackingOff);
}

#[test]
fn clock_far_from_zero_and_wrapping_works() {
    // The session keeps a wrapping 32-bit clock internally; a port's 64-bit uptime may start
    // anywhere.
    for start in [0u64, 1 << 31, u64::from(u32::MAX) - 1_500, 1 << 40] {
        let mut device = Device::new(DeviceConfig::provider("dev"), Stream);
        let mut host = Host::new();
        device.poll(start);
        assert_eq!(host.drain(&mut device)[0].0, kind::HELLO, "start {start}");
        host.welcome(&mut device, "node-a", 1);
        device
            .publish_provider_state(&PROVIDER, &RESOURCES)
            .unwrap();
        device.poll(start + 1);
        assert_eq!(host.drain(&mut device)[0].0, kind::PROVIDER_STATE);
        // Unacknowledged: retransmitted after 200 ms, then pinged at the heartbeat.
        device.poll(start + 202);
        assert_eq!(host.drain(&mut device)[0].0, kind::PROVIDER_STATE);
        device.poll(start + 1_001);
        let kinds: Vec<u8> = host.drain(&mut device).iter().map(|f| f.0).collect();
        assert!(kinds.contains(&kind::PING), "start {start}: {kinds:?}");
        // Host silent for three heartbeats since the Welcome (first seen at `start + 1`): lost.
        device.poll(start + 3_000);
        assert!(device.is_connected());
        device.poll(start + 3_001);
        assert!(!device.is_connected(), "start {start}");
    }
}

#[test]
fn node_ids_longer_than_the_capacity_are_not_reported() {
    let mut device = Device::new(DeviceConfig::provider("dev"), Stream);
    let mut host = Host::new();
    device.poll(0);
    let long = "n".repeat(orion_link::device::NODE_ID_CAPACITY + 1);
    host.welcome(&mut device, &long, 3);
    assert!(device.is_connected());
    assert_eq!(device.node_id(), None);
    assert_eq!(device.session_id(), Some(3));
}

#[test]
fn oversized_publishes_keep_the_previous_snapshot() {
    let (mut device, _host) = connected();
    device
        .publish_provider_state(&PROVIDER, &RESOURCES)
        .unwrap();
    let many = [RESOURCES[0]; 8];
    assert!(device.publish_provider_state(&PROVIDER, &many).is_err());
    assert!(device.state_pending());
    let huge = [StatusView::new("blob", Value::Bytes(&[0; 200]))];
    assert!(device.publish_status(&huge).is_err());
    assert!(!device.status_pending());
}

#[test]
fn can_device_writes_segments_into_caller_buffers() {
    use orion_link::device::CanDevice;
    use orion_link::{Packet, Reassembler};

    let mut device = CanDevice::<128, 128>::new(DeviceConfig::provider("dev"), Packet::CLASSIC);
    device.poll(0);
    let mut rx = Reassembler::<128>::new();
    let mut hello = None;
    let mut segments = 0;
    let mut data = [0u8; 8];
    loop {
        let n = device.transmit_segment(&mut data);
        if n == 0 {
            break;
        }
        segments += 1;
        if let Some(frame) = rx.push(&data[..n]).unwrap() {
            hello = Some(parts(&frame));
        }
    }
    assert!(segments > 1);
    let (kind, _, payload) = hello.expect("a complete Hello");
    assert_eq!(kind, kind::HELLO);
    assert_eq!(payload, b"\x03dev\x01\x80\x01");
    // A buffer shorter than a segment is refused without losing the segment.
    device.poll(250);
    assert_eq!(device.transmit_segment(&mut [0u8; 2]), 0);
    assert!(device.transmit_segment(&mut data) > 0);
}
