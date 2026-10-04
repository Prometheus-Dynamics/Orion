//! The device session against a scripted host (runs without `std`: only `alloc` APIs are used).

use orion_core::{NodeId, ProviderId, ResourceId};
use orion_link::device::{
    CanDevice, DeviceConfig, DeviceEvent, LinkState, PublishError, StreamDevice,
};
use orion_link::message::{
    Hello, LeaseRecord, Message, ProviderRecord, RejectReason, ResourceRecord, Roles, Welcome, kind,
};
use orion_link::{FrameHeader, Packet, Reassembler, Stream, StreamDecoder, StreamEncoder};

type Device = StreamDevice<256, 256>;

/// Collects the frames a device sends, decoded.
struct Host {
    decoder: StreamDecoder<1024>,
    seq: u16,
}

impl Host {
    fn new() -> Self {
        Self {
            decoder: StreamDecoder::new(),
            seq: 100,
        }
    }

    fn drain(&mut self, device: &mut Device) -> Vec<(FrameHeader, Message)> {
        let mut bytes = Vec::new();
        let mut chunk = [0u8; 16];
        loop {
            let n = device.transmit(&mut chunk);
            if n == 0 {
                break;
            }
            bytes.extend_from_slice(&chunk[..n]);
        }
        let mut out = Vec::new();
        let mut rest = &bytes[..];
        while !rest.is_empty() {
            let (used, result) = self.decoder.push_slice(rest);
            if let Ok(Some(frame)) = result {
                out.push((frame.header(), Message::decode(&frame).unwrap()));
            }
            rest = &rest[used..];
        }
        out
    }

    fn send(&mut self, device: &mut Device, message: &Message) -> u16 {
        self.seq = self.seq.wrapping_add(1);
        self.send_with_seq(device, message, self.seq);
        self.seq
    }

    fn send_with_seq(&mut self, device: &mut Device, message: &Message, seq: u16) {
        let mut frame = vec![0u8; 1024];
        let len = message.encode(seq, &mut frame).unwrap();
        let wire: Vec<u8> = StreamEncoder::for_frame(&frame[..len]).collect();
        device.receive(&wire);
    }
}

fn welcome(session_id: u32) -> Message {
    Message::Welcome(Welcome {
        node_id: NodeId::new("node-a"),
        session_id,
        heartbeat_ms: 1_000,
        max_frame: 4_096,
    })
}

fn provider() -> ProviderRecord {
    ProviderRecord::builder(ProviderId::new("provider.dev"), NodeId::new("node-a")).build()
}

fn resource(n: u32) -> ResourceRecord {
    ResourceRecord::builder(
        ResourceId::new(format!("dev.r{n}")),
        "imu.sample_source",
        ProviderId::new("provider.dev"),
    )
    .build()
}

fn events(device: &mut Device) -> Vec<DeviceEvent> {
    core::iter::from_fn(|| device.next_event()).collect()
}

fn kinds(frames: &[(FrameHeader, Message)]) -> Vec<u8> {
    frames.iter().map(|(h, _)| h.kind).collect()
}

/// Polls every `step` ms until `until` and returns the times at which a Hello was sent.
fn hello_times(device: &mut Device, host: &mut Host, from: u64, until: u64) -> Vec<u64> {
    let mut times = Vec::new();
    let mut now = from;
    while now <= until {
        device.poll(now);
        if kinds(&host.drain(device)).contains(&kind::HELLO) {
            times.push(now);
        }
        now += 10;
    }
    times
}

#[test]
fn hello_backs_off_exponentially_up_to_the_cap() {
    let mut device = Device::new(DeviceConfig::provider("dev"), Stream);
    let mut host = Host::new();
    let times = hello_times(&mut device, &mut host, 0, 20_000);
    assert_eq!(
        times,
        vec![0, 250, 750, 1_750, 3_750, 7_750, 11_750, 15_750, 19_750]
    );
    let (_, hello) = {
        device.poll(1_000_000);
        host.drain(&mut device).remove(0)
    };
    assert_eq!(
        hello,
        Message::Hello(Hello {
            device_name: "dev".into(),
            roles: Roles::PROVIDER,
            max_frame: 256,
        })
    );
}

#[test]
fn welcome_connects_and_sends_the_snapshot_then_pings() {
    let mut device = Device::new(DeviceConfig::provider("dev"), Stream);
    let mut host = Host::new();
    device
        .publish_provider_state(&provider(), &[resource(1)])
        .unwrap();
    device.poll(0);
    assert_eq!(kinds(&host.drain(&mut device)), vec![kind::HELLO]);
    host.send(&mut device, &welcome(7));
    assert_eq!(
        events(&mut device),
        vec![DeviceEvent::Connected {
            node_id: NodeId::new("node-a"),
            session_id: 7
        }]
    );
    device.poll(5);
    let frames = host.drain(&mut device);
    let (header, Message::ProviderState(state)) = &frames[0] else {
        panic!("expected a snapshot, got {frames:?}");
    };
    assert_eq!(state.resources, vec![resource(1)]);
    host.send(&mut device, &Message::Ack { seq: header.seq });
    assert_eq!(events(&mut device), vec![DeviceEvent::StateAcked]);
    assert!(!device.state_pending());

    // A ping per heartbeat; a host ping is answered with a pong echoing its time.
    device.poll(1_005);
    assert_eq!(kinds(&host.drain(&mut device)), vec![kind::PING]);
    host.send(&mut device, &Message::Ping { now_ms: 42 });
    device.poll(1_010);
    let frames = host.drain(&mut device);
    assert_eq!(frames[0].1, Message::Pong { now_ms: 42 });
}

#[test]
fn stale_acks_and_duplicates_are_ignored() {
    let mut device = Device::new(DeviceConfig::provider("dev"), Stream);
    let mut host = Host::new();
    device.poll(0);
    host.drain(&mut device);
    host.send(&mut device, &welcome(1));
    device
        .publish_provider_state(&provider(), &[resource(1)])
        .unwrap();
    device.poll(1);
    let first = host.drain(&mut device)[0].0.seq;
    // A newer snapshot replaces the first before it is acked.
    device
        .publish_provider_state(&provider(), &[resource(2)])
        .unwrap();
    device.poll(2);
    let second = host.drain(&mut device)[0].0.seq;
    assert_ne!(first, second);
    events(&mut device);
    host.send(&mut device, &Message::Ack { seq: first });
    assert!(
        device.state_pending(),
        "an ack for an older snapshot does not count"
    );
    // Same seq twice: the second copy is a duplicate.
    let seq = host.send(&mut device, &Message::Ack { seq: second });
    assert!(!device.state_pending());
    host.send_with_seq(&mut device, &Message::Ack { seq: second }, seq);
    assert_eq!(device.stats().duplicates, 1);
    assert_eq!(events(&mut device), vec![DeviceEvent::StateAcked]);
}

#[test]
fn identical_lease_sets_are_reported_once() {
    let mut device = Device::new(DeviceConfig::provider("dev"), Stream);
    let mut host = Host::new();
    device.poll(0);
    host.send(&mut device, &welcome(1));
    events(&mut device);
    let set = vec![LeaseRecord::builder(ResourceId::new("dev.r1")).build()];
    host.send(&mut device, &Message::Leases(set.clone()));
    host.send(&mut device, &Message::Leases(set.clone()));
    host.send(&mut device, &Message::Leases(Vec::new()));
    assert_eq!(
        events(&mut device),
        // The last two collapse into the newest set while queued.
        vec![DeviceEvent::Leases(Vec::new())]
    );
    host.send(&mut device, &Message::Leases(set.clone()));
    assert_eq!(events(&mut device), vec![DeviceEvent::Leases(set)]);
}

#[test]
fn reject_backs_off_and_no_session_reconnects_at_once() {
    let mut device = Device::new(DeviceConfig::provider("dev"), Stream);
    let mut host = Host::new();
    device.poll(0);
    host.drain(&mut device);
    host.send(&mut device, &Message::Reject(RejectReason::UnknownDevice));
    host.send(&mut device, &Message::Reject(RejectReason::UnknownDevice));
    assert_eq!(
        events(&mut device),
        vec![DeviceEvent::Rejected(RejectReason::UnknownDevice)]
    );
    assert_eq!(device.link_state(), LinkState::BackingOff);
    let times = hello_times(&mut device, &mut host, 10, 6_000);
    // Silent for the reject backoff, then Hello with the normal backoff again.
    assert_eq!(times, vec![5_000, 5_250, 5_750]);

    host.send(&mut device, &welcome(3));
    events(&mut device);
    host.send(&mut device, &Message::Reject(RejectReason::NoSession));
    assert_eq!(events(&mut device), vec![DeviceEvent::Disconnected]);
    device.poll(6_010);
    assert_eq!(kinds(&host.drain(&mut device)), vec![kind::HELLO]);
}

#[test]
fn reject_from_another_protocol_version_is_understood() {
    let mut device = Device::new(DeviceConfig::provider("dev"), Stream);
    device.poll(0);
    // Version-2 host: a Reject whose body is a code this build does not know.
    let header = FrameHeader {
        version: 2,
        kind: kind::REJECT,
        seq: 1,
    };
    let wire: Vec<u8> = StreamEncoder::for_message(header, &[0x42]).collect();
    device.receive(&wire);
    assert_eq!(
        events(&mut device),
        vec![DeviceEvent::Rejected(RejectReason::Other(0x42))]
    );
    // Other kinds from another version are ignored.
    let mut device = Device::new(DeviceConfig::provider("dev"), Stream);
    let header = FrameHeader {
        version: 2,
        kind: kind::WELCOME,
        seq: 1,
    };
    let wire: Vec<u8> = StreamEncoder::for_message(header, &[]).collect();
    device.receive(&wire);
    assert!(events(&mut device).is_empty());
    assert_eq!(device.link_state(), LinkState::Connecting);
}

#[test]
fn host_silence_disconnects_after_missed_heartbeats() {
    let mut device = Device::new(DeviceConfig::provider("dev"), Stream);
    let mut host = Host::new();
    device.poll(0);
    host.send(&mut device, &welcome(1));
    device.poll(0);
    events(&mut device);
    device.poll(2_999);
    assert!(device.is_connected());
    device.poll(3_000);
    assert_eq!(events(&mut device), vec![DeviceEvent::Disconnected]);
    assert_eq!(kinds(&host.drain(&mut device)).last(), Some(&kind::HELLO));
}

#[test]
fn oversized_snapshots_are_refused_and_keep_the_previous_one() {
    let mut device = StreamDevice::<256, 128>::new(DeviceConfig::provider("dev"), Stream);
    device
        .publish_provider_state(&provider(), &[resource(1)])
        .unwrap();
    let many: Vec<_> = (0..20).map(resource).collect();
    assert!(matches!(
        device.publish_provider_state(&provider(), &many),
        Err(PublishError::TooLarge { max_frame: 128, .. })
    ));
    assert!(device.state_pending(), "previous snapshot is still pending");
    assert_eq!(device.max_frame(), 128);
}

#[test]
fn snapshot_too_large_for_the_negotiated_size_is_dropped_on_connect() {
    let mut device = Device::new(DeviceConfig::provider("dev"), Stream);
    let mut host = Host::new();
    let many: Vec<_> = (0..4).map(resource).collect();
    device.publish_provider_state(&provider(), &many).unwrap();
    device.poll(0);
    host.drain(&mut device);
    let mut small = welcome(1);
    if let Message::Welcome(w) = &mut small {
        w.max_frame = 64;
    }
    host.send(&mut device, &small);
    let events = events(&mut device);
    assert!(matches!(
        events.as_slice(),
        [
            DeviceEvent::Connected { .. },
            DeviceEvent::StateTooLarge { max_frame: 64, .. }
        ]
    ));
    assert!(!device.state_pending());
}

#[test]
fn unknown_and_reserved_kinds_are_ignored() {
    let mut device = Device::new(DeviceConfig::provider("dev"), Stream);
    let mut host = Host::new();
    device.poll(0);
    host.send(&mut device, &welcome(1));
    events(&mut device);
    for kind in [0x7E, kind::EXECUTOR_STATE, kind::WORKLOADS, kind::STATUS] {
        let header = FrameHeader::new(kind, host.seq.wrapping_add(1));
        host.seq = host.seq.wrapping_add(1);
        let wire: Vec<u8> = StreamEncoder::for_message(header, b"future body").collect();
        device.receive(&wire);
    }
    assert!(events(&mut device).is_empty());
    assert_eq!(device.stats().decode_errors, 0);
    assert!(device.is_connected());
}

#[test]
fn can_device_peek_and_commit_segments() {
    let mut device = CanDevice::<256, 256>::new(DeviceConfig::provider("dev"), Packet::CLASSIC);
    device.poll(0);
    let mut rx = Reassembler::<256>::new();
    let mut delivered = None;
    let mut segments = 0;
    while let Some(segment) = device.peek_segment() {
        // Peeking twice yields the same segment until it is committed.
        assert_eq!(device.peek_segment(), Some(segment));
        device.commit_segment();
        segments += 1;
        if let Some(frame) = rx.push(segment.as_bytes()).unwrap() {
            delivered = Some(Message::decode(&frame).unwrap());
        }
    }
    assert!(segments > 1);
    assert!(matches!(delivered, Some(Message::Hello(h)) if h.device_name == "dev"));
    assert_eq!(device.next_segment(), None);
}
