//! The device's session against a host played by the test: hello at a steady pace, a
//! session with pings, a host that goes quiet, a host that says no, a counter that wraps,
//! and the same over CAN. Nothing allocates.

use orion_device_core::{
    CanDevice, DeviceConfig, Event, RejectReason, Roles, SegmentMtu, SendError, State, StreamDevice,
};
use orion_msg_session::{Hello, Ping, Pong, Reject, Welcome};
use orion_wire::{
    Body, FrameView, Reassembler, Segmenter, StreamDecoder, decode_body, encode_body, encode_frame,
    kind,
};

#[global_allocator]
static GLOBAL: orion_measure::CountingAlloc = orion_measure::CountingAlloc::system();

type Device = StreamDevice<128, 96>;

/// The host's end of a byte stream: what the device sent, as frames.
#[derive(Default)]
struct Host {
    rx: StreamDecoder<256>,
    seq: u16,
}

impl Host {
    /// Everything the device has to send now, as `(kind, payload)`.
    fn take(&mut self, device: &mut Device) -> Vec<(u8, Vec<u8>)> {
        let mut frames = Vec::new();
        let mut out = [0_u8; 7];
        loop {
            let n = device.transmit(&mut out);
            if n == 0 {
                break;
            }
            for byte in &out[..n] {
                if let Ok(Some(frame)) = self.rx.push(*byte) {
                    frames.push((frame.kind(), frame.payload().to_vec()));
                }
            }
        }
        frames
    }

    /// One frame from the host, as the bytes on the stream.
    fn frame<B: Body + serde::Serialize>(&mut self, body: &B) -> Vec<u8> {
        self.seq += 1;
        let mut frame = [0_u8; 200];
        let len = encode_body(body, self.seq, &mut frame).expect("encode");
        let mut wire = [0_u8; 256];
        let n = encode_frame(&frame[..len], &mut wire).expect("cobs");
        wire[..n].to_vec()
    }

    fn welcome(&mut self, heartbeat_ms: u32, max_frame: u32) -> Vec<u8> {
        self.frame(&Welcome {
            node: "board".parse().expect("name"),
            session: 7,
            heartbeat_ms,
            max_frame,
            revision: 1,
            idle_ms: 0,
            level: orion_wire::INTERFACE_LEVEL,
        })
    }
}

/// Sends `body` as the general codec writes it.
fn send<const TX: usize, B: Body + serde::Serialize>(
    link: &mut orion_device_core::Link<TX>,
    body: &B,
) -> Result<u16, SendError> {
    link.send_with(B::KIND, |payload| {
        orion_wire::body::encode_fields(body, payload).ok()
    })
}

fn nothing(_: &mut orion_device_core::Link<96>, _: &FrameView<'_>) {}

/// A device that has said hello and been welcomed at `now`, with a 100 ms heartbeat.
fn connected(now: u32) -> (Device, Host) {
    let mut device = Device::new(DeviceConfig::new("imu-board"));
    let mut host = Host::default();
    device.link.poll(now);
    assert_eq!(host.take(&mut device)[0].0, kind::HELLO);
    device.receive(&host.welcome(100, 4096), nothing);
    assert_eq!(
        device.link.next_event(),
        Some(Event::Connected { session: 7 })
    );
    (device, host)
}

#[test]
fn hello_is_said_at_the_same_pace_until_someone_answers() {
    let mut device = Device::new(DeviceConfig::new("imu-board"));
    let mut host = Host::default();
    assert_eq!(device.link.state(), State::Connecting);
    let mut said = Vec::new();
    for now in 0..5_000_u32 {
        device.link.poll(now);
        for (kind, payload) in host.take(&mut device) {
            assert_eq!(kind, kind::HELLO);
            said.push((now, payload));
        }
    }
    // Every 250 ms for five seconds, the twentieth as soon after the nineteenth as the
    // second after the first.
    let times: Vec<u32> = said.iter().map(|(now, _)| *now).collect();
    assert_eq!(times, (0..20).map(|n| n * 250).collect::<Vec<_>>());
    // What it says: its name, that it is a device, and the longest frame it takes.
    let frame = {
        let mut bytes = [0_u8; 128];
        let len = orion_wire::frame::encode(
            orion_wire::FrameHeader::new(kind::HELLO, 1),
            &said[0].1,
            &mut bytes,
        )
        .expect("frame");
        bytes[..len].to_vec()
    };
    let view = orion_wire::frame::decode(&frame).expect("frame");
    let hello: Hello<'_> = decode_body(&view).expect("hello");
    assert_eq!(
        (hello.name, hello.max_frame, hello.revision),
        ("imu-board", 128, 1)
    );
    assert_eq!(hello.roles, Roles::DEVICE);
    // Nothing is to be had of a device with no session.
    assert_eq!(
        send(&mut device.link, &Ping { now_ms: 1 }),
        Err(SendError::NotConnected)
    );
    assert_eq!(
        (device.link.session(), device.link.next_event()),
        (None, None)
    );
}

#[test]
fn a_session_pings_answers_pings_and_carries_frames() {
    let (mut device, mut host) = connected(1_000);
    assert_eq!(
        (
            device.link.state(),
            device.link.session(),
            device.link.node()
        ),
        (State::Connected, Some(7), b"board".as_slice())
    );
    // The host takes 4096 and the send buffer is 96: the smaller is the limit.
    assert_eq!(device.link.max_frame(), 96);

    // A ping at once and then every heartbeat, each with the device's time in it.
    let mut pings = Vec::new();
    for now in 1_000..1_450_u32 {
        device.link.poll(now);
        for (kind, payload) in host.take(&mut device) {
            assert_eq!(kind, kind::PING);
            pings.push((now, payload));
        }
        // The host answers each, so it is heard from.
        if pings.last().is_some_and(|(at, _)| *at == now) {
            let pong = host.frame(&Pong {
                now_ms: u64::from(now),
            });
            device.receive(&pong, nothing);
        }
    }
    assert_eq!(
        pings.iter().map(|(now, _)| *now).collect::<Vec<_>>(),
        [1_000, 1_100, 1_200, 1_300, 1_400]
    );
    assert_eq!(device.link.next_event(), None);

    // The host's own ping is answered with what it carried.
    let ping = host.frame(&Ping { now_ms: 0xABCD });
    device.receive(&ping, nothing);
    device.link.poll(1_450);
    let answered = host.take(&mut device);
    assert_eq!(answered.len(), 1);
    assert_eq!(answered[0].0, kind::PONG);

    // A frame that is not the session's reaches the layers above, with the link to
    // answer on; noise before it and half a frame after it do not.
    let mut wire = vec![0x55, 0xAA, 0x00, 0x13];
    wire.extend(host.frame(&Reject {
        reason: RejectReason::Busy,
        node_revision: 0,
        peer_revision: 0,
    }));
    let request = host.frame(&orion_msg_session::Refusal {
        request_seq: 9,
        code: 3,
        detail: "no",
    });
    wire.extend(&request);
    wire.extend(&request[..request.len() - 3]);
    let mut seen = Vec::new();
    device.receive(&wire, |link, frame| {
        seen.push(frame.kind());
        // Answered from inside, while the frame is still being looked at.
        send(link, &Pong { now_ms: 5 }).expect("room");
    });
    assert_eq!(seen, [kind::REFUSAL]);

    // One frame out at a time: the next is taken when the port has had the last.
    assert!(!device.link.is_free());
    assert_eq!(
        send(&mut device.link, &Pong { now_ms: 6 }),
        Err(SendError::Busy)
    );
    assert_eq!(host.take(&mut device).len(), 1);
    assert!(device.link.is_free());
    let first = send(&mut device.link, &Pong { now_ms: 6 }).expect("room");
    assert_eq!(host.take(&mut device)[0].0, kind::PONG);
    let second = send(&mut device.link, &Pong { now_ms: 7 }).expect("room");
    assert_eq!(second, first.wrapping_add(1));
    host.take(&mut device);
    // A frame longer than the limit is not taken, and nothing is left half written.
    let long = [0_u8; 200];
    let too_long = device.link.send_with(0x70, |payload| {
        payload.get_mut(..long.len())?.copy_from_slice(&long);
        Some(long.len())
    });
    assert_eq!(too_long, Err(SendError::TooLarge));
    assert!(device.link.is_free());
    let fits = device.link.send_with(0x70, |payload| {
        payload[..80].fill(0);
        Some(80)
    });
    assert!(fits.is_ok());
    let sent = host.take(&mut device);
    // Eighty zero bytes, each of which the stream has to write round: still one frame.
    assert_eq!((sent[0].0, sent[0].1.len()), (0x70, 80));
}

#[test]
fn a_host_that_takes_less_than_the_send_buffer_sets_the_limit() {
    let mut device = Device::new(DeviceConfig::new("imu-board"));
    let mut host = Host::default();
    device.link.poll(0);
    host.take(&mut device);
    device.receive(&host.welcome(100, 40), nothing);
    assert_eq!(device.link.max_frame(), 40);
    let fits = |len: usize| move |payload: &mut [u8]| payload.get_mut(..len).map(|room| room.len());
    assert_eq!(
        device.link.send_with(0x70, fits(33)),
        Err(SendError::TooLarge)
    );
    assert!(device.link.send_with(0x70, fits(32)).is_ok());
}

#[test]
fn a_host_that_goes_quiet_is_taken_for_gone_and_hello_starts_again() {
    let (mut device, mut host) = connected(0);
    send(&mut device.link, &Pong { now_ms: 1 }).expect("room");
    let mut events = Vec::new();
    let mut hellos = Vec::new();
    for now in 0..1_000_u32 {
        device.link.poll(now);
        for (kind, _) in host.take(&mut device) {
            if kind == kind::HELLO {
                hellos.push(now);
            }
        }
        while let Some(event) = device.link.next_event() {
            events.push((now, event));
        }
    }
    // Three heartbeats of 100 ms with nothing heard, and it is over; hello at once, and
    // then at its pace.
    assert_eq!(events, [(301, Event::Disconnected)]);
    assert_eq!(hellos, [301, 551, 801]);
    assert_eq!(
        (device.link.state(), device.link.session()),
        (State::Connecting, None)
    );
    assert_eq!(
        send(&mut device.link, &Pong { now_ms: 1 }),
        Err(SendError::NotConnected)
    );
    // The host is back: a session again.
    device.receive(&host.welcome(100, 4096), nothing);
    assert_eq!(
        device.link.next_event(),
        Some(Event::Connected { session: 7 })
    );
    assert!(device.link.is_free());
}

#[test]
fn a_host_that_says_no_is_left_alone_for_a_while() {
    let mut device = Device::new(DeviceConfig::new("imu-board"));
    let mut host = Host::default();
    device.link.poll(0);
    host.take(&mut device);
    let no = host.frame(&Reject {
        reason: RejectReason::Denied,
        node_revision: 0,
        peer_revision: 0,
    });
    device.receive(&no, nothing);
    assert_eq!(
        device.link.next_event(),
        Some(Event::Rejected(RejectReason::Denied))
    );
    assert_eq!(device.link.state(), State::Rejected);
    let mut hellos = Vec::new();
    for now in 1..6_000_u32 {
        device.link.poll(now);
        if !host.take(&mut device).is_empty() {
            hellos.push(now);
        }
    }
    // Five seconds from when the clock was next read, then hello at its pace.
    assert_eq!(hellos, [5_001, 5_251, 5_501, 5_751]);
}

#[test]
fn a_host_of_another_wire_revision_is_taken_as_one_that_said_no() {
    let mut device = Device::new(DeviceConfig::new("imu-board"));
    let mut host = Host::default();
    device.link.poll(0);
    host.take(&mut device);
    let other = host.frame(&Welcome {
        node: "board".parse().expect("name"),
        session: 7,
        heartbeat_ms: 100,
        max_frame: 4096,
        revision: orion_wire::INTERFACE_REVISION + 1,
        idle_ms: 0,
        level: 0,
    });
    device.receive(&other, nothing);
    assert_eq!(
        device.link.next_event(),
        Some(Event::Rejected(RejectReason::RevisionMismatch))
    );
    assert_eq!(
        (device.link.state(), device.link.session()),
        (State::Rejected, None)
    );
}

#[test]
fn a_counter_that_wraps_changes_nothing() {
    let start = u32::MAX - 120;
    let (mut device, mut host) = connected(start);
    let mut pings = 0;
    for step in 0..400_u32 {
        let now = start.wrapping_add(step);
        device.link.poll(now);
        for (kind, _) in host.take(&mut device) {
            assert_eq!(kind, kind::PING);
            pings += 1;
            let pong = host.frame(&Pong { now_ms: 0 });
            device.receive(&pong, nothing);
        }
    }
    // Every 100 ms, across the wrap, and the session never ended.
    assert_eq!(pings, 4);
    assert_eq!(
        (device.link.state(), device.link.next_event()),
        (State::Connected, None)
    );
}

#[test]
fn the_same_session_over_can_classic_and_fd() {
    for mtu in [SegmentMtu::CLASSIC, SegmentMtu::FD] {
        let mut device = CanDevice::<128, 96>::new(DeviceConfig::new("motor-3"), mtu);
        let mut from_device = Reassembler::<256>::new();
        // What the device has to send, as frames, each segment a whole CAN frame's data.
        let mut take = |device: &mut CanDevice<128, 96>| {
            let mut frames = Vec::new();
            let mut segment = [0_u8; 64];
            while let Some(len) = device.next_segment(&mut segment) {
                assert!(len <= mtu.frame_len());
                if let Ok(Some(frame)) = from_device.push(&segment[..len]) {
                    frames.push((frame.kind(), frame.payload().to_vec()));
                }
            }
            frames
        };
        let to_device = |device: &mut CanDevice<128, 96>, frame: &[u8], seen: &mut Vec<u8>| {
            let mut segmenter = Segmenter::for_frame(frame, mtu);
            let mut segment = [0_u8; 64];
            while let Ok(Some(len)) = segmenter.next_into(&mut segment) {
                device.receive_segment(&segment[..len], |_, frame| seen.push(frame.kind()));
            }
        };
        let mut seen = Vec::new();
        device.link.poll(0);
        // A hello is longer than one classic CAN frame holds: it comes in several.
        let hello = take(&mut device);
        assert_eq!(hello.len(), 1);
        assert_eq!(hello[0].0, kind::HELLO);
        let mut frame = [0_u8; 128];
        let welcome = Welcome {
            node: "board".parse().expect("name"),
            session: 3,
            heartbeat_ms: 100,
            max_frame: 512,
            revision: 1,
            idle_ms: 0,
            level: orion_wire::INTERFACE_LEVEL,
        };
        let len = encode_body(&welcome, 1, &mut frame).expect("encode");
        to_device(&mut device, &frame[..len], &mut seen);
        assert_eq!(
            device.link.next_event(),
            Some(Event::Connected { session: 3 })
        );
        device.link.poll(1);
        assert_eq!(take(&mut device)[0].0, kind::PING);
        // A frame from above, and one for above.
        send(&mut device.link, &Pong { now_ms: 1 }).expect("room");
        assert_eq!(take(&mut device)[0].0, kind::PONG);
        let len = encode_body(
            &orion_msg_session::Refusal {
                request_seq: 1,
                code: 2,
                detail: "a longer reason than fits one frame",
            },
            2,
            &mut frame,
        )
        .expect("encode");
        to_device(&mut device, &frame[..len], &mut seen);
        assert_eq!(seen, [kind::REFUSAL]);
    }
}

#[test]
fn a_session_that_is_running_allocates_nothing() {
    let (mut device, mut host) = connected(0);
    let pong = host.frame(&Pong { now_ms: 0 });
    let other = host.frame(&orion_msg_session::Refusal {
        request_seq: 1,
        code: 2,
        detail: "x",
    });
    let mut out = [0_u8; 64];
    let mut now = 0_u32;
    let mut seen = 0_u32;
    orion_measure::harness::assert_steady_no_allocs("device session", 64, 4_096, || {
        now += 7;
        device.link.poll(now);
        device.receive(&pong, |_, _| {});
        device.receive(&other, |link, _| {
            seen += 1;
            let _ = send(link, &Ping { now_ms: 1 });
        });
        while device.transmit(&mut out) != 0 {}
        while device.link.next_event().is_some() {}
    });
    assert!(seen > 4_000);
    assert_eq!(device.link.state(), State::Connected);
}
