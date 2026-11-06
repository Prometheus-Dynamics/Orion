//! Heap allocations on the link's hot paths in steady state, counted by a global allocator.
//!
//! Each case runs a warm-up, then counts allocations over many rounds on this thread only. The
//! bounds are the current costs; a change that adds allocations fails here.

#![cfg(all(feature = "std", feature = "alloc"))]

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

use orion_control_plane as cp;
use orion_link::device::{DeviceConfig, StreamDevice};
use orion_link::host::{HostConfig, HostEvent, HostSession};
use orion_link::message::{
    Hello, Message, NodeId, Roles, StatusEntry, StreamData, StreamDeclare, StreamPublish,
    TypedConfigValue,
};
use orion_link::{Stream, StreamDecoder, StreamEncoder};

struct Counting;

thread_local! {
    static ALLOCATIONS: Cell<u64> = const { Cell::new(0) };
}

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let _ = ALLOCATIONS.try_with(|count| count.set(count.get() + 1));
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let _ = ALLOCATIONS.try_with(|count| count.set(count.get() + 1));
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

const ROUNDS: u64 = 1_000;

/// Allocations per round of `round`, after a warm-up.
fn per_round(mut round: impl FnMut(u64)) -> f64 {
    for i in 0..50 {
        round(i);
    }
    let before = ALLOCATIONS.with(Cell::get);
    for i in 0..ROUNDS {
        round(50 + i);
    }
    (ALLOCATIONS.with(Cell::get) - before) as f64 / ROUNDS as f64
}

fn host_config() -> HostConfig {
    let mut config = HostConfig::new(NodeId::new("node-a"));
    config.serve_clients = true;
    config.serve_streams = true;
    config
}

/// A device and a host joined by a lossless in-memory wire, with reused buffers.
struct Pair {
    device: StreamDevice<512, 512>,
    host: HostSession<Stream>,
    chunk: Vec<u8>,
    frame: Vec<u8>,
    now: u64,
}

impl Pair {
    fn connected() -> Self {
        let mut pair = Self {
            device: StreamDevice::new(DeviceConfig::provider("imu-board"), Stream),
            host: HostSession::stream(host_config()),
            chunk: vec![0u8; 512],
            frame: Vec::with_capacity(4096),
            now: 0,
        };
        for _ in 0..20 {
            pair.step();
        }
        assert!(pair.device.is_connected());
        while pair.host.next_event().is_some() {}
        pair
    }

    fn step(&mut self) {
        self.now += 5;
        self.device.poll(self.now);
        self.host.poll(self.now);
        loop {
            let n = self.device.transmit(&mut self.chunk);
            if n == 0 {
                break;
            }
            self.host.receive(&self.chunk[..n]);
        }
        while self.host.transmit_into(&mut self.frame) {
            self.device.receive(&self.frame);
        }
    }
}

#[test]
fn steady_state_allocations_per_round() {
    // The device publishes a status value; the host decodes it into an event.
    let mut pair = Pair::connected();
    let mut entries = [StatusEntry::new("temperature_mc", TypedConfigValue::Int(0))];
    let status = per_round(|i| {
        entries[0].value = TypedConfigValue::Int(i as i64);
        pair.device.publish_status(&entries).unwrap();
        pair.step();
        while pair.host.next_event().is_some() {}
    });

    // The host's own traffic: a heartbeat exchange per round (the device pings, the host answers).
    let mut pair = Pair::connected();
    let heartbeat = per_round(|_| {
        pair.now += 1_000;
        pair.step();
    });

    // A provider session publishes a stream batch of 10 samples.
    let (mut provider, mut seq) = (HostSession::stream(host_config()), 0u16);
    let mut wire = vec![0u8; 4096];
    let mut send = |host: &mut HostSession<Stream>, seq: &mut u16, message: &Message| {
        *seq = seq.wrapping_add(1);
        let len = message.encode(*seq, &mut wire).unwrap();
        let bytes: Vec<u8> = StreamEncoder::for_frame(&wire[..len]).collect();
        host.receive(&bytes);
    };
    send(
        &mut provider,
        &mut seq,
        &Message::Hello(Hello {
            device_name: "imu-board".into(),
            roles: Roles::PROVIDER,
            max_frame: 4096,
            revision: 2,
        }),
    );
    send(
        &mut provider,
        &mut seq,
        &Message::StreamDeclare(StreamDeclare {
            req: 1,
            id: "imu".into(),
            series: vec![cp::SeriesDecl::new(
                "imu.accel_x",
                cp::SeriesKind::F32,
                1_000,
            )],
        }),
    );
    while provider.next_event().is_some() {}
    let mut builder = cp::SampleChunkBuilder::new(0, cp::SeriesKind::F32);
    for i in 0..10u64 {
        builder.push(1_000_000 + i * 1_000, i as f64);
    }
    let batch = Message::StreamPublish(StreamPublish {
        id: "imu".into(),
        chunks: vec![builder.take()],
    });
    let mut frame = vec![0u8; 4096];
    let mut cobs = Vec::with_capacity(4096);
    let publish = per_round(|i| {
        let len = batch.encode(i as u16, &mut frame).unwrap();
        cobs.clear();
        cobs.extend(StreamEncoder::for_frame(&frame[..len]));
        provider.receive(&cobs);
        while let Some(event) = provider.next_event() {
            assert!(matches!(event, HostEvent::StreamPublish { .. }));
        }
    });

    // A client session is sent a batch of 10 samples.
    let mut client = HostSession::stream(host_config());
    send(
        &mut client,
        &mut seq,
        &Message::Hello(Hello {
            device_name: "console".into(),
            roles: Roles::CLIENT,
            max_frame: 4096,
            revision: 2,
        }),
    );
    let mut out = Vec::with_capacity(4096);
    while client.transmit_into(&mut out) {}
    let Message::StreamPublish(StreamPublish { chunks, .. }) = batch else {
        unreachable!()
    };
    let data = StreamData {
        req: 1,
        cursor: cp::StreamCursor { epoch: 1, seq: 1 },
        prev_seq: 0,
        backfill: false,
        more: false,
        chunks,
    };
    let mut decoder = StreamDecoder::<4096>::new();
    let deliver = per_round(|_| {
        client.send_stream_data(&data).unwrap();
        while client.transmit_into(&mut out) {
            for &byte in &out {
                let _ = decoder.push(byte);
            }
        }
    });

    println!(
        "allocations per round: status {status}, heartbeat {heartbeat}, stream publish \
         {publish}, stream delivery {deliver}"
    );
    assert_eq!(heartbeat, 0.0, "the host's heartbeat answers allocate");
    assert_eq!(deliver, 0.0, "stream delivery allocates");
    // Decoding into owned events: the entry's key (status, coalesced on the device), or the
    // stream id, the chunk vector, the chunk's data and the event's device name (publish).
    assert!(status <= 1.0, "status: {status}");
    assert!(publish <= 4.0, "publish: {publish}");
}
