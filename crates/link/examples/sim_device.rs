//! A simulated device talking to a host over an in-memory serial line.
//!
//! ```text
//! cargo run -p orion-link --example sim_device --features std
//! ```
//!
//! The device side is exactly what an MCU port runs (`StreamDevice` fed with bytes, polled with a
//! millisecond clock); the host side is what the node gateway runs (`HostSession`). The serial
//! line is two byte queues with a 115200-baud delay. A virtual clock drives both, so the
//! timeline is deterministic and the run takes no real time. Halfway through, the cable is
//! unplugged for five seconds.

use std::collections::VecDeque;

use orion_link::Stream;
use orion_link::device::{DeviceConfig, DeviceEvent, StreamDevice};
use orion_link::host::{HostConfig, HostEvent, HostSession};
use orion_link::message::{LeaseRecord, NodeId, ProviderRecord, ResourceRecord};

/// One direction of a serial line: bytes become readable after their transmission time.
struct Line {
    queue: VecDeque<(u64, u8)>,
    free_at_us: u64,
    sent: usize,
}

impl Line {
    /// 115200 baud, 10 bits per byte.
    const BYTE_US: u64 = 87;

    fn new() -> Self {
        Self {
            queue: VecDeque::new(),
            free_at_us: 0,
            sent: 0,
        }
    }

    fn write(&mut self, now_ms: u64, bytes: &[u8], plugged: bool) {
        for &byte in bytes {
            self.free_at_us = self.free_at_us.max(now_ms * 1_000) + Self::BYTE_US;
            self.sent += 1;
            if plugged {
                self.queue.push_back((self.free_at_us, byte));
            }
        }
    }

    fn read(&mut self, now_ms: u64) -> Vec<u8> {
        let mut out = Vec::new();
        while let Some(&(ready_us, byte)) = self.queue.front() {
            if ready_us > now_ms * 1_000 {
                break;
            }
            out.push(byte);
            self.queue.pop_front();
        }
        out
    }
}

fn main() {
    let provider = ProviderRecord::builder("provider.imu-board", NodeId::new("unassigned"))
        .resource_type("imu.sample_source")
        .build();
    let resource = |rate: u64| {
        vec![
            ResourceRecord::builder("imu-board.imu-0", "imu.sample_source", "provider.imu-board")
                .label(format!("rate={rate}hz"))
                .build(),
        ]
    };

    // Device: 256-byte receive and transmit buffers, as on a small MCU.
    let mut device = StreamDevice::<256, 256>::new(DeviceConfig::provider("imu-board"), Stream);
    let mut host = HostSession::stream(HostConfig::new(NodeId::new("node-a")));
    let mut to_host = Line::new();
    let mut to_device = Line::new();
    let mut uart_fifo = [0u8; 16];

    device
        .publish_provider_state(&provider, &resource(100))
        .expect("snapshot fits");

    for now in 0..=14_000u64 {
        let plugged = !(6_000..11_000).contains(&now);
        match now {
            1_500 => {
                println!("{now:>6} ms  host     set_leases(imu-board.imu-0 -> workload.pose)");
                host.set_leases(vec![
                    LeaseRecord::builder("imu-board.imu-0")
                        .holder_node(NodeId::new("node-a"))
                        .holder_workload("workload.pose")
                        .build(),
                ]);
            }
            3_000 => {
                println!("{now:>6} ms  device   publish_provider_state(rate=200hz)");
                device
                    .publish_provider_state(&provider, &resource(200))
                    .expect("snapshot fits");
            }
            6_000 => println!("{now:>6} ms  -------- cable unplugged --------"),
            11_000 => println!("{now:>6} ms  -------- cable plugged in --------"),
            _ => {}
        }

        // Device main loop: RX bytes in, poll with the clock, TX bytes out.
        device.receive(&to_device.read(now));
        device.poll(now);
        while let Some(event) = device.next_event() {
            match event {
                DeviceEvent::Leases(leases) => {
                    let held: Vec<_> = leases.iter().map(|l| l.resource_id.as_str()).collect();
                    println!("{now:>6} ms  device   leases {held:?}");
                }
                other => println!("{now:>6} ms  device   {other:?}"),
            }
        }
        loop {
            let n = device.transmit(&mut uart_fifo);
            if n == 0 {
                break;
            }
            to_host.write(now, &uart_fifo[..n], plugged);
        }

        // Gateway loop.
        host.receive(&to_host.read(now));
        host.poll(now);
        while let Some(event) = host.next_event() {
            match event {
                HostEvent::ProviderState {
                    device_name,
                    resources,
                    ..
                } => {
                    let labels: Vec<_> = resources.iter().flat_map(|r| r.labels.clone()).collect();
                    println!("{now:>6} ms  host     ProviderState from {device_name}: {labels:?}");
                }
                other => println!("{now:>6} ms  host     {other:?}"),
            }
        }
        while let Some(bytes) = host.transmit() {
            to_device.write(now, &bytes, plugged);
        }
    }

    let stats = device.stats();
    println!(
        "\nbytes on the wire: device->host {}, host->device {}; device frames sent {}, received {}, \
         snapshot retransmits {}",
        to_host.sent,
        to_device.sent,
        stats.frames_sent,
        stats.frames_received,
        stats.state_retransmits
    );
}
