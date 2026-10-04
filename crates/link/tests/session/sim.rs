//! Deterministic in-process link simulation: virtual clock, lossy wires, device ↔ host pairs.

use orion_core::{ProviderId, ResourceId};
use orion_link::device::{CanDevice, DeviceConfig, DeviceEvent, StreamDevice};
use orion_link::host::{HostBus, HostConfig, HostEvent, HostSession};
use orion_link::message::{LeaseRecord, NodeId, ProviderRecord, ResourceRecord};
use orion_link::{CanLinkIds, Packet, Stream, StreamDecoder};

use crate::common::Rng;

pub const HEARTBEAT_MS: u32 = 1_000;
pub const STEP_MS: u64 = 5;

pub fn host_config() -> HostConfig {
    let mut config = HostConfig::new(NodeId::new("node-a"));
    config.heartbeat_ms = HEARTBEAT_MS;
    config
}

pub fn provider(name: &str) -> ProviderRecord {
    ProviderRecord::builder(
        ProviderId::new(format!("provider.{name}")),
        NodeId::new("node-a"),
    )
    .resource_type("imu.sample_source")
    .build()
}

pub fn resources(name: &str, generation: u64) -> Vec<ResourceRecord> {
    vec![
        ResourceRecord::builder(
            ResourceId::new(format!("{name}.imu-{generation}")),
            "imu.sample_source",
            ProviderId::new(format!("provider.{name}")),
        )
        .label("imu")
        .build(),
    ]
}

pub fn leases(name: &str, generation: u64) -> Vec<LeaseRecord> {
    vec![
        LeaseRecord::builder(ResourceId::new(format!("{name}.imu-{generation}")))
            .holder_node(NodeId::new("node-a"))
            .build(),
    ]
}

/// Fault injection for one direction of a link.
#[derive(Debug, Clone, Copy, Default)]
pub struct Faults {
    /// Drop each frame (stream) or segment (CAN) with this probability (percent).
    pub drop_pct: usize,
    /// Flip a random byte of a frame or segment with this probability (percent).
    pub corrupt_pct: usize,
    /// Deliver a frame or segment twice with this probability (percent).
    pub dup_pct: usize,
    /// Drop everything.
    pub cut: bool,
}

impl Faults {
    pub fn lossy(pct: usize) -> Self {
        Self {
            drop_pct: pct,
            corrupt_pct: pct,
            dup_pct: pct,
            cut: false,
        }
    }
}

/// One direction of a link. `drop_kinds` drops the next frames of the listed kinds (each entry
/// drops one frame), which makes targeted loss deterministic.
pub struct Wire {
    rng: Rng,
    pub faults: Faults,
    pub drop_kinds: Vec<u8>,
    /// Kinds of every frame handed to the wire (before faults).
    pub sent_kinds: Vec<u8>,
    /// Dropping the rest of the current CAN message.
    dropping_message: bool,
}

impl Wire {
    pub fn new(seed: u64) -> Self {
        Self {
            rng: Rng::new(seed),
            faults: Faults::default(),
            drop_kinds: Vec::new(),
            sent_kinds: Vec::new(),
            dropping_message: false,
        }
    }

    fn targeted_drop(&mut self, kind: u8) -> bool {
        self.sent_kinds.push(kind);
        if let Some(index) = self.drop_kinds.iter().position(|&k| k == kind) {
            self.drop_kinds.remove(index);
            return true;
        }
        false
    }

    fn mangle(&mut self, mut bytes: Vec<u8>) -> Vec<Vec<u8>> {
        if self.faults.cut || self.rng.chance(self.faults.drop_pct) {
            return Vec::new();
        }
        if !bytes.is_empty() && self.rng.chance(self.faults.corrupt_pct) {
            let index = self.rng.below(bytes.len());
            bytes[index] ^= 1 << self.rng.below(8);
        }
        if self.rng.chance(self.faults.dup_pct) {
            vec![bytes.clone(), bytes]
        } else {
            vec![bytes]
        }
    }

    /// Splits stream bytes into frames (each ends with its `0x00`), applies faults per frame, and
    /// returns the surviving bytes in random chunks.
    pub fn stream(&mut self, bytes: &[u8]) -> Vec<Vec<u8>> {
        let mut delivered = Vec::new();
        for packet in bytes.split_inclusive(|&b| b == 0) {
            if packet == [0] {
                delivered.push(0);
                continue;
            }
            let mut decoder = StreamDecoder::<8192>::new();
            let (_, result) = decoder.push_slice(packet);
            let kind = match result {
                Ok(Some(frame)) => frame.kind(),
                _ => 0xFF,
            };
            if self.targeted_drop(kind) {
                continue;
            }
            for copy in self.mangle(packet.to_vec()) {
                delivered.extend_from_slice(&copy);
            }
        }
        let mut chunks = Vec::new();
        let mut rest = &delivered[..];
        while !rest.is_empty() {
            let n = 1 + self.rng.below(rest.len().min(40));
            chunks.push(rest[..n].to_vec());
            rest = &rest[n..];
        }
        chunks
    }

    /// Applies faults to one CAN segment. A targeted drop removes the whole message.
    pub fn segment(&mut self, data: &[u8]) -> Vec<Vec<u8>> {
        let start = data.first().is_some_and(|h| h & 0x80 != 0);
        if start {
            // Segment header, then frame header [version][kind]...
            let kind = data.get(2).copied().unwrap_or(0xFF);
            self.dropping_message = self.targeted_drop(kind);
        }
        if self.dropping_message {
            return Vec::new();
        }
        self.mangle(data.to_vec())
    }
}

/// A device and a host over a simulated byte stream.
pub struct StreamPair {
    pub device: StreamDevice<512, 512>,
    pub host: HostSession<Stream>,
    pub now: u64,
    pub up: Wire,
    pub down: Wire,
    pub device_events: Vec<(u64, DeviceEvent)>,
    pub host_events: Vec<(u64, HostEvent)>,
    /// Stop polling/serving the device (simulates a crashed device).
    pub device_frozen: bool,
}

impl StreamPair {
    pub fn new(name: &str, config: HostConfig, seed: u64) -> Self {
        Self::with_device(
            StreamDevice::new(DeviceConfig::provider(name), Stream),
            config,
            seed,
        )
    }

    pub fn with_device(device: StreamDevice<512, 512>, config: HostConfig, seed: u64) -> Self {
        Self {
            device,
            host: HostSession::stream(config),
            now: 0,
            up: Wire::new(seed),
            down: Wire::new(seed ^ 0xA5A5),
            device_events: Vec::new(),
            host_events: Vec::new(),
            device_frozen: false,
        }
    }

    pub fn step(&mut self) {
        self.now += STEP_MS;
        if !self.device_frozen {
            self.device.poll(self.now);
        }
        self.host.poll(self.now);
        if !self.device_frozen {
            let mut bytes = Vec::new();
            let mut chunk = [0u8; 37];
            loop {
                let n = self.device.transmit(&mut chunk);
                if n == 0 {
                    break;
                }
                bytes.extend_from_slice(&chunk[..n]);
            }
            for piece in self.up.stream(&bytes) {
                self.host.receive(&piece);
            }
        }
        let mut bytes = Vec::new();
        while let Some(frame) = self.host.transmit() {
            bytes.extend_from_slice(&frame);
        }
        for piece in self.down.stream(&bytes) {
            if !self.device_frozen {
                self.device.receive(&piece);
            }
        }
        self.drain();
    }

    fn drain(&mut self) {
        while let Some(event) = self.device.next_event() {
            self.device_events.push((self.now, event));
        }
        while let Some(event) = self.host.next_event() {
            self.host_events.push((self.now, event));
        }
    }

    pub fn run(&mut self, ms: u64) {
        let end = self.now + ms;
        while self.now < end {
            self.step();
        }
    }

    /// Steps until `done` holds or `max_ms` elapse; returns whether it held.
    pub fn run_until(&mut self, max_ms: u64, mut done: impl FnMut(&Self) -> bool) -> bool {
        let end = self.now + max_ms;
        while self.now < end {
            self.step();
            if done(self) {
                return true;
            }
        }
        done(self)
    }

    pub fn connected(&self) -> bool {
        self.device.is_connected() && self.host.is_connected()
    }

    pub fn host_states(&self) -> Vec<&Vec<ResourceRecord>> {
        self.host_events
            .iter()
            .filter_map(|(_, e)| match e {
                HostEvent::ProviderState { resources, .. } => Some(resources),
                _ => None,
            })
            .collect()
    }

    pub fn device_leases(&self) -> Vec<&Vec<LeaseRecord>> {
        self.device_events
            .iter()
            .filter_map(|(_, e)| match e {
                DeviceEvent::Leases(leases) => Some(leases),
                _ => None,
            })
            .collect()
    }

    pub fn count_device(&self, pred: impl Fn(&DeviceEvent) -> bool) -> usize {
        self.device_events.iter().filter(|(_, e)| pred(e)).count()
    }

    pub fn count_host(&self, pred: impl Fn(&HostEvent) -> bool) -> usize {
        self.host_events.iter().filter(|(_, e)| pred(e)).count()
    }
}

/// One simulated CAN device on a [`CanBus`].
pub struct BusNode {
    pub address: u32,
    pub ids: CanLinkIds,
    pub device: CanDevice<512, 512>,
    pub up: Wire,
    pub down: Wire,
    pub events: Vec<(u64, DeviceEvent)>,
    pub frozen: bool,
}

/// Several devices and a [`HostBus`] on one simulated CAN bus.
pub struct CanBus {
    pub host: HostBus,
    pub nodes: Vec<BusNode>,
    pub now: u64,
    pub host_events: Vec<(u64, u32, HostEvent)>,
}

pub const BASE_IDS: CanLinkIds = CanLinkIds::new(0x100, 0x180, false);

impl CanBus {
    pub fn new(config: HostConfig, transport: Packet, addresses: &[u32], seed: u64) -> Self {
        let nodes = addresses
            .iter()
            .map(|&address| BusNode {
                address,
                ids: CanLinkIds::for_address(BASE_IDS, address).expect("valid address"),
                device: CanDevice::new(DeviceConfig::provider(format!("dev-{address}")), transport),
                up: Wire::new(seed.wrapping_add(u64::from(address))),
                down: Wire::new(seed.wrapping_mul(31).wrapping_add(u64::from(address))),
                events: Vec::new(),
                frozen: false,
            })
            .collect();
        Self {
            host: HostBus::new(config, BASE_IDS, transport, 1..=63),
            nodes,
            now: 0,
            host_events: Vec::new(),
        }
    }

    pub fn node(&mut self, address: u32) -> &mut BusNode {
        self.nodes
            .iter_mut()
            .find(|n| n.address == address)
            .expect("node exists")
    }

    pub fn step(&mut self) {
        self.now += STEP_MS;
        self.host.poll(self.now);
        // Interleave the devices' segments on the bus one at a time, like arbitration would.
        for node in &mut self.nodes {
            if !node.frozen {
                node.device.poll(self.now);
            }
        }
        loop {
            let mut any = false;
            for node in &mut self.nodes {
                if node.frozen {
                    continue;
                }
                if let Some(segment) = node.device.peek_segment() {
                    node.device.commit_segment();
                    any = true;
                    for data in node.up.segment(segment.as_bytes()) {
                        assert!(self.host.receive(node.ids.device_to_host, false, &data));
                    }
                }
            }
            if !any {
                break;
            }
        }
        while let Some(frame) = self.host.next_frame() {
            let node = self
                .nodes
                .iter_mut()
                .find(|n| n.ids.host_to_device == frame.id)
                .expect("frame for a known node");
            assert_eq!(node.address, frame.address);
            for data in node.down.segment(frame.segment.as_bytes()) {
                if !node.frozen {
                    node.device.receive_segment(&data);
                }
            }
        }
        for node in &mut self.nodes {
            while let Some(event) = node.device.next_event() {
                node.events.push((self.now, event));
            }
        }
        while let Some(event) = self.host.next_event() {
            self.host_events
                .push((self.now, event.address, event.event));
        }
    }

    pub fn run_until(&mut self, max_ms: u64, mut done: impl FnMut(&Self) -> bool) -> bool {
        let end = self.now + max_ms;
        while self.now < end {
            self.step();
            if done(self) {
                return true;
            }
        }
        done(self)
    }

    pub fn run(&mut self, ms: u64) {
        let end = self.now + ms;
        while self.now < end {
            self.step();
        }
    }
}
