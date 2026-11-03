//! Firmware transfer over simulated classic CAN: the host driver on a `HostBus`, a two-slot device
//! (`SimFlash`) on address 1 whose segments can be lost, and a plain device on address 2 that
//! shares the bus and must keep working while the transfer runs. Chunks are sized to the CAN
//! segment budget (at most 32 frames each, `orion_link::wire::max_chunk_for_frame`).

use std::io::Cursor;

use orion_link::device::{CanDevice, DeviceConfig, DeviceSession};
use orion_link::host::xfer::{
    DriverConfig, FirmwareTransfer, Phase, TransferPlan, XferReply, XferRequest,
};
use orion_link::host::{HostBus, HostEvent};
use orion_link::wire::{ActivateMode, max_chunk_for_frame};
use orion_link::{CanLinkIds, Packet, SegmentMtu, crc32c};

use crate::common::Rng;
use crate::flash::SimFlash;
use crate::sim::{BASE_IDS, Faults, HEARTBEAT_MS, Wire, host_config, provider, resources};

/// Segments one chunk may take: the budget the CAN link task applies.
const CHUNK_SEGMENTS: usize = 32;
/// The chunk length the budget allows on classic CAN.
const CLASSIC_CHUNK: usize = max_chunk_for_frame(CHUNK_SEGMENTS * 7);
const STEP_MS: u64 = 5;
const FIRMWARE: u32 = 1;
const PLAIN: u32 = 2;

type FlashDevice = DeviceSession<Packet, 512, 512, String, 512, SimFlash>;

fn firmware_device(flash: SimFlash) -> FlashDevice {
    let mut device = DeviceSession::with_firmware(
        DeviceConfig::provider("motor-a".to_owned()),
        Packet::CLASSIC,
        flash,
    );
    device
        .publish_provider_state(&provider("motor-a"), &resources("motor-a", 1))
        .expect("snapshot fits");
    device
}

/// The bus, both devices, and the transfer under test, on one virtual clock.
struct CanBench {
    bus: HostBus,
    firmware: FlashDevice,
    plain: CanDevice<512, 512, String>,
    /// Wires of the firmware device (`[to device, from device]`); the plain device's are clean.
    down: Wire,
    up: Wire,
    xfer: FirmwareTransfer,
    image: Cursor<Vec<u8>>,
    now: u64,
    replies: Vec<XferReply>,
    infos: Vec<String>,
    connects: usize,
    lost: usize,
    largest_chunk: usize,
    /// Host events of the plain device (sessions and provider states it delivered).
    plain_events: usize,
}

impl CanBench {
    fn new(flash: SimFlash, xfer: FirmwareTransfer, image: Vec<u8>, seed: u64) -> Self {
        let mut plain = CanDevice::new(DeviceConfig::provider("dev-2".to_owned()), Packet::CLASSIC);
        plain
            .publish_provider_state(&provider("dev-2"), &resources("dev-2", 1))
            .expect("snapshot fits");
        Self {
            bus: HostBus::new(host_config(), BASE_IDS, Packet::CLASSIC, 1..=63),
            firmware: firmware_device(flash),
            plain,
            down: Wire::new(seed),
            up: Wire::new(seed.wrapping_mul(31)),
            xfer,
            image: Cursor::new(image),
            now: 0,
            replies: Vec::new(),
            infos: Vec::new(),
            connects: 0,
            lost: 0,
            largest_chunk: 0,
            plain_events: 0,
        }
    }

    fn ids(address: u32) -> CanLinkIds {
        CanLinkIds::for_address(BASE_IDS, address).expect("valid address")
    }

    /// One turn: both devices run and send segments, the bus delivers them to the host (through
    /// the firmware device's faulty wire), the driver reacts, and the host's segments go out.
    fn step(&mut self) {
        self.now += STEP_MS;
        self.bus.poll(self.now);
        self.firmware.poll(self.now);
        while self.firmware.next_event().is_some() {}
        self.plain.poll(self.now);
        while self.plain.next_event().is_some() {}

        // Device → host. Each device's segments go out one at a time, like arbitration.
        loop {
            let mut any = false;
            if let Some(segment) = self.firmware.next_segment() {
                any = true;
                for data in self.up.segment(segment.as_bytes()) {
                    self.bus
                        .receive(Self::ids(FIRMWARE).device_to_host, false, &data);
                }
            }
            if let Some(segment) = self.plain.next_segment() {
                any = true;
                self.bus
                    .receive(Self::ids(PLAIN).device_to_host, false, segment.as_bytes());
            }
            if !any {
                break;
            }
        }

        while let Some(event) = self.bus.next_event() {
            if event.address == PLAIN {
                self.plain_events += 1;
                continue;
            }
            match event.event {
                HostEvent::DeviceConnected { max_frame, .. } => {
                    self.connects += 1;
                    self.xfer.on_connected(max_frame);
                }
                HostEvent::DeviceInfo { info, .. } => {
                    self.infos.push(info.fw_version.clone());
                    self.xfer.on_info(info);
                }
                HostEvent::Xfer { reply, .. } => {
                    self.replies.push(reply);
                    self.xfer.on_reply(&reply);
                }
                HostEvent::DeviceLost { .. } => {
                    self.lost += 1;
                    self.xfer.on_lost();
                }
                _ => {}
            }
        }

        if let Some(request) = self
            .xfer
            .poll(self.now, &mut self.image)
            .expect("image reads")
        {
            if let XferRequest::Chunk { data, .. } = &request {
                self.largest_chunk = self.largest_chunk.max(data.len());
            }
            // Only the addressed device is sent to; the plain device gets nothing from it.
            let _ = self.bus.send_xfer(FIRMWARE, &request);
        }

        // Host → device, through the faulty wire to the firmware device only.
        while let Some(frame) = self.bus.next_frame() {
            if frame.address == FIRMWARE {
                for data in self.down.segment(frame.segment.as_bytes()) {
                    self.firmware.receive_segment(&data);
                }
            } else if frame.address == PLAIN {
                self.plain.receive_segment(frame.segment.as_bytes());
            }
        }

        if self.firmware.firmware().reboot_pending {
            let mut flash = self.firmware.firmware().clone();
            flash.power_cycle();
            self.firmware = firmware_device(flash);
        }
    }

    fn run_until(&mut self, limit_ms: u64, done: impl Fn(&CanBench) -> bool) {
        while !done(self) && self.now < limit_ms {
            self.step();
        }
    }

    fn finished(&self) -> bool {
        self.xfer.is_finished()
    }

    /// Whether the plain device is connected and still provides its resource.
    fn plain_serving(&self) -> bool {
        self.bus
            .session(PLAIN)
            .is_some_and(|session| session.device_name() == Some("dev-2"))
    }
}

/// Deterministic test image of `len` bytes.
fn image(seed: u64, len: usize) -> Vec<u8> {
    let mut rng = Rng::new(seed);
    (0..len).map(|_| rng.byte()).collect()
}

fn plan(image: &[u8], version: &str) -> TransferPlan {
    TransferPlan {
        xfer_id: 9,
        component: "motor-app".to_owned(),
        version: version.to_owned(),
        target_slot: 1,
        total_len: image.len() as u32,
        image_crc32c: crc32c(image),
        chunk_len: 128,
        activate: ActivateMode::Trial,
    }
}

/// A driver with the classic CAN chunk budget applied, as the node's CAN link task does.
fn budgeted_driver(image: &[u8], version: &str) -> FirmwareTransfer {
    let mut xfer = FirmwareTransfer::new(plan(image, version)).with_config(DriverConfig::default());
    xfer.set_chunk_len(CLASSIC_CHUNK as u16);
    xfer
}

#[test]
fn classic_can_budget_is_32_segments() {
    assert_eq!(CLASSIC_CHUNK, 199);
    assert_eq!(
        orion_link::wire::max_chunk_for_frame(CHUNK_SEGMENTS * SegmentMtu::CLASSIC.payload_len()),
        CLASSIC_CHUNK
    );
}

#[test]
fn transfer_over_lossy_segments_is_confirmed_and_the_neighbour_keeps_working() {
    let data = image(11, 2_400);
    let mut bench = CanBench::new(
        SimFlash::factory("1.2"),
        budgeted_driver(&data, "1.3"),
        data.clone(),
        5,
    );
    bench.xfer.set_confirm_gate(true);
    // Light loss on the firmware device's wire only: a dropped segment costs one chunk resend.
    bench.down.faults = Faults {
        drop_pct: 1,
        ..Faults::default()
    };
    bench.up.faults = Faults {
        drop_pct: 1,
        ..Faults::default()
    };
    bench.run_until(600_000, CanBench::finished);

    assert_eq!(bench.xfer.phase(), Phase::Succeeded, "{:?}", bench.replies);
    assert!(
        bench.largest_chunk <= CLASSIC_CHUNK,
        "a chunk spans at most the budget: {} > {CLASSIC_CHUNK}",
        bench.largest_chunk
    );
    assert!(bench.largest_chunk > 0);
    let flash = bench.firmware.firmware();
    assert_eq!(flash.active, 1);
    assert_eq!(flash.slots[1].image, data, "the device holds the image");
    assert!(!flash.trial, "confirmed");
    assert_eq!(bench.infos.last().map(String::as_str), Some("1.3"));
    assert!(
        bench.plain_serving(),
        "the neighbour on the bus keeps its session"
    );
    assert!(
        bench.plain_events > 0,
        "the neighbour's traffic keeps flowing"
    );
    assert_eq!(bench.lost, 0, "the firmware device is not lost");
}

#[test]
fn a_bus_cut_longer_than_the_heartbeat_resumes_on_the_next_session() {
    let data = image(12, 3_000);
    let mut bench = CanBench::new(
        SimFlash::factory("1.2"),
        budgeted_driver(&data, "1.3"),
        data.clone(),
        9,
    );
    bench.xfer.set_confirm_gate(true);
    // Run until part of the image is on the device, then cut the firmware device's wire for
    // several heartbeats so the host loses it.
    bench.run_until(600_000, |b| {
        b.firmware.firmware().slots[1].image.len() > 600
            || b.firmware
                .firmware()
                .staging
                .as_ref()
                .is_some_and(|s| s.data.len() > 600)
    });
    let before = bench
        .firmware
        .firmware()
        .staging
        .as_ref()
        .map_or(0, |s| s.data.len());
    assert!(
        before > 0 && before < data.len(),
        "the cut lands mid-transfer"
    );
    bench.down.faults.cut = true;
    bench.up.faults.cut = true;
    let until = bench.now + u64::from(HEARTBEAT_MS) * 4;
    bench.run_until(until, |_| false);
    bench.down.faults.cut = false;
    bench.up.faults.cut = false;
    bench.run_until(600_000, CanBench::finished);

    assert_eq!(bench.xfer.phase(), Phase::Succeeded, "{:?}", bench.replies);
    assert!(bench.lost >= 1, "the host lost the device during the cut");
    assert!(bench.connects >= 2, "the device reconnected");
    assert_eq!(bench.firmware.firmware().slots[1].image, data);
    assert!(bench.plain_serving());
}

#[test]
fn chunk_budget_holds_when_the_device_advertises_a_larger_chunk() {
    // The device would take 400-byte chunks; the budget still caps them on classic CAN.
    let data = image(13, 1_200);
    let mut flash = SimFlash::factory("1.2");
    flash.max_chunk = 400;
    let mut bench = CanBench::new(flash, budgeted_driver(&data, "1.3"), data.clone(), 3);
    bench.xfer.set_confirm_gate(true);
    bench.run_until(600_000, CanBench::finished);

    assert_eq!(bench.xfer.phase(), Phase::Succeeded, "{:?}", bench.replies);
    assert!(
        bench.largest_chunk <= CLASSIC_CHUNK,
        "{}",
        bench.largest_chunk
    );
    assert_eq!(bench.firmware.firmware().slots[1].image, data);
}
