//! The device session on the two-slot sink, driven by the host's `FirmwareTransfer` over a
//! lossless in-memory link. A power cycle is the bootloader's `boot` on the flash the device left.

use std::io::Cursor;

use orion_link::device::{DeviceConfig, DeviceSession};
use orion_link::host::xfer::{Failure, FirmwareTransfer, Phase, TransferPlan};
use orion_link::host::{HostConfig, HostEvent, HostSession};
use orion_link::message::NodeId;
use orion_link::wire::{ActivateMode, FirmwareSink, RollbackReason};
use orion_link::{Stream, crc32c};
use orion_mcu_template::slots::{NO_SLOT, ResetReason, SlotFirmware, boot};

use crate::flash::{MemFlash, image};

const STEP_MS: u64 = 5;
const LIMIT_MS: u64 = 600_000;

type Device = DeviceSession<Stream, 128, 128, &'static str, 128, SlotFirmware<MemFlash>>;

fn device(flash: MemFlash) -> Device {
    DeviceSession::with_firmware(
        DeviceConfig::provider("motor-a"),
        Stream,
        SlotFirmware::new(flash),
    )
}

/// The device, the host, the transfer under test, and a virtual clock.
struct Bench {
    device: Device,
    host: HostSession<Stream>,
    xfer: FirmwareTransfer,
    image: Cursor<Vec<u8>>,
    now: u64,
    /// Power cycles so far (the activation's reset and any later ones).
    cycles: usize,
}

impl Bench {
    fn new(flash: MemFlash, xfer: FirmwareTransfer, image: Vec<u8>) -> Self {
        let mut config = HostConfig::new(NodeId::new("node-a"));
        config.heartbeat_ms = 1_000;
        Self {
            device: device(flash),
            host: HostSession::stream(config),
            xfer,
            image: Cursor::new(image),
            now: 0,
            cycles: 0,
        }
    }

    /// One turn: the device runs and talks, the host reads and runs the driver, and the bytes
    /// cross the link. A reset requested by the device is a power cycle.
    fn step(&mut self) {
        self.now += STEP_MS;
        self.device.poll(self.now);
        while self.device.next_event().is_some() {}
        let mut down = Vec::new();
        let mut buf = [0u8; 64];
        loop {
            let n = self.device.transmit(&mut buf);
            if n == 0 {
                break;
            }
            down.extend_from_slice(&buf[..n]);
        }
        self.host.receive(&down);

        self.host.poll(self.now);
        while let Some(event) = self.host.next_event() {
            match event {
                HostEvent::DeviceConnected { max_frame, .. } => self.xfer.on_connected(max_frame),
                HostEvent::DeviceInfo { info, .. } => self.xfer.on_info(info),
                HostEvent::Xfer { reply, .. } => self.xfer.on_reply(&reply),
                HostEvent::DeviceLost { .. } => self.xfer.on_lost(),
                _ => {}
            }
        }
        if let Some(request) = self
            .xfer
            .poll(self.now, &mut self.image)
            .expect("image reads")
        {
            assert!(self.host.send_xfer(&request), "request fits the frame");
        }
        let mut up = Vec::new();
        while let Some(bytes) = self.host.transmit() {
            up.extend(bytes);
        }
        self.device.receive(&up);

        if self.device.firmware().flash().reset_requested {
            self.power_cycle(ResetReason::Normal);
        }
    }

    /// The bootloader runs on the flash the device left, and the device starts over on it. The
    /// host sees a new session and resumes or reads the device's state.
    fn power_cycle(&mut self, reason: ResetReason) {
        let mut flash = self.device.firmware().flash().clone();
        flash.reset_requested = false;
        boot(&mut flash, reason).expect("a verified slot starts");
        self.device = device(flash);
        self.cycles += 1;
    }

    fn run_until(&mut self, done: impl Fn(&Bench) -> bool) {
        while !done(self) && self.now < LIMIT_MS {
            self.step();
        }
    }

    fn finished(&self) -> bool {
        self.xfer.is_finished()
    }
}

fn plan(data: &[u8], version: &str) -> TransferPlan {
    TransferPlan {
        xfer_id: 7,
        component: "motor-app".to_owned(),
        version: version.to_owned(),
        target_slot: 1,
        total_len: u32::try_from(data.len()).expect("image length"),
        image_crc32c: crc32c(data),
        chunk_len: 128,
        activate: ActivateMode::Trial,
    }
}

#[test]
fn a_transfer_is_activated_as_a_trial_and_confirmed_by_the_host() {
    let old = image(1, 2_000);
    let new = image(2, 3_000);
    let mut bench = Bench::new(
        MemFlash::with_factory("1.2", &old),
        FirmwareTransfer::new(plan(&new, "1.3")),
        new.clone(),
    );
    bench.xfer.set_confirm_gate(true);
    bench.run_until(Bench::finished);

    assert_eq!(
        bench.xfer.phase(),
        Phase::Succeeded,
        "{:?}",
        bench.xfer.phase()
    );
    assert_eq!(
        bench.cycles, 1,
        "the activation resets the device; the confirm does not"
    );
    let record = bench.device.firmware().record();
    assert_eq!((record.active, record.trial), (1, NO_SLOT));
    assert_eq!(record.rollback, None);
    assert_eq!(bench.device.firmware().info().fw_version, "1.3");
}

#[test]
fn a_trial_reset_before_it_is_confirmed_is_rolled_back_and_reported() {
    let old = image(1, 2_000);
    let new = image(2, 3_000);
    let mut bench = Bench::new(
        MemFlash::with_factory("1.2", &old),
        FirmwareTransfer::new(plan(&new, "1.3")),
        new.clone(),
    );
    // The health gate does not pass: the host never confirms. The device is reset while the
    // trial is up, once it has booted.
    bench.xfer.set_confirm_gate(false);
    bench.run_until(|b| b.device.firmware().record().trial_boots == 1);
    assert_eq!(bench.device.firmware().record().trial, 1, "the trial is up");
    bench.power_cycle(ResetReason::Normal);
    bench.run_until(Bench::finished);

    assert_eq!(
        bench.xfer.phase(),
        Phase::Failed(Failure::RolledBack {
            reason: RollbackReason::NotConfirmed,
        }),
        "{:?}",
        bench.xfer.phase()
    );
    let rollback = bench.xfer.rollback().expect("the device's report is kept");
    assert_eq!(rollback.version, "1.3");
    assert_eq!(rollback.reason, RollbackReason::NotConfirmed);
    assert_eq!(rollback.note, "not confirmed");
    let flash = bench.device.firmware();
    assert_eq!(flash.info().active_slot, 0, "the old slot runs again");
    assert_eq!(flash.info().fw_version, "1.2");
    assert!(!flash.info().trial);
}
