//! Firmware transfer end to end: a simulated device with two A/B slots (feature `xfer`) and the
//! host driver, over a deterministic virtual clock and a link that can drop, corrupt, or cut
//! frames on command.

use std::collections::BTreeSet;
use std::io::Cursor;

use orion_link::device::{DeviceConfig, DeviceSession};
use orion_link::host::xfer::{
    DriverConfig, Failure, FirmwareTransfer, Phase, TransferPlan, XferReply, XferRequest,
};
use orion_link::host::{HostConfig, HostEvent, HostSession};
use orion_link::message::NodeId;
use orion_link::wire::{ActivateMode, RollbackReason, XferStatus, kind};
use orion_link::{Stream, StreamDecoder, crc32c};

use super::flash::SimFlash;
use crate::common::{Rng, stream_bytes};

const RX: usize = 128;
const TX: usize = 128;
const HEARTBEAT_MS: u32 = 1_000;
const STEP_MS: u64 = 5;

/// What the link does to frames, by their ordinal in one direction.
#[derive(Debug, Default, Clone)]
struct Rules {
    /// Drop these frames (ordinals among all frames in this direction).
    drop: BTreeSet<usize>,
    /// Corrupt the data of these `XferChunk` frames (ordinals among chunk frames), keeping a
    /// valid frame CRC, so only the chunk CRC can catch it.
    corrupt_chunks: BTreeSet<usize>,
    /// Lose everything.
    cut: bool,
    frames: usize,
    chunks: usize,
}

/// Applies `rules` to a byte stream by re-framing it: each frame is decoded, then forwarded,
/// dropped, or corrupted.
fn carry(bytes: &[u8], rules: &mut Rules) -> Vec<u8> {
    let mut decoder = StreamDecoder::<512>::new();
    let mut out = Vec::new();
    for &byte in bytes {
        let frame = match decoder.push(byte) {
            Ok(Some(frame)) => (frame.header(), frame.payload().to_vec()),
            _ => continue,
        };
        let (header, mut payload) = frame;
        let ordinal = rules.frames;
        rules.frames += 1;
        if rules.cut || rules.drop.contains(&ordinal) {
            continue;
        }
        if header.kind == kind::XFER_CHUNK {
            let chunk = rules.chunks;
            rules.chunks += 1;
            if rules.corrupt_chunks.contains(&chunk)
                && let Some(last) = payload.last_mut()
            {
                *last ^= 0x5A;
            }
        }
        out.extend(stream_bytes(header, &payload));
    }
    out
}

fn host_config() -> HostConfig {
    let mut config = HostConfig::new(NodeId::new("node-a"));
    config.heartbeat_ms = HEARTBEAT_MS;
    config
}

fn device_config() -> DeviceConfig<&'static str> {
    DeviceConfig::provider("motor-a")
}

/// Deterministic test image of `len` bytes.
fn image(seed: u64, len: usize) -> Vec<u8> {
    let mut rng = Rng::new(seed);
    (0..len).map(|_| rng.byte()).collect()
}

type Device = DeviceSession<Stream, RX, TX, &'static str, TX, SimFlash>;

/// The device, the host, the link, and the transfer under test, on one virtual clock.
struct Bench {
    device: Device,
    host: HostSession<Stream>,
    xfer: FirmwareTransfer,
    image: Cursor<Vec<u8>>,
    now: u64,
    up: Rules,
    down: Rules,
    /// Host events of interest, in order: device replies and state changes.
    replies: Vec<XferReply>,
    infos: Vec<String>,
    connects: usize,
    /// Image bytes the host sent in chunks (resends included).
    chunk_bytes_sent: usize,
    /// Reconnects after the first session (each is a new session with a resume).
    reconnects: usize,
    /// Index in `replies` at the first reconnect.
    replies_at_reconnect: Option<usize>,
}

impl Bench {
    fn new(flash: SimFlash, xfer: FirmwareTransfer, image: Vec<u8>) -> Self {
        Self {
            device: DeviceSession::with_firmware(device_config(), Stream, flash),
            host: HostSession::stream(host_config()),
            xfer,
            image: Cursor::new(image),
            now: 0,
            up: Rules::default(),
            down: Rules::default(),
            replies: Vec::new(),
            infos: Vec::new(),
            connects: 0,
            chunk_bytes_sent: 0,
            reconnects: 0,
            replies_at_reconnect: None,
        }
    }

    /// One 5 ms turn: the device runs and talks, the host reads and runs the driver, and both
    /// sides' bytes cross the (faulty) link.
    fn step(&mut self) {
        self.now += STEP_MS;
        self.device.poll(self.now);
        while self.device.next_event().is_some() {}
        let mut down_bytes = Vec::new();
        let mut buf = [0u8; 64];
        loop {
            let n = self.device.transmit(&mut buf);
            if n == 0 {
                break;
            }
            down_bytes.extend_from_slice(&buf[..n]);
        }
        let to_host = carry(&down_bytes, &mut self.down);
        self.host.receive(&to_host);

        self.host.poll(self.now);
        while let Some(event) = self.host.next_event() {
            match event {
                HostEvent::DeviceConnected { max_frame, .. } => {
                    self.connects += 1;
                    if self.connects > 1 {
                        self.reconnects += 1;
                        self.replies_at_reconnect.get_or_insert(self.replies.len());
                    }
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
                HostEvent::DeviceLost { .. } => self.xfer.on_lost(),
                _ => {}
            }
        }
        if let Some(request) = self
            .xfer
            .poll(self.now, &mut self.image)
            .expect("image reads")
        {
            if let XferRequest::Chunk { data, .. } = &request {
                self.chunk_bytes_sent += data.len();
            }
            assert!(self.host.send_xfer(&request), "request fits the frame");
        }
        let mut up_bytes = Vec::new();
        while let Some(bytes) = self.host.transmit() {
            up_bytes.extend(bytes);
        }
        let to_device = carry(&up_bytes, &mut self.up);
        self.device.receive(&to_device);

        if self.device.firmware().reboot_pending {
            self.reboot_into_new_image();
        }
    }

    /// The device restarts: the bootloader runs, and the session starts over (the host sees a
    /// new session and resumes).
    fn reboot_into_new_image(&mut self) {
        let mut flash = self.device.firmware().clone();
        flash.power_cycle();
        self.device = DeviceSession::with_firmware(device_config(), Stream, flash);
        self.down = Rules {
            frames: self.down.frames,
            chunks: self.down.chunks,
            ..Rules::default()
        };
        self.up = Rules {
            frames: self.up.frames,
            chunks: self.up.chunks,
            ..Rules::default()
        };
    }

    fn run_until(&mut self, limit_ms: u64, done: impl Fn(&Bench) -> bool) {
        while !done(self) && self.now < limit_ms {
            self.step();
        }
    }

    fn finished(&self) -> bool {
        self.xfer.is_finished()
    }
}

fn plan(image: &[u8], version: &str) -> TransferPlan {
    TransferPlan {
        xfer_id: 7,
        component: "motor-app".to_owned(),
        version: version.to_owned(),
        target_slot: 1,
        total_len: image.len() as u32,
        image_crc32c: crc32c(image),
        chunk_len: 128,
        activate: ActivateMode::Trial,
    }
}

fn driver(plan: TransferPlan, config: DriverConfig) -> FirmwareTransfer {
    FirmwareTransfer::new(plan).with_config(config)
}

fn default_driver(plan: TransferPlan) -> FirmwareTransfer {
    driver(plan, DriverConfig::default())
}

#[test]
fn full_transfer_trial_boots_and_is_confirmed() {
    let data = image(1, 3_000);
    let plan = plan(&data, "1.3");
    let mut bench = Bench::new(SimFlash::factory("1.2"), default_driver(plan), data.clone());
    bench.xfer.set_confirm_gate(true);
    bench.run_until(600_000, Bench::finished);

    assert_eq!(bench.xfer.phase(), Phase::Succeeded, "{:?}", bench.replies);
    let flash = bench.device.firmware();
    assert_eq!(flash.active, 1);
    assert_eq!(flash.slots[1].version, "1.3");
    assert_eq!(flash.slots[1].image, data);
    assert!(!flash.trial, "confirmed");
    assert_eq!(bench.infos.last().map(String::as_str), Some("1.3"));
    assert_eq!(
        bench.connects, 2,
        "one session before the reboot, one after"
    );
}

#[test]
fn interrupted_transfer_resumes_from_the_device_offset() {
    let data = image(2, 6_000);
    let plan = plan(&data, "1.3");
    let mut bench = Bench::new(SimFlash::factory("1.2"), default_driver(plan), data.clone());
    bench.xfer.set_confirm_gate(true);
    bench.run_until(600_000, |b| b.xfer.progress().0 >= 2_000);
    let before_cut = bench.xfer.progress().0;

    // The link goes dead long enough for both sides to declare the session lost.
    bench.up.cut = true;
    bench.down.cut = true;
    let resume_from = bench.now + 4_000;
    bench.run_until(resume_from, |_| false);
    let sent_before_resume = bench.chunk_bytes_sent;
    bench.up.cut = false;
    bench.down.cut = false;

    bench.run_until(600_000, Bench::finished);
    assert_eq!(bench.xfer.phase(), Phase::Succeeded, "{:?}", bench.replies);
    assert!(
        bench.reconnects >= 1,
        "the session was lost and re-established"
    );
    // After the reconnect the device answered the Begin with the offset it had stored.
    // The first reply after the reconnect answers the Begin: its offset is the resume point.
    let since = bench.replies_at_reconnect.expect("reconnected");
    let resumed = match bench.replies.get(since) {
        Some(XferReply::Ack { next_offset, .. }) => *next_offset,
        other => panic!("expected the resume ack, got {other:?}"),
    };
    assert!(
        resumed >= before_cut,
        "resumed at {resumed}, cut at {before_cut}"
    );
    // The host did not start over: it sent about the rest of the image after the cut.
    let sent_after_resume = bench.chunk_bytes_sent - sent_before_resume;
    assert!(
        sent_after_resume < data.len(),
        "sent {sent_after_resume} bytes after the resume"
    );
    assert_eq!(bench.device.firmware().slots[1].image, data);
}

#[test]
fn corrupted_chunk_is_refused_and_resent() {
    let data = image(3, 2_000);
    let plan = plan(&data, "1.3");
    let mut bench = Bench::new(SimFlash::factory("1.2"), default_driver(plan), data.clone());
    bench.xfer.set_confirm_gate(true);
    // The fourth chunk frame reaches the device with its data changed and a valid frame CRC.
    bench.up.corrupt_chunks.insert(3);
    bench.run_until(600_000, Bench::finished);

    assert!(
        bench.replies.iter().any(|reply| matches!(
            reply,
            XferReply::Ack {
                status: XferStatus::BadChunkCrc,
                ..
            }
        )),
        "the device refused the chunk: {:?}",
        bench.replies
    );
    assert_eq!(bench.xfer.phase(), Phase::Succeeded);
    assert_eq!(bench.device.firmware().slots[1].image, data);
    // One chunk was sent twice: the image plus the resent chunk.
    assert!(bench.chunk_bytes_sent > data.len());
}

#[test]
fn lost_frames_are_resent_after_the_ack_timeout() {
    let data = image(4, 1_000);
    let plan = plan(&data, "1.3");
    let mut bench = Bench::new(SimFlash::factory("1.2"), default_driver(plan), data.clone());
    bench.xfer.set_confirm_gate(true);
    // Lose two host -> device frames somewhere in the transfer: the ack timeout resends them.
    bench.up.drop.extend([10, 20]);
    bench.run_until(600_000, Bench::finished);
    assert_eq!(bench.xfer.phase(), Phase::Succeeded, "{:?}", bench.replies);
    assert_eq!(bench.device.firmware().slots[1].image, data);
}

#[test]
fn bad_digest_is_rejected_and_nothing_is_activated() {
    let data = image(5, 1_500);
    let mut wrong = plan(&data, "1.3");
    wrong.image_crc32c ^= 1;
    let mut bench = Bench::new(SimFlash::factory("1.2"), default_driver(wrong), data);
    bench.xfer.set_confirm_gate(true);
    bench.run_until(600_000, Bench::finished);

    assert_eq!(
        bench.xfer.phase(),
        Phase::Failed(Failure::Device(XferStatus::BadDigest)),
        "{:?}",
        bench.replies
    );
    let flash = bench.device.firmware();
    assert_eq!(flash.active, 0, "the old slot still runs");
    assert!(!flash.reboot_pending);
}

#[test]
fn trial_without_confirm_rolls_back() {
    let data = image(6, 1_000);
    let plan = plan(&data, "1.3");
    let config = DriverConfig {
        trial_timeout_ms: 2_000,
        ..DriverConfig::default()
    };
    let mut bench = Bench::new(SimFlash::factory("1.2"), driver(plan, config), data);
    // The health gate never passes: the trial is not confirmed.
    bench.xfer.set_confirm_gate(false);
    bench.run_until(600_000, Bench::finished);
    assert_eq!(bench.xfer.phase(), Phase::Failed(Failure::Timeout));

    // The device is still on trial; the bootloader reverts it on the next power cycle.
    assert!(bench.device.firmware().trial);
    // One more power cycle: the unconfirmed trial reverts on its second boot.
    bench.reboot_into_new_image();
    let flash = bench.device.firmware();
    assert_eq!(flash.active, 0);
    assert_eq!(flash.slots[0].version, "1.2");
    assert!(!flash.trial);
}

/// The trial is reset before it confirms (an unhealthy image): the bootloader reverts it on the
/// next boot and reports why, so the host ends `RolledBack` with that reason, not by timeout.
#[test]
fn a_trial_reset_before_confirm_reports_not_confirmed() {
    let data = image(10, 1_000);
    let mut flash = SimFlash::factory("1.2");
    flash.reset_on_confirm = true;
    let mut bench = Bench::new(flash, default_driver(plan(&data, "1.3")), data);
    bench.xfer.set_confirm_gate(true);
    bench.run_until(600_000, Bench::finished);

    assert_eq!(
        bench.xfer.phase(),
        Phase::Failed(Failure::RolledBack {
            reason: RollbackReason::NotConfirmed,
        }),
        "{:?}",
        bench.replies
    );
    let rollback = bench.xfer.rollback().expect("the device's report is kept");
    assert_eq!(rollback.version, "1.3");
    assert_eq!(rollback.reason, RollbackReason::NotConfirmed);
    assert!(rollback.note.is_empty());
    let flash = bench.device.firmware();
    assert_eq!(flash.active, 0, "the old slot runs again");
    assert!(!flash.trial);
}

/// The bootloader reverts the trial on its first boot, before the application runs. The device
/// reports `BootFailure` with its note, and the host does not re-send the activation.
#[test]
fn a_trial_reverted_on_first_boot_reports_boot_failure() {
    let data = image(11, 1_000);
    let mut flash = SimFlash::factory("1.2");
    flash.revert_on_first_boot = true;
    let mut bench = Bench::new(flash, default_driver(plan(&data, "1.3")), data);
    bench.xfer.set_confirm_gate(true);
    bench.run_until(600_000, Bench::finished);

    assert_eq!(
        bench.xfer.phase(),
        Phase::Failed(Failure::RolledBack {
            reason: RollbackReason::BootFailure,
        }),
        "{:?}",
        bench.replies
    );
    let rollback = bench.xfer.rollback().expect("the device's report is kept");
    assert_eq!(rollback.reason, RollbackReason::BootFailure);
    assert_eq!(rollback.note, "boot check failed");
    assert_eq!(bench.device.firmware().active, 0, "the old slot runs again");
}

/// The same first-boot revert from a bootloader that reports nothing: the host falls back to
/// `NotActivated` after its bounded activations.
#[test]
fn a_silent_revert_falls_back_to_not_activated() {
    let data = image(12, 1_000);
    let mut flash = SimFlash::factory("1.2");
    flash.revert_on_first_boot = true;
    flash.silent_revert = true;
    let mut bench = Bench::new(flash, default_driver(plan(&data, "1.3")), data);
    bench.xfer.set_confirm_gate(true);
    bench.run_until(600_000, Bench::finished);

    assert_eq!(
        bench.xfer.phase(),
        Phase::Failed(Failure::NotActivated),
        "{:?}",
        bench.replies
    );
    assert!(bench.xfer.rollback().is_none(), "nothing was reported");
    assert_eq!(bench.device.firmware().active, 0);
}

#[test]
fn begin_refused_for_the_active_slot() {
    let data = image(7, 300);
    let mut plan = plan(&data, "1.3");
    plan.target_slot = 0;
    let mut bench = Bench::new(SimFlash::factory("1.2"), default_driver(plan), data);
    bench.xfer.set_confirm_gate(true);
    bench.run_until(60_000, Bench::finished);
    assert_eq!(
        bench.xfer.phase(),
        Phase::Failed(Failure::Device(XferStatus::BadSlot)),
        "{:?}",
        bench.replies
    );
}

#[test]
fn chunk_requests_fit_the_negotiated_frame() {
    // Frame limit 64 bytes: the driver must size chunks from the frame, not the plan.
    let data = image(8, 500);
    let mut driver = default_driver(plan(&data, "1.3"));
    let mut reader = Cursor::new(data.clone());
    driver.on_connected(64);
    driver.on_info(orion_link::host::xfer::DeviceInfo {
        fw_version: "1.2".to_owned(),
        active_slot: 0,
        slots: 2,
        trial: false,
        xfer_max_chunk: 96,
        xfer_window: 1,
        bootloader: String::new(),
        rollback: None,
    });
    let request = driver
        .poll(0, &mut reader)
        .expect("reads")
        .expect("begin is due");
    assert!(matches!(request, XferRequest::Begin { .. }));
    driver.on_reply(&XferReply::Ack {
        xfer_id: 7,
        next_offset: 0,
        status: XferStatus::Ok,
    });
    let chunk = driver
        .poll(5, &mut reader)
        .expect("reads")
        .expect("chunk due");
    let XferRequest::Chunk { data: bytes, .. } = chunk else {
        panic!("expected a chunk");
    };
    // 64 - 25 bytes of frame and body overhead.
    assert_eq!(bytes.len(), 64 - 25);
}

#[test]
fn device_info_is_reported_once_per_session_and_acked() {
    let data = image(9, 200);
    let mut bench = Bench::new(
        SimFlash::factory("1.2"),
        default_driver(plan(&data, "1.3")),
        data,
    );
    bench.xfer.set_confirm_gate(true);
    bench.run_until(600_000, Bench::finished);
    // One report per distinct state: before the transfer, the trial after the reboot, and the
    // confirmed image (same version, trial cleared).
    assert_eq!(
        bench.infos,
        vec!["1.2".to_owned(), "1.3".to_owned(), "1.3".to_owned()]
    );
    assert_eq!(bench.xfer.phase(), Phase::Succeeded);
}
