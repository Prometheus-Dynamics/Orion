//! Firmware over a serial link and the link release, end to end: a simulated two-slot device
//! (`DeviceSession` with a `FirmwareSink`, on the master side of a pseudo-terminal) runs the node's
//! `link.firmware.update`, `link.release` and `link.attach` actions. No hardware needed.

use super::serial::{Pty, WAIT, open_pty, serial_link, start};
use super::*;
use crate::link_gateway::LinkGatewayHandle;
use orion::NodeId;
use orion::ProviderId;
use orion::control_plane::{
    ActionQuery, ActionRequest, ActionResult, ActionState, ActionTarget, BlobBegin, BlobChunk,
    BlobState, ControlMessage, TypedConfigValue,
};
use orion_link::Stream;
use orion_link::crc32c;
use orion_link::device::{DeviceConfig, DeviceSession, StreamDevice};
use orion_link::wire::{
    ActivateMode, FirmwareInfo, FirmwareSink, RollbackReason, TrialRollback, XferBeginView,
    XferStatus,
};
use sha2::{Digest, Sha256};
use std::os::fd::{AsRawFd, OwnedFd};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::task::JoinHandle;

pub(super) const LINK_DEVICE: &str = "motor-a";
pub(super) const PROVIDER: &str = "provider.motor-a";
pub(super) const RESOURCE: &str = "motor-a.speed";
pub(super) const CHUNK_MAX: usize = 96;

/// One slot of the simulated flash.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Slot {
    pub(super) version: String,
    pub(super) image: Vec<u8>,
}

/// The image being written into the inactive slot.
#[derive(Debug, Clone)]
pub(super) struct Staging {
    xfer_id: u32,
    total: u32,
    crc32c: u32,
    slot: u8,
    version: String,
    data: Vec<u8>,
    verified: bool,
}

/// A trial the bootloader reverted, as the device keeps it for `DeviceInfo`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct RolledBack {
    pub(super) version: String,
    pub(super) reason: RollbackReason,
    pub(super) note: String,
}

/// A two-slot A/B device: flash, a bootloader that reverts an unconfirmed trial on its second
/// boot, and the switches the tests use (a refused confirmation, a handoff to the bootloader).
#[derive(Debug, Clone)]
pub(super) struct Flash {
    pub(super) slots: [Slot; 2],
    pub(super) active: u8,
    previous: u8,
    pub(super) trial: bool,
    trial_boots: u8,
    pub(super) staging: Option<Staging>,
    pub(super) reboot_pending: bool,
    pub(super) handoff: bool,
    refuse_confirm: bool,
    /// The trial's health check fails: the device resets instead of confirming, and the bootloader
    /// reverts the trial on that second boot.
    pub(super) unhealthy_trial: bool,
    /// The bootloader reverts a trial on its first boot, before the host can see the new version.
    pub(super) revert_on_first_boot: bool,
    /// Largest chunk the device advertises.
    pub(super) max_chunk: u16,
    /// Largest chunk written so far (the chunk budget check).
    pub(super) largest_write: usize,
    /// The last trial the bootloader reverted.
    pub(super) rolled_back: Option<RolledBack>,
    /// Whether the device reports a rollback in `DeviceInfo` (false: an older bootloader).
    pub(super) report_rollback: bool,
}

impl Flash {
    pub(super) fn factory(version: &str) -> Self {
        Self {
            slots: [
                Slot {
                    version: version.to_owned(),
                    image: Vec::new(),
                },
                Slot {
                    version: String::new(),
                    image: Vec::new(),
                },
            ],
            active: 0,
            previous: 0,
            trial: false,
            trial_boots: 0,
            staging: None,
            reboot_pending: false,
            handoff: false,
            refuse_confirm: false,
            unhealthy_trial: false,
            revert_on_first_boot: false,
            max_chunk: CHUNK_MAX as u16,
            largest_write: 0,
            rolled_back: None,
            report_rollback: true,
        }
    }

    fn refusing_confirm(mut self) -> Self {
        self.refuse_confirm = true;
        self
    }

    /// The bootloader's part of a power cycle. A reverted trial is recorded with its reason.
    pub(super) fn power_cycle(&mut self) {
        self.reboot_pending = false;
        if self.trial {
            if self.trial_boots >= 1 || self.revert_on_first_boot {
                let (reason, note) = if self.trial_boots >= 1 {
                    (RollbackReason::NotConfirmed, "")
                } else {
                    (RollbackReason::BootFailure, "boot check failed")
                };
                self.rolled_back = Some(RolledBack {
                    version: self.slots[usize::from(self.active)].version.clone(),
                    reason,
                    note: note.to_owned(),
                });
                self.active = self.previous;
                self.trial = false;
                self.trial_boots = 0;
            } else {
                self.trial_boots += 1;
            }
        }
    }
}

impl FirmwareSink for Flash {
    fn info(&self) -> FirmwareInfo<'_> {
        FirmwareInfo {
            fw_version: &self.slots[usize::from(self.active)].version,
            active_slot: self.active,
            slots: 2,
            trial: self.trial,
            max_chunk: self.max_chunk,
            window: 1,
            bootloader: "node-test-boot 1",
        }
    }

    fn last_trial(&self) -> Option<TrialRollback<'_>> {
        if !self.report_rollback {
            return None;
        }
        self.rolled_back.as_ref().map(|rolled| TrialRollback {
            version: &rolled.version,
            reason: rolled.reason,
            note: &rolled.note,
        })
    }

    fn begin(&mut self, begin: &XferBeginView<'_>) -> Result<u32, XferStatus> {
        if begin.target_slot > 1 || begin.target_slot == self.active {
            return Err(XferStatus::BadSlot);
        }
        if let Some(staging) = &self.staging
            && staging.xfer_id == begin.xfer_id
            && staging.total == begin.total_len
            && staging.crc32c == begin.image_crc32c
            && staging.slot == begin.target_slot
        {
            return Ok(staging.data.len() as u32);
        }
        self.staging = Some(Staging {
            xfer_id: begin.xfer_id,
            total: begin.total_len,
            crc32c: begin.image_crc32c,
            slot: begin.target_slot,
            version: begin.version.to_owned(),
            data: Vec::new(),
            verified: false,
        });
        Ok(0)
    }

    fn write(&mut self, offset: u32, data: &[u8]) -> Result<(), XferStatus> {
        let staging = self.staging.as_mut().ok_or(XferStatus::UnknownXfer)?;
        if offset as usize != staging.data.len() {
            return Err(XferStatus::BadOffset);
        }
        if staging.data.len() + data.len() > staging.total as usize {
            return Err(XferStatus::NoSpace);
        }
        staging.data.extend_from_slice(data);
        self.largest_write = self.largest_write.max(data.len());
        Ok(())
    }

    fn finish(&mut self, xfer_id: u32, total_len: u32, crc: u32) -> Result<(), XferStatus> {
        let staging = self.staging.as_mut().ok_or(XferStatus::UnknownXfer)?;
        if staging.xfer_id != xfer_id {
            return Err(XferStatus::UnknownXfer);
        }
        if staging.data.len() != total_len as usize || crc32c(&staging.data) != crc {
            return Err(XferStatus::BadDigest);
        }
        staging.verified = true;
        Ok(())
    }

    fn activate(&mut self, xfer_id: u32, mode: ActivateMode) -> Result<(), XferStatus> {
        let staging = self.staging.as_ref().ok_or(XferStatus::UnknownXfer)?;
        if staging.xfer_id != xfer_id {
            return Err(XferStatus::UnknownXfer);
        }
        if !staging.verified {
            return Err(XferStatus::BadDigest);
        }
        match mode {
            ActivateMode::Trial => {
                let slot = usize::from(staging.slot);
                self.slots[slot] = Slot {
                    version: staging.version.clone(),
                    image: staging.data.clone(),
                };
                self.previous = self.active;
                self.active = staging.slot;
                self.trial = true;
                self.trial_boots = 0;
                self.reboot_pending = true;
                // A new trial starts: the previous rollback is no longer what the device reports.
                self.rolled_back = None;
                Ok(())
            }
            // The bootloader takes over the port: the device goes silent (see the device loop).
            ActivateMode::Handoff => {
                self.handoff = true;
                Ok(())
            }
            _ => Err(XferStatus::Unsupported),
        }
    }

    fn confirm(&mut self, slot: u8) -> Result<(), XferStatus> {
        if self.refuse_confirm || slot != self.active || !self.trial {
            return Err(XferStatus::BadSlot);
        }
        if self.unhealthy_trial {
            // The health check fails: the device resets before it confirms, so the bootloader
            // sees an unconfirmed trial on its next boot.
            self.reboot_pending = true;
            return Ok(());
        }
        self.trial = false;
        Ok(())
    }
}

type Device = StreamDevice<512, 512, String, 512, Flash>;

fn device_config(name: &str) -> DeviceConfig<String> {
    fast_device(name)
}

/// A device on the master side of `pty`, running its own loop; it reboots when its firmware asks
/// and goes silent after a handoff.
struct FirmwareDevice {
    device: Arc<Mutex<Device>>,
    task: JoinHandle<()>,
}

impl FirmwareDevice {
    fn start(pty: Pty, flash: Flash) -> Self {
        let provider = device_provider(PROVIDER);
        let resources = vec![device_resource(RESOURCE, PROVIDER, "rpm")];
        let fresh = {
            let (provider, resources) = (provider.clone(), resources.clone());
            move |flash: Flash| -> Device {
                let mut device =
                    DeviceSession::with_firmware(device_config(LINK_DEVICE), Stream, flash);
                device
                    .publish_provider_state(&provider, &resources)
                    .expect("snapshot fits");
                device
            }
        };
        let device = Arc::new(Mutex::new(fresh(flash)));
        let silent = Arc::new(AtomicBool::new(false));
        let task = {
            let (device, silent) = (device.clone(), silent.clone());
            tokio::spawn(async move {
                let started = Instant::now();
                let mut buf = [0u8; 1024];
                let mut out_buf = [0u8; 256];
                loop {
                    let mut out = Vec::new();
                    {
                        let mut device = device.lock().expect("device lock");
                        loop {
                            let n = read_master(&pty.master, &mut buf);
                            if n == 0 {
                                break;
                            }
                            if !silent.load(Ordering::SeqCst) {
                                device.receive(&buf[..n]);
                            }
                        }
                        device.poll(started.elapsed().as_millis() as u64);
                        while device.next_event().is_some() {}
                        if device.firmware().handoff {
                            silent.store(true, Ordering::SeqCst);
                        }
                        if device.firmware().reboot_pending {
                            let mut flash = device.firmware().clone();
                            flash.power_cycle();
                            *device = fresh(flash);
                        }
                        if !silent.load(Ordering::SeqCst) {
                            loop {
                                let n = device.transmit(&mut out_buf);
                                if n == 0 {
                                    break;
                                }
                                out.extend_from_slice(&out_buf[..n]);
                            }
                        }
                    }
                    if !out.is_empty() {
                        write_master(&pty.master, &out);
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                }
            })
        };
        Self { device, task }
    }

    fn is_connected(&self) -> bool {
        self.device.lock().expect("device lock").is_connected()
    }

    fn flash(&self) -> Flash {
        self.device.lock().expect("device lock").firmware().clone()
    }
}

impl Drop for FirmwareDevice {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn read_master(fd: &OwnedFd, buf: &mut [u8]) -> usize {
    // SAFETY: `buf` is valid for `buf.len()` bytes.
    let n = unsafe { libc::read(fd.as_raw_fd(), buf.as_mut_ptr().cast(), buf.len()) };
    if n > 0 { n.unsigned_abs() } else { 0 }
}

fn write_master(fd: &OwnedFd, mut bytes: &[u8]) {
    let deadline = Instant::now() + std::time::Duration::from_secs(1);
    while !bytes.is_empty() && Instant::now() < deadline {
        // SAFETY: `bytes` is valid for `bytes.len()` bytes.
        let n = unsafe { libc::write(fd.as_raw_fd(), bytes.as_ptr().cast(), bytes.len()) };
        if n > 0 {
            bytes = bytes.get(n.unsigned_abs()..).unwrap_or_default();
        } else {
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
    }
}

/// A node with a blob directory of its own, so blob push works in the test.
pub(super) fn firmware_app(name: &str) -> (NodeApp, PathBuf) {
    let dir = std::env::temp_dir().join(format!("orion-lg-fw-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let socket =
        std::env::temp_dir().join(format!("orion-lg-fw-{name}-{}.sock", std::process::id()));
    let mut config = crate::NodeConfig::for_local_node(NODE).with_ipc_socket_path(socket);
    config.runtime_tuning.blobs.dir = Some(dir.clone());
    let app = NodeApp::try_new(config).expect("node app should build");
    (app, dir)
}

/// Deterministic image bytes.
pub(super) fn image(len: usize) -> Vec<u8> {
    (0..len)
        .map(|index| (index.wrapping_mul(2_654_435_761) >> 11) as u8)
        .collect()
}

/// Commits `data` as blob `id` through the local blob messages (`docs/blobs.md`).
pub(super) fn commit_blob(app: &NodeApp, id: &str, data: &[u8]) {
    let sha256 = Sha256::digest(data)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let begin = BlobBegin {
        blob_id: id.to_owned(),
        size: data.len() as u64,
        sha256,
    };
    app.apply_local_blob_message(ControlMessage::BeginBlob(begin))
        .expect("begin");
    let chunk = BlobChunk {
        blob_id: id.to_owned(),
        offset: 0,
        data: data.to_vec(),
    };
    app.apply_local_blob_message(ControlMessage::WriteBlobChunk(chunk))
        .expect("write");
    app.apply_local_blob_message(ControlMessage::CommitBlob(id.to_owned()))
        .expect("commit");
}

pub(super) fn blob_committed(app: &NodeApp, id: &str) -> bool {
    matches!(
        app.apply_local_blob_message(ControlMessage::QueryBlobs(Some(id.to_owned()))),
        Ok(ControlMessage::Blobs(blobs))
            if blobs.iter().any(|blob| blob.state == BlobState::Committed)
    )
}

pub(super) async fn committed(app: &NodeApp, id: &str, data: &[u8]) {
    commit_blob(app, id, data);
    assert!(
        wait_until(WAIT, || blob_committed(app, id)).await,
        "blob {id} did not commit"
    );
}

pub(super) fn result(app: &NodeApp, action_id: &str) -> Option<ActionResult> {
    app.query_actions(&ActionQuery {
        action_id: Some(action_id.to_owned()),
        ..ActionQuery::default()
    })
    .into_iter()
    .next()
}

/// Waits for `action_id` to finish. Returns the final result and the progress values seen.
pub(super) async fn settle(app: &NodeApp, action_id: &str) -> (ActionResult, Vec<u16>) {
    let deadline = Instant::now() + std::time::Duration::from_secs(20);
    let mut progress = Vec::new();
    loop {
        let current = result(app, action_id).expect("action is tracked");
        if let ActionState::Running {
            progress: Some(value),
        } = current.state
            && progress.last() != Some(&value)
        {
            progress.push(value);
        }
        if current.state.is_terminal() {
            return (current, progress);
        }
        assert!(
            Instant::now() < deadline,
            "action {action_id} did not finish"
        );
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }
}

pub(super) fn update_request(
    action_id: &str,
    link: &str,
    blob: &str,
    version: &str,
    activate: Option<&str>,
) -> ActionRequest {
    let mut request = ActionRequest::new(
        action_id,
        ActionTarget::Node(NodeId::new(NODE)),
        "link.firmware.update",
    )
    .with_arg("link", TypedConfigValue::String(link.to_owned()))
    .with_arg("device", TypedConfigValue::String(LINK_DEVICE.to_owned()))
    .with_arg("blob", TypedConfigValue::String(blob.to_owned()))
    .with_arg(
        "component",
        TypedConfigValue::String("motor-app".to_owned()),
    )
    .with_arg("version", TypedConfigValue::String(version.to_owned()))
    .with_arg("target_slot", TypedConfigValue::UInt(1));
    if let Some(mode) = activate {
        request = request.with_arg("activate", TypedConfigValue::String(mode.to_owned()));
    }
    request
}

pub(super) fn link_request(action_id: &str, name: &str, link: &str) -> ActionRequest {
    ActionRequest::new(
        action_id,
        ActionTarget::Node(NodeId::new(NODE)),
        name.to_owned(),
    )
    .with_arg("link", TypedConfigValue::String(link.to_owned()))
    .with_arg("device", TypedConfigValue::String(LINK_DEVICE.to_owned()))
}

/// The node with `motor-a` connected on a serial link, its provider published.
async fn setup(
    name: &str,
    flash: Flash,
) -> (NodeApp, FirmwareDevice, LinkGatewayHandle, String, PathBuf) {
    let (app, dir) = firmware_app(name);
    let pty = open_pty();
    let link = serial_link(&pty, &format!("&allow={LINK_DEVICE}"));
    let link_name = link.name.clone();
    let gateway = start(&app, vec![link]);
    let device = FirmwareDevice::start(pty, flash);
    assert!(
        wait_until(WAIT, || has_provider(&app, PROVIDER)
            && device.is_connected())
        .await,
        "device did not connect"
    );
    (app, device, gateway, link_name, dir)
}

pub(super) fn output_text(result: &ActionResult, key: &str) -> Option<String> {
    match result.output.get(key) {
        Some(TypedConfigValue::String(value)) => Some(value.clone()),
        _ => None,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn trial_update_is_transferred_activated_confirmed_and_reported() {
    let (app, device, gateway, link, dir) = setup("fw-trial", Flash::factory("1.2")).await;
    let data = image(3_000);
    committed(&app, "app-1-3", &data).await;

    app.run_action(
        update_request("fw-trial-1", &link, "app-1-3", "1.3", None),
        "test",
    )
    .expect("accepted");
    let (done, progress) = settle(&app, "fw-trial-1").await;

    assert_eq!(done.state, ActionState::Succeeded, "{done:?}");
    assert_eq!(output_text(&done, "phase").as_deref(), Some("installed"));
    assert_eq!(output_text(&done, "version").as_deref(), Some("1.3"));
    assert_eq!(
        done.output.get("active_slot"),
        Some(&TypedConfigValue::UInt(1))
    );
    assert!(
        progress.iter().any(|value| *value > 0 && *value < 1000),
        "progress is reported while the image moves: {progress:?}"
    );
    let flash = device.flash();
    assert_eq!(flash.active, 1, "the new slot runs");
    assert_eq!(flash.slots[1].version, "1.3");
    assert_eq!(flash.slots[1].image, data, "the device holds the image");
    assert!(!flash.trial, "confirmed by the node after the health gate");

    gateway.shutdown().await;
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_refused_confirmation_fails_the_action_and_the_trial_stays_open() {
    let (app, device, gateway, link, dir) =
        setup("fw-refused", Flash::factory("1.2").refusing_confirm()).await;
    committed(&app, "app-1-3", &image(1_500)).await;

    app.run_action(
        update_request("fw-refuse-1", &link, "app-1-3", "1.3", None),
        "test",
    )
    .expect("accepted");
    let (done, _) = settle(&app, "fw-refuse-1").await;

    match &done.state {
        ActionState::Failed { reason } => {
            assert!(reason.contains("refused"), "reason: {reason}");
        }
        other => panic!("expected Failed, got {other:?}"),
    }
    let flash = device.flash();
    assert!(
        flash.trial,
        "the trial is not confirmed; the bootloader reverts it"
    );
    gateway.shutdown().await;
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_handoff_update_succeeds_and_leaves_the_link_released() {
    let (app, device, gateway, link, dir) = setup("fw-handoff", Flash::factory("1.2")).await;
    committed(&app, "app-1-3", &image(1_000)).await;

    app.run_action(
        update_request("fw-handoff-1", &link, "app-1-3", "1.3", Some("handoff")),
        "test",
    )
    .expect("accepted");
    let (done, _) = settle(&app, "fw-handoff-1").await;

    assert_eq!(done.state, ActionState::Succeeded, "{done:?}");
    assert_eq!(output_text(&done, "phase").as_deref(), Some("handoff"));
    assert!(
        wait_until(WAIT, || !app.link_status()[0].open).await,
        "the link is released after the handoff"
    );
    // The bootloader owns the port now: the node must not reopen it.
    tokio::time::sleep(std::time::Duration::from_millis(1_200)).await;
    assert!(!app.link_status()[0].open, "a handed-off link stays closed");
    assert!(!device.is_connected());
    gateway.shutdown().await;
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn release_stops_the_port_and_attach_brings_the_device_back() {
    let (app, device, gateway, link, dir) = setup("fw-release", Flash::factory("1.2")).await;
    assert!(
        observed_resource(&app, RESOURCE)
            .is_some_and(|r| r.availability == AvailabilityState::Available)
    );

    app.run_action(link_request("rel-1", "link.release", &link), "test")
        .expect("accepted");
    let (done, _) = settle(&app, "rel-1").await;
    assert_eq!(done.state, ActionState::Succeeded, "{done:?}");
    assert_eq!(output_text(&done, "state").as_deref(), Some("released"));
    assert!(
        wait_until(WAIT, || !app.link_status()[0].open).await,
        "the port is closed"
    );
    assert!(
        wait_until(WAIT, || observed_resource(&app, RESOURCE)
            .is_some_and(|r| r.availability == AvailabilityState::Unavailable))
        .await,
        "the device's providers become unavailable"
    );

    // Device actions are refused while the link is released.
    let refused = app
        .run_action(
            ActionRequest::new(
                "rel-locate",
                ActionTarget::Provider(ProviderId::new(PROVIDER)),
                "locate",
            ),
            "test",
        )
        .expect("accepted");
    assert!(matches!(refused.state, ActionState::Rejected { .. }));

    // The link does not reopen on its own.
    tokio::time::sleep(std::time::Duration::from_millis(1_200)).await;
    assert!(
        !app.link_status()[0].open,
        "a released link does not reopen"
    );
    assert!(!device.is_connected());

    app.run_action(link_request("att-1", "link.attach", &link), "test")
        .expect("accepted");
    let (attached, _) = settle(&app, "att-1").await;
    assert_eq!(attached.state, ActionState::Succeeded, "{attached:?}");
    assert_eq!(output_text(&attached, "state").as_deref(), Some("attached"));
    assert!(
        wait_until(WAIT, || {
            app.link_status()[0].open
                && observed_resource(&app, RESOURCE)
                    .is_some_and(|r| r.availability == AvailabilityState::Available)
        })
        .await,
        "attach reopens the port and the device comes back"
    );
    gateway.shutdown().await;
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn firmware_requests_are_refused_before_a_transfer_starts() {
    let (app, dir) = firmware_app("fw-refuse");
    // An uncommitted blob, and a blob that is committed but on a link that is not running.
    app.run_action(
        update_request("no-blob", "serial:/dev/none", "missing", "1.3", None),
        "test",
    )
    .expect("accepted");
    assert!(matches!(
        settle(&app, "no-blob").await.0.state,
        ActionState::Rejected { reason } if reason.contains("no blob")
    ));

    committed(&app, "app-1-3", &image(256)).await;
    app.run_action(
        update_request("not-running", "serial:/dev/none", "app-1-3", "1.3", None),
        "test",
    )
    .expect("accepted");
    assert!(matches!(
        settle(&app, "not-running").await.0.state,
        ActionState::Rejected { reason } if reason.contains("not running")
    ));

    // Release is refused for a device the node has never seen on the link.
    app.run_action(
        link_request("unknown-dev", "link.release", "can:vcan0"),
        "test",
    )
    .expect("accepted");
    assert!(matches!(
        settle(&app, "unknown-dev").await.0.state,
        ActionState::Rejected { .. }
    ));
    let _ = std::fs::remove_dir_all(dir);
}

/// The trial's health check fails: the device resets instead of confirming, and its bootloader
/// reverts the trial on that second boot. The device comes back on the old version, which the host
/// reports as a rollback.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_trial_that_fails_its_health_check_rolls_back_and_the_action_says_so() {
    let mut flash = Flash::factory("1.2");
    flash.unhealthy_trial = true;
    let (app, device, gateway, link, dir) = setup("fw-rollback", flash).await;
    committed(&app, "app-1-3", &image(1_500)).await;

    app.run_action(
        update_request("fw-rollback-1", &link, "app-1-3", "1.3", None),
        "test",
    )
    .expect("accepted");
    let (done, _) = settle(&app, "fw-rollback-1").await;

    match &done.state {
        ActionState::Failed { reason } => {
            assert!(
                reason.contains("rolled back") && reason.contains("not confirmed"),
                "reason: {reason}"
            );
        }
        other => panic!("expected Failed, got {other:?}"),
    }
    assert_eq!(
        output_text(&done, "rollback_reason").as_deref(),
        Some("not_confirmed"),
        "the device's report is in the result: {done:?}"
    );
    assert!(
        wait_until(WAIT, || device.is_connected()).await,
        "the device comes back on its previous image"
    );
    let flash = device.flash();
    assert_eq!(
        flash.active, 0,
        "the bootloader reverted to the previous slot"
    );
    assert_eq!(flash.slots[0].version, "1.2", "the old version runs");
    assert!(!flash.trial, "the reverted image is not on trial");
    gateway.shutdown().await;
    let _ = std::fs::remove_dir_all(dir);
}

/// The bootloader reverts a trial on its first boot, before the host ever sees the new version,
/// and reports it: the action fails `rolled back` with the bootloader's reason and note.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_trial_reverted_on_first_boot_reports_its_reason() {
    let mut flash = Flash::factory("1.2");
    flash.revert_on_first_boot = true;
    let (app, device, gateway, link, dir) = setup("fw-first-boot", flash).await;
    committed(&app, "app-1-3", &image(1_500)).await;

    app.run_action(
        update_request("fw-first-boot-1", &link, "app-1-3", "1.3", None),
        "test",
    )
    .expect("accepted");
    let (done, _) = settle(&app, "fw-first-boot-1").await;

    match &done.state {
        ActionState::Failed { reason } => {
            assert!(
                reason.contains("rolled back") && reason.contains("did not boot"),
                "reason: {reason}"
            );
        }
        other => panic!("expected Failed, got {other:?}"),
    }
    assert_eq!(
        output_text(&done, "rollback_reason").as_deref(),
        Some("boot_failure")
    );
    assert_eq!(
        output_text(&done, "rollback_note").as_deref(),
        Some("boot check failed")
    );
    assert!(
        wait_until(WAIT, || device.is_connected()).await,
        "the device comes back on its previous image"
    );
    assert_eq!(device.flash().active, 0, "the old slot runs");
    gateway.shutdown().await;
    let _ = std::fs::remove_dir_all(dir);
}

/// The same first-boot revert from a bootloader that reports nothing: the host has no rollback to
/// name, so it re-sends the activation a bounded number of times and then reports the image as not
/// activated. The device still runs the old version.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_silent_revert_is_reported_as_not_activated() {
    let mut flash = Flash::factory("1.2");
    flash.revert_on_first_boot = true;
    flash.report_rollback = false;
    let (app, device, gateway, link, dir) = setup("fw-reverted", flash).await;
    committed(&app, "app-1-3", &image(1_500)).await;

    app.run_action(
        update_request("fw-reverted-1", &link, "app-1-3", "1.3", None),
        "test",
    )
    .expect("accepted");
    let (done, _) = settle(&app, "fw-reverted-1").await;

    match &done.state {
        ActionState::Failed { reason } => {
            assert!(
                reason.contains("did not come back running the new version"),
                "reason: {reason}"
            );
        }
        other => panic!("expected Failed, got {other:?}"),
    }
    assert_eq!(output_text(&done, "rollback_reason"), None);
    let flash = device.flash();
    assert_eq!(flash.active, 0, "the old slot runs");
    assert_eq!(flash.slots[0].version, "1.2");
    gateway.shutdown().await;
    let _ = std::fs::remove_dir_all(dir);
}
