//! Host side of firmware transfer (feature `xfer`): owned device reports and a sans-IO driver that
//! streams an image to a device, resumes it after a reconnect, and follows it through trial boot
//! and confirmation.
//!
//! The driver never reads a clock or touches a link. The gateway feeds it what the device reported
//! ([`FirmwareTransfer::on_info`], [`FirmwareTransfer::on_reply`]), its connection changes
//! ([`FirmwareTransfer::on_connected`], [`FirmwareTransfer::on_lost`]), and the time, and sends
//! what [`FirmwareTransfer::poll`] returns through [`crate::host::HostSession::send_xfer`]. Chunks
//! are read from any `Read + Seek` image (a slice through `io::Cursor`, a file, a blob reader).
//!
//! ```no_run
//! use std::io::Cursor;
//! use orion_link::host::{HostConfig, HostSession};
//! use orion_link::host::xfer::{FirmwareTransfer, XferReply};
//! use orion_link::message::NodeId;
//! use orion_link::wire::firmware::ActivateMode;
//! # fn now_ms() -> u64 { 0 }
//!
//! let mut image = Cursor::new(std::fs::read("app-1.3.bin").unwrap());
//! let mut xfer = FirmwareTransfer::from_image(
//!     7, "motor-app", "1.3", 1, ActivateMode::Trial, &mut image).unwrap();
//! let mut link = HostSession::stream(HostConfig::new(NodeId::new("node-a")));
//! loop {
//!     // Per DeviceConnected { max_frame }: xfer.on_connected(max_frame);
//!     // Per DeviceInfo { info }:           xfer.on_info(info);
//!     // Per Xfer { reply }:                xfer.on_reply(&reply);
//!     xfer.set_confirm_gate(true); // once the device's health gate passes
//!     if let Some(request) = xfer.poll(now_ms(), &mut image).unwrap() {
//!         link.send_xfer(&request);
//!     }
//!     if xfer.is_finished() { break; }
//! }
//! ```

use std::io::{self, Read, Seek, SeekFrom};
use std::string::String;
use std::vec;
use std::vec::Vec;

use crate::crc::{Crc32c, crc32c};
use crate::wire::{
    ActivateMode, DeviceInfoView, RollbackReason, XferAbortView, XferAckView, XferResultView,
    XferStatus, max_chunk_for_frame,
};

/// A trial the device's bootloader rolled back, as the device reported it (owned copy of
/// [`crate::wire::TrialRollback`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Rollback {
    /// Version of the rolled-back trial image.
    pub version: String,
    /// Why the bootloader rolled it back.
    pub reason: RollbackReason,
    /// The device's short note (empty if none).
    pub note: String,
}

/// Device identity and firmware state, as the device reported it (owned copy of
/// [`DeviceInfoView`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeviceInfo {
    /// Running application version.
    pub fw_version: String,
    /// Running slot.
    pub active_slot: u8,
    /// Number of slots.
    pub slots: u8,
    /// Whether the running slot is an unconfirmed trial.
    pub trial: bool,
    /// Largest chunk the device accepts.
    pub xfer_max_chunk: u16,
    /// Chunks in flight allowed (1 is stop-and-wait).
    pub xfer_window: u8,
    /// Bootloader name and version (empty if unknown).
    pub bootloader: String,
    /// The last trial the bootloader rolled back, if it reports one.
    pub rollback: Option<Rollback>,
}

impl DeviceInfo {
    /// Copies a decoded `DeviceInfo` body.
    #[must_use]
    pub fn from_view(view: &DeviceInfoView<'_>) -> Self {
        Self {
            fw_version: String::from(view.fw_version),
            active_slot: view.active_slot,
            slots: view.slots,
            trial: view.trial,
            xfer_max_chunk: view.xfer_max_chunk,
            xfer_window: view.xfer_window,
            bootloader: String::from(view.bootloader),
            rollback: view.rollback.map(|rollback| Rollback {
                version: String::from(rollback.version),
                reason: rollback.reason,
                note: String::from(rollback.note),
            }),
        }
    }
}

/// A device's reply to a transfer request (owned).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum XferReply {
    /// `XferAck`: the cumulative offset and the outcome of the request.
    Ack {
        /// The transfer.
        xfer_id: u32,
        /// Next offset the device expects (the resume point).
        next_offset: u32,
        /// Outcome.
        status: XferStatus,
    },
    /// `XferResult`: the verdict on the whole image.
    Result {
        /// The transfer.
        xfer_id: u32,
        /// `Ok` if verified.
        status: XferStatus,
    },
    /// `XferAbort`: the device stopped the transfer, or a confirmation failed (`xfer_id` 0).
    Abort {
        /// The transfer (0 if not about one).
        xfer_id: u32,
        /// Why.
        reason: XferStatus,
    },
}

impl XferReply {
    /// Decodes an `XferAck` payload.
    pub(crate) fn ack(payload: &[u8]) -> Option<Self> {
        XferAckView::decode(payload).ok().map(|view| Self::Ack {
            xfer_id: view.xfer_id,
            next_offset: view.next_offset,
            status: view.status,
        })
    }

    /// Decodes an `XferResult` payload.
    pub(crate) fn result(payload: &[u8]) -> Option<Self> {
        XferResultView::decode(payload)
            .ok()
            .map(|view| Self::Result {
                xfer_id: view.xfer_id,
                status: view.status,
            })
    }

    /// Decodes an `XferAbort` payload.
    pub(crate) fn abort(payload: &[u8]) -> Option<Self> {
        XferAbortView::decode(payload).ok().map(|view| Self::Abort {
            xfer_id: view.xfer_id,
            reason: view.reason,
        })
    }
}

/// A host → device transfer message, to be sent with [`crate::host::HostSession::send_xfer`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum XferRequest {
    /// `XferBegin`: start or resume.
    Begin {
        /// The transfer id (the same across resumes).
        xfer_id: u32,
        /// Component the image is for.
        component: String,
        /// Version the image carries.
        version: String,
        /// Image length.
        total_len: u32,
        /// Largest chunk the host sends.
        chunk_len: u16,
        /// CRC-32C of the image.
        image_crc32c: u32,
        /// Target slot.
        target_slot: u8,
    },
    /// `XferChunk`.
    Chunk {
        /// The transfer.
        xfer_id: u32,
        /// Offset of `data` in the image.
        offset: u32,
        /// CRC-32C of `data`.
        crc32c: u32,
        /// The bytes.
        data: Vec<u8>,
    },
    /// `XferEnd`: verify.
    End {
        /// The transfer.
        xfer_id: u32,
    },
    /// `Activate`.
    Activate {
        /// The transfer.
        xfer_id: u32,
        /// How to run it.
        mode: ActivateMode,
    },
    /// `Confirm`.
    Confirm {
        /// The slot to keep.
        slot: u8,
    },
}

/// Where a transfer stands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    /// Waiting for `DeviceInfo` and a session, then sending `Begin` (or resuming).
    Begin,
    /// Sending chunks from the device's acknowledged offset.
    Upload,
    /// Sending `End` until the device's verdict arrives.
    Finish,
    /// Sending `Activate`.
    Activate,
    /// Trial boot requested: waiting for the device to come back with the new version.
    AwaitTrial,
    /// Handoff requested: waiting for the link to go quiet (the bootloader takes over).
    AwaitHandoff,
    /// The new version runs on trial; waiting for the health gate, then `Confirm`.
    Confirm,
    /// Finished: the new version runs and is confirmed.
    Succeeded,
    /// Finished: the device hands the port to its bootloader; the gateway releases the link.
    Handoff,
    /// Finished unsuccessfully (see [`Failure`]).
    Failed(Failure),
}

/// Why a transfer failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Failure {
    /// The device refused a step with this status (bad digest, flash error, ...).
    Device(XferStatus),
    /// Too many chunk or request retries without progress.
    Retries,
    /// The device's bootloader rolled the trial back and reported why (see
    /// [`FirmwareTransfer::rollback`] for the device's note).
    RolledBack {
        /// Why the bootloader rolled it back.
        reason: RollbackReason,
    },
    /// The device did not come back running the new version after activation, and reported no
    /// rollback (an older bootloader, or a device without the report). The silent fallback.
    NotActivated,
    /// A deadline passed (no reconnect, or no confirmation).
    Timeout,
}

/// What the image is and how to install it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransferPlan {
    /// Transfer id (chosen by the host; resumes reuse it).
    pub xfer_id: u32,
    /// Component the image is for.
    pub component: String,
    /// Version the image carries; the device reports it once it runs.
    pub version: String,
    /// Slot to write.
    pub target_slot: u8,
    /// Image length.
    pub total_len: u32,
    /// CRC-32C of the image.
    pub image_crc32c: u32,
    /// Largest chunk the host wants to send (the device's and the frame's limits apply too).
    pub chunk_len: u16,
    /// How to run the verified image.
    pub activate: ActivateMode,
}

/// Timing and retry limits of a driver.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DriverConfig {
    /// How long a request waits for its reply before it is sent again.
    pub ack_timeout_ms: u64,
    /// Retries (timeouts and refused chunks) without progress before the transfer fails.
    pub max_retries: u32,
    /// How long a trial boot or confirmation may take.
    pub trial_timeout_ms: u64,
    /// `Activate` sends before the activation counts as not taken.
    pub max_activate_attempts: u32,
}

impl Default for DriverConfig {
    fn default() -> Self {
        Self {
            ack_timeout_ms: 500,
            max_retries: 8,
            trial_timeout_ms: 30_000,
            max_activate_attempts: 3,
        }
    }
}

/// The host's state machine for one firmware transfer. See the [module docs](self).
#[derive(Debug)]
pub struct FirmwareTransfer {
    plan: TransferPlan,
    config: DriverConfig,
    phase: Phase,
    /// Bytes the device has acknowledged.
    acked: u32,
    info: Option<DeviceInfo>,
    /// The rollback the device reported for this transfer's trial (set when it failed that way).
    rollback: Option<Rollback>,
    connected: bool,
    max_frame: usize,
    /// When the request in flight was sent (`None`: nothing in flight).
    pending_since: Option<u64>,
    retries: u32,
    activate_attempts: u32,
    seen_target: bool,
    gate: bool,
    now: u64,
    phase_since: u64,
}

impl FirmwareTransfer {
    /// A driver for `plan`, with the default [`DriverConfig`].
    #[must_use]
    pub fn new(plan: TransferPlan) -> Self {
        Self {
            plan,
            config: DriverConfig::default(),
            phase: Phase::Begin,
            acked: 0,
            info: None,
            rollback: None,
            connected: false,
            max_frame: 0,
            pending_since: None,
            retries: 0,
            activate_attempts: 0,
            seen_target: false,
            gate: false,
            now: 0,
            phase_since: 0,
        }
    }

    /// A driver for an image read from `image`: its length and CRC-32C are computed first (one
    /// read pass; `image` is rewound afterwards).
    ///
    /// # Errors
    ///
    /// Any read error, or an image longer than `u32::MAX` bytes.
    pub fn from_image<R: Read + Seek>(
        xfer_id: u32,
        component: &str,
        version: &str,
        target_slot: u8,
        activate: ActivateMode,
        image: &mut R,
    ) -> io::Result<Self> {
        let (total_len, image_crc32c) = image_digest(image)?;
        Ok(Self::new(TransferPlan {
            xfer_id,
            component: String::from(component),
            version: String::from(version),
            target_slot,
            total_len,
            image_crc32c,
            chunk_len: 128,
            activate,
        }))
    }

    /// Sets the largest chunk the host asks for. The device's limit and the frame's limit still
    /// apply on top (see [`FirmwareTransfer::poll`]); a transport with a segment budget sets this
    /// so a chunk never spans more CAN frames than the budget allows. Call it before the transfer
    /// starts.
    pub fn set_chunk_len(&mut self, chunk_len: u16) {
        self.plan.chunk_len = chunk_len.max(1);
    }

    /// Replaces the timing and retry limits.
    #[must_use]
    pub fn with_config(mut self, config: DriverConfig) -> Self {
        self.config = config;
        self
    }

    /// The current phase.
    #[must_use]
    pub fn phase(&self) -> Phase {
        self.phase
    }

    /// Whether the transfer has finished (succeeded, handed off, or failed).
    #[must_use]
    pub fn is_finished(&self) -> bool {
        matches!(
            self.phase,
            Phase::Succeeded | Phase::Handoff | Phase::Failed(_)
        )
    }

    /// The plan this driver follows.
    #[must_use]
    pub fn plan(&self) -> &TransferPlan {
        &self.plan
    }

    /// Bytes the device has acknowledged, and the image length (progress for `Running`).
    #[must_use]
    pub fn progress(&self) -> (u32, u32) {
        (self.acked, self.plan.total_len)
    }

    /// The device's last reported state.
    #[must_use]
    pub fn device_info(&self) -> Option<&DeviceInfo> {
        self.info.as_ref()
    }

    /// The rollback the device reported for this transfer's trial, when the transfer failed with
    /// [`Failure::RolledBack`] (it carries the device's note, which the failure does not).
    #[must_use]
    pub fn rollback(&self) -> Option<&Rollback> {
        self.rollback.as_ref()
    }

    /// Sets the health gate: the device may be confirmed once this is `true` (after the trial
    /// came up). Until then the driver waits; a trial that stays unconfirmed times out.
    pub fn set_confirm_gate(&mut self, healthy: bool) {
        self.gate = healthy;
    }

    /// A session was (re)established with `max_frame` negotiated. Any request in flight is
    /// forgotten: after a `Begin` the device says where to resume.
    pub fn on_connected(&mut self, max_frame: usize) {
        self.connected = true;
        self.max_frame = max_frame;
        self.pending_since = None;
        self.info = None;
        if matches!(self.phase, Phase::Begin | Phase::Upload | Phase::Finish) {
            self.set_phase(Phase::Begin);
        }
    }

    /// The session ended. A handoff that was requested is complete now.
    pub fn on_lost(&mut self) {
        self.connected = false;
        self.pending_since = None;
        if self.phase == Phase::AwaitHandoff {
            self.set_phase(Phase::Handoff);
        }
    }

    /// The device reported its state.
    pub fn on_info(&mut self, info: DeviceInfo) {
        let at_target = info.fw_version == self.plan.version;
        let trial = info.trial;
        // A rollback report counts for this transfer's trial only, and only while the device runs
        // something else (a report for the running version is stale).
        let reported = info
            .rollback
            .clone()
            .filter(|rollback| rollback.version == self.plan.version && !at_target);
        self.info = Some(info);
        if let Some(rollback) = reported
            && matches!(self.phase, Phase::AwaitTrial | Phase::Confirm)
        {
            let reason = rollback.reason;
            self.rollback = Some(rollback);
            self.fail(Failure::RolledBack { reason });
            return;
        }
        match self.phase {
            Phase::AwaitTrial | Phase::Activate => {
                if at_target {
                    self.seen_target = true;
                    if trial {
                        self.set_phase(Phase::Confirm);
                    } else {
                        self.set_phase(Phase::Succeeded);
                    }
                } else if self.phase == Phase::AwaitTrial {
                    if self.seen_target {
                        self.fail(Failure::NotActivated);
                    } else if self.activate_attempts < self.config.max_activate_attempts {
                        // The activation did not reach the device: send it again.
                        self.set_phase(Phase::Activate);
                    } else {
                        self.fail(Failure::NotActivated);
                    }
                }
            }
            Phase::Confirm => {
                if at_target && !trial {
                    self.set_phase(Phase::Succeeded);
                } else if !at_target {
                    // Back on the old version with no rollback report: the silent fallback.
                    self.fail(Failure::NotActivated);
                }
            }
            // The device may still report (its activation message) before the link goes
            // quiet; the handoff completes on `on_lost`, so reports are not failures here.
            Phase::AwaitHandoff => {}
            _ => {}
        }
    }

    /// The device answered a request.
    pub fn on_reply(&mut self, reply: &XferReply) {
        match *reply {
            XferReply::Ack {
                xfer_id,
                next_offset,
                status,
            } if xfer_id == self.plan.xfer_id => self.on_ack(next_offset, status),
            XferReply::Result { xfer_id, status } if xfer_id == self.plan.xfer_id => {
                if self.phase != Phase::Finish {
                    return;
                }
                self.pending_since = None;
                if status == XferStatus::Ok {
                    self.retries = 0;
                    self.activate_attempts = 0;
                    self.set_phase(Phase::Activate);
                } else {
                    self.fail(Failure::Device(status));
                }
            }
            XferReply::Abort { xfer_id, reason } => {
                let about_us =
                    xfer_id == self.plan.xfer_id || (xfer_id == 0 && self.phase == Phase::Confirm);
                if about_us && !self.is_finished() {
                    self.fail(Failure::Device(reason));
                }
            }
            _ => {}
        }
    }

    fn on_ack(&mut self, next_offset: u32, status: XferStatus) {
        match self.phase {
            Phase::Begin => {
                if status == XferStatus::Ok {
                    self.pending_since = None;
                    self.acked = next_offset.min(self.plan.total_len);
                    self.retries = 0;
                    let phase = if self.acked == self.plan.total_len {
                        Phase::Finish
                    } else {
                        Phase::Upload
                    };
                    self.set_phase(phase);
                } else if status == XferStatus::Busy {
                    self.pending_since = None;
                } else {
                    self.fail(Failure::Device(status));
                }
            }
            Phase::Upload => {
                self.pending_since = None;
                // The device's offset is authoritative, whatever the status.
                self.acked = next_offset.min(self.plan.total_len);
                if status == XferStatus::Ok {
                    self.retries = 0;
                    if self.acked == self.plan.total_len {
                        self.set_phase(Phase::Finish);
                    }
                } else {
                    self.retries += 1;
                    if self.retries > self.config.max_retries {
                        self.fail(Failure::Device(status));
                    }
                }
            }
            _ => {}
        }
    }

    /// The next request to send at time `now_ms`, or `None` if nothing is due. Reads the chunk
    /// from `image` when an upload chunk is due.
    ///
    /// # Errors
    ///
    /// A read error on `image`; the transfer is not failed by it (the caller decides).
    pub fn poll<R: Read + Seek>(
        &mut self,
        now_ms: u64,
        image: &mut R,
    ) -> io::Result<Option<XferRequest>> {
        self.now = now_ms;
        if self.is_finished() {
            return Ok(None);
        }
        if let Some(sent) = self.pending_since
            && now_ms.saturating_sub(sent) >= self.config.ack_timeout_ms
        {
            // The request or its reply was lost: count it and send again below.
            self.pending_since = None;
            self.retries += 1;
            if self.retries > self.config.max_retries {
                self.fail(Failure::Retries);
                return Ok(None);
            }
        }
        if !self.connected {
            return Ok(None);
        }
        match self.phase {
            Phase::Begin => {
                if self.pending_since.is_some() || self.info.is_none() {
                    return Ok(None);
                }
                self.pending_since = Some(now_ms);
                Ok(Some(self.begin_request()))
            }
            Phase::Upload => {
                if self.pending_since.is_some() {
                    return Ok(None);
                }
                let remaining = self.plan.total_len.saturating_sub(self.acked);
                let len = self.chunk_limit().min(remaining as usize);
                if len == 0 {
                    return Ok(None);
                }
                let mut data = vec![0u8; len];
                image.seek(SeekFrom::Start(u64::from(self.acked)))?;
                image.read_exact(&mut data)?;
                self.pending_since = Some(now_ms);
                Ok(Some(XferRequest::Chunk {
                    xfer_id: self.plan.xfer_id,
                    offset: self.acked,
                    crc32c: crc32c(&data),
                    data,
                }))
            }
            Phase::Finish => {
                if self.pending_since.is_some() {
                    return Ok(None);
                }
                self.pending_since = Some(now_ms);
                Ok(Some(XferRequest::End {
                    xfer_id: self.plan.xfer_id,
                }))
            }
            Phase::Activate => {
                self.activate_attempts += 1;
                self.phase_since = now_ms;
                let request = XferRequest::Activate {
                    xfer_id: self.plan.xfer_id,
                    mode: self.plan.activate,
                };
                self.set_phase(match self.plan.activate {
                    ActivateMode::Handoff => Phase::AwaitHandoff,
                    _ => Phase::AwaitTrial,
                });
                Ok(Some(request))
            }
            Phase::AwaitTrial => {
                if now_ms.saturating_sub(self.phase_since) >= self.config.trial_timeout_ms {
                    self.fail(Failure::Timeout);
                }
                Ok(None)
            }
            Phase::Confirm => {
                if now_ms.saturating_sub(self.phase_since) >= self.config.trial_timeout_ms {
                    self.fail(Failure::Timeout);
                    return Ok(None);
                }
                // Confirm only once this session reported its state: after a reconnect the device
                // may have rebooted into a rollback, and the report must come first.
                if !self.gate || self.pending_since.is_some() || self.info.is_none() {
                    return Ok(None);
                }
                self.pending_since = Some(now_ms);
                Ok(Some(XferRequest::Confirm {
                    slot: self.plan.target_slot,
                }))
            }
            Phase::AwaitHandoff | Phase::Succeeded | Phase::Handoff | Phase::Failed(_) => Ok(None),
        }
    }

    fn begin_request(&self) -> XferRequest {
        XferRequest::Begin {
            xfer_id: self.plan.xfer_id,
            component: self.plan.component.clone(),
            version: self.plan.version.clone(),
            total_len: self.plan.total_len,
            chunk_len: self.chunk_limit() as u16,
            image_crc32c: self.plan.image_crc32c,
            target_slot: self.plan.target_slot,
        }
    }

    /// Largest chunk to send: the plan's, the device's, and what one frame can carry.
    fn chunk_limit(&self) -> usize {
        let frame = max_chunk_for_frame(self.max_frame);
        let device = self
            .info
            .as_ref()
            .map_or(usize::MAX, |info| usize::from(info.xfer_max_chunk));
        usize::from(self.plan.chunk_len)
            .min(device)
            .min(frame)
            .max(1)
    }

    fn set_phase(&mut self, phase: Phase) {
        self.phase = phase;
        self.phase_since = self.now;
        self.pending_since = None;
        self.retries = 0;
        if phase == Phase::Confirm {
            self.seen_target = true;
        }
    }

    fn fail(&mut self, failure: Failure) {
        self.phase = Phase::Failed(failure);
        self.pending_since = None;
    }
}

/// Length and CRC-32C of everything `image` holds (rewinds it first and leaves it at the start).
///
/// # Errors
///
/// Any read or seek error, or an image longer than `u32::MAX` bytes.
pub fn image_digest<R: Read + Seek>(image: &mut R) -> io::Result<(u32, u32)> {
    image.seek(SeekFrom::Start(0))?;
    let mut crc = Crc32c::new();
    let mut len: u64 = 0;
    let mut buf = [0u8; 512];
    loop {
        let n = image.read(&mut buf)?;
        if n == 0 {
            break;
        }
        let bytes = buf.get(..n).unwrap_or_default();
        crc.update(bytes);
        len += n as u64;
    }
    image.seek(SeekFrom::Start(0))?;
    let total = u32::try_from(len)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "image larger than 4 GiB"))?;
    Ok((total, crc.finish()))
}
