//! Firmware transfer bodies (kinds `0x20`-`0x28`, see `docs/link-protocol.md`, "Firmware
//! transfer"): borrowed views with postcard-compatible encoders and decoders, plus the device
//! side's [`FirmwareSink`] trait.
//!
//! Like the rest of [`crate::wire`] these need no allocator. The bodies are postcard encodings: a
//! `u32` is a varint, a `u8` or `bool` one raw byte, strings and byte strings are length
//! prefixed. Only the session logic that acts on them is behind the `xfer` feature; the types and
//! codecs here are dead-stripped from a build that does not use them.

use super::WireError;
use super::codec::{Encode, Reader, Writer};

/// Result of a transfer step, one byte on the wire. Unknown codes decode to [`Self::Other`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum XferStatus {
    /// The step succeeded.
    Ok,
    /// The chunk's CRC-32C did not match its data; resend the chunk.
    BadChunkCrc,
    /// The offset is not the next expected one (ahead of it), or the chunk leaves the image.
    BadOffset,
    /// The whole image failed its length or CRC-32C check, or its signature did.
    BadDigest,
    /// Writing or erasing the target slot failed.
    Flash,
    /// The image does not fit the target slot.
    NoSpace,
    /// The device is busy with something else (for example an activation).
    Busy,
    /// The request is not supported by this device (for example an unknown slot or mode).
    Unsupported,
    /// No transfer with that id is known to the device.
    UnknownXfer,
    /// The target slot is the active one, or does not exist.
    BadSlot,
    /// A status code this build does not know.
    Other(u8),
}

impl XferStatus {
    /// Wire code.
    #[must_use]
    pub const fn code(self) -> u8 {
        match self {
            Self::Ok => 0,
            Self::BadChunkCrc => 1,
            Self::BadOffset => 2,
            Self::BadDigest => 3,
            Self::Flash => 4,
            Self::NoSpace => 5,
            Self::Busy => 6,
            Self::Unsupported => 7,
            Self::UnknownXfer => 8,
            Self::BadSlot => 9,
            Self::Other(code) => code,
        }
    }

    /// Parses a wire code.
    #[must_use]
    pub const fn from_code(code: u8) -> Self {
        match code {
            0 => Self::Ok,
            1 => Self::BadChunkCrc,
            2 => Self::BadOffset,
            3 => Self::BadDigest,
            4 => Self::Flash,
            5 => Self::NoSpace,
            6 => Self::Busy,
            7 => Self::Unsupported,
            8 => Self::UnknownXfer,
            9 => Self::BadSlot,
            other => Self::Other(other),
        }
    }
}

impl Encode for XferStatus {
    fn encode(&self, w: &mut Writer<'_>) {
        w.byte(self.code());
    }
}

/// How the device should run a verified image, in `XferActivate` (`Activate`, kind `0x27`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ActivateMode {
    /// Boot the new slot on trial: it stays on trial until a `Confirm`, and the device's
    /// bootloader reverts to the previous slot if none arrives.
    Trial,
    /// Hand the port to the device's resident bootloader, which takes over the write and the
    /// slot switch (the single-slot case; the gateway releases the link).
    Handoff,
    /// A mode this build does not know.
    Other(u8),
}

impl ActivateMode {
    /// Wire code.
    #[must_use]
    pub const fn code(self) -> u8 {
        match self {
            Self::Trial => 0,
            Self::Handoff => 1,
            Self::Other(code) => code,
        }
    }

    /// Parses a wire code.
    #[must_use]
    pub const fn from_code(code: u8) -> Self {
        match code {
            0 => Self::Trial,
            1 => Self::Handoff,
            other => Self::Other(other),
        }
    }
}

/// Why the device's bootloader rolled a trial back, in [`TrialRollback`]. One byte on the wire.
/// Unknown codes decode to [`Self::Other`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RollbackReason {
    /// The trial was not confirmed before the device rebooted (a reset, or the watchdog).
    NotConfirmed,
    /// The new image did not boot (a fault or a hang before the application was up).
    BootFailure,
    /// The new image failed verification at boot (its length, CRC-32C or signature).
    VerifyFailed,
    /// The application asked for the rollback.
    Requested,
    /// Any other reason, or a code this build does not know.
    Other(u8),
}

impl RollbackReason {
    /// Wire code.
    #[must_use]
    pub const fn code(self) -> u8 {
        match self {
            Self::NotConfirmed => 1,
            Self::BootFailure => 2,
            Self::VerifyFailed => 3,
            Self::Requested => 4,
            Self::Other(code) => code,
        }
    }

    /// Parses a wire code.
    #[must_use]
    pub const fn from_code(code: u8) -> Self {
        match code {
            1 => Self::NotConfirmed,
            2 => Self::BootFailure,
            3 => Self::VerifyFailed,
            4 => Self::Requested,
            other => Self::Other(other),
        }
    }

    /// A stable name for logs and results (`other` for [`Self::Other`]).
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::NotConfirmed => "not_confirmed",
            Self::BootFailure => "boot_failure",
            Self::VerifyFailed => "verify_failed",
            Self::Requested => "requested",
            Self::Other(_) => "other",
        }
    }
}

/// Longest trial version a [`TrialRollback`] carries, in bytes (the host's own version names fit).
pub const MAX_ROLLBACK_VERSION_BYTES: usize = 64;

/// Longest note a [`TrialRollback`] carries, in bytes.
pub const MAX_ROLLBACK_NOTE_BYTES: usize = 32;

/// The device's report that its bootloader rolled a trial back: the image of `version` was not
/// kept, and the device runs the previous slot again. Sent in [`DeviceInfoView::rollback`] until
/// the next trial starts. `note` is free text for the operator (empty if none).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TrialRollback<'a> {
    /// The version of the rolled-back trial image.
    pub version: &'a str,
    /// Why it was rolled back.
    pub reason: RollbackReason,
    /// Short free text (see [`MAX_ROLLBACK_NOTE_BYTES`]).
    pub note: &'a str,
}

impl<'a> TrialRollback<'a> {
    /// The report with its strings cut to the wire bounds, at a character boundary.
    #[must_use]
    pub fn clamped(self) -> Self {
        Self {
            version: clamp_str(self.version, MAX_ROLLBACK_VERSION_BYTES),
            reason: self.reason,
            note: clamp_str(self.note, MAX_ROLLBACK_NOTE_BYTES),
        }
    }
}

fn clamp_str(text: &str, max: usize) -> &str {
    if text.len() <= max {
        return text;
    }
    let mut end = max;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    text.get(..end).unwrap_or_default()
}

impl Encode for TrialRollback<'_> {
    fn encode(&self, w: &mut Writer<'_>) {
        self.version.encode(w);
        w.byte(self.reason.code());
        self.note.encode(w);
    }
}

/// Largest chunk that fits a receive buffer of `rx` bytes once the chunk frame's own overhead is
/// subtracted: frame header and CRC (8), and a worst-case `XferChunk` body prefix (three `u32`
/// varints of 5 bytes each and a 2-byte length prefix).
#[must_use]
pub const fn max_chunk_for_frame(rx: usize) -> usize {
    rx.saturating_sub(8 + 17)
}

/// Device identity and firmware state. Device → host, kind [`super::kind::DEVICE_INFO`]; sent
/// after `Welcome` and again after every change of the firmware state, until the host acknowledges
/// it with `Ack`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeviceInfoView<'a> {
    /// Version of the running application (the host compares it with the image it sent).
    pub fw_version: &'a str,
    /// The slot the device runs from.
    pub active_slot: u8,
    /// Number of image slots (1 for a single-slot device, 2 for A/B).
    pub slots: u8,
    /// Whether the running slot is on trial and not yet confirmed.
    pub trial: bool,
    /// Largest `XferChunk` data the device accepts.
    pub xfer_max_chunk: u16,
    /// Chunks the device accepts before it must be acknowledged (1 is stop-and-wait).
    pub xfer_window: u8,
    /// The resident bootloader's name and version, if it reports one (empty otherwise).
    pub bootloader: &'a str,
    /// The last trial the bootloader rolled back, if it reverted one (see [`TrialRollback`]).
    pub rollback: Option<TrialRollback<'a>>,
}

impl Encode for DeviceInfoView<'_> {
    fn encode(&self, w: &mut Writer<'_>) {
        self.fw_version.encode(w);
        self.active_slot.encode(w);
        self.slots.encode(w);
        self.trial.encode(w);
        self.xfer_max_chunk.encode(w);
        self.xfer_window.encode(w);
        self.bootloader.encode(w);
        self.rollback.encode(w);
    }
}

impl<'a> DeviceInfoView<'a> {
    /// Decodes a `DeviceInfo` body.
    ///
    /// # Errors
    ///
    /// [`WireError::Decode`] if the payload is not a valid body.
    pub fn decode(payload: &'a [u8]) -> Result<Self, WireError> {
        let mut r = Reader::new(payload);
        Ok(Self {
            fw_version: r.str()?,
            active_slot: r.byte()?,
            slots: r.byte()?,
            trial: read_bool(&mut r)?,
            xfer_max_chunk: r.varint_u16()?,
            xfer_window: r.byte()?,
            bootloader: r.str()?,
            rollback: read_rollback(&mut r)?,
        })
    }
}

/// Starts a transfer, or resumes one after a reconnect. Host → device, kind
/// [`super::kind::XFER_BEGIN`]. The device answers with an [`XferAckView`] whose `next_offset` is
/// where the host continues.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct XferBeginView<'a> {
    /// Host-chosen transfer id. Resuming uses the same id.
    pub xfer_id: u32,
    /// The component the image is for (free-form, for the device's own bookkeeping).
    pub component: &'a str,
    /// The version the image carries; `DeviceInfo` reports it once the image runs.
    pub version: &'a str,
    /// Image length in bytes.
    pub total_len: u32,
    /// Largest chunk the host will send.
    pub chunk_len: u16,
    /// CRC-32C of the whole image (see the digest choice in `docs/link-protocol.md`).
    pub image_crc32c: u32,
    /// The slot to write (the device refuses the active one).
    pub target_slot: u8,
}

impl Encode for XferBeginView<'_> {
    fn encode(&self, w: &mut Writer<'_>) {
        self.xfer_id.encode(w);
        self.component.encode(w);
        self.version.encode(w);
        self.total_len.encode(w);
        self.chunk_len.encode(w);
        self.image_crc32c.encode(w);
        self.target_slot.encode(w);
    }
}

impl<'a> XferBeginView<'a> {
    /// Decodes an `XferBegin` body.
    ///
    /// # Errors
    ///
    /// [`WireError::Decode`] if the payload is not a valid body.
    pub fn decode(payload: &'a [u8]) -> Result<Self, WireError> {
        let mut r = Reader::new(payload);
        Ok(Self {
            xfer_id: r.varint_u32()?,
            component: r.str()?,
            version: r.str()?,
            total_len: r.varint_u32()?,
            chunk_len: r.varint_u16()?,
            image_crc32c: r.varint_u32()?,
            target_slot: r.byte()?,
        })
    }
}

/// One chunk of the image at `offset`. Host → device, kind [`super::kind::XFER_CHUNK`]. Chunks are
/// offset-addressed, so a repeated chunk is harmless.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct XferChunkView<'a> {
    /// The transfer this chunk belongs to.
    pub xfer_id: u32,
    /// Offset of `data` in the image.
    pub offset: u32,
    /// CRC-32C of `data`; a mismatch is answered with `BadChunkCrc` and the chunk is resent.
    pub crc32c: u32,
    /// The image bytes.
    pub data: &'a [u8],
}

impl Encode for XferChunkView<'_> {
    fn encode(&self, w: &mut Writer<'_>) {
        self.xfer_id.encode(w);
        self.offset.encode(w);
        self.crc32c.encode(w);
        w.bytes(self.data);
    }
}

impl<'a> XferChunkView<'a> {
    /// Decodes an `XferChunk` body; `data` borrows from the payload.
    ///
    /// # Errors
    ///
    /// [`WireError::Decode`] if the payload is not a valid body.
    pub fn decode(payload: &'a [u8]) -> Result<Self, WireError> {
        let mut r = Reader::new(payload);
        Ok(Self {
            xfer_id: r.varint_u32()?,
            offset: r.varint_u32()?,
            crc32c: r.varint_u32()?,
            data: r.bytes()?,
        })
    }
}

/// Acknowledges a chunk (or a `Begin`). Device → host, kind [`super::kind::XFER_ACK`]. `next_offset`
/// is cumulative: every byte before it is stored. After a non-`Ok` status the host resends from
/// `next_offset`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct XferAckView {
    /// The transfer.
    pub xfer_id: u32,
    /// The next offset the device expects.
    pub next_offset: u32,
    /// Outcome of the chunk that prompted the ack.
    pub status: XferStatus,
}

impl Encode for XferAckView {
    fn encode(&self, w: &mut Writer<'_>) {
        self.xfer_id.encode(w);
        self.next_offset.encode(w);
        self.status.encode(w);
    }
}

impl XferAckView {
    /// Decodes an `XferAck` body.
    ///
    /// # Errors
    ///
    /// [`WireError::Decode`] if the payload is not a valid body.
    pub fn decode(payload: &[u8]) -> Result<Self, WireError> {
        let mut r = Reader::new(payload);
        Ok(Self {
            xfer_id: r.varint_u32()?,
            next_offset: r.varint_u32()?,
            status: XferStatus::from_code(r.byte()?),
        })
    }
}

/// Ends the image: asks the device to verify it. Host → device, kind [`super::kind::XFER_END`].
/// Answered with [`XferResultView`]; a repeated `End` gets the same answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct XferEndView {
    /// The transfer.
    pub xfer_id: u32,
}

impl Encode for XferEndView {
    fn encode(&self, w: &mut Writer<'_>) {
        self.xfer_id.encode(w);
    }
}

impl XferEndView {
    /// Decodes an `XferEnd` body.
    ///
    /// # Errors
    ///
    /// [`WireError::Decode`] if the payload is not a valid body.
    pub fn decode(payload: &[u8]) -> Result<Self, WireError> {
        Ok(Self {
            xfer_id: Reader::new(payload).varint_u32()?,
        })
    }
}

/// The device's verdict on the whole image. Device → host, kind [`super::kind::XFER_RESULT`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct XferResultView {
    /// The transfer.
    pub xfer_id: u32,
    /// `Ok` if the image is complete and verified.
    pub status: XferStatus,
}

impl Encode for XferResultView {
    fn encode(&self, w: &mut Writer<'_>) {
        self.xfer_id.encode(w);
        self.status.encode(w);
    }
}

impl XferResultView {
    /// Decodes an `XferResult` body.
    ///
    /// # Errors
    ///
    /// [`WireError::Decode`] if the payload is not a valid body.
    pub fn decode(payload: &[u8]) -> Result<Self, WireError> {
        let mut r = Reader::new(payload);
        Ok(Self {
            xfer_id: r.varint_u32()?,
            status: XferStatus::from_code(r.byte()?),
        })
    }
}

/// Stops a transfer, or reports that the device gave up on one. Either direction, kind
/// [`super::kind::XFER_ABORT`]. `xfer_id` 0 means "not about a transfer" (a `Confirm` that failed).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct XferAbortView {
    /// The transfer (0 if none).
    pub xfer_id: u32,
    /// Why.
    pub reason: XferStatus,
}

impl Encode for XferAbortView {
    fn encode(&self, w: &mut Writer<'_>) {
        self.xfer_id.encode(w);
        self.reason.encode(w);
    }
}

impl XferAbortView {
    /// Decodes an `XferAbort` body.
    ///
    /// # Errors
    ///
    /// [`WireError::Decode`] if the payload is not a valid body.
    pub fn decode(payload: &[u8]) -> Result<Self, WireError> {
        let mut r = Reader::new(payload);
        Ok(Self {
            xfer_id: r.varint_u32()?,
            reason: XferStatus::from_code(r.byte()?),
        })
    }
}

/// Activates a verified image. Host → device, kind [`super::kind::ACTIVATE`]. For
/// [`ActivateMode::Trial`] the device reboots into the slot; its next `DeviceInfo` shows whether
/// it runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ActivateView {
    /// The transfer whose image to run.
    pub xfer_id: u32,
    /// How to run it.
    pub mode: ActivateMode,
}

impl Encode for ActivateView {
    fn encode(&self, w: &mut Writer<'_>) {
        self.xfer_id.encode(w);
        w.byte(self.mode.code());
    }
}

impl ActivateView {
    /// Decodes an `Activate` body.
    ///
    /// # Errors
    ///
    /// [`WireError::Decode`] if the payload is not a valid body.
    pub fn decode(payload: &[u8]) -> Result<Self, WireError> {
        let mut r = Reader::new(payload);
        Ok(Self {
            xfer_id: r.varint_u32()?,
            mode: ActivateMode::from_code(r.byte()?),
        })
    }
}

/// Marks the trial slot as good. Host → device, kind [`super::kind::CONFIRM`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConfirmView {
    /// The slot to keep.
    pub slot: u8,
}

impl Encode for ConfirmView {
    fn encode(&self, w: &mut Writer<'_>) {
        w.byte(self.slot);
    }
}

impl ConfirmView {
    /// Decodes a `Confirm` body.
    ///
    /// # Errors
    ///
    /// [`WireError::Decode`] if the payload is not a valid body.
    pub fn decode(payload: &[u8]) -> Result<Self, WireError> {
        Ok(Self {
            slot: Reader::new(payload).byte()?,
        })
    }
}

fn read_bool(r: &mut Reader<'_>) -> Result<bool, WireError> {
    r.option()
}

/// The optional rollback report at the end of a `DeviceInfo` body. Strings longer than their wire
/// bounds are a decode error.
fn read_rollback<'a>(r: &mut Reader<'a>) -> Result<Option<TrialRollback<'a>>, WireError> {
    if !r.option()? {
        return Ok(None);
    }
    let version = r.str()?;
    let reason = RollbackReason::from_code(r.byte()?);
    let note = r.str()?;
    if version.len() > MAX_ROLLBACK_VERSION_BYTES || note.len() > MAX_ROLLBACK_NOTE_BYTES {
        return Err(WireError::Decode);
    }
    Ok(Some(TrialRollback {
        version,
        reason,
        note,
    }))
}

/// The firmware state a device reports in [`DeviceInfoView`] and the transfer rules it applies.
///
/// The session calls it from its own context (never from an interrupt); a port implements it over
/// its flash, a bootloader interface, or the in-RAM simulation in the tests. Errors are
/// [`XferStatus`] codes, reported to the host.
pub trait FirmwareSink {
    /// The current state, reported in `DeviceInfo`. `max_chunk` limits the data the device accepts
    /// per chunk (the session further caps it to its receive buffer).
    fn info(&self) -> FirmwareInfo<'_>;

    /// Starts a transfer, or resumes it when `begin.xfer_id` matches the transfer the sink holds.
    /// Returns the number of image bytes already stored for it (0 for a new transfer). A resumed
    /// transfer must have the same `total_len`, `image_crc32c` and `target_slot`; otherwise the
    /// sink starts over.
    ///
    /// # Errors
    ///
    /// Any status: the host gets it in an `XferAck`.
    fn begin(&mut self, begin: &XferBeginView<'_>) -> Result<u32, XferStatus>;

    /// Stores `data` at `offset`. Called only for `offset` equal to the next expected offset, and
    /// only with data whose CRC-32C was verified.
    ///
    /// # Errors
    ///
    /// Any status (typically [`XferStatus::Flash`]); the host resends the chunk.
    fn write(&mut self, offset: u32, data: &[u8]) -> Result<(), XferStatus>;

    /// Verifies the whole stored image: length `total_len` and CRC-32C `crc32c`. A sink that
    /// can (and wants to) read its storage back does so here.
    ///
    /// # Errors
    ///
    /// [`XferStatus::BadDigest`] when the image does not match, or any other status.
    fn finish(&mut self, xfer_id: u32, total_len: u32, crc32c: u32) -> Result<(), XferStatus>;

    /// Runs a verified image as `mode` (see [`ActivateMode`]). For a trial, the device reboots
    /// into the slot; the sink may not return to the session in that case, so a sink that
    /// reboots reports `Ok` and the session stops.
    ///
    /// # Errors
    ///
    /// Any status (for example [`XferStatus::Unsupported`] for an unknown mode).
    fn activate(&mut self, xfer_id: u32, mode: ActivateMode) -> Result<(), XferStatus>;

    /// Keeps the running trial slot. The bootloader must not revert it afterwards.
    ///
    /// # Errors
    ///
    /// Any status (for example [`XferStatus::BadSlot`] if `slot` is not running).
    fn confirm(&mut self, slot: u8) -> Result<(), XferStatus>;

    /// The last trial the bootloader rolled back, if it reverted one since the last trial started.
    /// The device reports it in `DeviceInfo`, so the host learns why its update did not stick
    /// (`RolledBack` rather than "did not come back running the new version"). A sink whose
    /// bootloader reverts silently keeps the default: none.
    fn last_trial(&self) -> Option<TrialRollback<'_>> {
        None
    }
}

/// The state a [`FirmwareSink`] reports.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FirmwareInfo<'a> {
    /// Running application version.
    pub fw_version: &'a str,
    /// Running slot.
    pub active_slot: u8,
    /// Number of slots.
    pub slots: u8,
    /// Whether the running slot is an unconfirmed trial.
    pub trial: bool,
    /// Largest chunk the sink accepts.
    pub max_chunk: u16,
    /// Chunks in flight allowed (1 for stop-and-wait).
    pub window: u8,
    /// Bootloader name and version, empty if unknown.
    pub bootloader: &'a str,
}

/// The sink of a device without firmware transfer. Every operation is [`XferStatus::Unsupported`]
/// and the device reports no slots.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct NoFirmware;

impl FirmwareSink for NoFirmware {
    fn info(&self) -> FirmwareInfo<'_> {
        FirmwareInfo {
            fw_version: "",
            active_slot: 0,
            slots: 0,
            trial: false,
            max_chunk: 0,
            window: 1,
            bootloader: "",
        }
    }

    fn begin(&mut self, _: &XferBeginView<'_>) -> Result<u32, XferStatus> {
        Err(XferStatus::Unsupported)
    }

    fn write(&mut self, _: u32, _: &[u8]) -> Result<(), XferStatus> {
        Err(XferStatus::Unsupported)
    }

    fn finish(&mut self, _: u32, _: u32, _: u32) -> Result<(), XferStatus> {
        Err(XferStatus::Unsupported)
    }

    fn activate(&mut self, _: u32, _: ActivateMode) -> Result<(), XferStatus> {
        Err(XferStatus::Unsupported)
    }

    fn confirm(&mut self, _: u8) -> Result<(), XferStatus> {
        Err(XferStatus::Unsupported)
    }
}
