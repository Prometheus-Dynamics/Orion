//! A/B image slots for firmware transfer (feature `xfer`): a generic, `no_std`, allocation-free
//! [`FirmwareSink`] over two image slots and a persistent [`BootRecord`], and the bootloader's
//! decision on reset ([`boot`], [`boot_select`]).
//!
//! The board port supplies the flash ([`SlotFlash`]): the two slot regions, the two copies of the
//! boot record, and a reset. Nothing here names a chip or a vendor.
//!
//! **Status: host-tested only.** The logic is exercised by the host tests in `tests/slots` (an
//! in-memory flash, and a device session driven by the host's `FirmwareTransfer`). It has not run
//! on any board. A port must check the things the host cannot: erase and program semantics of its
//! flash, record-region alignment, that `reset` really starts the bootloader, and that the reset
//! reason it passes to [`boot`] matches what its watchdog and fault handlers report.
//!
//! Flow (see `docs/update-recovery.md`, "Two-slot firmware in the template"):
//!
//! - `XferBegin` erases the inactive slot and clears the last rollback report. While a trial is
//!   pending it is refused with `Busy`.
//! - `XferChunk` / `XferEnd` write and verify the image (CRC-32C of the stored bytes).
//! - `Activate` (trial) records the new slot as the pending trial and resets the device.
//! - The bootloader's [`boot`] runs the trial once. The application calls `Confirm` (through
//!   [`SlotFirmware`]) to keep it. An unconfirmed trial is reverted on the next reset, and the
//!   revert is reported by [`FirmwareSink::last_trial`] until the next transfer.

mod boot;
mod record;

pub use boot::{ResetReason, boot, boot_select};
pub use record::{
    BootRecord, NO_SLOT, RECORD_LEN, Rollback, Slot, Text, load as load_record,
    store as store_record,
};

use orion_link::Crc32c;
use orion_link::wire::{
    ActivateMode, FirmwareInfo, FirmwareSink, TrialRollback, XferBeginView, XferStatus,
};

/// Image slots on the device (A and B).
pub const SLOTS: u8 = 2;
/// Longest version string a slot can hold (the `XferBegin` version must fit).
pub const MAX_VERSION_BYTES: usize = 32;
/// Longest note of a rollback report.
pub const MAX_NOTE_BYTES: usize = 32;
/// Largest chunk the sink accepts (the session further caps it to its receive buffer).
pub const MAX_CHUNK: u16 = 96;

/// A flash operation failed. The sink reports it as [`XferStatus::Flash`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FlashError;

/// The board's flash, as the slot logic needs it.
///
/// Slots are byte-addressed regions of [`slot_capacity`](Self::slot_capacity) bytes. The port
/// hides its erase units and alignment: `erase_slot` leaves the whole slot erased, and
/// `write_slot` may be called in any order of offsets inside an erased slot. The two boot record
/// copies are separate regions, each erased and programmed as a whole by `write_record`.
pub trait SlotFlash {
    /// Bytes in one image slot (the largest image).
    fn slot_capacity(&self) -> u32;

    /// Erases a whole image slot.
    ///
    /// # Errors
    ///
    /// [`FlashError`] if the erase fails.
    fn erase_slot(&mut self, slot: u8) -> Result<(), FlashError>;

    /// Programs `data` at `offset` of an erased slot.
    ///
    /// # Errors
    ///
    /// [`FlashError`] if the write fails.
    fn write_slot(&mut self, slot: u8, offset: u32, data: &[u8]) -> Result<(), FlashError>;

    /// Reads `out.len()` bytes of a slot at `offset`.
    ///
    /// # Errors
    ///
    /// [`FlashError`] if the read fails or the range leaves the slot.
    fn read_slot(&self, slot: u8, offset: u32, out: &mut [u8]) -> Result<(), FlashError>;

    /// Reads boot record copy `copy` (0 or 1). An erased or torn copy reads as garbage; the record
    /// code rejects it by its CRC.
    ///
    /// # Errors
    ///
    /// [`FlashError`] if the read fails.
    fn read_record(&self, copy: u8, out: &mut [u8; RECORD_LEN]) -> Result<(), FlashError>;

    /// Erases and programs boot record copy `copy`. A power cut may leave it torn; the caller
    /// treats an error as "not written", and readers reject a torn copy by its CRC.
    ///
    /// # Errors
    ///
    /// [`FlashError`] if the erase or the program fails.
    fn write_record(&mut self, copy: u8, data: &[u8; RECORD_LEN]) -> Result<(), FlashError>;

    /// Starts the bootloader on the new trial (a system reset on a real device). It may not
    /// return; the sink then reports `Ok` and the session stops.
    fn reset(&mut self);
}

/// Digest of the first `len` bytes of `slot`, read in small pieces.
pub(crate) fn image_crc<F: SlotFlash + ?Sized>(
    flash: &F,
    slot: u8,
    len: u32,
) -> Result<u32, FlashError> {
    if len > flash.slot_capacity() {
        return Err(FlashError);
    }
    let mut crc = Crc32c::new();
    let mut buf = [0u8; 64];
    let mut at = 0u32;
    while at < len {
        let n = usize::try_from(len - at)
            .unwrap_or(usize::MAX)
            .min(buf.len());
        let chunk = &mut buf[..n];
        flash.read_slot(slot, at, chunk)?;
        crc.update(chunk);
        at += u32::try_from(n).map_err(|_| FlashError)?;
    }
    Ok(crc.finish())
}

/// The transfer in progress: the target slot being written.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Staging {
    xfer_id: u32,
    total: u32,
    crc32c: u32,
    slot: u8,
    version: Text<MAX_VERSION_BYTES>,
    written: u32,
    verified: bool,
}

impl Staging {
    fn matches(&self, begin: &XferBeginView<'_>) -> bool {
        self.xfer_id == begin.xfer_id
            && self.total == begin.total_len
            && self.crc32c == begin.image_crc32c
            && self.slot == begin.target_slot
    }
}

/// The firmware sink of a device with two image slots. See the [module docs](self).
///
/// The record is held in RAM and written through to the flash on every change, so the sink and
/// the bootloader always agree on what is persisted. A failed write is reported as
/// [`XferStatus::Flash`] and leaves the record as it was.
#[derive(Debug, Clone)]
pub struct SlotFirmware<F: SlotFlash> {
    flash: F,
    seq: u32,
    record: BootRecord,
    staging: Option<Staging>,
}

impl<F: SlotFlash> SlotFirmware<F> {
    /// The sink over `flash`, with the record the flash holds (the factory record if none).
    pub fn new(flash: F) -> Self {
        let (seq, record) = load_record(&flash);
        Self {
            flash,
            seq,
            record,
            staging: None,
        }
    }

    /// The record as the sink holds it.
    #[must_use]
    pub fn record(&self) -> &BootRecord {
        &self.record
    }

    /// The flash (for a board's reset path or a host test).
    #[must_use]
    pub fn flash(&self) -> &F {
        &self.flash
    }

    /// The flash, mutably.
    pub fn flash_mut(&mut self) -> &mut F {
        &mut self.flash
    }

    /// Gives the flash back (the device is about to reset).
    #[must_use]
    pub fn into_flash(self) -> F {
        self.flash
    }

    /// Persists `next` and makes it the sink's record.
    fn commit(&mut self, next: BootRecord) -> Result<(), XferStatus> {
        let seq = store_record(&mut self.flash, self.seq, &next).map_err(|_| XferStatus::Flash)?;
        self.seq = seq;
        self.record = next;
        Ok(())
    }
}

impl<F: SlotFlash> FirmwareSink for SlotFirmware<F> {
    fn info(&self) -> FirmwareInfo<'_> {
        let active = self.record.active;
        let fw_version = self
            .record
            .slots
            .get(usize::from(active))
            .map_or("", |slot| slot.version.as_str());
        FirmwareInfo {
            fw_version,
            active_slot: active,
            slots: SLOTS,
            trial: self.record.trial != NO_SLOT,
            max_chunk: MAX_CHUNK,
            window: 1,
            bootloader: "",
        }
    }

    fn begin(&mut self, begin: &XferBeginView<'_>) -> Result<u32, XferStatus> {
        if let Some(staging) = &self.staging
            && staging.matches(begin)
        {
            return Ok(staging.written); // resume
        }
        // The fallback slot of an unconfirmed trial must not be erased before the trial settles.
        if self.record.trial != NO_SLOT {
            return Err(XferStatus::Busy);
        }
        if begin.target_slot >= SLOTS || begin.target_slot == self.record.active {
            return Err(XferStatus::BadSlot);
        }
        if begin.total_len > self.flash.slot_capacity() {
            return Err(XferStatus::NoSpace);
        }
        let version = Text::new(begin.version).ok_or(XferStatus::Unsupported)?;
        let target = begin.target_slot;
        self.flash
            .erase_slot(target)
            .map_err(|_| XferStatus::Flash)?;
        // The target now holds no image, and a new transfer clears the rollback report.
        let mut next = self.record;
        next.rollback = None;
        next.slots[usize::from(target)] = Slot::EMPTY;
        self.commit(next)?;
        self.staging = Some(Staging {
            xfer_id: begin.xfer_id,
            total: begin.total_len,
            crc32c: begin.image_crc32c,
            slot: target,
            version,
            written: 0,
            verified: false,
        });
        Ok(0)
    }

    fn write(&mut self, offset: u32, data: &[u8]) -> Result<(), XferStatus> {
        let staging = self.staging.as_mut().ok_or(XferStatus::UnknownXfer)?;
        let len = u32::try_from(data.len()).map_err(|_| XferStatus::BadOffset)?;
        let end = offset
            .checked_add(len)
            .filter(|&end| offset == staging.written && end <= staging.total)
            .ok_or(XferStatus::BadOffset)?;
        self.flash
            .write_slot(staging.slot, offset, data)
            .map_err(|_| XferStatus::Flash)?;
        staging.written = end;
        Ok(())
    }

    fn finish(&mut self, xfer_id: u32, total_len: u32, crc32c: u32) -> Result<(), XferStatus> {
        let staging = self
            .staging
            .as_mut()
            .filter(|staging| staging.xfer_id == xfer_id)
            .ok_or(XferStatus::UnknownXfer)?;
        if staging.written != total_len || staging.total != total_len {
            return Err(XferStatus::BadDigest);
        }
        let stored =
            image_crc(&self.flash, staging.slot, total_len).map_err(|_| XferStatus::Flash)?;
        if stored != crc32c {
            return Err(XferStatus::BadDigest);
        }
        staging.verified = true;
        Ok(())
    }

    fn activate(&mut self, xfer_id: u32, mode: ActivateMode) -> Result<(), XferStatus> {
        let staging = self
            .staging
            .filter(|staging| staging.xfer_id == xfer_id)
            .ok_or(XferStatus::UnknownXfer)?;
        if !staging.verified {
            return Err(XferStatus::BadDigest);
        }
        if mode != ActivateMode::Trial {
            return Err(XferStatus::Unsupported);
        }
        let target = staging.slot;
        let mut next = self.record;
        next.previous = next.active;
        next.active = target;
        next.trial = target;
        next.trial_boots = 0;
        next.rollback = None;
        next.slots[usize::from(target)] = Slot {
            version: staging.version,
            len: staging.total,
            crc32c: staging.crc32c,
        };
        self.commit(next)?;
        self.staging = None;
        self.flash.reset();
        Ok(())
    }

    fn confirm(&mut self, slot: u8) -> Result<(), XferStatus> {
        if slot >= SLOTS || slot != self.record.active || self.record.trial != slot {
            return Err(XferStatus::BadSlot);
        }
        let mut next = self.record;
        next.trial = NO_SLOT;
        next.trial_boots = 0;
        self.commit(next)
    }

    fn last_trial(&self) -> Option<TrialRollback<'_>> {
        self.record.rollback.as_ref().map(|rollback| TrialRollback {
            version: rollback.version.as_str(),
            reason: rollback.reason,
            note: rollback.note.as_str(),
        })
    }
}
