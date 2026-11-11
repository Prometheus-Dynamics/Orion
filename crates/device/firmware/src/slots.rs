//! Two image slots: one runs, the other takes the next image.

use orion_wire::Crc32c;

use crate::flash::{FlashError, SlotFlash};
use crate::record::{BootRecord, NO_SLOT, Slot, load, store};
use crate::sha256::Sha256;

/// Why a transfer was turned away. The text goes to the sender as the status's detail.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Refused {
    /// A trial has not been confirmed yet: its fallback must not be erased.
    TrialPending,
    /// Larger than a slot.
    TooLarge,
    /// The transfer is not the one in progress.
    UnknownTransfer,
    /// Not every byte has arrived.
    Incomplete,
    /// The bytes do not have the digest the sender gave.
    BadDigest,
    /// The flash failed.
    Flash,
}

impl Refused {
    /// For people.
    #[must_use]
    pub const fn detail(self) -> &'static str {
        match self {
            Self::TrialPending => "a trial is waiting to be confirmed",
            Self::TooLarge => "larger than a slot",
            Self::UnknownTransfer => "not the transfer in progress",
            Self::Incomplete => "not every byte has arrived",
            Self::BadDigest => "the digest does not match",
            Self::Flash => "the flash failed",
        }
    }
}

impl From<FlashError> for Refused {
    fn from(_: FlashError) -> Self {
        Self::Flash
    }
}

/// The image arriving.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Staging {
    transfer: u64,
    size: u32,
    digest: [u8; 32],
    slot: u8,
    written: u32,
    /// Checked and on trial: a repeated commit gets the same answer.
    committed: bool,
}

/// Two slots over the port's flash, and the boot record.
///
/// The record is held here and written through on every change, so this and the
/// bootloader always agree. A write that fails leaves the record as it was.
#[derive(Debug)]
pub struct Slots<F> {
    flash: F,
    seq: u32,
    record: BootRecord,
    staging: Option<Staging>,
}

impl<F: SlotFlash> Slots<F> {
    /// The slots of `flash`, with the record it holds.
    pub fn new(flash: F) -> Self {
        let (seq, record) = load(&flash);
        Self {
            flash,
            seq,
            record,
            staging: None,
        }
    }

    /// The record.
    #[must_use]
    pub const fn record(&self) -> &BootRecord {
        &self.record
    }

    /// The flash.
    #[must_use]
    pub const fn flash(&self) -> &F {
        &self.flash
    }

    /// The flash, to change behind the slots' back: a test's power cut.
    pub fn flash_mut(&mut self) -> &mut F {
        &mut self.flash
    }

    fn write_record(&mut self, next: BootRecord) -> Result<(), Refused> {
        self.seq = store(&mut self.flash, self.seq, &next)?;
        self.record = next;
        Ok(())
    }

    /// Starts transfer `transfer` of an image of `size` bytes with SHA-256 `digest`, or
    /// resumes it. Says how many bytes are already here.
    ///
    /// # Errors
    ///
    /// [`Refused`].
    pub fn begin(&mut self, transfer: u64, size: u64, digest: &[u8; 32]) -> Result<u32, Refused> {
        if let Some(staging) = &self.staging
            && staging.transfer == transfer
            && u64::from(staging.size) == size
            && staging.digest == *digest
        {
            return Ok(staging.written);
        }
        if self.record.trial != NO_SLOT {
            return Err(Refused::TrialPending);
        }
        let size = u32::try_from(size)
            .ok()
            .filter(|&size| size <= self.flash.slot_capacity())
            .ok_or(Refused::TooLarge)?;
        let slot = self.record.active ^ 1;
        // The slot holds no image from the moment it is erased, so the record says so first.
        let mut next = self.record;
        next.rollback = None;
        if let Some(held) = next.slots.get_mut(usize::from(slot)) {
            *held = Slot::EMPTY;
        }
        self.write_record(next)?;
        self.staging = None;
        self.flash.erase_slot(slot)?;
        self.staging = Some(Staging {
            transfer,
            size,
            digest: *digest,
            slot,
            written: 0,
            committed: false,
        });
        Ok(0)
    }

    /// Bytes of transfer `transfer` at `offset`. Says how many bytes are here in order: a
    /// chunk that is not next is not written, and the answer tells the sender where to go
    /// on from.
    ///
    /// # Errors
    ///
    /// [`Refused`].
    pub fn write(&mut self, transfer: u64, offset: u64, data: &[u8]) -> Result<u32, Refused> {
        let staging = self
            .staging
            .as_mut()
            .filter(|staging| staging.transfer == transfer)
            .ok_or(Refused::UnknownTransfer)?;
        let fits = u32::try_from(data.len())
            .ok()
            .and_then(|len| staging.written.checked_add(len))
            .filter(|&end| end <= staging.size);
        if offset == u64::from(staging.written)
            && !staging.committed
            && let Some(end) = fits
        {
            self.flash.write_slot(staging.slot, staging.written, data)?;
            staging.written = end;
        }
        Ok(staging.written)
    }

    /// Checks transfer `transfer` against its digest and, if it holds, puts it on trial:
    /// the bootloader starts it once at the next reset. Says which slot.
    ///
    /// # Errors
    ///
    /// [`Refused`]. A bad image is left where it is and never started.
    pub fn commit(&mut self, transfer: u64) -> Result<u8, Refused> {
        let staging = self
            .staging
            .filter(|staging| staging.transfer == transfer)
            .ok_or(Refused::UnknownTransfer)?;
        if staging.committed {
            return Ok(staging.slot);
        }
        if staging.written != staging.size {
            return Err(Refused::Incomplete);
        }
        let (digest, crc) = image_digest(&self.flash, staging.slot, staging.size)?;
        if digest != staging.digest {
            self.staging = None;
            return Err(Refused::BadDigest);
        }
        let mut next = self.record;
        next.previous = next.active;
        next.active = staging.slot;
        next.trial = staging.slot;
        next.trial_boots = 0;
        next.rollback = None;
        if let Some(held) = next.slots.get_mut(usize::from(staging.slot)) {
            *held = Slot {
                len: staging.size,
                crc,
                digest,
            };
        }
        self.write_record(next)?;
        if let Some(staging) = &mut self.staging {
            staging.committed = true;
        }
        Ok(staging.slot)
    }

    /// The application says the image on trial works: it stays. Call it once the firmware
    /// knows it is healthy, such as when a session with the host has started.
    ///
    /// # Errors
    ///
    /// [`Refused::Flash`]. Nothing is on trial is not an error.
    pub fn confirm(&mut self) -> Result<(), Refused> {
        if self.record.trial == NO_SLOT || self.record.trial != self.record.active {
            return Ok(());
        }
        let mut next = self.record;
        next.trial = NO_SLOT;
        next.trial_boots = 0;
        self.write_record(next)
    }
}

/// Reads `len` bytes of `slot` in small pieces.
fn read_image<F: SlotFlash + ?Sized>(
    flash: &F,
    slot: u8,
    len: u32,
    mut each: impl FnMut(&[u8]),
) -> Result<(), FlashError> {
    if len > flash.slot_capacity() {
        return Err(FlashError);
    }
    let mut buf = [0_u8; 64];
    let mut at = 0;
    while at < len {
        let n = (len - at).min(buf.len() as u32);
        let piece = buf.get_mut(..n as usize).ok_or(FlashError)?;
        flash.read_slot(slot, at, piece)?;
        each(piece);
        at += n;
    }
    Ok(())
}

/// The CRC-32C of `len` bytes of `slot`: what the bootloader checks.
pub(crate) fn image_crc<F: SlotFlash + ?Sized>(
    flash: &F,
    slot: u8,
    len: u32,
) -> Result<u32, FlashError> {
    let mut crc = Crc32c::new();
    read_image(flash, slot, len, |piece| crc.update(piece))?;
    Ok(crc.finish())
}

/// The SHA-256 and CRC-32C of `len` bytes of `slot`, read back as stored.
fn image_digest<F: SlotFlash + ?Sized>(
    flash: &F,
    slot: u8,
    len: u32,
) -> Result<([u8; 32], u32), FlashError> {
    let mut sha = Sha256::new();
    let mut crc = Crc32c::new();
    read_image(flash, slot, len, |piece| {
        sha.update(piece);
        crc.update(piece);
    })?;
    Ok((sha.finish(), crc.finish()))
}
