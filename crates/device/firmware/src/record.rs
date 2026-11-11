//! The boot record: which slot runs, which is on trial, and what each holds. It is kept
//! twice; a write goes to the older copy, so a power cut during one leaves the other.
//!
//! A copy is fixed bytes, little endian, with a CRC-32C over everything before it:
//!
//! ```text
//! [seq u32][active u8][previous u8][trial u8][trial_boots u8][rollback u8]
//! [slot 0: len u32, crc u32, digest 32][slot 1: the same][zeros][crc32c u32]
//! ```

use orion_wire::crc32c;

use crate::flash::{RECORD_LEN, SlotFlash};

/// No slot: no trial is pending.
pub const NO_SLOT: u8 = 0xFF;

/// What one slot holds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Slot {
    /// The image's length. Zero for none.
    pub len: u32,
    /// CRC-32C of the image as stored, checked at every boot.
    pub crc: u32,
    /// The image's SHA-256, as the sender gave it and the device checked it.
    pub digest: [u8; 32],
}

impl Slot {
    /// A slot with no image.
    pub const EMPTY: Self = Self {
        len: 0,
        crc: 0,
        digest: [0; 32],
    };
}

/// Why a trial was given up.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Rollback {
    /// The image on trial failed its check before it ran.
    VerifyFailed = 1,
    /// It ran and the board reset without it being confirmed.
    NotConfirmed = 2,
    /// It ran and the board reset from a fault.
    BootFailure = 3,
}

impl Rollback {
    const fn from_code(code: u8) -> Option<Self> {
        match code {
            1 => Some(Self::VerifyFailed),
            2 => Some(Self::NotConfirmed),
            3 => Some(Self::BootFailure),
            _ => None,
        }
    }
}

/// The boot record.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BootRecord {
    /// The slot that runs.
    pub active: u8,
    /// The slot that ran before the trial: where a failed trial goes back to.
    pub previous: u8,
    /// The slot on trial, or [`NO_SLOT`].
    pub trial: u8,
    /// How many times the trial has been started.
    pub trial_boots: u8,
    /// Why the last trial was given up, until the next transfer.
    pub rollback: Option<Rollback>,
    /// What each slot holds.
    pub slots: [Slot; 2],
}

impl BootRecord {
    /// A board as it leaves the factory: slot 0 runs, and what it holds is not known, so it
    /// is not checked.
    pub const FACTORY: Self = Self {
        active: 0,
        previous: 0,
        trial: NO_SLOT,
        trial_boots: 0,
        rollback: None,
        slots: [Slot::EMPTY; 2],
    };

    fn encode(&self, seq: u32) -> [u8; RECORD_LEN] {
        let mut out = [0_u8; RECORD_LEN];
        let mut at = 0;
        let mut put = |bytes: &[u8]| {
            for byte in bytes {
                if let Some(slot) = out.get_mut(at) {
                    *slot = *byte;
                }
                at += 1;
            }
        };
        put(&seq.to_le_bytes());
        put(&[
            self.active,
            self.previous,
            self.trial,
            self.trial_boots,
            self.rollback.map_or(0, |r| r as u8),
        ]);
        for slot in &self.slots {
            put(&slot.len.to_le_bytes());
            put(&slot.crc.to_le_bytes());
            put(&slot.digest);
        }
        let (body, tail) = out.split_at_mut(RECORD_LEN - 4);
        let crc = crc32c(body).to_le_bytes();
        for (to, from) in tail.iter_mut().zip(crc) {
            *to = from;
        }
        out
    }

    fn decode(bytes: &[u8; RECORD_LEN]) -> Option<(u32, Self)> {
        let (body, tail) = bytes.split_at(RECORD_LEN - 4);
        if crc32c(body).to_le_bytes() != *tail {
            return None;
        }
        let seq = u32_at(body, 0)?;
        let &[active, previous, trial, trial_boots, rollback] = body.get(4..9)? else {
            return None;
        };
        let mut slots = [Slot::EMPTY; 2];
        for (n, slot) in slots.iter_mut().enumerate() {
            let at = 9 + n * 40;
            slot.len = u32_at(body, at)?;
            slot.crc = u32_at(body, at + 4)?;
            slot.digest = body.get(at + 8..at + 40)?.try_into().ok()?;
        }
        Some((
            seq,
            Self {
                active,
                previous,
                trial,
                trial_boots,
                rollback: Rollback::from_code(rollback),
                slots,
            },
        ))
    }
}

/// The little-endian `u32` at `at`.
fn u32_at(bytes: &[u8], at: usize) -> Option<u32> {
    bytes
        .get(at..at + 4)?
        .try_into()
        .ok()
        .map(u32::from_le_bytes)
}

/// The newest good copy and its sequence number, or the factory record and zero.
pub fn load<F: SlotFlash + ?Sized>(flash: &F) -> (u32, BootRecord) {
    let mut best: Option<(u32, BootRecord)> = None;
    for copy in 0..2 {
        let mut bytes = [0_u8; RECORD_LEN];
        if flash.read_record(copy, &mut bytes).is_err() {
            continue;
        }
        if let Some((seq, record)) = BootRecord::decode(&bytes)
            && best.is_none_or(|(newest, _)| seq.wrapping_sub(newest) as i32 > 0)
        {
            best = Some((seq, record));
        }
    }
    best.unwrap_or((0, BootRecord::FACTORY))
}

/// Writes `record` over the older copy, as one newer than `seq`, and returns its sequence
/// number.
///
/// # Errors
///
/// [`crate::FlashError`] if the write failed. The other copy still holds the record before.
pub fn store<F: SlotFlash + ?Sized>(
    flash: &mut F,
    seq: u32,
    record: &BootRecord,
) -> Result<u32, crate::FlashError> {
    let next = seq.wrapping_add(1);
    // Copy `next % 2` is the older one: copies take turns.
    flash.write_record((next % 2) as u8, &record.encode(next))?;
    Ok(next)
}
