//! The bootloader's decision on reset: which slot to run, and what to record about a trial.
//!
//! [`boot_select`] is the pure part: it takes the record, the reset reason, and a verification
//! callback, and returns the slot to run (or `None`). [`boot`] is the stage a board calls from its
//! reset path: it loads the record, runs the decision, and persists any change.
//!
//! Rules, in order:
//!
//! 1. A pending trial that has not booted yet (armed by an activation) must verify. If it does, it
//!    is booted, once: its boot count goes to 1. If it does not, the trial is reverted with
//!    [`RollbackReason::VerifyFailed`].
//! 2. A trial that has booted before and is still unconfirmed is reverted: with
//!    [`RollbackReason::BootFailure`] if this reset is a fault (watchdog, hard fault, or any reset
//!    the port classifies as one), otherwise with [`RollbackReason::NotConfirmed`].
//! 3. A revert runs the previous slot, which must verify too.
//! 4. Without a trial, the active slot runs if it verifies. If it does not, the other slot runs
//!    when it verifies, and the failure is recorded as a verify failure (no trial is involved).
//!
//! `None` means no slot may run: the board must not start an image (it halts or recovers). It is
//! also returned when the decision cannot be persisted, since a trial that is booted without its
//! boot count recorded would never be reverted.

use orion_link::wire::RollbackReason;

use super::record::{BootRecord, NO_SLOT, Rollback, Slot, Text, load, store};
use super::{MAX_NOTE_BYTES, SLOTS, SlotFlash, image_crc};

/// What the reset was, as the port classifies it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResetReason {
    /// A power-on, external, or application (software) reset.
    Normal,
    /// A watchdog reset, a hard fault, or any other reset that means the image failed to run.
    Fault,
}

const NOTE_NOT_CONFIRMED: &str = "not confirmed";
const NOTE_BOOT_FAILURE: &str = "boot fault";
const NOTE_VERIFY_FAILED: &str = "verify failed";

impl BootRecord {
    /// Reverts the pending trial: its slot is recorded as rolled back, and the previous slot is
    /// active again with no trial.
    fn revert(&mut self, reason: RollbackReason) {
        let note = match reason {
            RollbackReason::NotConfirmed => NOTE_NOT_CONFIRMED,
            RollbackReason::BootFailure => NOTE_BOOT_FAILURE,
            _ => NOTE_VERIFY_FAILED,
        };
        self.rollback = Some(Rollback {
            version: self.version(self.trial),
            reason,
            note: text_or_empty::<MAX_NOTE_BYTES>(note),
        });
        self.active = self.previous;
        self.trial = NO_SLOT;
        self.trial_boots = 0;
    }
}

fn text_or_empty<const N: usize>(text: &str) -> Text<N> {
    Text::new(text).unwrap_or(Text::EMPTY)
}

/// The bootloader's decision (see the [module docs](self)). `verify(slot)` says whether the image
/// in `slot` is intact; it is called only for slots that may run. Returns the slot to run, or
/// `None` if no slot may run. `record` is updated to the state the decision leaves.
pub fn boot_select(
    record: &mut BootRecord,
    reason: ResetReason,
    verify: impl Fn(u8) -> bool,
) -> Option<u8> {
    if record.trial != NO_SLOT {
        let trial = record.trial;
        if record.trial_boots == 0 && verify(trial) {
            record.trial_boots = 1;
            return Some(trial);
        }
        let why = if record.trial_boots == 0 {
            RollbackReason::VerifyFailed
        } else if reason == ResetReason::Fault {
            RollbackReason::BootFailure
        } else {
            RollbackReason::NotConfirmed
        };
        record.revert(why);
        return runnable(record.active, &verify);
    }
    let active = record.active;
    if runnable(active, &verify).is_some() {
        return Some(active);
    }
    // The active image fails its check: run the other slot if it passes, and record why.
    let other = active ^ 1;
    if active < SLOTS && runnable(other, &verify).is_some() {
        record.rollback = Some(Rollback {
            version: record.version(active),
            reason: RollbackReason::VerifyFailed,
            note: text_or_empty::<MAX_NOTE_BYTES>(NOTE_VERIFY_FAILED),
        });
        record.previous = active;
        record.active = other;
        return Some(other);
    }
    None
}

fn runnable(slot: u8, verify: &impl Fn(u8) -> bool) -> Option<u8> {
    (slot < SLOTS && verify(slot)).then_some(slot)
}

/// The boot stage a board runs on reset: loads the record, decides (see [`boot_select`]), and
/// persists the record if the decision changed it. Returns the slot to start, or `None`.
///
/// The verification reads the image of each slot from `flash`, against the digests in the record.
pub fn boot<F: SlotFlash>(flash: &mut F, reason: ResetReason) -> Option<u8> {
    let (seq, mut record) = load(flash);
    let before = record;
    let slots: [Slot; SLOTS as usize] = record.slots;
    let chosen = {
        let flash_ref: &F = &*flash;
        boot_select(&mut record, reason, |slot| {
            slots.get(usize::from(slot)).is_some_and(|info| {
                info.len != 0 && image_crc(flash_ref, slot, info.len) == Ok(info.crc32c)
            })
        })
    };
    if record != before && store(flash, seq, &record).is_err() {
        return None;
    }
    chosen
}
