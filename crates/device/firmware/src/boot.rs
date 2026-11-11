//! What the bootloader runs after a reset.
//!
//! A trial runs once. If the board resets before the application confirms it, the trial
//! is given up and the slot before it runs again. A slot runs only if what it holds is
//! what was checked when it arrived; an image that never checked out is never started.

use crate::flash::SlotFlash;
use crate::record::{BootRecord, NO_SLOT, Rollback, load, store};
use crate::slots::image_crc;

/// Why the board reset, as the port's reset handler tells it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ResetReason {
    /// Power on, a reset asked for, or one the port cannot tell apart from those.
    Normal,
    /// A watchdog or a fault.
    Fault,
}

impl BootRecord {
    fn give_up_trial(&mut self, why: Rollback) {
        self.rollback = Some(why);
        self.active = self.previous;
        self.trial = NO_SLOT;
        self.trial_boots = 0;
    }

    /// Whether `slot` may be started, as far as the record says: a slot that holds a
    /// checked image whose stored bytes `check` finds unchanged, or the slot the board
    /// left the factory with, which was never sent and so is never checked.
    fn runnable(&self, slot: u8, check: &impl Fn(u8) -> bool) -> bool {
        match self.slots.get(usize::from(slot)) {
            Some(held) if held.len == 0 => slot == self.active && self.trial == NO_SLOT,
            Some(_) => check(slot),
            None => false,
        }
    }
}

/// Chooses the slot to start, changing `record` as the choice requires: a trial counted,
/// or given up. `check` says whether a slot's stored bytes still match its record. `None`
/// when no slot may be started.
pub fn boot_select(
    record: &mut BootRecord,
    reason: ResetReason,
    check: impl Fn(u8) -> bool,
) -> Option<u8> {
    if record.trial != NO_SLOT {
        let trial = record.trial;
        if record.trial_boots == 0 && record.runnable(trial, &check) {
            record.trial_boots = 1;
            return Some(trial);
        }
        let why = if record.trial_boots == 0 {
            Rollback::VerifyFailed
        } else if reason == ResetReason::Fault {
            Rollback::BootFailure
        } else {
            Rollback::NotConfirmed
        };
        record.give_up_trial(why);
    }
    let active = record.active;
    if record.runnable(active, &check) {
        return Some(active);
    }
    // The running image went bad in place: the other one, if it holds a good one.
    let other = active ^ 1;
    if active < 2
        && record
            .slots
            .get(usize::from(other))
            .is_some_and(|s| s.len != 0)
        && check(other)
    {
        record.rollback = Some(Rollback::VerifyFailed);
        record.previous = active;
        record.active = other;
        return Some(other);
    }
    None
}

/// What the bootloader calls after a reset: loads the record, chooses, writes the record
/// back if the choice changed it, and says which slot to start. `None`: start nothing.
pub fn boot<F: SlotFlash + ?Sized>(flash: &mut F, reason: ResetReason) -> Option<u8> {
    let (seq, mut record) = load(flash);
    let before = record;
    let slots = record.slots;
    let chosen = {
        let flash: &F = flash;
        boot_select(&mut record, reason, |slot| {
            slots.get(usize::from(slot)).is_some_and(|held| {
                image_crc(flash, slot, held.len).is_ok_and(|crc| crc == held.crc)
            })
        })
    };
    // A trial that is started must be counted first, or a reset loop would start it for
    // ever: a record that cannot be written starts nothing.
    if record != before && store(flash, seq, &record).is_err() {
        return None;
    }
    chosen
}
