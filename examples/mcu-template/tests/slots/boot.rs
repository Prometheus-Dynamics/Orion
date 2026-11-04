//! The bootloader's decision (`boot_select`), table by table, on hand-built records.

use orion_link::wire::RollbackReason;
use orion_mcu_template::slots::{BootRecord, NO_SLOT, ResetReason, Slot, Text, boot_select};

/// Slot 0 runs 1.2, slot 1 runs 1.3; `verifies[slot]` is the boot check's answer.
fn record(active: u8, previous: u8, trial: u8, trial_boots: u8) -> BootRecord {
    let mut record = BootRecord::factory();
    record.slots[0] = Slot {
        version: Text::new("1.2").expect("version"),
        len: 1,
        crc32c: 0,
    };
    record.slots[1] = Slot {
        version: Text::new("1.3").expect("version"),
        len: 1,
        crc32c: 0,
    };
    record.active = active;
    record.previous = previous;
    record.trial = trial;
    record.trial_boots = trial_boots;
    record
}

fn check(verifies: [bool; 2]) -> impl Fn(u8) -> bool {
    move |slot| verifies.get(usize::from(slot)).copied().unwrap_or(false)
}

#[test]
fn an_armed_trial_that_verifies_boots_once() {
    let mut r = record(1, 0, 1, 0);
    assert_eq!(
        boot_select(&mut r, ResetReason::Normal, check([true, true])),
        Some(1)
    );
    assert_eq!((r.trial, r.trial_boots, r.active), (1, 1, 1));
    assert_eq!(r.rollback, None);
}

#[test]
fn an_armed_trial_that_fails_verification_reverts_to_the_previous_slot() {
    let mut r = record(1, 0, 1, 0);
    assert_eq!(
        boot_select(&mut r, ResetReason::Normal, check([true, false])),
        Some(0)
    );
    assert_eq!((r.trial, r.trial_boots, r.active), (NO_SLOT, 0, 0));
    let rollback = r.rollback.expect("recorded");
    assert_eq!(rollback.reason, RollbackReason::VerifyFailed);
    assert_eq!(rollback.version.as_str(), "1.3");
}

#[test]
fn a_booted_unconfirmed_trial_reverts_on_a_plain_reset() {
    let mut r = record(1, 0, 1, 1);
    assert_eq!(
        boot_select(&mut r, ResetReason::Normal, check([true, true])),
        Some(0)
    );
    assert_eq!(
        r.rollback.map(|rollback| rollback.reason),
        Some(RollbackReason::NotConfirmed)
    );
}

#[test]
fn a_booted_unconfirmed_trial_after_a_fault_reports_boot_failure() {
    let mut r = record(1, 0, 1, 1);
    assert_eq!(
        boot_select(&mut r, ResetReason::Fault, check([true, true])),
        Some(0)
    );
    let rollback = r.rollback.expect("recorded");
    assert_eq!(rollback.reason, RollbackReason::BootFailure);
    assert_eq!(rollback.note.as_str(), "boot fault");
}

#[test]
fn a_revert_to_a_slot_that_fails_verification_starts_nothing() {
    let mut r = record(1, 0, 1, 1);
    assert_eq!(
        boot_select(&mut r, ResetReason::Normal, check([false, true])),
        None
    );
    // The revert is still recorded: the next boot must not run the trial again.
    assert_eq!(r.trial, NO_SLOT);
    assert_eq!(
        r.rollback.map(|rollback| rollback.reason),
        Some(RollbackReason::NotConfirmed)
    );
}

#[test]
fn an_active_slot_without_a_trial_runs_when_it_verifies() {
    let mut r = record(0, 1, NO_SLOT, 0);
    let before = r;
    assert_eq!(
        boot_select(&mut r, ResetReason::Normal, check([true, true])),
        Some(0)
    );
    assert_eq!(r, before, "nothing changes");
}

#[test]
fn a_failing_active_slot_falls_back_to_the_other_one() {
    let mut r = record(0, 1, NO_SLOT, 0);
    assert_eq!(
        boot_select(&mut r, ResetReason::Normal, check([false, true])),
        Some(1)
    );
    assert_eq!((r.active, r.previous), (1, 0));
    assert_eq!(
        r.rollback.map(|rollback| rollback.reason),
        Some(RollbackReason::VerifyFailed)
    );
}

#[test]
fn no_verified_slot_starts_nothing() {
    let mut r = record(0, 1, NO_SLOT, 0);
    assert_eq!(
        boot_select(&mut r, ResetReason::Normal, check([false, false])),
        None
    );
    let mut factory = BootRecord::factory();
    assert_eq!(
        boot_select(&mut factory, ResetReason::Normal, check([false, false])),
        None
    );
}
