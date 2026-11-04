//! The sink's transfer rules and the boot record, against the in-memory flash.

use orion_link::crc32c;
use orion_link::wire::{ActivateMode, FirmwareSink, RollbackReason, XferStatus};
use orion_mcu_template::slots::{BootRecord, NO_SLOT, RECORD_LEN, ResetReason, SlotFirmware, boot};

use crate::flash::{MemFlash, begin_view, image};

/// Transfers `data` into `target` in one chunk and verifies it.
fn stage(sink: &mut SlotFirmware<MemFlash>, xfer_id: u32, version: &str, data: &[u8], target: u8) {
    let view = begin_view(xfer_id, version, data, target);
    assert_eq!(sink.begin(&view), Ok(0));
    sink.write(0, data).expect("write");
    let len = u32::try_from(data.len()).expect("length");
    sink.finish(xfer_id, len, crc32c(data)).expect("finish");
}

/// A reset with the bootloader run on the way back up (the flash's reset request is consumed).
fn power_cycle(flash: &mut MemFlash, reason: ResetReason) -> Option<u8> {
    flash.reset_requested = false;
    boot(flash, reason)
}

/// Flashed with 1.2 in slot 0, a 1.3 trial activated into slot 1 and reset: the device is at the
/// first boot of the trial, with the bootloader's decision still to come.
fn activated_trial() -> MemFlash {
    let old = image(1, 300);
    let new = image(2, 500);
    let mut sink = SlotFirmware::new(MemFlash::with_factory("1.2", &old));
    stage(&mut sink, 7, "1.3", &new, 1);
    assert_eq!(sink.activate(7, ActivateMode::Trial), Ok(()));
    let flash = sink.into_flash();
    assert!(flash.reset_requested, "activation resets the device");
    flash
}

#[test]
fn a_trial_boots_once_and_a_confirm_keeps_it() {
    let new = image(2, 500);
    let mut flash = activated_trial();
    assert_eq!(
        power_cycle(&mut flash, ResetReason::Normal),
        Some(1),
        "the trial boots"
    );

    let mut sink = SlotFirmware::new(flash);
    {
        let info = sink.info();
        assert_eq!(
            (info.active_slot, info.trial, info.fw_version),
            (1, true, "1.3")
        );
    }
    assert_eq!(sink.last_trial(), None);
    // The fallback slot of an unconfirmed trial is not erased: a new transfer waits for the verdict.
    assert_eq!(
        sink.begin(&begin_view(8, "1.4", &new, 0)),
        Err(XferStatus::Busy)
    );
    assert_eq!(
        sink.confirm(0),
        Err(XferStatus::BadSlot),
        "only the running trial"
    );
    assert_eq!(sink.confirm(1), Ok(()));
    assert!(!sink.info().trial);

    let mut flash = sink.into_flash();
    assert_eq!(
        power_cycle(&mut flash, ResetReason::Normal),
        Some(1),
        "a confirmed slot stays"
    );
    assert_eq!(SlotFirmware::new(flash).info().fw_version, "1.3");
}

#[test]
fn an_unconfirmed_trial_reverts_on_reset_and_is_reported() {
    let mut flash = activated_trial();
    assert_eq!(power_cycle(&mut flash, ResetReason::Normal), Some(1));
    // The trial runs and is not confirmed; the next reset reverts it.
    assert_eq!(power_cycle(&mut flash, ResetReason::Normal), Some(0));

    let sink = SlotFirmware::new(flash);
    {
        let info = sink.info();
        assert_eq!(
            (info.active_slot, info.trial, info.fw_version),
            (0, false, "1.2")
        );
    }
    let rollback = sink.last_trial().expect("the revert is reported");
    assert_eq!(rollback.version, "1.3");
    assert_eq!(rollback.reason, RollbackReason::NotConfirmed);
    assert_eq!(rollback.note, "not confirmed");
}

#[test]
fn a_fault_while_the_trial_is_unconfirmed_reports_boot_failure() {
    let mut flash = activated_trial();
    assert_eq!(power_cycle(&mut flash, ResetReason::Normal), Some(1));
    assert_eq!(power_cycle(&mut flash, ResetReason::Fault), Some(0));

    let sink = SlotFirmware::new(flash);
    let rollback = sink.last_trial().expect("the revert is reported");
    assert_eq!(rollback.reason, RollbackReason::BootFailure);
    assert_eq!(rollback.note, "boot fault");
}

#[test]
fn an_image_that_fails_verification_at_boot_is_reverted_before_it_runs() {
    let mut flash = activated_trial();
    flash.corrupt_slot(1, 100);
    assert_eq!(power_cycle(&mut flash, ResetReason::Normal), Some(0));

    let sink = SlotFirmware::new(flash);
    assert_eq!(sink.info().fw_version, "1.2");
    assert_eq!(sink.record().trial, NO_SLOT, "the trial was never booted");
    let rollback = sink.last_trial().expect("the revert is reported");
    assert_eq!(rollback.reason, RollbackReason::VerifyFailed);
    assert_eq!(rollback.version, "1.3");
}

#[test]
fn when_neither_image_verifies_nothing_is_started() {
    let mut flash = activated_trial();
    flash.corrupt_slot(1, 100);
    flash.corrupt_slot(0, 100);
    assert_eq!(power_cycle(&mut flash, ResetReason::Normal), None);
    // The decision is persisted even though nothing runs.
    let sink = SlotFirmware::new(flash);
    assert_eq!(sink.record().trial, NO_SLOT);
    assert_eq!(
        sink.last_trial().map(|rollback| rollback.reason),
        Some(RollbackReason::VerifyFailed)
    );
}

/// A transfer staged in slot 1 over a factory 1.2, with the next record write (the activation's)
/// set to tear after `tear` bytes.
fn staged_with_torn_activation(tear: usize) -> SlotFirmware<MemFlash> {
    let old = image(1, 300);
    let new = image(2, 500);
    let mut sink = SlotFirmware::new(MemFlash::with_factory("1.2", &old));
    stage(&mut sink, 7, "1.3", &new, 1);
    sink.flash_mut().tear_next_record_write = Some(tear);
    assert_eq!(
        sink.activate(7, ActivateMode::Trial),
        Err(XferStatus::Flash)
    );
    assert!(
        !sink.flash().reset_requested,
        "no reset after a failed record write"
    );
    assert_eq!(
        sink.record().trial,
        NO_SLOT,
        "the sink still holds the old record"
    );
    sink
}

#[test]
fn a_power_cut_after_a_torn_activation_runs_the_previous_slot() {
    // The torn copy is rejected by its CRC, and the older record (no trial) runs.
    let sink = staged_with_torn_activation(40);
    let mut flash = sink.into_flash();
    assert_eq!(power_cycle(&mut flash, ResetReason::Normal), Some(0));
    let sink = SlotFirmware::new(flash);
    assert_eq!(sink.info().fw_version, "1.2");
    assert_eq!(sink.record().trial, NO_SLOT);
    assert_eq!(sink.last_trial(), None);
}

#[test]
fn a_retry_after_a_torn_activation_writes_the_record_over_the_torn_copy() {
    // The transfer is still staged, so the activation can be retried at once.
    let mut sink = staged_with_torn_activation(40);
    assert_eq!(sink.activate(7, ActivateMode::Trial), Ok(()));
    let mut flash = sink.into_flash();
    assert_eq!(power_cycle(&mut flash, ResetReason::Normal), Some(1));
}

#[test]
fn a_corrupted_newest_copy_falls_back_to_the_older_one() {
    let mut flash = activated_trial();
    // Damage the copy that holds the newest record (the activation's); the older one, written
    // before the transfer began, has no trial.
    let newest = (0u8..2)
        .filter_map(|copy| BootRecord::decode(&read_copy(&flash, copy)).map(|(seq, _)| (seq, copy)))
        .max()
        .map(|(_, copy)| copy)
        .expect("a valid record");
    flash.corrupt_record_copy(usize::from(newest), 10);
    let sink = SlotFirmware::new(flash);
    assert_eq!(
        sink.record().trial,
        NO_SLOT,
        "the older record has no trial"
    );
    assert_eq!(sink.info().fw_version, "1.2");
}

fn read_copy(flash: &MemFlash, copy: u8) -> [u8; RECORD_LEN] {
    use orion_mcu_template::slots::SlotFlash;
    let mut buf = [0u8; RECORD_LEN];
    flash.read_record(copy, &mut buf).expect("read");
    buf
}

#[test]
fn the_next_transfer_clears_the_rollback_report() {
    let new = image(3, 200);
    let mut flash = activated_trial();
    assert_eq!(power_cycle(&mut flash, ResetReason::Normal), Some(1));
    assert_eq!(power_cycle(&mut flash, ResetReason::Normal), Some(0));
    let mut sink = SlotFirmware::new(flash);
    assert!(sink.last_trial().is_some());

    assert_eq!(sink.begin(&begin_view(9, "1.4", &new, 1)), Ok(0));
    assert_eq!(sink.last_trial(), None, "a new transfer clears the report");
    let flash = sink.into_flash();
    let sink = SlotFirmware::new(flash);
    assert_eq!(sink.last_trial(), None, "and the clearing is persisted");
}

#[test]
fn the_record_round_trips_through_its_encoding() {
    use orion_mcu_template::slots::{Rollback, Text};
    let mut record = BootRecord::factory();
    record.active = 1;
    record.previous = 0;
    record.trial = 1;
    record.trial_boots = 1;
    record.slots[1].version = Text::new("1.3").expect("version");
    record.slots[1].len = 500;
    record.slots[1].crc32c = 0xDEAD_BEEF;
    record.rollback = Some(Rollback {
        version: Text::new("1.1").expect("version"),
        reason: RollbackReason::BootFailure,
        note: Text::new("boot fault").expect("note"),
    });
    let encoded = record.encode(5);
    assert_eq!(BootRecord::decode(&encoded), Some((5, record)));
    assert_eq!(
        BootRecord::decode(&[0xFF; RECORD_LEN]),
        None,
        "an erased copy is not a record"
    );
}

#[test]
fn transfers_are_refused_for_bad_slots_sizes_and_order() {
    let data = image(4, 300);
    let mut sink = SlotFirmware::new(MemFlash::with_factory("1.2", &image(1, 300)));
    // The active slot cannot be written.
    assert_eq!(
        sink.begin(&begin_view(1, "1.3", &data, 0)),
        Err(XferStatus::BadSlot)
    );
    // Larger than a slot, and a version longer than the slot record holds.
    let huge = vec![0u8; 5000];
    assert_eq!(
        sink.begin(&begin_view(1, "1.3", &huge, 1)),
        Err(XferStatus::NoSpace)
    );
    let long_version = "v".repeat(33);
    assert_eq!(
        sink.begin(&begin_view(1, &long_version, &data, 1)),
        Err(XferStatus::Unsupported)
    );
    assert_eq!(sink.begin(&begin_view(1, "1.3", &data, 1)), Ok(0));
    // Chunks must be in order, and the image must verify before it is activated.
    assert_eq!(sink.write(5, &data[..10]), Err(XferStatus::BadOffset));
    sink.write(0, &data[..100]).expect("first chunk");
    assert_eq!(
        sink.finish(1, 300, crc32c(&data)),
        Err(XferStatus::BadDigest),
        "an incomplete image does not verify"
    );
    sink.write(100, &data[100..]).expect("rest");
    assert_eq!(
        sink.finish(1, 300, crc32c(&image(9, 300))),
        Err(XferStatus::BadDigest),
        "a wrong digest is refused"
    );
    assert_eq!(
        sink.activate(1, ActivateMode::Trial),
        Err(XferStatus::BadDigest)
    );
    sink.finish(1, 300, crc32c(&data)).expect("verifies");
    assert_eq!(
        sink.activate(2, ActivateMode::Trial),
        Err(XferStatus::UnknownXfer)
    );
    assert_eq!(
        sink.activate(1, ActivateMode::Handoff),
        Err(XferStatus::Unsupported)
    );
}
