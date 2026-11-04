//! The C port's route to two-slot firmware (feature `ffi-slots`): `SlotFirmware` and `orion_boot`
//! through the `orion_port_*` hooks, here defined over the in-memory flash. Built on its own
//! (`cargo test --features ffi-slots --test port_hooks`), since every other test binary would need
//! the hooks too. Host-tested only: no board has run this code.
#![cfg(feature = "ffi-slots")]

#[allow(dead_code)]
#[path = "slots/flash.rs"]
mod flash;

use flash::{MemFlash, begin_view, image};
use orion_link::crc32c;
use orion_link::wire::{ActivateMode, FirmwareSink, RollbackReason};
use orion_mcu_template::firmware::Firmware;
use orion_mcu_template::firmware::port_flash::orion_boot;
use orion_mcu_template::slots::{RECORD_LEN, SlotFlash};
use std::sync::Mutex;

static FLASH: Mutex<Option<MemFlash>> = Mutex::new(None);

fn with_flash<T>(use_flash: impl FnOnce(&mut MemFlash) -> T) -> T {
    use_flash(
        FLASH
            .lock()
            .expect("flash lock")
            .as_mut()
            .expect("flash installed"),
    )
}

#[unsafe(no_mangle)]
extern "C" fn orion_port_slot_capacity() -> u32 {
    with_flash(|flash| flash.slot_capacity())
}

#[unsafe(no_mangle)]
extern "C" fn orion_port_erase_slot(slot: u8) -> bool {
    with_flash(|flash| flash.erase_slot(slot).is_ok())
}

#[unsafe(no_mangle)]
unsafe extern "C" fn orion_port_write_slot(
    slot: u8,
    offset: u32,
    data: *const u8,
    len: usize,
) -> bool {
    let data = unsafe { std::slice::from_raw_parts(data, len) };
    with_flash(|flash| flash.write_slot(slot, offset, data).is_ok())
}

#[unsafe(no_mangle)]
unsafe extern "C" fn orion_port_read_slot(slot: u8, offset: u32, out: *mut u8, len: usize) -> bool {
    let out = unsafe { std::slice::from_raw_parts_mut(out, len) };
    with_flash(|flash| flash.read_slot(slot, offset, out).is_ok())
}

#[unsafe(no_mangle)]
unsafe extern "C" fn orion_port_read_record(copy: u8, out: *mut u8, len: usize) -> bool {
    assert_eq!(len, RECORD_LEN);
    let out = unsafe { &mut *out.cast::<[u8; RECORD_LEN]>() };
    with_flash(|flash| flash.read_record(copy, out).is_ok())
}

#[unsafe(no_mangle)]
unsafe extern "C" fn orion_port_write_record(copy: u8, data: *const u8, len: usize) -> bool {
    assert_eq!(len, RECORD_LEN);
    let data = unsafe { &*data.cast::<[u8; RECORD_LEN]>() };
    with_flash(|flash| flash.write_record(copy, data).is_ok())
}

#[unsafe(no_mangle)]
extern "C" fn orion_port_reset() {
    with_flash(SlotFlash::reset);
}

#[test]
fn an_unconfirmed_trial_reverts_through_the_c_hooks() {
    let old = image(1, 300);
    let new = image(2, 500);
    *FLASH.lock().expect("flash lock") = Some(MemFlash::with_factory("1.2", &old));

    let mut sink = Firmware::default();
    assert_eq!(sink.begin(&begin_view(7, "1.3", &new, 1)), Ok(0));
    sink.write(0, &new).expect("write");
    let len = u32::try_from(new.len()).expect("length");
    sink.finish(7, len, crc32c(&new)).expect("finish");
    assert_eq!(sink.activate(7, ActivateMode::Trial), Ok(()));
    assert!(with_flash(|flash| flash.reset_requested));

    assert_eq!(orion_boot(false), 1, "the trial boots once");
    assert_eq!(orion_boot(false), 0, "unconfirmed, it reverts");

    let sink = Firmware::default();
    let rollback = sink.last_trial().expect("the revert is reported");
    assert_eq!(
        (rollback.version, rollback.reason),
        ("1.3", RollbackReason::NotConfirmed)
    );
}
