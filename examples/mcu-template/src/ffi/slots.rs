//! New firmware into two slots, over flash the C port reaches with its `orion_port_*` hooks.

use core::ffi::c_char;

use super::{ORION_ERR_ARG, ORION_ERR_LINK, ORION_ERR_STATE, PORT, kept};

/// Takes new firmware: every file whose destination starts with `prefix`, such as
/// "devices/imu-board/firmware/", goes into the slot that is not running. Call before the
/// first `orion_poll`. Returns 0 or `ORION_ERR_*`.
///
/// # Safety
///
/// `prefix` is a NUL-terminated string that stays as it is for as long as the firmware
/// runs.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn orion_firmware(prefix: *const c_char) -> i32 {
    // SAFETY: the caller's promise.
    let Some(prefix) = (unsafe { kept(prefix, 200) }).filter(|prefix| !prefix.is_empty()) else {
        return ORION_ERR_ARG;
    };
    PORT.declaring(|tables| {
        tables.prefix = prefix;
        0
    })
    .unwrap_or(ORION_ERR_STATE)
}

/// The firmware on trial works: keep it. Call once the application has shown it does;
/// until then a reset goes back to the slot before. 0 or `ORION_ERR_*`.
#[unsafe(no_mangle)]
pub extern "C" fn orion_firmware_confirm() -> i32 {
    PORT.running(|running| match &mut running.layers.firmware {
        Some(firmware) => match firmware.confirm() {
            Ok(()) => 0,
            Err(_) => ORION_ERR_LINK,
        },
        None => ORION_ERR_STATE,
    })
    .unwrap_or(ORION_ERR_STATE)
}

/// For the bootloader, after every reset: the slot to start, or 255 for none. `fault` is
/// true after a watchdog or fault reset, which ends a trial and goes back.
#[unsafe(no_mangle)]
pub extern "C" fn orion_boot(fault: bool) -> u8 {
    let reason = if fault {
        orion_device_firmware::ResetReason::Fault
    } else {
        orion_device_firmware::ResetReason::Normal
    };
    orion_device_firmware::boot(&mut Hooks, reason).unwrap_or(255)
}

pub(crate) use hooks::Hooks;

/// The flash, as the C port reaches it.
mod hooks {
    use orion_device_firmware::{FlashError, RECORD_LEN, SlotFlash};

    unsafe extern "C" {
        /// How many bytes a slot holds.
        fn orion_port_slot_capacity() -> u32;
        /// Erases slot `slot` (0 or 1). True if it worked.
        fn orion_port_erase_slot(slot: u8) -> bool;
        /// Writes `len` bytes at `offset` in slot `slot`, which is erased there.
        fn orion_port_write_slot(slot: u8, offset: u32, data: *const u8, len: usize) -> bool;
        /// Reads `len` bytes at `offset` in slot `slot`.
        fn orion_port_read_slot(slot: u8, offset: u32, out: *mut u8, len: usize) -> bool;
        /// Reads copy `copy` (0 or 1) of the boot record, 96 bytes.
        fn orion_port_read_record(copy: u8, out: *mut u8) -> bool;
        /// Erases and writes copy `copy` of the boot record, 96 bytes.
        fn orion_port_write_record(copy: u8, data: *const u8) -> bool;
    }

    /// The port's flash.
    #[derive(Debug)]
    pub struct Hooks;

    const fn done(worked: bool) -> Result<(), FlashError> {
        if worked { Ok(()) } else { Err(FlashError) }
    }

    impl SlotFlash for Hooks {
        fn slot_capacity(&self) -> u32 {
            // SAFETY: a hook the port defines, as the header declares it.
            unsafe { orion_port_slot_capacity() }
        }

        fn erase_slot(&mut self, slot: u8) -> Result<(), FlashError> {
            // SAFETY: as above.
            done(unsafe { orion_port_erase_slot(slot) })
        }

        fn write_slot(&mut self, slot: u8, offset: u32, data: &[u8]) -> Result<(), FlashError> {
            // SAFETY: as above; `data` is valid for its length.
            done(unsafe { orion_port_write_slot(slot, offset, data.as_ptr(), data.len()) })
        }

        fn read_slot(&self, slot: u8, offset: u32, out: &mut [u8]) -> Result<(), FlashError> {
            // SAFETY: as above; `out` is valid for its length.
            done(unsafe { orion_port_read_slot(slot, offset, out.as_mut_ptr(), out.len()) })
        }

        fn read_record(&self, copy: u8, out: &mut [u8; RECORD_LEN]) -> Result<(), FlashError> {
            // SAFETY: as above; `out` holds a record.
            done(unsafe { orion_port_read_record(copy, out.as_mut_ptr()) })
        }

        fn write_record(&mut self, copy: u8, data: &[u8; RECORD_LEN]) -> Result<(), FlashError> {
            // SAFETY: as above; `data` holds a record.
            done(unsafe { orion_port_write_record(copy, data.as_ptr()) })
        }
    }
}
