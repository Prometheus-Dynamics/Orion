//! Firmware sink of the template's C entry points (feature `xfer`).
//!
//! The placeholder answers every query like a two-slot device that has no flash driver yet: it
//! accepts a `Begin`, refuses every write with `Flash`, and refuses activation. The C entry points
//! run it through the [`Firmware`] alias. A port with flash replaces the alias with
//! `slots::SlotFirmware<ItsFlash>`, the A/B sink in [`crate::slots`] (host-tested only; see that
//! module for what a port must check). Its code size is what the session's transfer logic costs,
//! which is what `scripts/mcu-size.sh` reports for the `uart-xfer` variant.

use orion_link::wire::{FirmwareInfo, FirmwareSink, XferBeginView, XferStatus};

/// The placeholder sink (no flash). See the [module docs](self).
#[derive(Debug, Clone, Copy, Default)]
pub struct PlaceholderFirmware;

/// The sink the C entry points run with. A port that has flash replaces this alias with
/// `SlotFirmware<ItsFlash>` (see [`crate::slots`]): the same session, with real slots and a
/// rollback report. Until then the placeholder keeps the C API's transfer behavior unchanged.
/// A C port enables feature `ffi-slots` instead, and defines the `orion_port_*` hooks.
#[cfg(not(feature = "ffi-slots"))]
pub type Firmware = PlaceholderFirmware;

/// The sink the C entry points run with: real slots over the port's flash hooks.
#[cfg(feature = "ffi-slots")]
pub type Firmware = crate::slots::SlotFirmware<port_flash::PortFlash>;

/// [`SlotFlash`](crate::slots::SlotFlash) over C functions the port defines (feature `ffi-slots`).
/// The hooks keep the meaning of the trait's methods and return `true` on success.
#[cfg(feature = "ffi-slots")]
pub mod port_flash {
    use crate::slots::{FlashError, RECORD_LEN, ResetReason, SlotFirmware, SlotFlash, boot};

    unsafe extern "C" {
        fn orion_port_slot_capacity() -> u32;
        fn orion_port_erase_slot(slot: u8) -> bool;
        fn orion_port_write_slot(slot: u8, offset: u32, data: *const u8, len: usize) -> bool;
        fn orion_port_read_slot(slot: u8, offset: u32, out: *mut u8, len: usize) -> bool;
        fn orion_port_read_record(copy: u8, out: *mut u8, len: usize) -> bool;
        fn orion_port_write_record(copy: u8, data: *const u8, len: usize) -> bool;
        fn orion_port_reset();
    }

    fn checked(ok: bool) -> Result<(), FlashError> {
        if ok { Ok(()) } else { Err(FlashError) }
    }

    /// The port's flash, reached through its `orion_port_*` hooks.
    #[derive(Debug, Clone, Copy, Default)]
    pub struct PortFlash;

    // SAFETY of the calls below: each pointer and length come from one live slice, and the port's
    // hooks only touch that range for the duration of the call.
    impl SlotFlash for PortFlash {
        fn slot_capacity(&self) -> u32 {
            unsafe { orion_port_slot_capacity() }
        }

        fn erase_slot(&mut self, slot: u8) -> Result<(), FlashError> {
            checked(unsafe { orion_port_erase_slot(slot) })
        }

        fn write_slot(&mut self, slot: u8, offset: u32, data: &[u8]) -> Result<(), FlashError> {
            checked(unsafe { orion_port_write_slot(slot, offset, data.as_ptr(), data.len()) })
        }

        fn read_slot(&self, slot: u8, offset: u32, out: &mut [u8]) -> Result<(), FlashError> {
            checked(unsafe { orion_port_read_slot(slot, offset, out.as_mut_ptr(), out.len()) })
        }

        fn read_record(&self, copy: u8, out: &mut [u8; RECORD_LEN]) -> Result<(), FlashError> {
            checked(unsafe { orion_port_read_record(copy, out.as_mut_ptr(), RECORD_LEN) })
        }

        fn write_record(&mut self, copy: u8, data: &[u8; RECORD_LEN]) -> Result<(), FlashError> {
            checked(unsafe { orion_port_write_record(copy, data.as_ptr(), RECORD_LEN) })
        }

        fn reset(&mut self) {
            unsafe { orion_port_reset() }
        }
    }

    impl Default for SlotFirmware<PortFlash> {
        fn default() -> Self {
            Self::new(PortFlash)
        }
    }

    /// The boot stage's decision on reset: the slot to start, or -1 if none may run. `fault` is
    /// true after a watchdog reset, a hard fault, or any reset that means the image failed to run.
    #[unsafe(no_mangle)]
    pub extern "C" fn orion_boot(fault: bool) -> i32 {
        let reason = if fault {
            ResetReason::Fault
        } else {
            ResetReason::Normal
        };
        boot(&mut PortFlash, reason).map_or(-1, i32::from)
    }
}

impl FirmwareSink for PlaceholderFirmware {
    fn info(&self) -> FirmwareInfo<'_> {
        FirmwareInfo {
            fw_version: "0.0.0",
            active_slot: 0,
            slots: 2,
            trial: false,
            max_chunk: 96,
            window: 1,
            bootloader: "",
        }
    }

    fn begin(&mut self, _begin: &XferBeginView<'_>) -> Result<u32, XferStatus> {
        Ok(0)
    }

    fn write(&mut self, _offset: u32, _data: &[u8]) -> Result<(), XferStatus> {
        Err(XferStatus::Flash)
    }

    fn finish(&mut self, _xfer_id: u32, _total_len: u32, _crc32c: u32) -> Result<(), XferStatus> {
        Err(XferStatus::Flash)
    }

    fn activate(
        &mut self,
        _xfer_id: u32,
        _mode: orion_link::wire::ActivateMode,
    ) -> Result<(), XferStatus> {
        Err(XferStatus::Unsupported)
    }

    fn confirm(&mut self, _slot: u8) -> Result<(), XferStatus> {
        Err(XferStatus::BadSlot)
    }
}
