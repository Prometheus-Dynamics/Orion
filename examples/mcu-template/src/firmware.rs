//! Firmware sink of the template's C entry points (feature `xfer`).
//!
//! The placeholder answers every query like a two-slot device that has no flash driver yet: it
//! accepts a `Begin`, refuses every write with `Flash`, and refuses activation. A port replaces it
//! with a sink that writes its image slots and runs the bootloader's handoff or trial boot (see
//! `docs/update-recovery.md`, "MCU firmware over the link"). Its code size is what the session's
//! transfer logic costs, which is what `scripts/mcu-size.sh` reports for the `uart-xfer` variant.

use orion_link::wire::{FirmwareInfo, FirmwareSink, XferBeginView, XferStatus};

/// The placeholder sink (no flash). See the [module docs](self).
#[derive(Debug, Clone, Copy, Default)]
pub struct PlaceholderFirmware;

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
