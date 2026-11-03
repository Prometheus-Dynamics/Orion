//! The simulated two-slot A/B flash of a firmware device, shared by the stream and CAN transfer
//! simulations (`firmware.rs`, `firmware_can.rs`). Its bootloader gives an unconfirmed trial one
//! boot and reverts it on the second.

use orion_link::crc32c;
use orion_link::wire::{ActivateMode, FirmwareInfo, FirmwareSink, XferBeginView, XferStatus};

/// One slot of the simulated flash.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Slot {
    pub(super) version: String,
    pub(super) image: Vec<u8>,
}

/// The image being written into the inactive slot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Staging {
    pub(super) xfer_id: u32,
    pub(super) total: u32,
    pub(super) crc32c: u32,
    pub(super) slot: u8,
    pub(super) version: String,
    pub(super) data: Vec<u8>,
    pub(super) verified: bool,
}

/// A two-slot A/B device: flash, a bootloader that gives an unconfirmed trial one boot, and the
/// `FirmwareSink` the device session calls.
#[derive(Debug, Clone)]
pub(super) struct SimFlash {
    pub(super) slots: [Slot; 2],
    pub(super) active: u8,
    pub(super) previous: u8,
    pub(super) trial: bool,
    /// Boots the running trial image has had (the bootloader reverts it on the second).
    pub(super) trial_boots: u8,
    pub(super) staging: Option<Staging>,
    pub(super) reboot_pending: bool,
    /// Largest chunk the device advertises (`DeviceInfo`).
    pub(super) max_chunk: u16,
}

impl SimFlash {
    pub(super) fn factory(version: &str) -> Self {
        Self {
            slots: [
                Slot {
                    version: version.to_owned(),
                    image: Vec::new(),
                },
                Slot {
                    version: String::new(),
                    image: Vec::new(),
                },
            ],
            active: 0,
            previous: 0,
            trial: false,
            trial_boots: 0,
            staging: None,
            reboot_pending: false,
            max_chunk: 96,
        }
    }

    /// The bootloader's part of a power cycle: an unconfirmed trial reverts on its second boot.
    pub(super) fn power_cycle(&mut self) {
        self.reboot_pending = false;
        if self.trial {
            if self.trial_boots >= 1 {
                self.active = self.previous;
                self.trial = false;
                self.trial_boots = 0;
            } else {
                self.trial_boots += 1;
            }
        }
    }
}

impl FirmwareSink for SimFlash {
    fn info(&self) -> FirmwareInfo<'_> {
        FirmwareInfo {
            fw_version: &self.slots[usize::from(self.active)].version,
            active_slot: self.active,
            slots: 2,
            trial: self.trial,
            max_chunk: self.max_chunk,
            window: 1,
            bootloader: "sim-boot 1",
        }
    }

    fn begin(&mut self, begin: &XferBeginView<'_>) -> Result<u32, XferStatus> {
        if begin.target_slot > 1 || begin.target_slot == self.active {
            return Err(XferStatus::BadSlot);
        }
        if let Some(staging) = &self.staging
            && staging.xfer_id == begin.xfer_id
            && staging.total == begin.total_len
            && staging.crc32c == begin.image_crc32c
            && staging.slot == begin.target_slot
        {
            return Ok(staging.data.len() as u32); // resume
        }
        self.staging = Some(Staging {
            xfer_id: begin.xfer_id,
            total: begin.total_len,
            crc32c: begin.image_crc32c,
            slot: begin.target_slot,
            version: begin.version.to_owned(),
            data: Vec::new(),
            verified: false,
        });
        Ok(0)
    }

    fn write(&mut self, offset: u32, data: &[u8]) -> Result<(), XferStatus> {
        let staging = self.staging.as_mut().ok_or(XferStatus::UnknownXfer)?;
        if offset as usize != staging.data.len() {
            return Err(XferStatus::BadOffset);
        }
        if staging.data.len() + data.len() > staging.total as usize {
            return Err(XferStatus::NoSpace);
        }
        staging.data.extend_from_slice(data);
        Ok(())
    }

    fn finish(&mut self, xfer_id: u32, total_len: u32, crc: u32) -> Result<(), XferStatus> {
        let staging = self.staging.as_mut().ok_or(XferStatus::UnknownXfer)?;
        if staging.xfer_id != xfer_id {
            return Err(XferStatus::UnknownXfer);
        }
        if staging.data.len() != total_len as usize || crc32c(&staging.data) != crc {
            return Err(XferStatus::BadDigest);
        }
        staging.verified = true;
        Ok(())
    }

    fn activate(&mut self, xfer_id: u32, mode: ActivateMode) -> Result<(), XferStatus> {
        let staging = self.staging.as_ref().ok_or(XferStatus::UnknownXfer)?;
        if staging.xfer_id != xfer_id {
            return Err(XferStatus::UnknownXfer);
        }
        if !staging.verified {
            return Err(XferStatus::BadDigest);
        }
        if mode != ActivateMode::Trial {
            return Err(XferStatus::Unsupported);
        }
        let slot = usize::from(staging.slot);
        self.slots[slot] = Slot {
            version: staging.version.clone(),
            image: staging.data.clone(),
        };
        self.previous = self.active;
        self.active = staging.slot;
        self.trial = true;
        self.trial_boots = 0;
        self.reboot_pending = true;
        Ok(())
    }

    fn confirm(&mut self, slot: u8) -> Result<(), XferStatus> {
        if slot != self.active || !self.trial {
            return Err(XferStatus::BadSlot);
        }
        self.trial = false;
        Ok(())
    }
}
