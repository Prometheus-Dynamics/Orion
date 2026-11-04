//! Two-slot firmware (feature `xfer`) on an in-memory flash: the sink's transfer rules, the boot
//! record and its torn writes, the bootloader's decision, and a device session driven by the
//! host's `FirmwareTransfer`. Host-tested only: no board has run this code.
#![cfg(feature = "xfer")]

mod boot;
mod flash;
mod lifecycle;
mod session;
