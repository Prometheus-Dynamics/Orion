//! Session-layer tests: the device and host state machines driven by a deterministic virtual
//! clock over simulated links. Nothing sleeps.

#![cfg(feature = "alloc")]

#[path = "../common/mod.rs"]
mod common;

#[cfg(all(feature = "std", feature = "actions", feature = "xfer"))]
mod ack_rule;
#[cfg(all(feature = "std", feature = "actions"))]
mod actions;
#[cfg(feature = "std")]
mod can_e2e;
mod device_scripted;
#[cfg(all(feature = "std", feature = "xfer"))]
mod firmware;
#[cfg(all(feature = "std", feature = "xfer"))]
mod firmware_can;
#[cfg(all(feature = "std", feature = "xfer"))]
mod flash;
#[cfg(feature = "std")]
mod sim;
#[cfg(feature = "std")]
mod status;
#[cfg(feature = "std")]
mod stream_e2e;
#[cfg(feature = "std")]
mod views_e2e;
