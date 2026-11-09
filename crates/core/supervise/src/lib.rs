//! The failure rule, in one place.
//!
//! Never give up. Backoff is capped and clears itself on recovery. Nothing switches itself off
//! unless someone configured that. Manual off is one call. Access paths (the ways in: USB link,
//! serial, SSH, the network) are never backed off and can't be set to switch themselves off, because
//! a flaky way in may be the only way in.
//!
//! Nothing here reads a clock. Callers pass milliseconds, which makes every rule testable.

#![cfg_attr(not(feature = "std"), no_std)]

mod backoff;
mod log;
mod supervisor;

#[cfg(feature = "std")]
mod contain;

pub use backoff::{ACCESS_RETRY_MS, Backoff, FIRST_DELAY_MS, MAX_DELAY_MS, Path};
#[cfg(feature = "std")]
pub use contain::{Contained, Panicked, contain};
pub use log::LogLimit;
pub use supervisor::{AutoOff, AutoOffRefused, Decision, Off, Report, State, Status, Supervisor};
