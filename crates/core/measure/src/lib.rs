//! Measuring what the fast paths cost.
//!
//! The rule the rest of Orion is held to is zero allocations on steady-state paths. Feature
//! `alloc-counter` gives a counting allocator and [`harness`], which later PRs use to gate that
//! rule. Feature `costs` gives per-message cost and wake-up counters. With a feature off, its part
//! is gone from the build. Feature `sched` gives [`sched`]: a proof that a path makes no system
//! call, and thread pinning and CPU time for benches. Feature `clock` gives [`clock`]: whether
//! the kernel holds the wall clock synchronized, and the two calls that correct it.

mod costs;

#[cfg(feature = "alloc-counter")]
mod alloc_counter;
#[cfg(feature = "alloc-counter")]
pub mod harness;

#[cfg(feature = "alloc-counter")]
pub use alloc_counter::{AllocCount, CountingAlloc, thread_counts};
pub use costs::{CostSnapshot, Costs};

#[cfg(all(feature = "clock", target_os = "linux"))]
pub mod clock;
#[cfg(all(feature = "sched", target_os = "linux"))]
pub mod sched;
