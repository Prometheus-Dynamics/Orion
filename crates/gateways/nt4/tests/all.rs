//! The crate's integration tests, in one binary: one link instead of one for each file.
//! Each file is a module, and its tests are named `<file>::<test>`.

/// Counts allocations, each thread its own, for the tests that check a path makes none.
/// A binary has one global allocator, so it is set here, once for every file.
#[cfg(all(feature = "client", feature = "server"))]
#[global_allocator]
static GLOBAL: orion_measure::CountingAlloc = orion_measure::CountingAlloc::system();

#[path = "alloc_count.rs"]
mod alloc_count;
#[path = "hardening.rs"]
mod hardening;
#[path = "liveness.rs"]
mod liveness;
#[path = "roundtrip.rs"]
mod roundtrip;
#[path = "shared_port.rs"]
mod shared_port;
