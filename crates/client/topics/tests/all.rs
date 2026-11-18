//! The crate's integration tests, in one binary: one link instead of one for each file.
//! Each file is a module, and its tests are named `<file>::<test>`.

/// Counts allocations, each thread its own, for the tests that check a path makes none.
/// A binary has one global allocator, so it is set here, once for every file.
#[cfg(target_os = "linux")]
#[global_allocator]
static GLOBAL: orion_measure::CountingAlloc = orion_measure::CountingAlloc::system();

#[path = "old_rings.rs"]
mod old_rings;
#[path = "topics.rs"]
mod topics;
