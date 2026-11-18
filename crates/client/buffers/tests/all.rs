//! The crate's integration tests, in one binary: one link instead of one for each file.
//! Each file is a module, and its tests are named `<file>::<test>`.

/// Counts allocations, each thread its own, for the tests that check a path makes none.
/// A binary has one global allocator, so it is set here, once for every file.
#[global_allocator]
static GLOBAL: orion_measure::CountingAlloc = orion_measure::CountingAlloc::system();

#[path = "adopt.rs"]
mod adopt;
#[path = "ended.rs"]
mod ended;
#[path = "frames.rs"]
mod frames;
#[path = "old_pool.rs"]
mod old_pool;
