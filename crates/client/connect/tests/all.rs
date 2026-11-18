//! The crate's integration tests, in one binary: one link instead of one for each file.
//! Each file is a module, and its tests are named `<file>::<test>`.

// What several test files share, once for the binary, where they are built.
#[cfg(target_os = "linux")]
mod common;

/// Counts allocations, each thread its own, for the tests that check a path makes none.
/// A binary has one global allocator, so it is set here, once for every file.
#[cfg(target_os = "linux")]
#[global_allocator]
static GLOBAL: orion_measure::CountingAlloc = orion_measure::CountingAlloc::system();

#[path = "alive.rs"]
mod alive;
#[path = "connect.rs"]
mod connect;
#[path = "link.rs"]
mod link;
#[path = "link_stale.rs"]
mod link_stale;
#[path = "older.rs"]
mod older;
#[path = "path.rs"]
mod path;
#[path = "patience.rs"]
mod patience;
