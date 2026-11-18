//! The crate's integration tests, in one binary: one link instead of one for each file.
//! Each file is a module, and its tests are named `<file>::<test>`.

// What several test files share, once for the binary.
mod support;

#[path = "comm_core.rs"]
mod comm_core;
#[path = "contained.rs"]
mod contained;
#[path = "node_logic.rs"]
mod node_logic;
#[path = "providers.rs"]
mod providers;
#[path = "workloads.rs"]
mod workloads;
