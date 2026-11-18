//! The crate's integration tests, in one binary: one link instead of one for each file.
//! Each file is a module, and its tests are named `<file>::<test>`.

// What several test files share, once for the binary.
mod common;

#[path = "broker.rs"]
mod broker;
#[path = "processes.rs"]
mod processes;
