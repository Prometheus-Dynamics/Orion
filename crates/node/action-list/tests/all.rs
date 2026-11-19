//! The crate's integration tests, in one binary: one link instead of one for each file.
//! Each file is a module, and its tests are named `<file>::<test>`.

// What several test files share, once for the binary.
mod common;

#[path = "caller_gone.rs"]
mod caller_gone;
#[path = "ended.rs"]
mod ended;
#[path = "handles.rs"]
mod handles;
#[path = "list.rs"]
mod list;
#[path = "stopped.rs"]
mod stopped;
