//! The crate's integration tests, in one binary: one link instead of one for each file.
//! Each file is a module, and its tests are named `<file>::<test>`.

#[path = "configured.rs"]
mod configured;
#[path = "fetch.rs"]
mod fetch;
#[path = "own.rs"]
mod own;
