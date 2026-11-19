//! The crate's integration tests, in one binary: one link instead of one for each file.
//! Each file is a module, and its tests are named `<file>::<test>`.

#[path = "abort.rs"]
mod abort;
#[path = "configured.rs"]
mod configured;
#[path = "fetch.rs"]
mod fetch;
#[path = "own.rs"]
mod own;
#[path = "paused.rs"]
mod paused;
#[path = "quiet.rs"]
mod quiet;
