//! The crate's integration tests, in one binary: one link instead of one for each file.
//! Each file is a module, and its tests are named `<file>::<test>`.

#[path = "service.rs"]
mod service;
#[path = "store.rs"]
mod store;
