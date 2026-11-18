//! The crate's integration tests, in one binary: one link instead of one for each file.
//! Each file is a module, and its tests are named `<file>::<test>`.

#[path = "one_list.rs"]
mod one_list;
#[path = "port.rs"]
mod port;
