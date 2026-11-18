//! The crate's integration tests, in one binary: one link instead of one for each file.
//! Each file is a module, and its tests are named `<file>::<test>`.

#[cfg(feature = "full")]
#[path = "facade.rs"]
mod facade;
#[cfg(feature = "full")]
#[path = "older.rs"]
mod older;
