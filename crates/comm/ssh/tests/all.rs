//! The crate's integration tests, in one binary: one link instead of one for each file.
//! Each file is a module, and its tests are named `<file>::<test>`.

// What several test files share, once for the binary.
mod common;

#[cfg(feature = "forward")]
#[path = "forward.rs"]
mod forward;
#[path = "interface.rs"]
mod interface;
#[cfg(feature = "login-helper")]
#[path = "login.rs"]
mod login;
#[path = "openssh.rs"]
mod openssh;
