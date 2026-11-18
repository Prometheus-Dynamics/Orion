//! The crate's integration tests, in one binary: one link instead of one for each file.
//! Each file is a module, and its tests are named `<file>::<test>`.

#[path = "browser.rs"]
mod browser;
#[path = "next.rs"]
mod next;
#[path = "opening.rs"]
mod opening;
#[path = "ws.rs"]
mod ws;
