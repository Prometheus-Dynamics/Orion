//! The crate's integration tests, in one binary: one link instead of one for each file.
//! Each file is a module, and its tests are named `<file>::<test>`.

#[path = "based_on.rs"]
mod based_on;
#[path = "board.rs"]
mod board;
#[path = "modes.rs"]
mod modes;
#[path = "receivers.rs"]
mod receivers;
#[path = "roots.rs"]
mod roots;
#[path = "sinks.rs"]
mod sinks;
#[path = "time.rs"]
mod time;
