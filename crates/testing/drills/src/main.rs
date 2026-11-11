//! `orion-drill`: `orion` itself, and the programs a drill runs beside it.
//!
//! ```text
//! orion-drill comm FILE | node FILE | <anything orion takes>
//! orion-drill publisher SOCKET | subscriber SOCKET [stall] | camera SOCKET | viewer SOCKET
//! orion-drill executor SOCKET | provider SOCKET
//! ```
//!
//! The comm core and node logic are the real ones: what is not a drill role goes to `orion`.
//! Each role prints what it sees, one line at a time, for the drill to read.

use std::process::ExitCode;

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match orion_drills::role(&args) {
        Some(code) => code,
        None => orion_bin::main(),
    }
}
