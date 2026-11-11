//! The sample executor as a program.
//!
//! ```text
//! orion-sample-executor [--socket PATH] [--kind KIND]...
//! ```
//!
//! It offers `process` unless told other kinds, and runs until it is killed.

use std::path::PathBuf;
use std::process::ExitCode;

use orion_sample_executor::{Sample, run};

fn main() -> ExitCode {
    let mut socket = PathBuf::from("/run/orion/orion.sock");
    let mut kinds = Vec::new();
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match (arg.as_str(), args.next()) {
            ("--socket", Some(path)) => socket = PathBuf::from(path),
            ("--kind", Some(kind)) => match kind.parse() {
                Ok(kind) => kinds.push(kind),
                Err(error) => {
                    eprintln!("{kind:?} is not a kind: {error}");
                    return ExitCode::from(2);
                }
            },
            _ => {
                eprintln!("usage: orion-sample-executor [--socket PATH] [--kind KIND]...");
                return ExitCode::from(2);
            }
        }
    }
    if kinds.is_empty() {
        match "process".parse() {
            Ok(kind) => kinds.push(kind),
            Err(_) => return ExitCode::FAILURE,
        }
    }
    let mut sample = Sample::new(kinds);
    match run(&socket, &mut sample, || true) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("orion-sample-executor: {error}");
            ExitCode::FAILURE
        }
    }
}
