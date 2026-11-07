//! `liborion-consumer`: prints the library's version; with a link socket path, also every status
//! entry the node has (one query), then exits.
//!
//!     liborion-consumer [/run/orion/link.sock]

use std::path::Path;
use std::process::ExitCode;
use std::time::Duration;

use liborion_rs::{EventKind, Link, check_version, version};

fn main() -> ExitCode {
    let library = version();
    println!(
        "liborion {}.{} layout {:#x}",
        library.major, library.minor, library.layout
    );
    if let Err(error) = check_version() {
        eprintln!("{error}");
        return ExitCode::FAILURE;
    }
    let Some(socket) = std::env::args().nth(1) else {
        return ExitCode::SUCCESS;
    };
    match status(Path::new(&socket)) {
        Ok(count) => {
            println!("{count} status entries");
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}

fn status(socket: &Path) -> Result<usize, liborion_rs::Error> {
    let mut link = Link::connect(socket, "liborion-consumer", Duration::from_secs(2))?;
    let req = link.status_query(None, None)?;
    let mut count = 0;
    while let Some(event) = link.next(Some(Duration::from_secs(5)))? {
        if event.req() != req {
            continue;
        }
        match event.kind() {
            EventKind::Status => {
                println!("{} {} {:?}", event.subject_str(), event.key_str(), event.value());
                count += 1;
            }
            EventKind::Answered | EventKind::RequestError => break,
            _ => {}
        }
    }
    Ok(count)
}
