//! `orion-recovery`: the comm core alone, as one file.
//!
//! For an initramfs and a flasher image, where the only thing wanted of Orion is the way
//! in: SSH, sftp and scp, and the comm core's own protocol for tools. It is linked with
//! everything it needs, so the image holds this file and a shell and nothing else of
//! Orion's.
//!
//! ```text
//! orion-recovery [settings file]
//! ```
//!
//! - With no settings file where it looks (`/etc/orion/orion.conf`) it listens for SSH on
//!   port 22 and is otherwise a comm core with nothing set.
//! - With settings that cannot be read, or that it cannot start as they say, it does not
//!   end: it opens the recovery way in (see `orion-recovery`, the crate).
//! - It is also its own helper programs: started with `ssh-session`, `ssh-sftp` or
//!   `ssh-connection` it is the one the comm core asked for.
//!
//! The environment can set anything the file can: `ssh.listen` is `ORION_SSH_LISTEN`.
//! `ORION_RECOVERY_KEYS` and `ORION_RECOVERY_LISTEN` hold even when the file is broken.
//! `ORION_LOG` names how much is logged, to standard error.

use std::path::PathBuf;
use std::process::ExitCode;

use orion_comm_app::{CommSettings, RecoverWith, run_or_recover};
use orion_config::{ConfigError, Source, Warning};

/// Where the settings are unless the command line says otherwise.
const SETTINGS: &str = "/etc/orion/orion.conf";

/// What a board with no settings file at all is: the way in, and nothing more asked of it.
const NOTHING_SET: &str = "ssh.listen = 0.0.0.0:22\n";

/// The settings in `path`, or the ones for a board that has no such file. Whether a
/// variable of some other program's is one of Orion's is not this program's to say, so
/// only the ones a comm core reads are looked at.
fn settings(path: &std::path::Path) -> Result<(CommSettings, Vec<Warning>), ConfigError> {
    let text = match std::fs::read_to_string(path) {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => NOTHING_SET.to_owned(),
        Err(error) => {
            return Err(ConfigError {
                key: path.display().to_string(),
                message: error.to_string(),
            });
        }
    };
    let mut source = Source::parse(
        &text,
        std::env::vars().filter(|(name, _)| name != "ORION_LOG"),
    )?;
    let settings = CommSettings::from_source(&mut source)?;
    Ok((settings, source.warnings()))
}

fn main() -> ExitCode {
    // Started as one of the comm core's helpers: be that, and nothing else.
    if let Some(code) = orion_comm_app::helper(std::env::args_os().skip(1)) {
        return ExitCode::from(u8::try_from(code).unwrap_or(1));
    }
    let mut args = std::env::args_os().skip(1);
    let path = match args.next() {
        Some(flag) if flag == "--version" || flag == "-V" => {
            println!("orion-recovery {}", env!("CARGO_PKG_VERSION"));
            return ExitCode::SUCCESS;
        }
        Some(flag) if flag == "--help" || flag == "-h" => {
            println!(
                "usage: orion-recovery [settings file]\n\nThe comm core alone. Settings are read from {SETTINGS} unless a file is named."
            );
            return ExitCode::SUCCESS;
        }
        Some(path) => PathBuf::from(path),
        None => PathBuf::from(SETTINGS),
    };
    orion_log::install();
    let (settings, unknown) = match settings(&path) {
        Ok((settings, unknown)) => (Ok(settings), unknown),
        Err(error) => (Err(error), Vec::new()),
    };
    match run_or_recover(settings, &unknown, RecoverWith::from_env()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            tracing::error!("{error}");
            ExitCode::FAILURE
        }
    }
}
