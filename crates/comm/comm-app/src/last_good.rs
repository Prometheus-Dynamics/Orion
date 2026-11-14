//! The record of the last good start: the ways in the comm core opened, written in the
//! settings' own words to `last-good.carriers` in the state directory on every start that
//! got as far as serving. When the settings cannot be read, recovery opens the same ways in
//! from it, so a board broken by its settings file is still reached the way it was reached
//! before. Nothing secret is in it: names, paths and addresses.

use std::fmt::Write as _;
use std::path::{Path, PathBuf};

use orion_config::Source;
use tracing::warn;

use crate::CommSettings;

/// The record's name in the state directory.
pub const FILE: &str = "last-good.carriers";

/// Where the record is when the settings cannot say: the usual state directory.
pub const USUAL: &str = "/var/lib/orion/last-good.carriers";

/// The record of `settings`, as settings text.
#[must_use]
pub fn text(settings: &CommSettings) -> String {
    let mut text =
        String::from("# Written by the comm core on its last good start. Read by recovery.\n");
    let mut line = |key: &str, value: &dyn std::fmt::Display| {
        let _ = writeln!(text, "{key} = {value}");
    };
    line("node.name", &settings.config.node_name);
    line("socket", &settings.config.socket.display());
    line("socket.mode", &format_args!("{:o}", settings.socket_mode));
    if !settings.node_uids.is_empty() {
        line("socket.node_uids", &join(&settings.node_uids));
    }
    #[cfg(feature = "link-usb")]
    if !settings.usb_links.is_empty() {
        line("links.usb", &join(&settings.usb_links));
    }
    #[cfg(feature = "ssh")]
    if let Some(ssh) = &settings.ssh {
        line("ssh.listen", &ssh.listen);
        if let Some(interface) = &ssh.interface {
            line("ssh.interface", interface);
        }
    }
    #[cfg(feature = "recovery")]
    {
        line("recovery.keys", &settings.recovery_keys.display());
        if !settings.recovery_listen.is_empty() {
            line("recovery.listen", &join(&settings.recovery_listen));
        }
    }
    text
}

fn join<T: std::fmt::Display>(items: &[T]) -> String {
    items
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(", ")
}

/// Writes the record of `settings` to their state directory, beside and renamed over, so
/// there is never half a record. A record that cannot be written is said, and the start goes
/// on: it is for a later recovery, not for this run.
pub fn write(settings: &CommSettings) {
    let path = settings.config.state_dir.join(FILE);
    let beside = path.with_extension("new");
    let wrote =
        std::fs::write(&beside, text(settings)).and_then(|()| std::fs::rename(&beside, &path));
    if let Err(error) = wrote {
        warn!(
            "the record of this start cannot be kept at {}: {error}",
            path.display()
        );
    }
}

/// The settings the record at `path` holds, if there is one that reads. Anything else in
/// the file is left out: only the ways in are believed, and security is on.
#[must_use]
pub fn read(path: &Path) -> Option<CommSettings> {
    let text = std::fs::read_to_string(path).ok()?;
    let mut source = Source::parse(&text, Vec::new()).ok()?;
    let mut settings = CommSettings::from_source(&mut source).ok()?;
    settings.config.security = orion_config::Security::On;
    Some(settings)
}

/// Where the record is: `ORION_RECOVERY_LAST_GOOD`, or the usual place.
#[must_use]
pub fn path_from_env() -> PathBuf {
    std::env::var_os("ORION_RECOVERY_LAST_GOOD").map_or_else(|| USUAL.into(), PathBuf::from)
}
