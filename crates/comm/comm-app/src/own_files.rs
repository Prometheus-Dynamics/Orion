//! The comm core's own files, under `orion/`, in running and in recovery: what explains a
//! board's trouble from Orion's side, fetched as any offered file is (`orion-ctl fetch`), with
//! no login and no program running. Each is made when it is asked for, is bounded, and is
//! read-only; `orion/index` lists them with their bounds.
//!
//! - `orion/log`: the newest lines of the comm core's log (`log.window_bytes`);
//! - `orion/settings`: what was set and where, the values of keys that may be secret left out;
//! - `orion/last-good.carriers`: the record of the last good start, as recovery reads it;
//! - `orion/state`: `running`, `flasher`, or `recovery` and why;
//! - `orion/sessions`: who is connected, as what;
//! - `orion/version`: this build, its interface revision and level, and a digest of its wire.

use std::fmt::Write as _;
use std::path::PathBuf;

use orion_file_offers::{FileOffersService, OwnFiles, OwnName};
use orion_server::Sessions;
use sha2::{Digest, Sha256};

use crate::CommSettings;

const PREFIX: &str = "orion/";

/// The bounds of those whose size is not set.
const SETTINGS_BYTES: u64 = 64 * 1024;
const LAST_GOOD_BYTES: u64 = 16 * 1024;
const STATE_BYTES: u64 = 4 * 1024;
const SESSIONS_BYTES: u64 = 64 * 1024;
const VERSION_BYTES: u64 = 4 * 1024;

/// The comm core's own files.
#[derive(Debug)]
pub(crate) struct OrionFiles {
    settings: String,
    state_dir: PathBuf,
    state: String,
}

impl OrionFiles {
    /// The files of a comm core run by `settings`, if they were read, in `state`: the hello
    /// document's word, then why, in recovery.
    pub(crate) fn new(settings: Option<&CommSettings>, state: String) -> Self {
        let window = settings.map_or(orion_log::WINDOW_BYTES, |settings| {
            settings.log_window_bytes
        });
        orion_log::set_window_bytes(window);
        Self {
            settings: settings.map_or_else(
                || "# The settings could not be read: orion/state says why.\n".to_owned(),
                |settings| settings.said.clone(),
            ),
            state_dir: settings.map_or_else(
                || PathBuf::from("/var/lib/orion"),
                |settings| settings.config.state_dir.clone(),
            ),
            state,
        }
    }

    /// The files service, with these as its own.
    pub(crate) fn service(self) -> FileOffersService {
        FileOffersService::standard().with_own(self)
    }
}

impl OwnFiles for OrionFiles {
    fn prefix(&self) -> &str {
        PREFIX
    }

    fn names(&self) -> Vec<OwnName> {
        let name = |last: &str, max_bytes, user_data| OwnName {
            name: format!("{PREFIX}{last}"),
            max_bytes,
            user_data,
        };
        vec![
            name("log", orion_log::window_bytes() as u64, true),
            name("settings", SETTINGS_BYTES, false),
            name("last-good.carriers", LAST_GOOD_BYTES, false),
            name("state", STATE_BYTES, false),
            name("sessions", SESSIONS_BYTES, true),
            name("version", VERSION_BYTES, false),
        ]
    }

    fn read(&mut self, name: &str, sessions: &Sessions) -> Option<Vec<u8>> {
        let text = match name.strip_prefix(PREFIX)? {
            "log" => return Some(orion_log::window()),
            "settings" => self.settings.clone(),
            "last-good.carriers" => {
                return std::fs::read(self.state_dir.join(crate::last_good::FILE)).ok();
            }
            "state" => self.state.clone(),
            "sessions" => sessions_text(sessions),
            "version" => version_text(),
            _ => return None,
        };
        Some(text.into_bytes())
    }
}

/// One line a session: its number, name, who it is, its roles and level, whether it is a
/// program on this board, and since when it has been quiet.
fn sessions_text(sessions: &Sessions) -> String {
    let mut text = String::from("# id name principal roles level local quiet_since_ms\n");
    for info in sessions.iter() {
        let name = if info.name.is_empty() {
            "-"
        } else {
            &info.name
        };
        let quiet = info
            .quiet_since
            .map_or_else(|| "-".to_owned(), |since| since.to_string());
        let _ = writeln!(
            text,
            "{} {} {} {:#06x} {} {} {quiet}",
            info.id.0,
            name.replace(char::is_whitespace, "_"),
            info.principal,
            info.roles.0,
            info.level,
            if info.local { "local" } else { "remote" },
        );
    }
    text
}

/// This build: its version, its interface revision and level, and the digest of its kinds, so
/// two builds that speak alike are seen to.
fn version_text() -> String {
    let mut table = Sha256::new();
    for kind in orion_wire::kind::TABLE {
        table.update(format!(
            "{:#04x} {:?} {}\n",
            kind.kind, kind.concept, kind.name
        ));
    }
    let mut digest = String::new();
    for byte in table.finalize() {
        let _ = write!(digest, "{byte:02x}");
    }
    format!(
        "version {}\nprotocol {}\ninterface_revision {}\ninterface_level {}\nkinds_sha256 {digest}\n",
        env!("CARGO_PKG_VERSION"),
        orion_wire::PROTOCOL_VERSION,
        orion_wire::INTERFACE_REVISION,
        orion_wire::INTERFACE_LEVEL,
    )
}
