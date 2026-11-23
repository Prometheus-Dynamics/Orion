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

use orion_file_offers::{FileOffersService, OwnFiles, OwnName, Reading, Ready};
use orion_server::{Comm, Sessions};
use sha2::{Digest, Sha256};

use crate::CommSettings;

const PREFIX: &str = "orion/";

/// The bounds of those whose size is not set.
const SETTINGS_BYTES: u64 = 64 * 1024;
const LAST_GOOD_BYTES: u64 = 16 * 1024;
const STATE_BYTES: u64 = 4 * 1024;
const SESSIONS_BYTES: u64 = 64 * 1024;
const VERSION_BYTES: u64 = 4 * 1024;

/// The files service of a comm core running as `settings` say, with its own files and those the
/// settings name.
///
/// # Errors
///
/// Why the files the settings name cannot be served.
pub(crate) fn running(
    comm: &Comm,
    settings: &CommSettings,
) -> Result<FileOffersService, crate::StartError> {
    #[allow(unused_mut)]
    let mut state = format!("{}\n", settings.board_state.word());
    #[cfg(feature = "announce")]
    if let Some(why) = crate::announce::withheld(settings) {
        state.push_str(&format!("mdns: {why}\n"));
    }
    match OrionFiles::new(Some(settings), state).service(comm, Some(settings)) {
        (service, None) => Ok(service),
        (_, Some(why)) => Err(crate::error::failed("offers.file", why)),
    }
}

/// The files service in recovery, because of `why`: the comm core's own files, and those the
/// settings name if they were read and can be served.
#[cfg(feature = "recovery")]
pub(crate) fn in_recovery(
    comm: &Comm,
    settings: Option<&CommSettings>,
    why: &str,
) -> FileOffersService {
    let state = format!("recovery\nwhy: {why}\n");
    let (service, not) = OrionFiles::new(settings, state).service(comm, settings);
    if let Some(not) = not {
        tracing::warn!("recovery: the files the settings name are not served: {not}");
    }
    service
}

/// The comm core's own files.
#[derive(Debug)]
pub(crate) struct OrionFiles {
    /// This core's own window on the process's log: two cores in one process each keep theirs.
    window: orion_log::Window,
    settings: String,
    state_dir: PathBuf,
    state: String,
}

impl OrionFiles {
    /// The files of a comm core run by `settings`, if they were read, in `state`: the hello
    /// document's word, then why, in recovery.
    pub(crate) fn new(settings: Option<&CommSettings>, state: String) -> Self {
        let window = orion_log::Window::new(settings.map_or(orion_log::WINDOW_BYTES, |settings| {
            settings.log_window_bytes
        }));
        Self {
            window,
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

    /// The files service of `comm`, with these as its own, and the prefixes `settings` name,
    /// unless they cannot be served: then why, too.
    pub(crate) fn service(
        self,
        comm: &Comm,
        settings: Option<&CommSettings>,
    ) -> (FileOffersService, Option<String>) {
        let mut service = FileOffersService::standard().with_own(comm, self);
        let Some(settings) = settings else {
            return (service, None);
        };
        let user = settings.offers_user.as_deref();
        match crate::configured::load(&settings.offers_file, settings.offers_owner, user) {
            Ok(prefixes) => {
                for prefix in prefixes {
                    service = service.with_configured(comm, prefix);
                }
                (service, None)
            }
            Err(why) => (service, Some(why)),
        }
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
            name("log", self.window.bytes() as u64, true),
            name("settings", SETTINGS_BYTES, false),
            name("last-good.carriers", LAST_GOOD_BYTES, false),
            name("state", STATE_BYTES, false),
            name("sessions", SESSIONS_BYTES, true),
            name("version", VERSION_BYTES, false),
        ]
    }

    fn read(&mut self, name: &str, sessions: &Sessions, _: Ready) -> Reading {
        let text = match name.strip_prefix(PREFIX).unwrap_or_default() {
            "log" => return Reading::Now(self.window.read()),
            "settings" => self.settings.clone(),
            "last-good.carriers" => {
                return std::fs::read(self.state_dir.join(crate::last_good::FILE)).map_or_else(
                    |_| Reading::Not(format!("{name}: cannot be read now")),
                    Reading::Now,
                );
            }
            "state" => self.state.clone(),
            "sessions" => sessions_text(sessions),
            "version" => version_text(),
            _ => return Reading::Not(format!("{name}: no such file")),
        };
        Reading::Now(text.into_bytes())
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
