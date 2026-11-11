//! `orion`: the comm core, node logic and the command line in one file.
//!
//! A board runs the file twice, once as each service, so the two processes map the same
//! pages of code and the board holds one copy of what they share. A person runs it a third
//! time, as the command line.
//!
//! ```text
//! orion comm [settings file]     the comm core: every way into the board
//! orion node [settings file]     node logic: the records, and what it answers for
//! orion [ctl] <concept> <verb>   the command line (`orion help` for its verbs)
//! ```
//!
//! - Both services read the same file, `/etc/orion/orion.conf` unless one is named, and the
//!   environment (`ORION_SOCKET` is `socket`). A key that neither reads is logged by both,
//!   so a typo is seen; one that only the other reads is not.
//! - `orion node show`, `orion node peers` and `orion node time` are the command line's: a
//!   settings file is never called `show`, `peers` or `time`.
//! - Started as `orionctl`, `orion-comm` or `orion-node` (a link to the file), it is that
//!   mode with no word for it.
//! - It is also the comm core's helper programs: started with `ssh-session`, `ssh-sftp` or
//!   `ssh-connection`, it is the one the comm core asked for.
//! - `ORION_LOG` names how much the services log, to standard error.
//!
//! The program is [`main`]; it is a library too so that a program that is `orion` and more
//! (the failure drills' `orion-drill`) runs the same code.

use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use orion_comm_app::{CommSettings, RecoverWith, run_or_recover};
use orion_config::{ConfigError, Source, Warning};
use orion_node_app::NodeSettings;

/// Where the settings are unless the command line says otherwise.
const SETTINGS: &str = "/etc/orion/orion.conf";

/// The command line's own verbs of `node`, which are never a settings file.
const CTL_NODE_VERBS: [&str; 3] = ["show", "peers", "time"];

const USAGE: &str = "\
usage: orion comm [settings file]     the comm core
       orion node [settings file]     node logic
       orion [ctl] <concept> <verb>   the command line; `orion help` for its verbs

Both services read /etc/orion/orion.conf unless a file is named, then the environment.";

/// What this run of the file is.
enum Mode {
    Comm(Vec<OsString>),
    Node(Vec<OsString>),
    Ctl(Vec<OsString>),
    Version,
}

fn mode(program: &str, mut args: Vec<OsString>) -> Mode {
    match program {
        "orionctl" | "orion-ctl" => return Mode::Ctl(args),
        "orion-comm" => return Mode::Comm(args),
        "orion-node" => return Mode::Node(args),
        _ => {}
    }
    let first = args.first().and_then(|word| word.to_str()).unwrap_or("");
    let second = args.get(1).and_then(|word| word.to_str());
    match first {
        "comm" => Mode::Comm(args.split_off(1)),
        "node" if !second.is_some_and(|verb| CTL_NODE_VERBS.contains(&verb)) => {
            Mode::Node(args.split_off(1))
        }
        "ctl" => Mode::Ctl(args.split_off(1)),
        "--version" | "-V" => Mode::Version,
        _ => Mode::Ctl(args),
    }
}

/// The settings file a service was given, or the usual one. `None` after `--help`.
fn settings_path(args: &[OsString]) -> Result<Option<PathBuf>, String> {
    match args {
        [] => Ok(Some(PathBuf::from(SETTINGS))),
        [flag] if flag == "--help" || flag == "-h" => Ok(None),
        [path] => Ok(Some(PathBuf::from(path))),
        _ => Err("a service takes at most a settings file".to_owned()),
    }
}

/// The file's text. No file is no settings: every default.
fn read(path: &Path) -> Result<String, ConfigError> {
    match std::fs::read_to_string(path) {
        Ok(text) => Ok(text),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(String::new()),
        Err(error) => Err(ConfigError {
            key: path.display().to_string(),
            message: error.to_string(),
        }),
    }
}

/// The settings as one part reads them. The variables that are not settings of either
/// service are left out.
fn source(text: &str) -> Result<Source, ConfigError> {
    Source::parse(
        text,
        std::env::vars().filter(|(name, _)| {
            name != "ORION_LOG" && name != "ORION_RECOVERY_KEYS" && name != "ORION_RECOVERY_LISTEN"
        }),
    )
}

/// Keys that `mine` left and that the other service would leave too: what nobody reads.
fn unread_by_both(mine: Vec<Warning>, theirs: Option<Vec<Warning>>) -> Vec<Warning> {
    match theirs {
        Some(theirs) => mine.into_iter().filter(|w| theirs.contains(w)).collect(),
        None => mine,
    }
}

/// What node logic leaves unread in `text`, or `None` if its settings do not parse.
fn node_unread(text: &str) -> Option<Vec<Warning>> {
    let mut source = source(text).ok()?;
    NodeSettings::from_source(&mut source).ok()?;
    Some(source.warnings())
}

/// What the comm core leaves unread in `text`, or `None` if its settings do not parse.
fn comm_unread(text: &str) -> Option<Vec<Warning>> {
    let mut source = source(text).ok()?;
    CommSettings::from_source(&mut source).ok()?;
    Some(source.warnings())
}

fn comm(args: &[OsString]) -> ExitCode {
    let path = match settings_path(args) {
        Ok(Some(path)) => path,
        Ok(None) => {
            println!("{USAGE}");
            return ExitCode::SUCCESS;
        }
        Err(message) => {
            eprintln!("orion comm: {message}\n\n{USAGE}");
            return ExitCode::from(2);
        }
    };
    orion_log::install();
    let read = read(&path).and_then(|text| {
        let mut source = source(&text)?;
        let settings = CommSettings::from_source(&mut source)?;
        Ok((
            settings,
            unread_by_both(source.warnings(), node_unread(&text)),
        ))
    });
    let (settings, unknown) = match read {
        Ok((settings, unknown)) => (Ok(settings), unknown),
        Err(error) => (Err(error), Vec::new()),
    };
    // A comm core whose settings are broken still opens the recovery way in.
    match run_or_recover(settings, &unknown, RecoverWith::from_env()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            tracing::error!("{error}");
            ExitCode::FAILURE
        }
    }
}

fn node(args: &[OsString]) -> ExitCode {
    let path = match settings_path(args) {
        Ok(Some(path)) => path,
        Ok(None) => {
            println!("{USAGE}");
            return ExitCode::SUCCESS;
        }
        Err(message) => {
            eprintln!("orion node: {message}\n\n{USAGE}");
            return ExitCode::from(2);
        }
    };
    orion_log::install();
    let read = read(&path).and_then(|text| {
        let mut source = source(&text)?;
        let settings = NodeSettings::from_source(&mut source)?;
        Ok((
            settings,
            unread_by_both(source.warnings(), comm_unread(&text)),
        ))
    });
    let result = match read {
        Ok((settings, unknown)) => {
            orion_node_app::run(settings, &unknown).map_err(|e| e.to_string())
        }
        Err(error) => Err(format!("the settings cannot be read: {error}")),
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            tracing::error!("{error}");
            ExitCode::FAILURE
        }
    }
}

fn ctl(args: Vec<OsString>) -> ExitCode {
    let mut words = Vec::with_capacity(args.len());
    for arg in args {
        match arg.into_string() {
            Ok(word) => words.push(word),
            Err(arg) => {
                eprintln!("orion: {} is not text", arg.to_string_lossy());
                return ExitCode::from(2);
            }
        }
    }
    let code = orion_ctl::run(&words, &mut std::io::stdout(), &mut std::io::stderr());
    ExitCode::from(u8::try_from(code).unwrap_or(1))
}

/// `orion`, from its command line.
#[must_use]
pub fn main() -> ExitCode {
    // Started as one of the comm core's helpers: be that, and nothing else.
    if let Some(code) = orion_comm_app::helper(std::env::args_os().skip(1)) {
        return ExitCode::from(u8::try_from(code).unwrap_or(1));
    }
    let mut args: Vec<OsString> = std::env::args_os().collect();
    let rest = args.split_off(1.min(args.len()));
    let program = args
        .first()
        .and_then(|program| Path::new(program).file_name())
        .and_then(|name| name.to_str())
        .unwrap_or("orion")
        .to_owned();
    match mode(&program, rest) {
        Mode::Comm(args) => comm(&args),
        Mode::Node(args) => node(&args),
        Mode::Ctl(args) => ctl(args),
        Mode::Version => {
            println!("orion {}", env!("CARGO_PKG_VERSION"));
            ExitCode::SUCCESS
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn words(text: &str) -> Vec<OsString> {
        text.split_whitespace().map(OsString::from).collect()
    }

    fn which(program: &str, text: &str) -> (&'static str, Vec<OsString>) {
        match mode(program, words(text)) {
            Mode::Comm(args) => ("comm", args),
            Mode::Node(args) => ("node", args),
            Mode::Ctl(args) => ("ctl", args),
            Mode::Version => ("version", Vec::new()),
        }
    }

    #[test]
    fn the_first_word_picks_the_mode_and_the_rest_is_the_command_line() {
        assert_eq!(
            which("orion", "comm /etc/a.conf"),
            ("comm", words("/etc/a.conf"))
        );
        assert_eq!(which("orion", "node"), ("node", words("")));
        assert_eq!(
            which("orion", "node /etc/a.conf"),
            ("node", words("/etc/a.conf"))
        );
        assert_eq!(which("orion", "node show"), ("ctl", words("node show")));
        assert_eq!(which("orion", "node peers"), ("ctl", words("node peers")));
        assert_eq!(which("orion", "ctl node show"), ("ctl", words("node show")));
        assert_eq!(
            which("orion", "topic read imu"),
            ("ctl", words("topic read imu"))
        );
        assert_eq!(
            which("orion", "--address /tmp/s node show"),
            ("ctl", words("--address /tmp/s node show"))
        );
        assert_eq!(which("orion", "--version").0, "version");
        assert_eq!(which("orion", ""), ("ctl", words("")));
    }

    #[test]
    fn a_link_by_another_name_is_that_mode() {
        assert_eq!(which("orionctl", "node"), ("ctl", words("node")));
        assert_eq!(which("orion-comm", ""), ("comm", words("")));
        assert_eq!(which("orion-node", "a.conf"), ("node", words("a.conf")));
    }

    #[test]
    fn a_key_one_service_reads_is_not_unknown_to_the_other() {
        let text = "node.name = board\nfiles.max_size = 1024\nstatus.interval_ms = 50\ntypo = 1\n";
        let mut comm = source(text).expect("source");
        CommSettings::from_source(&mut comm).expect("comm");
        let unknown = unread_by_both(comm.warnings(), node_unread(text));
        let keys: Vec<_> = unknown.iter().map(|w| w.key.as_str()).collect();
        assert_eq!(keys, ["typo"]);

        let mut node = source(text).expect("source");
        NodeSettings::from_source(&mut node).expect("node");
        let unknown = unread_by_both(node.warnings(), comm_unread(text));
        let keys: Vec<_> = unknown.iter().map(|w| w.key.as_str()).collect();
        assert_eq!(keys, ["typo"]);
    }
}
