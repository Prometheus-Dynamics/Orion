//! One typed config, from a file and the environment.
//!
//! The file is plain `key = value` lines, with `#` comments. The environment overrides it:
//! `peer.listen` is `ORION_PEER_LISTEN`. A key nobody read comes back as a warning, so a typo is
//! visible and never fatal. A value that can't be parsed is an error, so a bad setting can't be
//! mistaken for the default.
//!
//! [`Config`] holds what every build needs. A part with its own settings reads them from the same
//! [`Source`], which is what keeps "unknown key" accurate as parts are added.

use std::collections::BTreeMap;
use std::fmt;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::str::FromStr;

pub use orion_check::Security;
use orion_types::NodeName;

const ENV_PREFIX: &str = "ORION_";

/// A setting that couldn't be used.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConfigError {
    /// The key, or the file for a problem with the file as a whole.
    pub key: String,
    /// What was wrong.
    pub message: String,
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.key, self.message)
    }
}

impl std::error::Error for ConfigError {}

/// Something worth telling the operator that doesn't stop startup.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Warning {
    /// The key.
    pub key: String,
    /// Where it came from.
    pub from: Origin,
}

impl fmt::Display for Warning {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.from {
            Origin::File => write!(f, "unknown key in the config file: {}", self.key),
            Origin::Environment => write!(f, "unknown variable in the environment: {}", self.key),
        }
    }
}

/// Where a value came from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Origin {
    /// The config file.
    File,
    /// The environment.
    Environment,
}

#[derive(Debug)]
struct Entry {
    value: String,
    from: Origin,
    /// The name to report if nobody reads it.
    name: String,
    read: bool,
}

/// The raw settings, before anything is typed.
#[derive(Debug, Default)]
pub struct Source {
    entries: BTreeMap<String, Entry>,
}

impl Source {
    /// Reads the file at `path`, if there is one, and then applies `env`.
    ///
    /// # Errors
    ///
    /// A file that exists but can't be read or parsed.
    pub fn load(
        path: Option<&Path>,
        env: impl IntoIterator<Item = (String, String)>,
    ) -> Result<Self, ConfigError> {
        let text = match path {
            Some(path) => match std::fs::read_to_string(path) {
                Ok(text) => text,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => String::new(),
                Err(err) => {
                    return Err(ConfigError {
                        key: path.display().to_string(),
                        message: err.to_string(),
                    });
                }
            },
            None => String::new(),
        };
        Self::parse(&text, env)
    }

    /// Parses file text, then applies `env`.
    ///
    /// # Errors
    ///
    /// A line that isn't `key = value`.
    pub fn parse(
        text: &str,
        env: impl IntoIterator<Item = (String, String)>,
    ) -> Result<Self, ConfigError> {
        let mut source = Self::default();
        for (index, raw) in text.lines().enumerate() {
            let line = strip_comment(raw).trim();
            if line.is_empty() {
                continue;
            }
            let Some((key, value)) = line.split_once('=') else {
                return Err(ConfigError {
                    key: format!("line {}", index + 1),
                    message: "expected key = value".into(),
                });
            };
            let key = key.trim().to_ascii_lowercase();
            source.set(canonical(&key), unquote(value.trim()), Origin::File, key);
        }
        for (name, value) in env {
            if let Some(rest) = name.strip_prefix(ENV_PREFIX) {
                source.set(canonical(rest), value, Origin::Environment, name);
            }
        }
        Ok(source)
    }

    fn set(&mut self, key: String, value: String, from: Origin, name: String) {
        self.entries.insert(
            key,
            Entry {
                value,
                from,
                name,
                read: false,
            },
        );
    }

    /// The text of `key`, marking it as read.
    pub fn text(&mut self, key: &str) -> Option<&str> {
        let entry = self.entries.get_mut(&canonical(key))?;
        entry.read = true;
        Some(entry.value.as_str())
    }

    /// `key` parsed as a `T`, or `default` if it isn't set.
    ///
    /// # Errors
    ///
    /// A value that doesn't parse.
    pub fn get<T>(&mut self, key: &str, default: T) -> Result<T, ConfigError>
    where
        T: FromStr,
        T::Err: fmt::Display,
    {
        match self.text(key) {
            Some(text) => text.parse().map_err(|err: T::Err| ConfigError {
                key: key.into(),
                message: format!("invalid value {text:?}: {err}"),
            }),
            None => Ok(default),
        }
    }

    /// Like [`Source::get`], where `off` (or `none`, or an empty value) means no value.
    ///
    /// # Errors
    ///
    /// A value that doesn't parse.
    pub fn optional<T>(&mut self, key: &str) -> Result<Option<T>, ConfigError>
    where
        T: FromStr,
        T::Err: fmt::Display,
    {
        match self.text(key).map(str::to_owned) {
            None => Ok(None),
            Some(text) if matches!(text.as_str(), "" | "off" | "none") => Ok(None),
            Some(text) => text.parse().map(Some).map_err(|err: T::Err| ConfigError {
                key: key.into(),
                message: format!("invalid value {text:?}: {err}"),
            }),
        }
    }

    /// A comma-separated list of `T`.
    ///
    /// # Errors
    ///
    /// An element that doesn't parse.
    pub fn list<T>(&mut self, key: &str) -> Result<Vec<T>, ConfigError>
    where
        T: FromStr,
        T::Err: fmt::Display,
    {
        let Some(text) = self.text(key).map(str::to_owned) else {
            return Ok(Vec::new());
        };
        text.split(',')
            .map(str::trim)
            .filter(|item| !item.is_empty())
            .map(|item| {
                item.parse().map_err(|err: T::Err| ConfigError {
                    key: key.into(),
                    message: format!("invalid value {item:?}: {err}"),
                })
            })
            .collect()
    }

    /// Every key nobody read. Call this after every part has taken its settings.
    #[must_use]
    pub fn warnings(&self) -> Vec<Warning> {
        self.entries
            .values()
            .filter(|entry| !entry.read)
            .map(|entry| Warning {
                key: entry.name.clone(),
                from: entry.from,
            })
            .collect()
    }
}

/// A key as it is kept: `.` and `_` are one, so the environment, which has only `_`, names
/// every key, `socket.node_uids` (`ORION_SOCKET_NODE_UIDS`) as well as `peer.listen`.
fn canonical(key: &str) -> String {
    key.to_ascii_lowercase().replace('.', "_")
}

fn strip_comment(line: &str) -> &str {
    let mut quoted = false;
    for (index, ch) in line.char_indices() {
        match ch {
            '"' => quoted = !quoted,
            '#' if !quoted => return &line[..index],
            _ => {}
        }
    }
    line
}

fn unquote(value: &str) -> String {
    value
        .strip_prefix('"')
        .and_then(|inner| inner.strip_suffix('"'))
        .unwrap_or(value)
        .to_owned()
}

/// Reads the security switch: `on` or `off`.
fn parse_security(text: &str) -> Result<Security, String> {
    match text {
        "on" | "true" => Ok(Security::On),
        "off" | "false" => Ok(Security::Off),
        other => Err(format!("expected on or off, got {other:?}")),
    }
}

/// What every build needs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Config {
    /// `node.name`: this node's name.
    pub node_name: NodeName,
    /// `security`: the one switch. Off unless someone turns it on.
    pub security: Security,
    /// `socket`: the Unix socket programs on this board connect to.
    pub socket: PathBuf,
    /// `state.dir`: where the node keeps its state.
    pub state_dir: PathBuf,
    /// `keys.dir`: where the node's key and the trust file are, made and kept by the comm
    /// core and read by node logic. `state.dir` unless set: the two services can have
    /// different state directories and still share one key.
    pub keys_dir: PathBuf,
    /// `peer.listen`: where other nodes connect. `off` for nowhere.
    pub peer_listen: Option<SocketAddr>,
    /// `peer.connect`: nodes to connect to.
    pub peer_connect: Vec<SocketAddr>,
}

impl Config {
    /// Takes the shared settings out of `source`.
    ///
    /// # Errors
    ///
    /// A value that doesn't parse.
    pub fn from_source(source: &mut Source) -> Result<Self, ConfigError> {
        let default_name = "node.local"
            .parse()
            .map_err(|err: orion_types::Error| ConfigError {
                key: "node.name".into(),
                message: err.to_string(),
            })?;
        let security = match source.text("security").map(str::to_owned) {
            Some(text) => parse_security(&text).map_err(|message| ConfigError {
                key: "security".into(),
                message,
            })?,
            None => Security::Off,
        };
        Ok(Self {
            node_name: source.get("node.name", default_name)?,
            security,
            socket: source.get("socket", PathBuf::from("/run/orion/orion.sock"))?,
            state_dir: source.get("state.dir", PathBuf::from("/var/lib/orion"))?,
            keys_dir: PathBuf::new(),
            peer_listen: source.optional("peer.listen")?,
            peer_connect: source.list("peer.connect")?,
        })
        .and_then(|mut config| {
            config.keys_dir = source.get("keys.dir", config.state_dir.clone())?;
            Ok(config)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect()
    }

    #[test]
    fn nothing_set_gives_the_defaults_with_security_off() {
        let mut source = Source::parse("", env(&[])).unwrap();
        let config = Config::from_source(&mut source).unwrap();
        assert_eq!(config.node_name.as_str(), "node.local");
        assert_eq!(config.security, Security::Off);
        assert_eq!(config.socket, PathBuf::from("/run/orion/orion.sock"));
        assert_eq!(config.peer_listen, None);
        assert!(config.peer_connect.is_empty());
        assert!(source.warnings().is_empty());
    }

    #[test]
    fn the_file_is_read_with_comments_and_quotes() {
        let text = r#"
            # A comment.
            node.name = left        # trailing
            security = on
            socket = "/tmp/o#1.sock"
            peer.listen = 0.0.0.0:9200
            peer.connect = 10.0.0.1:9200, 10.0.0.2:9200
        "#;
        let mut source = Source::parse(text, env(&[])).unwrap();
        let config = Config::from_source(&mut source).unwrap();
        assert_eq!(config.node_name.as_str(), "left");
        assert_eq!(config.security, Security::On);
        assert_eq!(config.socket, PathBuf::from("/tmp/o#1.sock"));
        assert_eq!(config.peer_listen.unwrap().port(), 9200);
        assert_eq!(config.peer_connect.len(), 2);
        assert!(source.warnings().is_empty());
    }

    #[test]
    fn the_environment_overrides_the_file() {
        let mut source = Source::parse(
            "node.name = left\nsecurity = on",
            env(&[
                ("ORION_NODE_NAME", "right"),
                ("ORION_SECURITY", "off"),
                ("PATH", "/bin"),
            ]),
        )
        .unwrap();
        let config = Config::from_source(&mut source).unwrap();
        assert_eq!(config.node_name.as_str(), "right");
        assert_eq!(config.security, Security::Off);
        assert!(source.warnings().is_empty());
    }

    #[test]
    fn peer_listen_can_be_off() {
        let mut source = Source::parse("peer.listen = off", env(&[])).unwrap();
        assert_eq!(Config::from_source(&mut source).unwrap().peer_listen, None);
    }

    #[test]
    fn unknown_keys_warn_and_do_not_fail() {
        let mut source = Source::parse("secuirty = on", env(&[("ORION_TYPO", "1")])).unwrap();
        let config = Config::from_source(&mut source).unwrap();
        assert_eq!(config.security, Security::Off);
        let warnings = source.warnings();
        assert_eq!(warnings.len(), 2);
        assert!(
            warnings
                .iter()
                .any(|w| w.key == "secuirty" && w.from == Origin::File)
        );
        assert!(
            warnings
                .iter()
                .any(|w| w.key == "ORION_TYPO" && w.from == Origin::Environment)
        );
    }

    #[test]
    fn a_part_reading_its_own_setting_clears_the_warning() {
        let mut source = Source::parse("ssh.port = 22", env(&[])).unwrap();
        Config::from_source(&mut source).unwrap();
        assert_eq!(source.warnings().len(), 1);
        assert_eq!(source.get("ssh.port", 0_u16).unwrap(), 22);
        assert!(source.warnings().is_empty());
    }

    #[test]
    fn a_bad_value_is_an_error_naming_the_key() {
        for (text, key) in [
            ("security = maybe", "security"),
            ("node.name = a b", "node.name"),
            ("peer.listen = nowhere", "peer.listen"),
            ("peer.connect = 1.2.3.4:5, x", "peer.connect"),
        ] {
            let mut source = Source::parse(text, env(&[])).unwrap();
            let err = Config::from_source(&mut source).unwrap_err();
            assert_eq!(err.key, key, "{text}");
        }
    }

    #[test]
    fn a_line_that_is_not_a_setting_is_an_error() {
        assert_eq!(
            Source::parse("a = 1\nnonsense", env(&[])).unwrap_err().key,
            "line 2"
        );
    }

    #[test]
    fn a_missing_file_is_fine() {
        let source = Source::load(Some(Path::new("/nonexistent/orion.conf")), env(&[]));
        assert!(source.is_ok());
    }

    #[test]
    fn the_environment_names_keys_that_have_an_underscore() {
        let env = [
            ("ORION_SOCKET_NODE_UIDS".to_owned(), "901".to_owned()),
            ("ORION_RECORDS_SAVE_AFTER_MS".to_owned(), "10".to_owned()),
        ];
        let mut source = Source::parse("socket.node_uids = 5\n", env).unwrap();
        assert_eq!(source.list::<u32>("socket.node_uids").unwrap(), [901]);
        assert_eq!(source.get("records.save_after_ms", 500_u64).unwrap(), 10);
        assert!(source.warnings().is_empty(), "{:?}", source.warnings());
    }
}
