//! The settings of the time sync.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use orion_config::{ConfigError, Source};

use crate::operator::OPERATOR_SOURCE;

const DEFAULT_SYNCED_FILE: &str = "/run/orion/time-synced";
/// Another Orion node over UDP. `tcp://` is always one: only Orion answers SNTP over TCP.
pub(crate) const PEER_SCHEME: &str = "peer://";
pub(crate) const TCP_SCHEME: &str = "tcp://";

/// What the time sync does with what it measures.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TimeMode {
    /// Nothing is measured.
    Off,
    /// Measures the offset to the sources and publishes it. Never touches the clock.
    Report,
    /// Also adjusts the clock. Needs `CAP_SYS_TIME`.
    Discipline,
}

/// The settings of the time sync.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TimeConfig {
    /// `time.sync`: `off`, `report` or `discipline`.
    pub mode: TimeMode,
    /// `time.servers`: the sources, in rank order. `host[:port]` is an NTP server,
    /// `peer://host[:port]` another Orion node, `tcp://host:port` one asked over TCP, and
    /// `operator` the clock of a connected operator.
    pub servers: Vec<String>,
    /// `time.serve`: where this board's clock is served, over UDP and TCP.
    pub serve: Option<SocketAddr>,
    /// `time.serve_authority`: serve the clock as the network's reference even when nothing
    /// has synchronized it.
    pub serve_authority: bool,
    /// `time.poll_min_ms`: the shortest time between polls once synchronized.
    pub min_poll: Duration,
    /// `time.poll_max_ms`: the longest.
    pub max_poll: Duration,
    /// `time.synced_file`: exists while the clock is disciplined and synchronized. `off` for
    /// none.
    pub synced_file: Option<PathBuf>,
}

impl Default for TimeConfig {
    fn default() -> Self {
        Self {
            mode: TimeMode::Off,
            servers: Vec::new(),
            serve: None,
            serve_authority: false,
            min_poll: Duration::from_secs(64),
            max_poll: Duration::from_secs(1024),
            synced_file: Some(PathBuf::from(DEFAULT_SYNCED_FILE)),
        }
    }
}

/// A source as written, with the NTP port added when it names none.
fn with_port(server: &str) -> String {
    if server == OPERATOR_SOURCE {
        return server.to_owned();
    }
    let (scheme, host) = match (
        server.strip_prefix(TCP_SCHEME),
        server.strip_prefix(PEER_SCHEME),
    ) {
        (Some(host), _) => (TCP_SCHEME, host),
        (_, Some(host)) => (PEER_SCHEME, host),
        _ => ("", server),
    };
    let has_port = host
        .rsplit_once(':')
        .is_some_and(|(name, port)| !name.ends_with(':') && port.parse::<u16>().is_ok());
    if has_port {
        server.to_owned()
    } else {
        format!("{scheme}{host}:123")
    }
}

impl TimeConfig {
    /// Takes the time sync's settings out of `source`.
    ///
    /// # Errors
    ///
    /// A value that doesn't parse, a mode with no source, or poll limits that cross.
    pub fn from_source(source: &mut Source) -> Result<Self, ConfigError> {
        let error = |key: &str, message: String| ConfigError {
            key: key.into(),
            message,
        };
        let default = Self::default();
        let mode = match source
            .text("time.sync")
            .map(str::to_ascii_lowercase)
            .as_deref()
        {
            None | Some("" | "off") => TimeMode::Off,
            Some("report") => TimeMode::Report,
            Some("discipline") => TimeMode::Discipline,
            Some(other) => {
                return Err(error(
                    "time.sync",
                    format!("expected off, report or discipline, got {other:?}"),
                ));
            }
        };
        let servers: Vec<String> = source
            .list::<String>("time.servers")?
            .iter()
            .map(|server| with_port(server))
            .collect();
        if mode != TimeMode::Off && servers.is_empty() {
            return Err(error(
                "time.servers",
                "time.sync needs at least one source".into(),
            ));
        }
        let min_poll = Duration::from_millis(source.get("time.poll_min_ms", 64_000_u64)?);
        let max_poll = Duration::from_millis(source.get("time.poll_max_ms", 1_024_000_u64)?);
        if min_poll.is_zero() || max_poll < min_poll {
            return Err(error(
                "time.poll_min_ms",
                "must be above 0 and at most time.poll_max_ms".into(),
            ));
        }
        let synced_file = match source.text("time.synced_file") {
            Some("off") => None,
            Some(path) if !path.is_empty() => Some(PathBuf::from(path)),
            _ => default.synced_file,
        };
        Ok(Self {
            mode,
            servers,
            serve: source.optional("time.serve")?,
            serve_authority: source.get("time.serve_authority", false)?,
            min_poll,
            max_poll,
            synced_file,
        })
    }

    /// Whether there is anything to run.
    #[must_use]
    pub fn enabled(&self) -> bool {
        self.mode != TimeMode::Off || self.serve.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(text: &str) -> Result<TimeConfig, ConfigError> {
        TimeConfig::from_source(&mut Source::parse(text, Vec::new()).expect("source"))
    }

    #[test]
    fn nothing_set_is_off() {
        let config = parse("").expect("config");
        assert_eq!(config, TimeConfig::default());
        assert!(!config.enabled());
    }

    #[test]
    fn sources_get_the_ntp_port_unless_they_name_one() {
        let config = parse(
            "time.sync = report\ntime.servers = operator, pool.example.org, peer://10.0.0.2, tcp://10.0.0.2:4123, [fe80::1]:99, fe80::1\ntime.serve = 0.0.0.0:4123\ntime.synced_file = off\n",
        )
        .expect("config");
        assert_eq!(config.mode, TimeMode::Report);
        assert_eq!(
            config.servers,
            [
                "operator",
                "pool.example.org:123",
                "peer://10.0.0.2:123",
                "tcp://10.0.0.2:4123",
                "[fe80::1]:99",
                "fe80::1:123"
            ]
        );
        assert_eq!(config.serve, Some("0.0.0.0:4123".parse().expect("addr")));
        assert_eq!(config.synced_file, None);
        assert!(config.enabled());
    }

    #[test]
    fn what_cannot_work_is_refused() {
        assert_eq!(
            parse("time.sync = discipline").expect_err("no source").key,
            "time.servers"
        );
        assert_eq!(
            parse("time.sync = sometimes").expect_err("mode").key,
            "time.sync"
        );
        assert_eq!(
            parse("time.poll_min_ms = 5000\ntime.poll_max_ms = 1000")
                .expect_err("poll")
                .key,
            "time.poll_min_ms"
        );
        assert_eq!(
            parse("time.serve = nowhere").expect_err("address").key,
            "time.serve"
        );
        // Serving alone is something to run.
        assert!(
            parse("time.serve = 127.0.0.1:4123")
                .expect("config")
                .enabled()
        );
    }
}
