//! The settings of the time sync.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use orion_config::{ConfigError, Source};

use crate::operator::OPERATOR_SOURCE;

const DEFAULT_SYNCED_FILE: &str = "/run/orion/time-synced";
const DEFAULT_RTC: &str = "/sys/class/rtc/rtc0/since_epoch";
/// Another Orion node over UDP. `tcp://` is always one: only Orion answers SNTP over TCP.
pub(crate) const PEER_SCHEME: &str = "peer://";
pub(crate) const TCP_SCHEME: &str = "tcp://";
/// The default route's gateway, asked as an NTP server: a radio or a controller often
/// is one. Found again each time it is asked, so it follows the network.
pub const GATEWAY_SOURCE: &str = "gateway";
/// The host `nt4.server` names, asked as an NTP server.
pub const NT4_SOURCE: &str = "nt4";

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
    /// `time.interface`: the interfaces it is served on, by name, each with sockets of its
    /// own at `time.serve`, opened when the interface is there and again when it is made
    /// again. None is every interface the address reaches.
    pub serve_interfaces: Vec<String>,
    /// How often an interface that is not there is looked for again. Not a setting.
    pub serve_retry: Duration,
    /// The host of `nt4.server` at the NTP port, which the source `nt4` asks.
    pub nt4_host: Option<String>,
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
    /// `time.rtc`: the hardware clock's seconds since 1970, read once at start as a floor the
    /// clock is moved forward to. `off` for none.
    pub rtc: Option<PathBuf>,
}

impl Default for TimeConfig {
    fn default() -> Self {
        Self {
            mode: TimeMode::Off,
            servers: Vec::new(),
            serve: None,
            serve_interfaces: Vec::new(),
            serve_retry: Duration::from_secs(5),
            nt4_host: None,
            serve_authority: false,
            min_poll: Duration::from_secs(64),
            max_poll: Duration::from_secs(1024),
            synced_file: Some(PathBuf::from(DEFAULT_SYNCED_FILE)),
            rtc: Some(PathBuf::from(DEFAULT_RTC)),
        }
    }
}

/// A source as written, with the NTP port added when it names none.
fn with_port(server: &str) -> String {
    if [OPERATOR_SOURCE, GATEWAY_SOURCE, NT4_SOURCE].contains(&server) {
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

/// The host of `server`, written `host`, `host:port` or `[address]:port`, as an NTP server is
/// written: an IPv6 address in brackets.
fn host_of(server: &str) -> String {
    if let Ok(addr) = server.parse::<SocketAddr>() {
        return match addr.ip() {
            std::net::IpAddr::V6(v6) => format!("[{v6}]"),
            v4 => v4.to_string(),
        };
    }
    if server.parse::<std::net::Ipv6Addr>().is_ok() {
        return format!("[{server}]");
    }
    match server.rsplit_once(':') {
        Some((host, port)) if port.parse::<u16>().is_ok() && !host.contains(':') => host.to_owned(),
        _ => server.to_owned(),
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
        let nt4_host = source
            .text("nt4.server")
            .map(|server| format!("{}:123", host_of(server.trim())));
        if nt4_host.is_none() && servers.iter().any(|server| server == NT4_SOURCE) {
            return Err(error(
                "time.servers",
                "nt4 asks the host of nt4.server, which is not set".into(),
            ));
        }
        let serve_interfaces = source.interfaces("time.interface")?;
        let serve: Option<SocketAddr> = source.optional("time.serve")?;
        if !serve_interfaces.is_empty() && serve.is_none() {
            return Err(error(
                "time.interface",
                "needs time.serve: the address and port served on each".into(),
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
        let rtc = match source.text("time.rtc") {
            Some("off") => None,
            Some(path) if !path.is_empty() => Some(PathBuf::from(path)),
            _ => default.rtc,
        };
        Ok(Self {
            mode,
            servers,
            serve,
            serve_interfaces,
            serve_retry: default.serve_retry,
            nt4_host,
            serve_authority: source.get("time.serve_authority", false)?,
            min_poll,
            max_poll,
            synced_file,
            rtc,
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
            "time.sync = report\ntime.servers = operator, pool.example.org, peer://10.0.0.2, tcp://10.0.0.2:4123, [fe80::1]:99, fe80::1\ntime.serve = 0.0.0.0:4123\ntime.synced_file = off\ntime.rtc = off\n",
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
        assert_eq!(config.rtc, None);
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
        assert_eq!(
            parse("time.sync = report\ntime.servers = nt4")
                .expect_err("no nt4.server")
                .key,
            "time.servers"
        );
        assert_eq!(
            parse("time.interface = eth0").expect_err("no serve").key,
            "time.interface"
        );
        // Serving alone is something to run.
        assert!(
            parse("time.serve = 127.0.0.1:4123")
                .expect("config")
                .enabled()
        );
    }

    #[test]
    fn the_gateway_and_the_nt4_server_are_sources_by_name() {
        let config = parse(
            "time.sync = discipline\ntime.servers = operator, gateway, nt4, peer://cam2\n\
             nt4.server = 10.12.34.2:5810\ntime.serve = 0.0.0.0:123\ntime.interface = eth0, usb0\n",
        )
        .expect("config");
        assert_eq!(
            config.servers,
            ["operator", "gateway", "nt4", "peer://cam2:123"]
        );
        assert_eq!(config.nt4_host.as_deref(), Some("10.12.34.2:123"));
        assert_eq!(config.serve_interfaces, ["eth0", "usb0"]);
        for (written, host) in [
            ("controller-7.local", "controller-7.local"),
            ("controller-7.local:5810", "controller-7.local"),
            ("[fe80::1]:5810", "[fe80::1]"),
            ("fe80::1", "[fe80::1]"),
        ] {
            assert_eq!(host_of(written), host, "{written}");
        }
    }
}
