//! Time sync: keeps the wall clock right from SNTP servers or another Orion node, so an image
//! needs no `systemd-timesyncd` (2.8 MB PSS on the test board). Feature `time-sync`.
//!
//! - `ORION_NODE_TIME_SYNC`: `off` (the default), `report` (measure and publish `time.*` status
//!   keys, never touch the clock: safe beside timesyncd), or `discipline` (also slew or step the
//!   clock; needs `CAP_SYS_TIME`, and falls back to `report` while timesyncd runs).
//! - `ORION_NODE_TIME_SERVERS`: `host[:port]` list, port 123 by default. Another Orion node is a
//!   source like any server, at the port it serves on; `tcp://host:port` asks it over TCP.
//! - `ORION_NODE_TIME_SERVE`: serve this node's clock on `addr:port`, SNTP over UDP and the
//!   same exchange over TCP. Answers say
//!   "unsynchronized" until the clock is, unless `ORION_NODE_TIME_SERVE_AUTHORITY=1` makes this
//!   node the network's reference (a field network with no other time).
//! - `ORION_NODE_TIME_POLL_MIN_MS` and `ORION_NODE_TIME_POLL_MAX_MS`: 64 s and 1024 s.
//!
//! The last synchronized time is kept in the state directory; at start a clock behind it (a
//! board without a working RTC) is stepped forward to it. See `discipline` for the rules.

mod discipline;
mod sntp;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::Duration;

use orion::control_plane::TypedConfigValue;
use tokio::net::UdpSocket;
use tokio::sync::watch;
use tracing::{debug, info, warn};

use crate::NodeError;
use crate::config::{bool_env_or_false, duration_ms_env_or};
pub(crate) use discipline::{Action, Clock, Discipline, KernelClock, timesyncd_running};
pub(crate) use sntp::ServedClock;

/// Polls this often until the first sample is in (and while a step waits for agreement).
const FIRST_POLL: Duration = Duration::from_secs(2);
/// An offset below this lets the poll interval grow.
const CALM_NS: i64 = 50_000_000;
const SAVED_TIME_FILE: &str = "time-sync.last";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TimeSyncMode {
    Off,
    Report,
    Discipline,
}

#[derive(Clone, Debug)]
pub struct TimeSyncConfig {
    pub mode: TimeSyncMode,
    pub servers: Vec<String>,
    pub serve: Option<SocketAddr>,
    pub serve_authority: bool,
    pub min_poll: Duration,
    pub max_poll: Duration,
}

impl TimeSyncConfig {
    pub fn try_from_env() -> Result<Self, NodeError> {
        let mode = match std::env::var("ORION_NODE_TIME_SYNC")
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase()
            .as_str()
        {
            "" | "off" => TimeSyncMode::Off,
            "report" => TimeSyncMode::Report,
            "discipline" => TimeSyncMode::Discipline,
            other => {
                return Err(NodeError::Config(format!(
                    "ORION_NODE_TIME_SYNC has invalid value `{other}`; expected `off`, `report` or `discipline`"
                )));
            }
        };
        let servers = std::env::var("ORION_NODE_TIME_SERVERS")
            .unwrap_or_default()
            .split(',')
            .map(str::trim)
            .filter(|server| !server.is_empty())
            .map(|server| {
                let (scheme, host) = match server.strip_prefix("tcp://") {
                    Some(host) => ("tcp://", host),
                    None => ("", server),
                };
                if host
                    .rsplit_once(':')
                    .is_some_and(|(name, port)| !name.ends_with(':') && port.parse::<u16>().is_ok())
                {
                    server.to_owned()
                } else {
                    format!("{scheme}{host}:123")
                }
            })
            .collect::<Vec<_>>();
        if mode != TimeSyncMode::Off && servers.is_empty() {
            return Err(NodeError::Config(
                "ORION_NODE_TIME_SYNC needs ORION_NODE_TIME_SERVERS".to_owned(),
            ));
        }
        let serve = match std::env::var("ORION_NODE_TIME_SERVE") {
            Ok(value) if !value.trim().is_empty() => {
                Some(value.trim().parse::<SocketAddr>().map_err(|error| {
                    NodeError::Config(format!(
                        "ORION_NODE_TIME_SERVE has invalid value `{value}`: {error}"
                    ))
                })?)
            }
            _ => None,
        };
        let min_poll = duration_ms_env_or("ORION_NODE_TIME_POLL_MIN_MS", 64_000)?;
        let max_poll = duration_ms_env_or("ORION_NODE_TIME_POLL_MAX_MS", 1_024_000)?;
        if min_poll.is_zero() || max_poll < min_poll {
            return Err(NodeError::Config(
                "ORION_NODE_TIME_POLL_MIN_MS must be above 0 and at most ORION_NODE_TIME_POLL_MAX_MS"
                    .to_owned(),
            ));
        }
        Ok(Self {
            mode,
            servers,
            serve,
            serve_authority: bool_env_or_false("ORION_NODE_TIME_SERVE_AUTHORITY")?,
            min_poll,
            max_poll,
        })
    }

    pub fn enabled(&self) -> bool {
        self.mode != TimeSyncMode::Off || self.serve.is_some()
    }
}

/// What the sync knows, published as `time.*` status keys on the node.
#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct TimeStatus {
    pub(crate) mode: &'static str,
    pub(crate) source: Option<String>,
    pub(crate) offset_ns: Option<i64>,
    pub(crate) delay_ns: Option<i64>,
    pub(crate) stratum: Option<u8>,
    pub(crate) synced: bool,
    pub(crate) last_action: &'static str,
    pub(crate) error: Option<String>,
}

impl TimeStatus {
    pub(crate) fn entries(&self) -> Vec<(&'static str, TypedConfigValue)> {
        let mut entries = vec![
            ("time.mode", TypedConfigValue::String(self.mode.to_owned())),
            ("time.synced", TypedConfigValue::Bool(self.synced)),
            (
                "time.last_action",
                TypedConfigValue::String(self.last_action.to_owned()),
            ),
        ];
        if let Some(source) = &self.source {
            entries.push(("time.source", TypedConfigValue::String(source.clone())));
        }
        if let Some(offset) = self.offset_ns {
            entries.push(("time.offset_ms", TypedConfigValue::F64(offset as f64 / 1e6)));
        }
        if let Some(delay) = self.delay_ns {
            entries.push(("time.delay_ms", TypedConfigValue::F64(delay as f64 / 1e6)));
        }
        if let Some(stratum) = self.stratum {
            entries.push(("time.stratum", TypedConfigValue::UInt(u64::from(stratum))));
        }
        entries.push((
            "time.error",
            TypedConfigValue::String(self.error.clone().unwrap_or_default()),
        ));
        entries
    }
}

/// The served clock as one byte: 0 unsynchronized, 1 to 15 synchronized at that stratum.
#[derive(Clone, Default)]
pub(crate) struct ServedState(Arc<AtomicU8>);

impl ServedState {
    fn set_synced(&self, stratum: u8) {
        self.0.store(stratum.clamp(1, 15), Ordering::Relaxed);
    }

    fn served(&self, authority: bool) -> ServedClock {
        match self.0.load(Ordering::Relaxed) {
            0 if authority => ServedClock::Authority,
            0 => kernel_served_clock(),
            stratum => ServedClock::Synced(stratum.saturating_add(1)),
        }
    }
}

/// A clock this node does not discipline is served as synchronized when the kernel says so
/// (another daemon keeps it), at a stratum that admits it is second hand.
fn kernel_served_clock() -> ServedClock {
    use crate::clock::{ClockStatusSource, KernelClockStatusSource, STA_UNSYNC};
    match KernelClockStatusSource.read() {
        Some(reading) if reading.status & STA_UNSYNC == 0 => ServedClock::Synced(3),
        _ => ServedClock::Unsynced,
    }
}

/// The running time sync; dropping it stops it.
pub struct TimeSyncHandle {
    shutdown: watch::Sender<bool>,
    tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl TimeSyncHandle {
    pub async fn shutdown(self) {
        let _ = self.shutdown.send(true);
        for task in self.tasks {
            task.abort();
            let _ = task.await;
        }
    }
}

/// Where a sample came from, and the sample. A `tcp://` source is asked over TCP.
async fn best_sample(servers: &[String]) -> Result<(String, sntp::Sample), String> {
    let mut last_error = String::from("no server answered");
    for server in servers {
        let (carrier, host) = match server.strip_prefix("tcp://") {
            Some(host) => (sntp::Carrier::Tcp, host),
            None => (sntp::Carrier::Udp, server.as_str()),
        };
        let addrs = match tokio::net::lookup_host(host).await {
            Ok(addrs) => addrs,
            Err(error) => {
                last_error = format!("{server}: {error}");
                continue;
            }
        };
        for addr in addrs {
            match sntp::query_over(carrier, addr).await {
                Ok(sample) => {
                    debug!(%server, %addr, offset_ms = sample.offset_ns / 1_000_000, delay_ms = sample.delay_ns / 1_000_000, stratum = sample.stratum, "time sample");
                    return Ok((server.clone(), sample));
                }
                Err(error) => last_error = format!("{server} ({addr}): {error}"),
            }
        }
    }
    Err(last_error)
}

/// One sync round's state, kept between rounds.
pub(crate) struct Syncer {
    pub(crate) config: TimeSyncConfig,
    pub(crate) disciplining: bool,
    pub(crate) discipline: Discipline,
    pub(crate) clock: Arc<dyn Clock>,
    pub(crate) served: ServedState,
    pub(crate) saved_time: Option<PathBuf>,
    pub(crate) status: TimeStatus,
    pub(crate) poll: Duration,
}

impl Syncer {
    pub(crate) fn new(
        config: TimeSyncConfig,
        clock: Arc<dyn Clock>,
        served: ServedState,
        saved_time: Option<PathBuf>,
        timesyncd: bool,
    ) -> Self {
        let disciplining = config.mode == TimeSyncMode::Discipline && !timesyncd;
        if config.mode == TimeSyncMode::Discipline && timesyncd {
            warn!("systemd-timesyncd is running; time sync only reports until it is stopped");
        }
        let status = TimeStatus {
            mode: if disciplining { "discipline" } else { "report" },
            last_action: "none",
            ..TimeStatus::default()
        };
        Self {
            config,
            disciplining,
            discipline: Discipline::default(),
            clock,
            served,
            saved_time,
            status,
            poll: FIRST_POLL,
        }
    }

    /// At start: a clock behind the last synchronized time moves forward to it.
    pub(crate) fn restore_saved_time(&mut self) {
        let (true, Some(path)) = (self.disciplining, self.saved_time.as_ref()) else {
            return;
        };
        let Some(saved_ms) = std::fs::read_to_string(path)
            .ok()
            .and_then(|text| text.trim().parse::<i64>().ok())
        else {
            return;
        };
        let now_ms = (sntp::unix_now_ns() / 1_000_000) as i64;
        if saved_ms > now_ms {
            let ahead_ns = (saved_ms - now_ms).saturating_mul(1_000_000);
            match self.clock.step(ahead_ns) {
                Ok(()) => {
                    info!(
                        ahead_ms = saved_ms - now_ms,
                        "clock moved forward to the last synchronized time"
                    );
                    self.status.last_action = "restored";
                }
                Err(error) => {
                    warn!(%error, "could not move the clock to the last synchronized time")
                }
            }
        }
    }

    /// Applies one sample (or the failure to get one) and sets the next poll.
    pub(crate) fn apply(&mut self, result: Result<(String, sntp::Sample), String>) {
        let (source, sample) = match result {
            Ok(found) => found,
            Err(error) => {
                self.status.error = Some(error);
                self.poll = if self.discipline.synced() {
                    self.config.min_poll
                } else {
                    FIRST_POLL
                        .max(self.poll.saturating_mul(2))
                        .min(self.config.min_poll)
                };
                return;
            }
        };
        self.status.error = None;
        self.status.source = Some(source.clone());
        self.status.stratum = Some(sample.stratum);
        self.status.delay_ns = Some(sample.delay_ns);
        self.status.offset_ns = Some(sample.offset_ns);
        let action = self.discipline.on_sample(sample.offset_ns, sample.delay_ns);
        let applied = if self.disciplining {
            match action {
                Action::Step(offset) => self.clock.step(offset).map(|()| "stepped"),
                Action::Slew(offset) => self
                    .clock
                    .slew(offset, sample.delay_ns / 2)
                    .map(|()| "slewed"),
                Action::Wait => Ok("waiting for a second sample"),
                Action::Refuse(_) => Ok("refused a backward step"),
                Action::Discard => Ok("discarded a slow answer"),
            }
        } else {
            Ok(match action {
                Action::Step(_) => "would step",
                Action::Slew(_) => "would slew",
                Action::Wait => "waiting for a second sample",
                Action::Refuse(_) => "would refuse a backward step",
                Action::Discard => "discarded a slow answer",
            })
        };
        let stepped = matches!(action, Action::Step(_)) && applied.is_ok();
        match applied {
            Ok(what) => self.status.last_action = what,
            Err(error) => {
                warn!(%error, ?action, "could not adjust the clock");
                self.status.error = Some(format!("adjusting the clock: {error}"));
                self.status.last_action = "failed";
            }
        }
        if stepped {
            info!(%source, offset_ms = sample.offset_ns / 1_000_000, disciplining = self.disciplining, "clock step");
        }
        let synced_now =
            self.disciplining && self.discipline.synced() && self.status.error.is_none();
        self.status.synced = synced_now;
        if synced_now {
            self.served.set_synced(sample.stratum);
            if let Some(path) = &self.saved_time {
                let now_ms = (sntp::unix_now_ns() / 1_000_000).to_string();
                if let Err(error) = std::fs::write(path, now_ms) {
                    warn!(%error, path = %path.display(), "could not save the synchronized time");
                }
            }
        }
        self.poll = if matches!(action, Action::Wait) || !self.discipline.synced() {
            FIRST_POLL
        } else if sample.offset_ns.unsigned_abs() < CALM_NS.unsigned_abs() {
            self.poll
                .max(self.config.min_poll)
                .saturating_mul(2)
                .min(self.config.max_poll)
        } else {
            self.config.min_poll
        };
    }
}

/// Starts the time service and the sync loop, as configured. `publish` files the `time.*`
/// status keys (with a time-to-live of three polls).
pub(crate) fn spawn(
    config: TimeSyncConfig,
    state_dir: Option<PathBuf>,
    publish: impl Fn(&TimeStatus, Duration) + Send + 'static,
) -> Result<TimeSyncHandle, NodeError> {
    let (shutdown, mut stop) = watch::channel(false);
    let served = ServedState::default();
    let mut tasks = Vec::new();
    if let Some(addr) = config.serve {
        let std_socket = std::net::UdpSocket::bind(addr)
            .and_then(|socket| socket.set_nonblocking(true).map(|()| socket))
            .map_err(|error| NodeError::Config(format!("ORION_NODE_TIME_SERVE {addr}: {error}")))?;
        let socket = UdpSocket::from_std(std_socket)
            .map_err(|error| NodeError::Config(format!("ORION_NODE_TIME_SERVE {addr}: {error}")))?;
        let authority = config.serve_authority;
        let state = served.clone();
        info!(%addr, authority, "serving the node clock over SNTP");
        let tcp = std::net::TcpListener::bind(addr)
            .and_then(|listener| listener.set_nonblocking(true).map(|()| listener))
            .and_then(tokio::net::TcpListener::from_std)
            .map_err(|error| {
                NodeError::Config(format!("ORION_NODE_TIME_SERVE {addr} (tcp): {error}"))
            })?;
        let tcp_state = served.clone();
        tasks.push(tokio::spawn(sntp::serve(
            socket,
            move || state.served(authority),
            sntp::NtpTime::now,
        )));
        tasks.push(tokio::spawn(sntp::serve_tcp(
            tcp,
            move || tcp_state.served(authority),
            sntp::NtpTime::now,
        )));
    }
    if config.mode != TimeSyncMode::Off {
        let saved_time = state_dir.map(|dir| dir.join(SAVED_TIME_FILE));
        let mut syncer = Syncer::new(
            config,
            Arc::new(KernelClock),
            served,
            saved_time,
            timesyncd_running(),
        );
        tasks.push(tokio::spawn(async move {
            syncer.restore_saved_time();
            loop {
                let result = best_sample(&syncer.config.servers).await;
                syncer.apply(result);
                publish(&syncer.status, syncer.poll.saturating_mul(3));
                tokio::select! {
                    () = tokio::time::sleep(syncer.poll) => {}
                    changed = stop.changed() => {
                        if changed.is_err() || *stop.borrow() {
                            return;
                        }
                    }
                }
            }
        }));
    }
    Ok(TimeSyncHandle { shutdown, tasks })
}

#[cfg(test)]
mod tests;
