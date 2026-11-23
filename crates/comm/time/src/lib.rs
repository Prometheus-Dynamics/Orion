//! Time sync: keeps the wall clock right from SNTP servers, another Orion node or a connected
//! operator, so an image needs no NTP daemon.
//!
//! - **Sources**, in rank order (`time.servers`): `host[:port]` is an NTP server (RFC 4330),
//!   `peer://host[:port]` another Orion node serving its clock, `tcp://host:port` one asked over
//!   TCP, `operator` the clock of a connected operator who offers it, `gateway` the default
//!   route's gateway and `nt4` the host of `nt4.server`, each asked as an NTP server.
//! - **Modes** (`time.sync`): `report` measures the offset and publishes it, and never touches
//!   the clock. `discipline` also slews or steps it; that needs `CAP_SYS_TIME`, and falls back to
//!   `report` while `systemd-timesyncd` runs.
//! - **Serving** (`time.serve`): this board's clock, as SNTP over UDP and the same 48 bytes over
//!   TCP. Over a session, anyone may ask with `TimeRequest`.
//! - **State is a topic**, `orion.time`: `mode`, `synced`, `source`, `offset_ms`, `delay_ms`,
//!   `stratum`, `last_action`, `error`, and `monotonic_to_wall_ns`, which added to a sample
//!   time here gives its wall time. Published when it changes; the last by more than a
//!   millisecond. And, while the NT4 gateway is joined to a server, `nt4_offset_us` and
//!   `nt4_rtt_us`: that server's clock less this board's sample clock.
//!
//! The rules for what to do with an offset are in `discipline`; `docs/comm.md` has the whole
//! account.

#![cfg(target_os = "linux")]

mod config;
mod discipline;
mod operator;
mod serve;
mod sntp;
mod sync;
#[cfg(test)]
mod tests;

use std::cell::RefCell;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::time::{SystemTime, UNIX_EPOCH};

use orion_broker::{LocalPublisher, Topics};
use orion_msg_topics::FieldType;
use orion_server::Comm;
use shm_ring::{FieldId, SampleTime};
use tokio::task::JoinHandle;
use tracing::{info, warn};

pub use config::{GATEWAY_SOURCE, NT4_SOURCE, TimeConfig, TimeMode};
pub use discipline::{Clock, KernelClock};
pub use operator::{CLOCK_SET, OPERATOR_SOURCE};
pub use sync::TimeStatus;

use operator::{OperatorTime, TimeService};
use sync::{SAVED_TIME_FILE, ServedState, Syncer, best_sample, remove_synced_file};

/// The topic the sync's state is published as.
pub const TIME_TOPIC: &str = "orion.time";
/// The longest text in the topic. Longer is cut.
const MAX_TEXT: usize = 120;

/// How far `monotonic_to_wall_ns` moves before it is published again: a slew moves it a little
/// all the time.
const MOVED_NS: i64 = 1_000_000;

/// Wall time less monotonic time, now: what is added to a sample time here to get its wall time.
fn monotonic_to_wall_ns() -> i64 {
    let wall = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| since.as_nanos() as i64);
    wall.saturating_sub(SampleTime::now().as_nanos() as i64)
}

const FIELDS: [(&str, FieldType, &str); 11] = [
    ("mode", FieldType::Text, ""),
    ("synced", FieldType::Bool, ""),
    ("source", FieldType::Text, ""),
    ("offset_ms", FieldType::Float, "ms"),
    ("delay_ms", FieldType::Float, "ms"),
    ("stratum", FieldType::Int, ""),
    ("last_action", FieldType::Text, ""),
    ("error", FieldType::Text, ""),
    ("monotonic_to_wall_ns", FieldType::Int, "ns"),
    ("nt4_offset_us", FieldType::Int, "us"),
    ("nt4_rtt_us", FieldType::Int, "us"),
];

/// `text`, cut to what a field holds, on a character boundary.
fn cut(text: &str) -> &[u8] {
    let mut end = text.len().min(MAX_TEXT);
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    &text.as_bytes()[..end]
}

/// The sync's state as a topic, published when it changes.
struct StatusTopic {
    publisher: LocalPublisher,
    last: Option<TimeStatus>,
    /// The `monotonic_to_wall_ns` last published.
    last_offset: Option<i64>,
}

impl StatusTopic {
    fn publish(&mut self, status: &TimeStatus) {
        let last = self.last.as_ref();
        let ms = |ns: Option<i64>| ns.map(|ns| (ns as f64 / 1e6).to_le_bytes());
        let (offset, delay) = (ms(status.offset_ns), ms(status.delay_ns));
        let stratum = status
            .stratum
            .map(|stratum| i64::from(stratum).to_le_bytes());
        let synced = [u8::from(status.synced)];
        let offset_now = monotonic_to_wall_ns();
        let clocks = offset_now.to_le_bytes();
        let clocks_same = self
            .last_offset
            .is_some_and(|last| (last - offset_now).abs() <= MOVED_NS);
        let same =
            |is: fn(&TimeStatus, &TimeStatus) -> bool| last.is_some_and(|last| is(last, status));
        // The NT4 server's clock: no bytes once the server is lost, and nothing before the
        // first is known.
        let nt4 = status
            .nt4
            .map(|(offset, rtt)| (offset.to_le_bytes(), rtt.to_le_bytes()));
        let nt4_same = last.map_or(status.nt4.is_none(), |last| last.nt4 == status.nt4);
        let (nt4_offset, nt4_rtt): (&[u8], &[u8]) = match &nt4 {
            Some((offset, rtt)) => (offset, rtt),
            None => (&[], &[]),
        };
        // An error that went away is published as empty text.
        let fields: [(bool, Option<&[u8]>); 11] = [
            (same(|a, b| a.mode == b.mode), Some(cut(status.mode))),
            (same(|a, b| a.synced == b.synced), Some(&synced)),
            (
                same(|a, b| a.source == b.source),
                status.source.as_deref().map(cut),
            ),
            (
                same(|a, b| a.offset_ns == b.offset_ns),
                offset.as_ref().map(<[u8; 8]>::as_slice),
            ),
            (
                same(|a, b| a.delay_ns == b.delay_ns),
                delay.as_ref().map(<[u8; 8]>::as_slice),
            ),
            (
                same(|a, b| a.stratum == b.stratum),
                stratum.as_ref().map(<[u8; 8]>::as_slice),
            ),
            (
                same(|a, b| a.last_action == b.last_action),
                Some(cut(status.last_action)),
            ),
            (
                same(|a, b| a.error == b.error),
                Some(cut(status.error.as_deref().unwrap_or(""))),
            ),
            (clocks_same, Some(&clocks)),
            (nt4_same, Some(nt4_offset)),
            (nt4_same, Some(nt4_rtt)),
        ];
        let changed: Vec<(FieldId, &[u8])> = fields
            .iter()
            .enumerate()
            .filter_map(|(index, (same, value))| {
                Some((FieldId(index as u16), (*value).filter(|_| !same)?))
            })
            .collect();
        if changed.is_empty() {
            return;
        }
        if self.publisher.publish(SampleTime::now(), &changed).is_ok() {
            self.last = Some(status.clone());
            self.last_offset = Some(offset_now);
        }
    }
}

/// Where the NT4 gateway says how the server's clock compares with this board's: it goes into
/// `orion.time` as `nt4_offset_us` and `nt4_rtt_us`, at once.
#[derive(Clone)]
pub struct FarClock {
    status: Rc<RefCell<TimeStatus>>,
    topic: Rc<RefCell<Option<StatusTopic>>>,
}

impl FarClock {
    /// The server's clock less this board's sample clock and the round trip it was measured
    /// with, in microseconds; `None` when the server is lost.
    pub fn set(&self, clock: Option<(i64, i64)>) {
        self.status.borrow_mut().nt4 = clock;
        if let Some(topic) = &mut *self.topic.borrow_mut() {
            topic.publish(&self.status.borrow());
        }
    }
}

/// The longest `orion/state` line about the clock. Longer is cut.
const MAX_LINE: usize = 1024;

/// What others in the comm core say of the clock: the hello document's `clock` fact, the
/// announcement's `clock=` key and `orion/state`'s `clock:` line.
#[derive(Clone, Debug)]
pub struct TimeView {
    status: Rc<RefCell<TimeStatus>>,
    servers: Rc<[String]>,
    measuring: bool,
}

impl TimeView {
    /// `synced` while this board's time sync holds the clock synchronized, `unset` otherwise:
    /// after power-up on a board with no clock battery, the normal state until a source
    /// answers. `None` when nothing is measured (`time.sync = off`).
    #[must_use]
    pub fn word(&self) -> Option<&'static str> {
        self.measuring.then(|| {
            if self.status.borrow().synced {
                "synced"
            } else {
                "unset"
            }
        })
    }

    /// The `orion/state` line, ending in a newline, or nothing when nothing is measured.
    #[must_use]
    pub fn state_line(&self) -> String {
        if !self.measuring {
            return String::new();
        }
        let status = self.status.borrow();
        let mut line = match (&status.source, status.synced) {
            (Some(source), true) => format!("clock: synced from {source}"),
            _ => {
                let only = if status.mode == "report" {
                    "time.sync = report; "
                } else {
                    ""
                };
                let last = match (&status.error, status.last_action) {
                    (Some(error), _) => error.as_str(),
                    (None, "none" | "") => "not asked yet",
                    (None, action) => action,
                };
                format!(
                    "clock: not set ({only}sources tried: {}; last: {last})",
                    self.servers.join(", ")
                )
            }
        };
        if line.len() > MAX_LINE {
            let mut end = MAX_LINE;
            while !line.is_char_boundary(end) {
                end -= 1;
            }
            line.truncate(end);
        }
        line.push('\n');
        line
    }
}

/// The running time sync. Dropping it stops it.
#[derive(Debug)]
pub struct TimeSync {
    tasks: Vec<JoinHandle<()>>,
    synced_file: Option<PathBuf>,
    view: TimeView,
    serving: Option<std::net::SocketAddr>,
    far: FarClock,
    /// The clock to mark unsynchronized when this stops, if it was disciplined.
    disciplined: Option<Rc<dyn Clock>>,
}

impl std::fmt::Debug for FarClock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("FarClock")
    }
}

impl std::fmt::Debug for dyn Clock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Clock")
    }
}

impl TimeSync {
    /// Starts the time service and the sync loop, as configured, with the kernel's clock. Must
    /// be called inside the comm core's `LocalSet`.
    ///
    /// `topics` is where `orion.time` is declared; with none, the state is only in
    /// [`TimeSync::status`]. `state_dir` is where the last synchronized time is kept.
    ///
    /// # Errors
    ///
    /// If `time.serve` can't be bound, or the topic can't be declared.
    pub fn start(
        comm: &Comm,
        topics: Option<&Rc<RefCell<Topics>>>,
        config: TimeConfig,
        state_dir: Option<&Path>,
    ) -> std::io::Result<Self> {
        Self::start_with(
            comm,
            topics,
            config,
            state_dir,
            Rc::new(KernelClock),
            discipline::timesyncd_running(),
        )
    }

    /// As [`TimeSync::start`], with the clock to adjust and whether another daemon keeps it.
    ///
    /// # Errors
    ///
    /// As [`TimeSync::start`].
    pub fn start_with(
        comm: &Comm,
        topics: Option<&Rc<RefCell<Topics>>>,
        config: TimeConfig,
        state_dir: Option<&Path>,
        clock: Rc<dyn Clock>,
        timesyncd: bool,
    ) -> std::io::Result<Self> {
        let synced_file = config.synced_file.clone();
        // Left by a run that did not shut down: nothing vouches for the clock yet.
        remove_synced_file(synced_file.as_deref());
        let served = ServedState::default();
        let operator = Rc::new(OperatorTime::default());
        let mut tasks = Vec::new();
        let mut serving = None;
        if let Some(addr) = config.serve {
            let authority = config.serve_authority;
            serving = Some(addr);
            if config.serve_interfaces.is_empty() {
                let (udp, tcp, on, _) = serve::open(addr, None)?;
                serving = Some(on);
                info!(addr = %on, authority, "serving the clock over SNTP");
                tasks.extend(serve::serve(udp, tcp, &served, authority));
            }
            // One that is not there yet holds up nothing: each is its own to open.
            for name in &config.serve_interfaces {
                tasks.push(tokio::task::spawn_local(serve::keep(
                    addr,
                    name.clone(),
                    served.clone(),
                    authority,
                    config.serve_retry,
                )));
            }
        }
        let status = Rc::new(RefCell::new(TimeStatus::default()));
        let view = TimeView {
            status: status.clone(),
            servers: config.servers.clone().into(),
            measuring: config.mode != TimeMode::Off,
        };
        let mut disciplined = None;
        let shared_topic = Rc::new(RefCell::new(None));
        let far = FarClock {
            status: status.clone(),
            topic: shared_topic.clone(),
        };
        if config.mode != TimeMode::Off {
            *shared_topic.borrow_mut() = match topics {
                Some(topics) => Some(StatusTopic {
                    last_offset: None,
                    publisher: LocalPublisher::declare(
                        topics,
                        TIME_TOPIC,
                        &FIELDS,
                        2,
                        MAX_TEXT as u32,
                    )
                    .map_err(|error| {
                        std::io::Error::other(format!("declaring {TIME_TOPIC}: {error}"))
                    })?,
                    last: None,
                }),
                None => None,
            };
            if config
                .servers
                .iter()
                .any(|server| server == OPERATOR_SOURCE)
            {
                operator.listen();
            }
            // First in rank: an offer is worth acting on when it comes, not at the next poll.
            let operator_first = config
                .servers
                .first()
                .is_some_and(|server| server == OPERATOR_SOURCE);
            let saved_time = state_dir.map(|dir| dir.join(SAVED_TIME_FILE));
            let mut syncer = Syncer::new(config, clock.clone(), served, saved_time, timesyncd);
            status.borrow_mut().clone_from(&syncer.status);
            disciplined = syncer.disciplining.then_some(clock);
            let (comm, operator, shared) = (comm.clone(), operator.clone(), status.clone());
            tasks.push(tokio::task::spawn_local(async move {
                syncer.restore_saved_time();
                loop {
                    let result = best_sample(
                        &syncer.config.servers,
                        &operator,
                        Some(&comm),
                        syncer.saved_ms,
                        syncer.but.as_deref(),
                        syncer.config.nt4_host.as_deref(),
                    )
                    .await;
                    syncer.apply(result);
                    operator.set_poll(syncer.poll);
                    // The NT4 gateway's word is kept where everyone reads it.
                    syncer.status.nt4 = shared.borrow().nt4;
                    if let Some(topic) = &mut *shared_topic.borrow_mut() {
                        topic.publish(&syncer.status);
                    }
                    shared.borrow_mut().clone_from(&syncer.status);
                    tokio::select! {
                        () = tokio::time::sleep(syncer.poll) => {}
                        () = operator.offered.notified(), if operator_first => {}
                    }
                }
            }));
        }
        comm.add_service(TimeService(operator));
        Ok(Self {
            tasks,
            synced_file,
            view,
            serving,
            far,
            disciplined,
        })
    }

    /// Where the clock is served, if it is.
    #[must_use]
    pub const fn serving(&self) -> Option<std::net::SocketAddr> {
        self.serving
    }

    /// What the sync knew after its last poll.
    #[must_use]
    pub fn status(&self) -> TimeStatus {
        self.view.status.borrow().clone()
    }

    /// Where the NT4 gateway says how its server's clock compares with this board's.
    #[must_use]
    pub fn far_clock(&self) -> FarClock {
        self.far.clone()
    }

    /// What others say of the clock, kept up to date while this runs.
    #[must_use]
    pub fn view(&self) -> TimeView {
        self.view.clone()
    }
}

impl Drop for TimeSync {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
        remove_synced_file(self.synced_file.as_deref());
        // Nothing keeps the clock now.
        if let Some(clock) = &self.disciplined
            && let Err(error) = clock.unsync()
        {
            warn!(%error, "could not tell the kernel the clock is not synchronized");
        }
    }
}
