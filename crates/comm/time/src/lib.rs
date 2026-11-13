//! Time sync: keeps the wall clock right from SNTP servers, another Orion node or a connected
//! operator, so an image needs no NTP daemon.
//!
//! - **Sources**, in rank order (`time.servers`): `host[:port]` is an NTP server (RFC 4330),
//!   `peer://host[:port]` another Orion node serving its clock, `tcp://host:port` one asked over
//!   TCP, and `operator` the clock of a connected operator who offers it.
//! - **Modes** (`time.sync`): `report` measures the offset and publishes it, and never touches
//!   the clock. `discipline` also slews or steps it; that needs `CAP_SYS_TIME`, and falls back to
//!   `report` while `systemd-timesyncd` runs.
//! - **Serving** (`time.serve`): this board's clock, as SNTP over UDP and the same 48 bytes over
//!   TCP. Over a session, anyone may ask with `TimeRequest`.
//! - **State is a topic**, `orion.time`: `mode`, `synced`, `source`, `offset_ms`, `delay_ms`,
//!   `stratum`, `last_action`, `error`, and `monotonic_to_wall_ns`, which added to a sample
//!   time here gives its wall time. Published when it changes; the last by more than a
//!   millisecond.
//!
//! The rules for what to do with an offset are in `discipline`; `docs/comm.md` has the whole
//! account.

#![cfg(target_os = "linux")]

mod config;
mod discipline;
mod operator;
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
use tokio::net::{TcpListener, UdpSocket};
use tokio::task::JoinHandle;
use tracing::info;

pub use config::{TimeConfig, TimeMode};
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

const FIELDS: [(&str, FieldType, &str); 9] = [
    ("mode", FieldType::Text, ""),
    ("synced", FieldType::Bool, ""),
    ("source", FieldType::Text, ""),
    ("offset_ms", FieldType::Float, "ms"),
    ("delay_ms", FieldType::Float, "ms"),
    ("stratum", FieldType::Int, ""),
    ("last_action", FieldType::Text, ""),
    ("error", FieldType::Text, ""),
    ("monotonic_to_wall_ns", FieldType::Int, "ns"),
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
        // An error that went away is published as empty text.
        let fields: [(bool, Option<&[u8]>); 9] = [
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

/// The running time sync. Dropping it stops it.
#[derive(Debug)]
pub struct TimeSync {
    tasks: Vec<JoinHandle<()>>,
    synced_file: Option<PathBuf>,
    status: Rc<RefCell<TimeStatus>>,
    serving: Option<std::net::SocketAddr>,
}

fn context(what: &str, addr: std::net::SocketAddr, error: &std::io::Error) -> std::io::Error {
    std::io::Error::new(error.kind(), format!("time.serve {addr} ({what}): {error}"))
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
            // Both sockets are on one port. Asked for any port, the datagram socket gets
            // one, and the listener needs the same number, which something else may hold:
            // then another is tried.
            let tries = if addr.port() == 0 { 16 } else { 1 };
            let mut bound = None;
            let mut last = None;
            for _ in 0..tries {
                let udp = std::net::UdpSocket::bind(addr)
                    .and_then(|socket| socket.set_nonblocking(true).map(|()| socket))
                    .map_err(|error| context("udp", addr, &error))?;
                let on = udp.local_addr().unwrap_or(addr);
                match std::net::TcpListener::bind(on)
                    .and_then(|listener| listener.set_nonblocking(true).map(|()| listener))
                {
                    Ok(tcp) => {
                        bound = Some((udp, tcp, on));
                        break;
                    }
                    Err(error) => last = Some(context("tcp", on, &error)),
                }
            }
            let Some((udp, tcp, addr)) = bound else {
                return Err(last.unwrap_or_else(|| std::io::Error::other("time.serve: no port")));
            };
            let udp = UdpSocket::from_std(udp).map_err(|error| context("udp", addr, &error))?;
            let tcp = TcpListener::from_std(tcp).map_err(|error| context("tcp", addr, &error))?;
            serving = Some(addr);
            let authority = config.serve_authority;
            info!(%addr, authority, "serving the clock over SNTP");
            let (for_udp, for_tcp) = (served.clone(), served.clone());
            tasks.push(tokio::task::spawn_local(sntp::serve(
                udp,
                move || for_udp.served(authority),
                sntp::NtpTime::now,
            )));
            tasks.push(tokio::task::spawn_local(sntp::serve_tcp(
                tcp,
                move || for_tcp.served(authority),
                sntp::NtpTime::now,
            )));
        }
        let status = Rc::new(RefCell::new(TimeStatus::default()));
        if config.mode != TimeMode::Off {
            let mut topic = match topics {
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
            let mut syncer = Syncer::new(config, clock, served, saved_time, timesyncd);
            let (comm, operator, shared) = (comm.clone(), operator.clone(), status.clone());
            tasks.push(tokio::task::spawn_local(async move {
                syncer.restore_saved_time();
                loop {
                    let result = best_sample(
                        &syncer.config.servers,
                        &operator,
                        Some(&comm),
                        syncer.saved_ms,
                    )
                    .await;
                    syncer.apply(result);
                    operator.set_poll(syncer.poll);
                    if let Some(topic) = &mut topic {
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
            status,
            serving,
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
        self.status.borrow().clone()
    }
}

impl Drop for TimeSync {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
        remove_synced_file(self.synced_file.as_deref());
    }
}
