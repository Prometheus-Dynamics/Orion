//! One round of the time sync: ask the sources in rank order, apply the rules, set the next
//! poll.

use std::cell::Cell;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::time::Duration;

use orion_measure::clock;
use orion_server::Comm;
use tracing::{debug, info, warn};

use crate::config::{PEER_SCHEME, TCP_SCHEME, TimeConfig, TimeMode};
use crate::discipline::{Action, Clock, Discipline, EARLIEST_PLAUSIBLE_MS, implausible};
use crate::operator::{OPERATOR_SOURCE, OperatorTime};
use crate::sntp::{self, ServedClock};

/// Polls this often until the first sample is in (and while a step waits for agreement).
pub(crate) const FIRST_POLL: Duration = Duration::from_secs(2);
/// An offset below this lets the poll interval grow.
const CALM_NS: i64 = 50_000_000;
pub(crate) const SAVED_TIME_FILE: &str = "time-sync.last";
/// A synchronized clock has lost its sources when this many polls in a row get no answer, as
/// NTP's reach register has a server gone after eight.
pub(crate) const LOST_AFTER: u32 = 8;

/// What the sync knows. Published as the topic `orion.time`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TimeStatus {
    /// `report` or `discipline` (`report` while timesyncd runs).
    pub mode: &'static str,
    /// The source of the last sample.
    pub source: Option<String>,
    /// Source time minus local time at the last sample.
    pub offset_ns: Option<i64>,
    /// The last sample's round trip.
    pub delay_ns: Option<i64>,
    /// The source's stratum.
    pub stratum: Option<u8>,
    /// This board has synchronized the clock.
    pub synced: bool,
    /// `stepped`, `slewed`, `would step`, `waiting for a second sample`, and so on.
    pub last_action: &'static str,
    /// Why the last poll gave no sample.
    pub error: Option<String>,
}

/// The served clock as one byte: 0 unsynchronized, 1 to 15 synchronized at that stratum.
#[derive(Clone, Debug, Default)]
pub(crate) struct ServedState(Rc<Cell<u8>>);

impl ServedState {
    fn set_synced(&self, stratum: u8) {
        self.0.set(stratum.clamp(1, 15));
    }

    fn set_unsynced(&self) {
        self.0.set(0);
    }

    pub(crate) fn served(&self, authority: bool) -> ServedClock {
        match self.0.get() {
            0 if authority => ServedClock::Authority,
            // A clock this board does not discipline is served as synchronized when the
            // kernel says so (another daemon keeps it), at a stratum that admits it is second
            // hand.
            0 if clock::synchronized() == Some(true) => ServedClock::Synced(3),
            0 => ServedClock::Unsynced,
            stratum => ServedClock::Synced(stratum.saturating_add(1)),
        }
    }
}

pub(crate) fn remove_synced_file(path: Option<&Path>) {
    if let Some(path) = path
        && let Err(error) = std::fs::remove_file(path)
        && error.kind() != std::io::ErrorKind::NotFound
    {
        warn!(%error, path = %path.display(), "could not remove the time-synced file");
    }
}

/// The decimal number a file holds, if it holds one.
fn number_in(path: &Path) -> Option<i64> {
    std::fs::read_to_string(path).ok()?.trim().parse().ok()
}

/// The time a sample says it is, in unix milliseconds.
fn sample_time_ms(sample: &sntp::Sample) -> i64 {
    let ns = sntp::unix_now_ns() + i128::from(sample.offset_ns);
    i64::try_from(ns / 1_000_000).unwrap_or(i64::MAX)
}

/// The first source, in rank order, with an answer that is not obviously wrong, and that answer.
/// A `tcp://` source is asked over TCP.
pub(crate) async fn best_sample(
    servers: &[String],
    operator: &OperatorTime,
    comm: Option<&Comm>,
    saved_ms: Option<i64>,
) -> Result<(String, sntp::Sample), String> {
    let mut last_error = String::from("no server answered");
    for server in servers {
        if server == OPERATOR_SOURCE {
            let asked = match comm {
                Some(comm) => operator.ask(comm).await,
                None => Err("no operator is offering its clock"),
            };
            match asked {
                // Held to the build's date as every source is: a computer's clock can be wrong.
                Ok((sample, who)) => match implausible(sample_time_ms(&sample), saved_ms, false) {
                    Some(why) => last_error = format!("{who}: {why}"),
                    None => return Ok((who, sample)),
                },
                Err(why) => last_error = format!("{server}: {why}"),
            }
            continue;
        }
        let (carrier, host, fleet_peer) = match (
            server.strip_prefix(TCP_SCHEME),
            server.strip_prefix(PEER_SCHEME),
        ) {
            (Some(host), _) => (sntp::Carrier::Tcp, host, true),
            (_, Some(host)) => (sntp::Carrier::Udp, host, true),
            _ => (sntp::Carrier::Udp, server.as_str(), false),
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
                    if let Some(why) = implausible(sample_time_ms(&sample), saved_ms, fleet_peer) {
                        warn!(%server, %why, "ignoring a time source");
                        last_error = format!("{server}: {why}");
                        continue;
                    }
                    return Ok((server.clone(), sample));
                }
                Err(error) => last_error = format!("{server} ({addr}): {error}"),
            }
        }
    }
    Err(last_error)
}

/// The sync's state, kept between rounds.
pub(crate) struct Syncer {
    pub(crate) config: TimeConfig,
    pub(crate) disciplining: bool,
    pub(crate) discipline: Discipline,
    pub(crate) clock: Rc<dyn Clock>,
    pub(crate) served: ServedState,
    pub(crate) saved_time: Option<PathBuf>,
    /// The last synchronized time, the anchor for telling a wrong source from a right one.
    pub(crate) saved_ms: Option<i64>,
    pub(crate) status: TimeStatus,
    pub(crate) poll: Duration,
    synced_file_up: bool,
    /// Polls in a row with no answer.
    failures: u32,
}

impl Syncer {
    pub(crate) fn new(
        config: TimeConfig,
        clock: Rc<dyn Clock>,
        served: ServedState,
        saved_time: Option<PathBuf>,
        timesyncd: bool,
    ) -> Self {
        let disciplining = config.mode == TimeMode::Discipline && !timesyncd;
        if config.mode == TimeMode::Discipline && timesyncd {
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
            saved_ms: None,
            status,
            poll: FIRST_POLL,
            synced_file_up: false,
            failures: 0,
        }
    }

    /// At start: a clock behind the last synchronized time, or behind the hardware clock, moves
    /// forward to the later of them. Disciplining, the kernel is then told the clock is not
    /// synchronized: neither floor vouches for it, and nothing may copy it to the hardware clock
    /// until a source does.
    pub(crate) fn restore_saved_time(&mut self) {
        self.saved_ms = self.saved_time.as_deref().and_then(number_in);
        if !self.disciplining {
            return;
        }
        // A hardware clock with no battery reads 1970 after power-up: no floor at all.
        let rtc_ms = self
            .config
            .rtc
            .as_deref()
            .and_then(number_in)
            .map(|seconds| seconds.saturating_mul(1000))
            .filter(|ms| *ms >= EARLIEST_PLAUSIBLE_MS);
        let floor = match (self.saved_ms, rtc_ms) {
            (Some(saved), Some(rtc)) if rtc > saved => Some((rtc, "the hardware clock")),
            (Some(saved), _) => Some((saved, "the last synchronized time")),
            (None, Some(rtc)) => Some((rtc, "the hardware clock")),
            (None, None) => None,
        };
        let now_ms = (sntp::unix_now_ns() / 1_000_000) as i64;
        if let Some((floor_ms, from)) = floor
            && floor_ms > now_ms
        {
            let ahead_ns = (floor_ms - now_ms).saturating_mul(1_000_000);
            match self.clock.step(ahead_ns) {
                Ok(()) => {
                    info!(
                        ahead_ms = floor_ms - now_ms,
                        "clock moved forward to {from}"
                    );
                    self.status.last_action = "restored";
                }
                Err(error) => warn!(%error, "could not move the clock forward to {from}"),
            }
        }
        // Left synchronized by a run before this one, or by the step.
        if let Err(error) = self.clock.unsync() {
            warn!(%error, "could not tell the kernel the clock is not synchronized");
        }
    }

    /// Once synchronized, [`LOST_AFTER`] polls in a row with no answer: the clock is no longer
    /// said to be synchronized, here, to the kernel, or to those it serves. The rules for a
    /// synchronized clock stay: it is still not stepped backwards.
    fn lose_sources(&mut self) {
        info!(
            polls = self.failures,
            "the time sources stopped answering: the clock is no longer synchronized"
        );
        self.status.synced = false;
        self.status.last_action = "lost its sources";
        self.set_synced_file(false);
        self.served.set_unsynced();
        if self.disciplining
            && let Err(error) = self.clock.unsync()
        {
            warn!(%error, "could not tell the kernel the clock is not synchronized");
        }
    }

    /// Applies one sample (or the failure to get one) and sets the next poll.
    pub(crate) fn apply(&mut self, result: Result<(String, sntp::Sample), String>) {
        let (source, sample) = match result {
            Ok(found) => found,
            Err(error) => {
                self.status.error = Some(error);
                self.failures = self.failures.saturating_add(1);
                if self.status.synced && self.failures >= LOST_AFTER {
                    self.lose_sources();
                }
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
        self.failures = 0;
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
            info!(%source, offset_ms = sample.offset_ns / 1_000_000, "clock step");
        }
        self.status.source = Some(source);
        let synced_now =
            self.disciplining && self.discipline.synced() && self.status.error.is_none();
        self.status.synced = synced_now;
        self.set_synced_file(synced_now);
        if synced_now {
            self.served.set_synced(sample.stratum);
            if let Some(path) = &self.saved_time {
                let now_ms = sntp::unix_now_ns() / 1_000_000;
                match std::fs::write(path, now_ms.to_string()) {
                    Ok(()) => self.saved_ms = i64::try_from(now_ms).ok(),
                    Err(error) => {
                        warn!(%error, path = %path.display(), "could not save the synchronized time");
                    }
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

    /// Touches the file only when the state changes, so a steady clock writes nothing.
    pub(crate) fn set_synced_file(&mut self, up: bool) {
        if up == self.synced_file_up {
            return;
        }
        self.synced_file_up = up;
        if !up {
            remove_synced_file(self.config.synced_file.as_deref());
        } else if let Some(path) = &self.config.synced_file
            && let Err(error) = std::fs::write(path, b"")
        {
            warn!(%error, path = %path.display(), "could not write the time-synced file");
        }
    }
}
