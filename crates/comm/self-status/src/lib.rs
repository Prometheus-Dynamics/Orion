//! The board's vital signs and Orion's own state, as topics.
//!
//! There is no metrics endpoint and nothing to scrape. What a dashboard, a tool or another
//! program wants to know about the board is a topic it subscribes to, like any other value:
//!
//! | Topic | Fields |
//! | --- | --- |
//! | `orion.host` | `uptime_s`, `mem_total_kb`, `mem_available_kb`, `cpu_percent`, `temperature_c` |
//! | `orion.comm` | `sessions`, `opened`, `rejected`, `requests`, `refused`, `unknown`, `dropped`, `panics`, `topics`, `ring_bytes` |
//! | `orion.state.<part>` | `state`, `failures`, `restarts`, `retry_at_ms` |
//!
//! **Nothing is sampled while nobody watches.** `orion.host` and `orion.comm` are read only
//! while they have a subscriber: the first one gets a reading at once, then one every
//! interval, and only the fields that changed are published. When the last subscriber leaves,
//! the reading stops; there is no timer left running.
//!
//! **A part's state is published when it changes**, by the part: a link that lost its device,
//! a peer that cannot be reached and when it will be tried next. That is the failure rule's
//! "backoff state is visible", and it costs nothing while nothing changes.

#![cfg(target_os = "linux")]

mod host;

use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use orion_broker::{Broker, LocalPublisher, Topic, TopicId, TopicWatch, Topics};
use orion_msg_topics::{Declare, Field, FieldType};
use orion_ring::{FieldId, FrameTime, Spec};
use orion_server::{Comm, Error, SessionId, Sessions};
use orion_supervise::{State, Status};
use orion_types::TopicName;
use orion_wire::List;
use orion_wire::body::encode_fields;
use tokio::sync::Notify;

pub use host::{HostSample, HostSource, ProcSource};

/// The topic of the board's vital signs.
pub const HOST_TOPIC: &str = "orion.host";
/// The topic of the comm core's own counters.
pub const COMM_TOPIC: &str = "orion.comm";
/// What the topic of a part's state starts with.
pub const STATE_PREFIX: &str = "orion.state.";

const HOST_FIELDS: [(&str, FieldType, &str); 5] = [
    ("uptime_s", FieldType::Int, "s"),
    ("mem_total_kb", FieldType::Int, "kB"),
    ("mem_available_kb", FieldType::Int, "kB"),
    ("cpu_percent", FieldType::Float, "%"),
    ("temperature_c", FieldType::Float, "C"),
];
const COMM_FIELDS: [&str; 10] = [
    "sessions",
    "opened",
    "rejected",
    "requests",
    "refused",
    "unknown",
    "dropped",
    "panics",
    "topics",
    "ring_bytes",
];
const STATE_FIELDS: [&str; 4] = ["state", "failures", "restarts", "retry_at_ms"];

fn now() -> FrameTime {
    let since = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    FrameTime::from_nanos(since.as_nanos() as u64)
}

/// Declares a topic the comm core publishes itself: latest values of eight bytes each.
fn declare(
    topics: &Rc<RefCell<Topics>>,
    name: &str,
    fields: &[(&str, FieldType, &str)],
) -> Result<LocalPublisher, Error> {
    let name: TopicName = name.parse()?;
    let fields: Vec<Field<'_>> = fields
        .iter()
        .map(|(name, ty, unit)| Field {
            name,
            ty: *ty,
            unit,
            type_name: "",
        })
        .collect();
    let declare = Declare {
        topic: name.clone(),
        fields: List::new(&fields),
        history: 2,
        expiry_ms: 0,
        max_payload: 8,
    };
    let mut bytes = vec![0_u8; 64 + fields.len() * 64];
    let len = encode_fields(&declare, &mut bytes)?;
    let spec = Spec {
        fields: fields.len() as u32,
        history: 2,
        max_payload: 8,
        expiry_ms: 0,
    };
    let id = topics
        .borrow_mut()
        .declare_internal(&name, &bytes[..len], &spec)?;
    Ok(LocalPublisher::new(topics.clone(), id))
}

/// A topic of eight-byte values that publishes only what changed.
struct Values {
    publisher: LocalPublisher,
    last: Vec<Option<[u8; 8]>>,
}

impl Values {
    fn new(publisher: LocalPublisher, fields: usize) -> Self {
        Self {
            publisher,
            last: vec![None; fields],
        }
    }

    /// Publishes the fields of `values` that differ from what was last published. A `None` is
    /// a field with no value now, which is left as it was.
    fn publish(&mut self, values: &[Option<[u8; 8]>]) {
        let mut changed: [(FieldId, &[u8]); 16] = [(FieldId(0), &[]); 16];
        let mut count = 0;
        for (index, value) in values.iter().enumerate() {
            if let Some(value) = value
                && self
                    .last
                    .get(index)
                    .is_some_and(|last| last.as_ref() != Some(value))
                && count < changed.len()
            {
                changed[count] = (FieldId(index as u16), value);
                count += 1;
            }
        }
        if count > 0 && self.publisher.publish(now(), &changed[..count]).is_ok() {
            for (field, value) in &changed[..count] {
                self.last[usize::from(field.0)] = value.first_chunk().copied();
            }
        }
    }

    /// Forgets what was published, so the next reading is published whole: for a subscriber
    /// that has just arrived after a time with nobody watching.
    fn forget(&mut self) {
        self.last.fill(None);
    }
}

const fn int(value: u64) -> Option<[u8; 8]> {
    Some((value as i64).to_le_bytes())
}

/// Wakes the sampler when someone starts or stops watching one of its topics.
struct Watch {
    watched: Rc<Cell<[Option<TopicId>; 2]>>,
    changed: Rc<Notify>,
}

impl Watch {
    fn tell(&self, topic: &Topic) {
        if self.watched.get().contains(&Some(topic.id)) {
            self.changed.notify_one();
        }
    }
}

impl TopicWatch for Watch {
    fn declared(&mut self, _topic: &Topic, _sessions: &mut Sessions) {}

    fn subscribed(&mut self, topic: &Topic, _session: SessionId, _sessions: &mut Sessions) {
        self.tell(topic);
    }

    fn left(&mut self, topic: &Topic, _session: SessionId, _sessions: &mut Sessions) {
        self.tell(topic);
    }
}

/// The comm core's own topics.
pub struct SelfStatus {
    topics: Rc<RefCell<Topics>>,
    readings: Rc<Cell<u64>>,
}

impl std::fmt::Debug for SelfStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SelfStatus({} readings)", self.readings.get())
    }
}

impl SelfStatus {
    /// Declares `orion.host` and `orion.comm` and starts the task that reads them, every
    /// `interval`, while they are watched. Must be called inside the comm core's `LocalSet`.
    ///
    /// # Errors
    ///
    /// If the topics can't be declared: someone has those names, or there is no room.
    pub fn start(
        comm: &Comm,
        broker: &Broker,
        mut host: impl HostSource + 'static,
        interval: Duration,
    ) -> Result<Self, Error> {
        let topics = broker.topics();
        let mut host_values = Values::new(
            declare(&topics, HOST_TOPIC, &HOST_FIELDS)?,
            HOST_FIELDS.len(),
        );
        let comm_fields = COMM_FIELDS.map(|name| (name, FieldType::Int, ""));
        let mut comm_values = Values::new(
            declare(&topics, COMM_TOPIC, &comm_fields)?,
            COMM_FIELDS.len(),
        );
        let changed = Rc::new(Notify::new());
        let watched = Rc::new(Cell::new([
            Some(host_values.publisher.id()),
            Some(comm_values.publisher.id()),
        ]));
        topics.borrow_mut().watch(Watch {
            watched,
            changed: changed.clone(),
        });
        let readings = Rc::new(Cell::new(0));
        let (comm, counted, table) = (comm.clone(), readings.clone(), topics.clone());
        tokio::task::spawn_local(async move {
            let mut idle = true;
            loop {
                let (host_watched, comm_watched) = (
                    host_values.publisher.subscribers() > 0,
                    comm_values.publisher.subscribers() > 0,
                );
                if !host_watched && !comm_watched {
                    // Nobody is watching: no reading and no timer until someone is.
                    idle = true;
                    changed.notified().await;
                    continue;
                }
                if idle {
                    // The values in the ring are from when it was last watched.
                    host_values.forget();
                    comm_values.forget();
                    idle = false;
                }
                if host_watched {
                    counted.set(counted.get() + 1);
                    let sample = host.sample();
                    host_values.publish(&[
                        int(sample.uptime_s),
                        int(sample.mem_total_kb),
                        int(sample.mem_available_kb),
                        sample.cpu_percent.map(f64::to_le_bytes),
                        sample.temperature_c.map(f64::to_le_bytes),
                    ]);
                }
                if comm_watched {
                    let stats = comm.stats();
                    let sessions = comm.with(|sessions, _| sessions.len());
                    let (count, bytes) = {
                        let topics = table.borrow();
                        (topics.len(), topics.bytes())
                    };
                    comm_values.publish(&[
                        int(u64::from(sessions)),
                        int(stats.opened),
                        int(stats.rejected),
                        int(stats.requests),
                        int(stats.refused),
                        int(stats.unknown),
                        int(stats.dropped),
                        int(stats.panics),
                        int(count as u64),
                        int(bytes as u64),
                    ]);
                }
                tokio::select! {
                    () = tokio::time::sleep(interval) => {}
                    () = changed.notified() => {}
                }
            }
        });
        Ok(Self { topics, readings })
    }

    /// How many times the host has been read.
    #[must_use]
    pub fn readings(&self) -> u64 {
        self.readings.get()
    }

    /// The state topic of one part of the comm core: `orion.state.<name>`.
    ///
    /// # Errors
    ///
    /// If the topic can't be declared.
    pub fn part(&self, name: &str) -> Result<Part, Error> {
        let fields = STATE_FIELDS.map(|field| (field, FieldType::Int, ""));
        let publisher = declare(&self.topics, &format!("{STATE_PREFIX}{name}"), &fields)?;
        Ok(Part(Values::new(publisher, STATE_FIELDS.len())))
    }
}

/// The number a state is published as.
#[must_use]
pub const fn state_code(state: State) -> u64 {
    match state {
        State::Starting => 0,
        State::Running => 1,
        State::Retrying => 2,
        State::Off => 3,
    }
}

/// Where one part of the comm core says how it is doing.
pub struct Part(Values);

impl std::fmt::Debug for Part {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Part")
    }
}

impl Part {
    /// Publishes `status`, or whatever of it changed. Call it whenever the part's state
    /// changes: it does nothing when nothing did.
    pub fn report(&mut self, status: &Status) {
        self.0.publish(&[
            int(state_code(status.state)),
            int(u64::from(status.failures)),
            int(u64::from(status.restarts)),
            int(status.retry_at_ms.unwrap_or(0)),
        ]);
    }
}
