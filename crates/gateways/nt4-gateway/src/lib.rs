//! The NT4 gateway: topics to NetworkTables 4 and back.
//!
//! It runs inside the comm core as an NT4 server. Programs that speak NT4 connect to it and
//! see the topics the configuration names, under the names and types the configuration
//! gives them; what they write to the NT4 topics the configuration accepts is published
//! here as topics of the comm core's own.
//!
//! - **The mapping is configuration.** `nt4.publish` and `nt4.accept` are lists of
//!   `topic:field=type@name`. Nothing here knows what a name means or who reads it.
//! - **Latest value, at a period.** NT4 carries the latest value of a topic, not a stream.
//!   Every `nt4.period_ms` the gateway looks at each mapped field and, if it has a newer
//!   sample, sets the NT4 topic to it. A sample replaced within one period is never sent.
//!   Nothing is read by a publisher's leave: the gateway is a reader of the ring like any
//!   other, and a publisher pays nothing for it.
//! - **A value that expires is withdrawn.** A topic declared with an expiry has no value
//!   once its newest sample is older than that. The gateway then deletes the NT4 topic, so
//!   that NT4 clients do not go on reading a value nobody stands behind, and announces it
//!   again with the next sample. Age is counted from when the gateway read the sample, on
//!   its own clock. A topic without an expiry keeps its last value in NT4, as its ring
//!   keeps it here.
//! - **What comes back is the comm core's own.** For each topic named in `nt4.accept` the
//!   gateway declares a topic with the mapped fields and publishes what NT4 clients write.
//!   A value of another type than the field's is dropped, and said once.

#![cfg(target_os = "linux")]

pub mod convert;
mod mapping;

use std::cell::RefCell;
use std::net::SocketAddr;
use std::rc::Rc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use orion_broker::{LocalPublisher, TopicId, Topics};
use orion_msg_topics::{Declare, FieldType};
use orion_nt4::{Properties, Server, ServerConfig, ServerEvent, ServerHandle};
use orion_ring::{FieldId, FrameTime, Reader};
use orion_wire::Decode;
use tokio::sync::broadcast::error::RecvError;
use tokio::task::JoinHandle;
use tracing::{debug, warn};

pub use mapping::{Entry, Nt4Config, field_type};

/// What the gateway has done.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Counters {
    /// Values set in NT4 from topics here.
    pub sent: u64,
    /// Values from NT4 clients published here.
    pub taken: u64,
    /// NT4 topics withdrawn because their value expired.
    pub expired: u64,
    /// Values dropped because the types did not go together or the value did not fit.
    pub refused: u64,
}

/// One field that goes to NT4.
struct Out {
    entry: Entry,
    /// The topic, once it has been seen, and its epoch then.
    topic: Option<(TopicId, u64)>,
    reader: Option<Reader>,
    /// The field's place and type in the declaration the reader is for.
    field: Option<(FieldId, FieldType)>,
    /// How long a value stands. Zero for ever.
    expiry: Duration,
    /// How many samples the field had when it was last read. `u64::MAX` before the first.
    seen: u64,
    seen_at: Instant,
    /// Whether the NT4 topic is announced.
    live: bool,
    /// Whether a refusal has been said.
    said: bool,
}

impl Out {
    fn new(entry: Entry) -> Self {
        Self {
            entry,
            topic: None,
            reader: None,
            field: None,
            expiry: Duration::ZERO,
            seen: u64::MAX,
            seen_at: Instant::now(),
            live: false,
            said: false,
        }
    }

    /// Finds the topic and, if it is not the one last read, starts reading it.
    fn attach(&mut self, topics: &Topics) {
        let known = self
            .topic
            .and_then(|(id, _)| topics.get(id))
            .filter(|topic| topic.node.is_none() && topic.name == self.entry.topic);
        let topic = match known {
            Some(topic) => topic,
            None => match topics.find(&self.entry.topic).and_then(|id| topics.get(id)) {
                Some(topic) => topic,
                None => {
                    (self.topic, self.reader, self.field) = (None, None, None);
                    return;
                }
            },
        };
        if self.topic == Some((topic.id, topic.epoch)) {
            return;
        }
        self.topic = Some((topic.id, topic.epoch));
        (self.reader, self.field) = (None, None);
        let (Some(ring), Ok(declared)) = (&topic.ring, Declare::decode(&topic.declare)) else {
            return;
        };
        let found = declared
            .fields
            .iter()
            .map_while(Result::ok)
            .enumerate()
            .find(|(_, field)| field.name == self.entry.field);
        let Some((at, field)) = found else {
            let what = format!("has no field {}", self.entry.field);
            self.say(&what);
            return;
        };
        self.field = Some((FieldId(at as u16), field.ty));
        self.expiry = Duration::from_millis(u64::from(declared.expiry_ms));
        self.reader = ring.read_fd().ok().and_then(|fd| Reader::attach(fd).ok());
        // Another ring counts its samples from nothing again.
        self.seen = u64::MAX;
    }

    fn say(&mut self, what: &str) {
        if !std::mem::replace(&mut self.said, true) {
            warn!(
                "nt4: {} {what}: {} is not sent",
                self.entry.topic, self.entry.nt_name
            );
        }
    }

    /// Sets the NT4 topic to the field's newest sample if it has a newer one, and withdraws
    /// it if its value has expired.
    fn turn(&mut self, topics: &Topics, host: &ServerHandle, counters: &mut Counters) {
        self.attach(topics);
        let now = Instant::now();
        if let (Some(reader), Some((field, ty))) = (&mut self.reader, self.field) {
            let head = reader.lane_head(field);
            if head != 0 && head != self.seen {
                self.seen = head;
                self.seen_at = now;
                let value = reader
                    .latest(field)
                    .and_then(|sample| convert::to_nt4(ty, sample.payload, &self.entry.nt_type));
                match value {
                    Some(value) => {
                        if !self.live {
                            let made = host.publish(
                                &self.entry.nt_name,
                                &self.entry.nt_type,
                                Properties::new(),
                            );
                            self.live = made.is_ok();
                        }
                        if host.set_value(&self.entry.nt_name, value).is_ok() {
                            counters.sent += 1;
                        } else {
                            counters.refused += 1;
                        }
                    }
                    None => {
                        counters.refused += 1;
                        let what = format!(
                            "field {} ({ty:?}) is not a {}",
                            self.entry.field, self.entry.nt_type
                        );
                        self.say(&what);
                    }
                }
            }
        }
        if self.live && !self.expiry.is_zero() && now.duration_since(self.seen_at) > self.expiry {
            let _ = host.delete(&self.entry.nt_name);
            self.live = false;
            counters.expired += 1;
        }
    }
}

/// One NT4 topic whose values are published here.
struct In {
    entry: Entry,
    /// Which of the gateway's publishers, and the field in it.
    publisher: usize,
    field: FieldId,
    ty: FieldType,
    said: bool,
}

struct State {
    topics: Rc<RefCell<Topics>>,
    host: ServerHandle,
    out: Vec<Out>,
    accept: Vec<In>,
    publishers: Vec<LocalPublisher>,
    payload: Vec<u8>,
    max_payload: usize,
    counters: Counters,
}

fn now() -> FrameTime {
    let since = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    FrameTime::from_nanos(since.as_nanos() as u64)
}

impl State {
    /// Publishes here what an NT4 client wrote to `name`, if `name` is accepted.
    fn take(&mut self, name: &str, value: &orion_nt4::Value) {
        let Some(accepted) = self
            .accept
            .iter_mut()
            .find(|accepted| accepted.entry.nt_name == name)
        else {
            return;
        };
        let fits = convert::from_nt4(value, accepted.ty, &mut self.payload).is_some()
            && self.payload.len() <= self.max_payload;
        let published = fits
            && self.publishers[accepted.publisher]
                .publish(now(), &[(accepted.field, &self.payload)])
                .is_ok();
        if published {
            self.counters.taken += 1;
            return;
        }
        self.counters.refused += 1;
        if !std::mem::replace(&mut accepted.said, true) {
            warn!(
                "nt4: a value written to {name} is not a {:?} of at most {} bytes: not published as {}",
                accepted.ty, self.max_payload, accepted.entry.topic
            );
        }
    }

    /// Publishes what every accepted NT4 topic holds now: for after events were missed.
    fn take_all(&mut self) {
        for at in 0..self.accept.len() {
            let name = self.accept[at].entry.nt_name.clone();
            // Only what a client wrote: the gateway writes none of these itself.
            if let Some(value) = self.host.value(&name) {
                self.take(&name, &value);
            }
        }
    }
}

/// The gateway. Dropping it stops it and closes its NT4 server.
pub struct Gateway {
    state: Rc<RefCell<State>>,
    local_addr: SocketAddr,
    task: JoinHandle<()>,
    /// Kept so that it serves for as long as the gateway lives.
    _server: Server,
}

impl std::fmt::Debug for Gateway {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Gateway({})", self.local_addr)
    }
}

impl Drop for Gateway {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Gateway {
    /// Starts the NT4 server and the mapping. Call it inside the comm core's task set.
    ///
    /// # Errors
    ///
    /// If the address cannot be listened on, or a topic that comes from NT4 cannot be
    /// declared: its name is taken.
    pub async fn start(topics: &Rc<RefCell<Topics>>, config: Nt4Config) -> std::io::Result<Self> {
        config.check().map_err(std::io::Error::other)?;
        // The topics that come from NT4, each with the fields mapped to it.
        let mut publishers = Vec::new();
        let mut accept: Vec<In> = Vec::new();
        let mut names: Vec<&orion_types::TopicName> = Vec::new();
        for entry in &config.accept {
            if !names.contains(&&entry.topic) {
                names.push(&entry.topic);
            }
        }
        for name in names {
            let mut fields = Vec::new();
            for entry in config.accept.iter().filter(|entry| &entry.topic == name) {
                let ty = field_type(&entry.nt_type).ok_or_else(|| {
                    std::io::Error::other(format!("{} has no type here", entry.nt_type))
                })?;
                accept.push(In {
                    entry: entry.clone(),
                    publisher: publishers.len(),
                    field: FieldId(fields.len() as u16),
                    ty,
                    said: false,
                });
                fields.push((entry.field.as_str(), ty, ""));
            }
            let publisher = LocalPublisher::declare(
                topics,
                name.as_str(),
                &fields,
                8,
                config.max_payload.max(8),
            )
            .map_err(|error| std::io::Error::other(format!("nt4.accept {name}: {error:?}")))?;
            publishers.push(publisher);
        }

        let server = Server::start(ServerConfig {
            bind: config.listen,
            ..ServerConfig::default()
        })
        .await
        .map_err(std::io::Error::other)?;
        let local_addr = server.local_addr();
        let host = server.handle();
        let mut events = host.events();
        let state = Rc::new(RefCell::new(State {
            topics: topics.clone(),
            host,
            out: config.publish.into_iter().map(Out::new).collect(),
            accept,
            publishers,
            payload: Vec::new(),
            max_payload: config.max_payload.max(8) as usize,
            counters: Counters::default(),
        }));
        let running = state.clone();
        let task = tokio::task::spawn_local(async move {
            let mut period = tokio::time::interval(config.period);
            period.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    _ = period.tick() => {
                        let mut state = running.borrow_mut();
                        let State { topics, host, out, counters, .. } = &mut *state;
                        let topics = topics.borrow();
                        for field in out.iter_mut() {
                            field.turn(&topics, host, counters);
                        }
                    }
                    event = events.recv() => match event {
                        Ok(ServerEvent::ValueChanged { name, value, client: Some(_), .. }) => {
                            running.borrow_mut().take(&name, &value);
                        }
                        Ok(_) => {}
                        // Events were missed: what the topics hold now is what counts.
                        Err(RecvError::Lagged(missed)) => {
                            debug!("nt4: {missed} events missed, reading every accepted topic");
                            running.borrow_mut().take_all();
                        }
                        Err(RecvError::Closed) => break,
                    },
                }
            }
        });
        Ok(Self {
            state,
            local_addr,
            task,
            _server: server,
        })
    }

    /// Where NT4 clients connect.
    #[must_use]
    pub const fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// What has been counted.
    #[must_use]
    pub fn counters(&self) -> Counters {
        self.state.borrow().counters
    }
}
