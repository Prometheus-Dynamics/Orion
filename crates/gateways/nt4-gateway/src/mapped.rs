//! The mapping at work, whichever end of NT4 the gateway is: reading mapped fields for NT4
//! and publishing what comes back. The server form and the client form differ only in how
//! a value is set on the NT4 side, which is [`Far`].

use std::cell::RefCell;
use std::rc::Rc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use orion_broker::{LocalPublisher, TopicId, Topics};
use orion_msg_topics::{Declare, FieldType};
use orion_ring::{FieldId, Reader, SampleTime};
use orion_wire::Decode;
use tracing::warn;

use crate::convert;
use crate::mapping::{Entry, field_type};

/// The NT4 side: a server's table, or a client's publications on a remote server.
pub trait Far {
    /// Sets the NT4 topic of `entry`, which is number `at` of the fields that go to NT4.
    /// `first` if the topic is not announced: it has no value yet, or was withdrawn.
    /// Whether the value was taken.
    fn set(&mut self, at: usize, entry: &Entry, value: orion_nt4::Value, first: bool) -> bool;

    /// Withdraws the NT4 topic of `entry`: its value expired.
    fn withdraw(&mut self, at: usize, entry: &Entry);
}

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
    fn turn(&mut self, at: usize, topics: &Topics, far: &mut impl Far, counters: &mut Counters) {
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
                        if far.set(at, &self.entry, value, !self.live) {
                            self.live = true;
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
            far.withdraw(at, &self.entry);
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

/// The mapping at work: the fields that go to NT4 and the topics that come from it.
pub struct Mapped {
    topics: Rc<RefCell<Topics>>,
    out: Vec<Out>,
    accept: Vec<In>,
    publishers: Vec<LocalPublisher>,
    payload: Vec<u8>,
    max_payload: usize,
    counters: Counters,
}

impl std::fmt::Debug for Mapped {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Mapped({} out, {} in)",
            self.out.len(),
            self.accept.len()
        )
    }
}

fn now() -> SampleTime {
    let since = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    SampleTime::from_nanos(since.as_nanos() as u64)
}

impl Mapped {
    /// Declares the topics that come from NT4, each with the fields mapped to it, and gets
    /// ready to read the ones that go there.
    ///
    /// # Errors
    ///
    /// If a topic that comes from NT4 cannot be declared: its name is taken.
    pub fn new(
        topics: &Rc<RefCell<Topics>>,
        publish: Vec<Entry>,
        accepted: &[Entry],
        max_payload: u32,
    ) -> std::io::Result<Self> {
        let max_payload = max_payload.max(8);
        let mut publishers = Vec::new();
        let mut accept: Vec<In> = Vec::new();
        let mut names: Vec<&orion_types::TopicName> = Vec::new();
        for entry in accepted {
            if !names.contains(&&entry.topic) {
                names.push(&entry.topic);
            }
        }
        for name in names {
            let mut fields = Vec::new();
            for entry in accepted.iter().filter(|entry| &entry.topic == name) {
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
            let publisher = LocalPublisher::declare(topics, name.as_str(), &fields, 8, max_payload)
                .map_err(|error| std::io::Error::other(format!("nt4.accept {name}: {error:?}")))?;
            publishers.push(publisher);
        }
        Ok(Self {
            topics: topics.clone(),
            out: publish.into_iter().map(Out::new).collect(),
            accept,
            publishers,
            payload: Vec::new(),
            max_payload: max_payload as usize,
            counters: Counters::default(),
        })
    }

    /// One period: every field that goes to NT4 is looked at, set on `far` if it has a
    /// newer sample, and withdrawn if its value has expired.
    pub fn turn(&mut self, far: &mut impl Far) {
        let topics = self.topics.borrow();
        for (at, field) in self.out.iter_mut().enumerate() {
            field.turn(at, &topics, far, &mut self.counters);
        }
    }

    /// The NT4 names whose values are published here.
    pub fn accepted(&self) -> impl Iterator<Item = &str> {
        self.accept
            .iter()
            .map(|accepted| accepted.entry.nt_name.as_str())
    }

    /// What has been counted.
    #[must_use]
    pub const fn counters(&self) -> Counters {
        self.counters
    }

    /// Publishes here what was written to the NT4 topic `name`, if `name` is accepted.
    pub fn take(&mut self, name: &str, value: &orion_nt4::Value) {
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
}
