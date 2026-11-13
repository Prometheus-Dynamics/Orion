//! Samples for subscribers that cannot map the ring.
//!
//! On the board a subscriber reads a topic's ring. A program on another board, or a browser,
//! cannot: it subscribes like anyone else, is told `shared_memory: false`, and is sent each
//! sample as a message. This is what sends them. For every such subscription it keeps a
//! reader on the topic's ring, inside the comm core, and turns what it reads into `Sample`
//! frames for that session. So there is still one model: the publisher writes a ring and
//! knows nothing of who reads it.
//!
//! A connection is slower than memory, and a subscriber may be slower still. The rules:
//!
//! - **Each subscriber has its own byte limit.** A sample is sent only if what already waits
//!   to be sent to that session, plus the sample, is under the limit. Otherwise it is
//!   dropped for that subscriber and counted.
//! - **After a drop, skip forward to the next independent sample.** A publisher marks a
//!   sample "others depend on this one": a key frame. Once a subscriber has lost a sample of
//!   a field that uses the mark, what follows is of no use to it until the next marked
//!   sample, and is not sent. A field that never uses the mark has only independent samples,
//!   and the next one is sent.
//! - **A slow reader hurts nobody else.** Nothing here waits. The publisher is never held
//!   up, the ring is never held, and the other subscribers are sent everything they have
//!   room for. A subscriber that falls so far behind that the ring overwrites what it had
//!   not been sent has lost those samples, like any reader.
//!
//! Nothing is polled. The publisher rings one doorbell for all such subscribers, as it would
//! ring a subscriber's own, and only when this is asleep.
//!
//! A topic declared with `on_subscribe: Latest` gives a subscriber that starts being carried
//! the newest sample of each field first, unless it has expired, each with its own time. One
//! that was carried already, and is carried again because the publisher came back, is not
//! sent them again.

#![cfg(target_os = "linux")]

use std::cell::RefCell;
use std::os::fd::AsFd;
use std::rc::Rc;
use std::time::Duration;

use orion_broker::{Broker, Topic, TopicId, TopicWatch, Topics};
use orion_config::{ConfigError, Source};
use orion_msg_topics::{Declare, OnSubscribe, Sample, SampleFlags};
use orion_server::{Comm, Delivery, Error, SessionId, Sessions};
use orion_wire::Bytes;
use orion_wire::Decode;
use shm_ring::{FieldId, Flags, Next, Reader, SampleTime};
use shm_ring_doorbell::Doorbell;
use tokio::io::Interest;
use tokio::net::UnixDatagram;
use tokio::task::JoinHandle;
use tracing::warn;

/// What a subscriber's connection is allowed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OutboundLimits {
    /// `outbound.max_bytes`: the most bytes that may wait to be sent to one subscriber. A
    /// sample that would take it over is dropped for that subscriber.
    pub max_bytes: usize,
    /// How many samples one subscription is sent before the next one has its turn.
    pub turn: usize,
}

impl Default for OutboundLimits {
    fn default() -> Self {
        Self {
            max_bytes: 128 * 1024,
            turn: 256,
        }
    }
}

impl OutboundLimits {
    /// Takes the limits out of `source`.
    ///
    /// # Errors
    ///
    /// A value that doesn't parse.
    pub fn from_source(source: &mut Source) -> Result<Self, ConfigError> {
        let default = Self::default();
        Ok(Self {
            max_bytes: source.get("outbound.max_bytes", default.max_bytes)?,
            ..default
        })
    }
}

/// What has been counted.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Counters {
    /// Samples sent.
    pub sent: u64,
    /// Samples not sent because the subscriber had no room, or because they depended on one
    /// that was not sent.
    pub dropped: u64,
    /// Samples the ring overwrote before they could be looked at.
    pub overrun: u64,
}

/// One subscription that is carried.
struct Carried {
    topic: TopicId,
    session: SessionId,
    reader: Reader,
    /// Which fields use the "others depend on this one" mark, and which of those are being
    /// skipped until their next marked sample. One bit a field, for the first 64.
    keyed: u64,
    skipping: u64,
}

struct State {
    limits: OutboundLimits,
    carried: Vec<Carried>,
    counters: Counters,
}

/// The outbound part of a comm core. Dropping it stops it.
pub struct Outbound {
    state: Rc<RefCell<State>>,
    task: JoinHandle<()>,
}

impl std::fmt::Debug for Outbound {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Outbound({} subscriptions)", self.subscriptions())
    }
}

impl Drop for Outbound {
    fn drop(&mut self) {
        self.task.abort();
    }
}

const fn bit(field: u16) -> u64 {
    if field < 64 { 1 << field } else { 0 }
}

/// Told by the broker the moment a subscription is made or a topic changes hands. A reader
/// sees what is published after it is made, so it has to be made then and not later.
struct Watching(Rc<RefCell<State>>);

impl TopicWatch for Watching {
    fn declared(&mut self, topic: &Topic, sessions: &mut Sessions) {
        // A new ring, or a publisher that is back: every subscriber without a ring reads
        // the ring there is now.
        let mut state = self.0.borrow_mut();
        let before: Vec<SessionId> = state
            .carried
            .iter()
            .filter(|carried| carried.topic == topic.id)
            .map(|carried| carried.session)
            .collect();
        state.carried.retain(|carried| carried.topic != topic.id);
        for subscription in topic.subscribers.iter().filter(|sub| !sub.shared) {
            let first = !before.contains(&subscription.session);
            state.carry(topic, subscription.session, first, sessions);
        }
    }

    fn subscribed(&mut self, topic: &Topic, session: SessionId, sessions: &mut Sessions) {
        let mut state = self.0.borrow_mut();
        state
            .carried
            .retain(|carried| !(carried.topic == topic.id && carried.session == session));
        let without_ring = topic
            .subscribers
            .iter()
            .any(|subscription| subscription.session == session && !subscription.shared);
        if without_ring {
            state.carry(topic, session, true, sessions);
        }
    }

    fn left(&mut self, topic: &Topic, session: SessionId, _sessions: &mut Sessions) {
        self.0
            .borrow_mut()
            .carried
            .retain(|carried| !(carried.topic == topic.id && carried.session == session));
    }
}

impl State {
    /// Starts reading `topic`'s ring for `session`. `first` if it was not carried before:
    /// then a topic that asks for it sends the newest of each field first.
    fn carry(&mut self, topic: &Topic, session: SessionId, first: bool, sessions: &mut Sessions) {
        let reader = topic
            .ring
            .as_ref()
            .and_then(|ring| ring.read_fd().ok())
            .and_then(|fd| Reader::attach(fd).ok());
        let Some(mut reader) = reader else {
            warn!("topic {} cannot be read for a subscriber", topic.name);
            return;
        };
        if first
            && let Ok(declare) = Declare::decode(&topic.declare)
            && declare.on_subscribe == OnSubscribe::Latest
        {
            let now = SampleTime::now();
            let expiry_ns = u64::from(declare.expiry_ms) * 1_000_000;
            for field in 0..declare.fields.len() {
                let Ok(field) = u16::try_from(field) else {
                    break;
                };
                let Some(sample) = reader.latest(FieldId(field)) else {
                    continue;
                };
                if expiry_ns != 0 && now.saturating_since(sample.time) > expiry_ns {
                    continue;
                }
                let room =
                    sessions.pending(session) + sample.payload.len() + 32 <= self.limits.max_bytes;
                if room && send(sessions, session, topic.id, &sample) == Ok(()) {
                    self.counters.sent += 1;
                } else {
                    self.counters.dropped += 1;
                }
            }
        }
        self.carried.push(Carried {
            topic: topic.id,
            session,
            reader,
            keyed: 0,
            skipping: 0,
        });
    }

    /// Sends every carried subscription what it has room for. Whether anything was read.
    fn turn(&mut self, sessions: &mut Sessions) -> bool {
        let mut read_any = false;
        // A session that ended is the broker's to tell of. Until it has, nothing is read
        // for it.
        self.carried
            .retain(|carried| sessions.get(carried.session).is_some());
        let (limits, counters) = (self.limits, &mut self.counters);
        for carried in &mut self.carried {
            for _ in 0..limits.turn {
                let sample = match carried.reader.next() {
                    Next::Empty => break,
                    Next::Skipped(lost) => {
                        // The ring went round before these were looked at. Whatever of them
                        // others depended on is lost with them.
                        read_any = true;
                        counters.overrun += lost;
                        carried.skipping = carried.keyed;
                        continue;
                    }
                    Next::Sample(sample) => sample,
                };
                read_any = true;
                let field = bit(sample.field.0);
                let key = sample.flags.has(Flags::OTHERS_DEPEND);
                if key {
                    carried.keyed |= field;
                }
                if carried.skipping & field != 0 && !key {
                    // It depends on one this subscriber never got.
                    counters.dropped += 1;
                    continue;
                }
                let room = sessions.pending(carried.session) + sample.payload.len() + 32
                    <= limits.max_bytes;
                let sent =
                    room && send(sessions, carried.session, carried.topic, &sample) == Ok(());
                if sent {
                    counters.sent += 1;
                    carried.skipping &= !field;
                } else {
                    counters.dropped += 1;
                    // What depends on it is of no use now: on to the next independent one.
                    carried.skipping |= carried.keyed & field;
                }
            }
        }
        read_any
    }
}

fn send(
    sessions: &mut Sessions,
    session: SessionId,
    topic: TopicId,
    sample: &shm_ring::Sample<'_>,
) -> Result<(), Error> {
    let frame = Sample {
        handle: topic.0,
        field: sample.field.0,
        time: sample.time,
        flags: SampleFlags(sample.flags.0),
        payload: Bytes(sample.payload),
    };
    // A sample is one of a stream: a subscriber is never closed for not taking one.
    sessions.send(session, &frame, Delivery::MayDrop)
}

impl Outbound {
    /// Starts carrying samples for `broker`'s subscribers that were not given a ring. Call
    /// it inside the comm core's task set, before the first such subscriber.
    ///
    /// # Errors
    ///
    /// If the doorbell cannot be made.
    pub fn start(comm: &Comm, broker: &Broker, limits: OutboundLimits) -> std::io::Result<Self> {
        Self::on(comm, &broker.topics(), limits)
    }

    /// As [`Outbound::start`], for whoever has the broker's topics and not the broker.
    ///
    /// # Errors
    ///
    /// If the doorbell cannot be made.
    pub fn on(
        comm: &Comm,
        topics: &Rc<RefCell<Topics>>,
        limits: OutboundLimits,
    ) -> std::io::Result<Self> {
        let failed = |error| std::io::Error::other(format!("doorbell: {error:?}"));
        let mut doorbell = Doorbell::create().map_err(failed)?;
        let (eventfd, page) = doorbell.bell_fds().map_err(failed)?;
        topics.borrow_mut().set_offboard_bell(eventfd, page);
        // Tokio waits on a copy of the eventfd. It is only ever asked whether it is readable.
        let waiter = UnixDatagram::from_std(std::os::unix::net::UnixDatagram::from(
            doorbell.as_fd().try_clone_to_owned()?,
        ))?;
        let state = Rc::new(RefCell::new(State {
            limits,
            carried: Vec::new(),
            counters: Counters::default(),
        }));
        topics.borrow_mut().watch(Watching(state.clone()));
        let (comm, running) = (comm.clone(), state.clone());
        let task = tokio::task::spawn_local(async move {
            loop {
                let busy = {
                    let mut state = running.borrow_mut();
                    comm.with(|sessions, _| state.turn(sessions))
                };
                if busy {
                    // More may be waiting. Let the sessions write what was queued first.
                    tokio::task::yield_now().await;
                    continue;
                }
                // About to sleep: say so, and look once more, so that a sample published in
                // between is not slept through.
                doorbell.arm();
                let again = {
                    let mut state = running.borrow_mut();
                    comm.with(|sessions, _| state.turn(sessions))
                };
                if !again {
                    // Also woken now and then: a subscriber that had no room may have some.
                    let _ = tokio::time::timeout(
                        Duration::from_millis(20),
                        waiter.ready(Interest::READABLE),
                    )
                    .await;
                    let _ = waiter.try_io(Interest::READABLE, || {
                        Err::<(), _>(std::io::Error::from(std::io::ErrorKind::WouldBlock))
                    });
                }
                doorbell.disarm();
            }
        });
        Ok(Self { state, task })
    }

    /// What has been counted.
    #[must_use]
    pub fn counters(&self) -> Counters {
        self.state.borrow().counters
    }

    /// How many subscriptions are carried.
    #[must_use]
    pub fn subscriptions(&self) -> usize {
        self.state.borrow().carried.len()
    }
}
