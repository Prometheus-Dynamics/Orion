//! The client's topics.
//!
//! A topic is a named value with a time on every sample. [`Topics::declare`] gives a
//! [`Publisher`], [`Topics::subscribe`] a [`Subscriber`], and the calls on both are the same
//! wherever the comm core is.
//!
//! On the board, a topic is a ring in shared memory. Publishing writes to it and rings the
//! subscribers that are asleep; reading reads it. The comm core set the two up and is not in
//! the path: no message, no system call for a subscriber that is awake, no allocation.
//!
//! Off the board, across the network or a link, a ring cannot be shared. The same publisher
//! sends each sample as a message and the comm core writes it to the ring; the same
//! subscriber keeps a ring of its own that arriving samples are written to. The program reads
//! a ring either way.
//!
//! Rings are shared on Linux alone, where the board is. On Windows and macOS the same calls go
//! over the network: samples are messages both ways, and a subscriber's own ring is on the
//! heap. [`Topics::adopt`] and the doorbell are Linux's.
//!
//! A subscriber may come before the topic exists: it waits and is given the ring when the
//! topic is declared. One whose publisher goes away keeps the ring and the last values, and
//! carries on when the publisher is back.

mod mirror;
mod platform;
mod state;

use std::cell::RefCell;
use std::fmt;
use std::rc::Rc;
use std::time::{Duration, Instant};

use orion_connect::{BorrowedFd, Client, ClientError};
use orion_msg_topics::{
    Declare, Field, OnSubscribe, Sample as SampleMessage, SampleFlags, Subscribe, TopicStatus,
    TopicWithdraw,
};
use orion_types::TopicRef;
use orion_wire::body::encode_fields;
use orion_wire::{Bytes, List, kind};
use shm_ring::{FieldId, Flags, Next, PublishError, RingError, Sample, SampleTime};
use shm_ring_time::Lookup;

pub use mirror::RingReader;
use platform::{copy, keep, lend};
use state::State;

/// Why a call on a topic failed.
#[derive(Debug)]
pub enum TopicError {
    /// The session: the connection broke, or the comm core refused.
    Client(ClientError),
    /// The sample cannot go in this topic: a field or a size outside its declaration.
    Publish(PublishError),
    /// A ring could not be made or mapped.
    Ring(RingError),
    /// The name or the declaration is not one.
    Invalid(String),
    /// The publisher withdrew the topic.
    Withdrawn,
}

impl fmt::Display for TopicError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Client(error) => write!(f, "{error}"),
            Self::Publish(error) => write!(f, "the sample does not fit the topic: {error:?}"),
            Self::Ring(error) => write!(f, "ring: {error:?}"),
            Self::Invalid(what) => f.write_str(what),
            Self::Withdrawn => f.write_str("the topic was withdrawn"),
        }
    }
}

impl std::error::Error for TopicError {}

impl From<ClientError> for TopicError {
    fn from(error: ClientError) -> Self {
        Self::Client(error)
    }
}

impl From<RingError> for TopicError {
    fn from(error: RingError) -> Self {
        Self::Ring(error)
    }
}

/// What a topic is: its name, its fields, and how much of each it keeps.
#[derive(Clone, Copy, Debug)]
pub struct TopicDecl<'a> {
    /// The topic's name.
    pub name: &'a str,
    /// Its fields. A sample belongs to one, by its place in this list.
    pub fields: &'a [Field<'a>],
    /// How many samples of each field are kept.
    pub history: u32,
    /// After how long a value counts as stale. Zero for never.
    pub expiry_ms: u32,
    /// The largest sample, in bytes.
    pub max_payload: u32,
    /// What a subscriber that is sent samples as messages gets first: what is published from
    /// now on, or the newest of each field before that.
    pub on_subscribe: OnSubscribe,
}

/// What a subscriber to a topic of shared buffers brings with it.
#[derive(Clone, Copy, Debug, Default)]
pub struct Holding<'a> {
    /// Its page of hold marks, which the publisher reads.
    pub marks: Option<BorrowedFd<'a>>,
    /// How many buffers it asks to be allowed to hold. Zero for no limit.
    pub max_held: u32,
    /// For how long, in milliseconds. Zero for no limit.
    pub max_hold_ms: u32,
}

/// Where a subscribed topic is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Status {
    /// Nobody has declared it yet.
    Waiting,
    /// A publisher is there.
    Live,
    /// The publisher is gone. What it published is still readable.
    PublisherGone,
}

/// The topics of one session.
#[derive(Clone)]
pub struct Topics {
    client: Client,
    state: Rc<RefCell<State>>,
}

impl fmt::Debug for Topics {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Topics")
    }
}

impl Topics {
    /// The topics of `client`'s session.
    #[must_use]
    pub fn new(client: &Client) -> Self {
        let state = Rc::new(RefCell::new(State::default()));
        client.add(state.clone());
        Self {
            client: client.clone(),
            state,
        }
    }

    /// Declares a topic and returns its publisher. The same declaration again, from the same
    /// program, carries on in the ring that is there.
    ///
    /// # Errors
    ///
    /// If the comm core refuses: another program publishes it, or there is no room.
    pub fn declare(&self, topic: &TopicDecl<'_>) -> Result<Publisher, TopicError> {
        self.declare_in(topic, None)
    }

    /// Declares a topic in a ring the program made and writes itself, so that programs that
    /// reach it through Orion read the same ring as those that reach it some other way. The
    /// ring must have the declaration's shape, or the comm core refuses it, `malformed`.
    ///
    /// The program keeps its own writer and publishes with it; [`Publisher::wake`] after each
    /// publish wakes Orion's subscribers that are asleep, with no system call for one that is
    /// awake. A comm core that restarts is handed the same ring back by [`Topics::restore`].
    ///
    /// # Errors
    ///
    /// If the session is not on the board, the ring's descriptor cannot be duplicated, or the
    /// comm core refuses, as [`Topics::declare`].
    #[cfg(target_os = "linux")]
    pub fn adopt(
        &self,
        topic: &TopicDecl<'_>,
        ring: &shm_ring::Ring,
    ) -> Result<Publisher, TopicError> {
        use std::os::fd::AsFd;
        if !self.client.session(|session| session.passes_descriptors()) {
            return Err(TopicError::Invalid(
                "a ring is shared only with programs on the board".into(),
            ));
        }
        let fd = ring.write_fd().map_err(TopicError::Ring)?;
        self.declare_in(topic, Some(fd.as_fd()))
    }

    /// Withdraws this session's topic `name`: its subscribers are told the publisher is gone,
    /// and it is not declared again after a reconnect. Its [`Publisher`], if still held, can
    /// no longer publish. `false` if this session publishes no topic of that name.
    ///
    /// # Errors
    ///
    /// If the comm core refuses, or the connection broke. Either way the topic is not
    /// declared again after a reconnect.
    pub fn withdraw(&self, name: &str) -> Result<bool, ClientError> {
        let Some(index) = self.state.borrow().publisher_named(name) else {
            return Ok(false);
        };
        withdraw(&self.client, &self.state, index)?;
        Ok(true)
    }

    /// Declares `topic`, in `ring` if the program brings its own.
    fn declare_in(
        &self,
        topic: &TopicDecl<'_>,
        ring: Option<BorrowedFd<'_>>,
    ) -> Result<Publisher, TopicError> {
        let declare = Declare {
            topic: topic
                .name
                .parse()
                .map_err(|error| TopicError::Invalid(format!("{:?}: {error}", topic.name)))?,
            fields: List::new(topic.fields),
            history: topic.history,
            expiry_ms: topic.expiry_ms,
            max_payload: topic.max_payload,
            on_subscribe: topic.on_subscribe,
        };
        let mut raw = vec![0_u8; 128 + topic.fields.len() * 96];
        let len = encode_fields(&declare, &mut raw)
            .map_err(|_| TopicError::Invalid("the declaration is too long".into()))?;
        raw.truncate(len);
        let seq = self
            .client
            .send_frame(kind::TOPIC_DECLARE, &raw, ring.as_slice())?;
        let index = self
            .state
            .borrow_mut()
            .add_publisher(seq, topic.name.to_owned(), raw);
        // The program writes its ring itself: the answer's descriptor is kept to hand back to
        // a comm core that restarts, and no second writer is made on it.
        self.state.borrow_mut().publisher_mut(index).taken = ring.is_some();
        let opened = self
            .client
            .answered(seq, || self.state.borrow().publisher_open(index));
        if let Err(error) = opened {
            self.state.borrow_mut().drop_publisher(index);
            return Err(error.into());
        }
        Ok(Publisher {
            client: self.client.clone(),
            state: self.state.clone(),
            index,
        })
    }

    /// Sends a subscribe for `topic`, with this program's doorbell and its marks if it is on
    /// the board, and returns the `seq` it went with.
    fn send_subscribe(
        &self,
        topic: &str,
        request: &[u8],
        holding: &Holding<'_>,
    ) -> Result<u16, TopicError> {
        let topic = TopicRef::parse(topic)
            .map_err(|error| TopicError::Invalid(format!("{topic:?}: {error}")))?;
        let subscribe = Subscribe {
            topic,
            request: Bytes(request),
            max_held: holding.max_held,
            max_hold_ms: holding.max_hold_ms,
        };
        if !self.client.session(|session| session.passes_descriptors()) {
            return Ok(self.client.send(&subscribe)?);
        }
        self.send_subscribe_on_board(&subscribe, holding)
    }

    /// Sends `subscribe` on the board's own socket, where no descriptor travels.
    #[cfg(not(target_os = "linux"))]
    fn send_subscribe_on_board(
        &self,
        subscribe: &Subscribe<'_>,
        _holding: &Holding<'_>,
    ) -> Result<u16, TopicError> {
        Ok(self.client.send(subscribe)?)
    }

    /// Sends `subscribe` on the board's own socket, with this program's doorbell and its marks.
    #[cfg(target_os = "linux")]
    fn send_subscribe_on_board(
        &self,
        subscribe: &Subscribe<'_>,
        holding: &Holding<'_>,
    ) -> Result<u16, TopicError> {
        use shm_ring_doorbell::Doorbell;
        use std::os::fd::AsFd;
        // One doorbell for everything this program subscribes to.
        let (eventfd, page) = {
            let mut state = self.state.borrow_mut();
            if state.doorbell.is_none() {
                state.doorbell = Some(Doorbell::create()?);
            }
            state
                .doorbell
                .as_ref()
                .map(Doorbell::bell_fds)
                .transpose()?
                .ok_or(TopicError::Invalid("no doorbell".into()))?
        };
        Ok(match holding.marks {
            Some(marks) => self
                .client
                .send_with_fds(subscribe, &[eventfd.as_fd(), page.as_fd(), marks])?,
            None => self
                .client
                .send_with_fds(subscribe, &[eventfd.as_fd(), page.as_fd()])?,
        })
    }

    /// Says everything again to a comm core that has no memory of this session: every
    /// declaration, with its ring handed back, and every subscription. Call it after
    /// [`orion_connect::Session::reconnect`].
    ///
    /// A publisher on the board kept its ring and kept writing it while the comm core was
    /// away. The comm core takes that same ring back, so subscribers that kept their end of
    /// it missed nothing, and nothing that was flowing stopped.
    ///
    /// # Errors
    ///
    /// If the comm core refuses something it took before, or the connection broke again.
    pub fn restore(&self) -> Result<(), TopicError> {
        let shares = self.client.session(|session| session.passes_descriptors());
        // Publishers first: a ring is handed back before anyone asks to read it.
        let publishers = self.state.borrow().publishers_to_restore();
        for (index, declaration, ring) in publishers {
            let seq = match ring.as_ref().filter(|_| shares) {
                #[cfg(unix)]
                Some(ring) => self.client.send_frame(
                    kind::TOPIC_DECLARE,
                    &declaration,
                    &[std::os::fd::AsFd::as_fd(ring)],
                )?,
                #[cfg(not(unix))]
                Some(_) => unreachable!("no descriptor exists here"),
                None => self
                    .client
                    .send_frame(kind::TOPIC_DECLARE, &declaration, &[])?,
            };
            self.state.borrow_mut().redeclared(index, seq);
            self.client
                .answered(seq, || self.state.borrow().publisher_open(index))?;
        }
        let subscribers = self.state.borrow().subscribers_to_restore();
        for index in subscribers {
            let (topic, request, max_held, max_hold_ms, marks) = {
                let state = self.state.borrow();
                let asked = &state.subscriber(index).asked;
                let marks = asked.marks.as_ref().and_then(copy);
                (
                    asked.topic.clone(),
                    asked.request.clone(),
                    asked.max_held,
                    asked.max_hold_ms,
                    marks,
                )
            };
            let holding = Holding {
                marks: lend(marks.as_ref()),
                max_held,
                max_hold_ms,
            };
            let seq = self.send_subscribe(&topic, &request, &holding)?;
            self.state.borrow_mut().resubscribed(index, seq);
            self.client
                .answered(seq, || self.state.borrow().subscriber_answered(index))?;
        }
        Ok(())
    }

    /// Subscribes to a topic on this board, by name. `request` goes to the publisher unread:
    /// what this subscriber would like of it.
    ///
    /// # Errors
    ///
    /// If the comm core refuses. A topic nobody has declared is not an error: the subscriber
    /// waits for it.
    pub fn subscribe(&self, topic: &str, request: &[u8]) -> Result<Subscriber, TopicError> {
        self.subscribe_holding(topic, request, &Holding::default())
    }

    /// As [`Topics::subscribe`], for a topic whose samples name shared buffers: with the
    /// subscriber's page of hold marks and the limits it asks to be held to.
    ///
    /// # Errors
    ///
    /// As [`Topics::subscribe`].
    pub fn subscribe_holding(
        &self,
        topic: &str,
        request: &[u8],
        holding: &Holding<'_>,
    ) -> Result<Subscriber, TopicError> {
        let seq = self.send_subscribe(topic, request, holding)?;
        let topic_name = topic.to_owned();
        let asked = state::Asked {
            topic: topic_name,
            request: request.to_vec(),
            max_held: holding.max_held,
            max_hold_ms: holding.max_hold_ms,
            marks: holding.marks.and_then(keep),
        };
        let index = self.state.borrow_mut().add_subscriber(seq, asked);
        let answered = self
            .client
            .answered(seq, || self.state.borrow().subscriber_answered(index));
        if let Err(error) = answered {
            self.state.borrow_mut().drop_subscriber(index);
            return Err(error.into());
        }
        Ok(Subscriber {
            client: self.client.clone(),
            state: self.state.clone(),
            index,
            reader: None,
        })
    }
}

/// Withdraws the publisher at `index`.
fn withdraw(client: &Client, state: &Rc<RefCell<State>>, index: usize) -> Result<(), ClientError> {
    let (handle, open) = {
        let mut state = state.borrow_mut();
        let publisher = state.publisher_mut(index);
        if publisher.withdrawn {
            return Ok(());
        }
        publisher.withdrawn = true;
        publisher.writer = None;
        publisher.bells.clear();
        publisher.subscribers = 0;
        (publisher.handle, std::mem::take(&mut publisher.open))
    };
    // Not declared on this connection yet, after a reconnect: not declaring it is enough.
    if !open {
        return Ok(());
    }
    let seq = client.send(&TopicWithdraw { handle })?;
    state.borrow_mut().publisher_mut(index).withdrawing = Some(seq);
    let answered = client.answered(seq, || {
        state.borrow().publisher(index).withdrawing.is_none()
    });
    state.borrow_mut().publisher_mut(index).withdrawing = None;
    answered
}

/// The publisher of one topic.
pub struct Publisher {
    client: Client,
    state: Rc<RefCell<State>>,
    index: usize,
}

impl fmt::Debug for Publisher {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Publisher({})", self.index)
    }
}

impl Publisher {
    /// Publishes one sample of `field`, belonging to `time`.
    ///
    /// On the board this is a write to shared memory and, for each subscriber that is asleep,
    /// one wake. It makes no system call when they are awake and allocates nothing.
    ///
    /// # Errors
    ///
    /// If the sample does not fit the declaration, or, off the board, the connection broke.
    pub fn publish(
        &mut self,
        field: FieldId,
        time: SampleTime,
        payload: &[u8],
    ) -> Result<(), TopicError> {
        self.publish_flagged(field, time, Flags::NONE, payload)
    }

    /// As [`Publisher::publish`], with flags on the sample.
    ///
    /// # Errors
    ///
    /// As [`Publisher::publish`].
    pub fn publish_flagged(
        &mut self,
        field: FieldId,
        time: SampleTime,
        flags: Flags,
        payload: &[u8],
    ) -> Result<(), TopicError> {
        let handle = {
            let mut state = self.state.borrow_mut();
            let publisher = state.publisher_mut(self.index);
            if publisher.withdrawn {
                return Err(TopicError::Withdrawn);
            }
            if let Some(writer) = &mut publisher.writer {
                writer
                    .publish(field, time, flags, payload)
                    .map_err(TopicError::Publish)?;
                for (_, bell) in &mut publisher.bells {
                    bell.ring();
                }
                return Ok(());
            }
            publisher.handle
        };
        // No ring to write: the comm core writes it for this program.
        let sample = SampleMessage {
            handle,
            field: field.0,
            time,
            flags: SampleFlags(flags.0),
            payload: Bytes(payload),
        };
        self.client.send(&sample)?;
        Ok(())
    }

    /// Withdraws the topic: its subscribers are told the publisher is gone, and it is not
    /// declared again after a reconnect. Another program may then declare it.
    ///
    /// # Errors
    ///
    /// If the comm core refuses, or the connection broke. Either way the topic is not
    /// declared again after a reconnect.
    pub fn withdraw(self) -> Result<(), ClientError> {
        withdraw(&self.client, &self.state, self.index)
    }

    /// Whether the topic was withdrawn, by [`Publisher::withdraw`] or [`Topics::withdraw`].
    #[must_use]
    pub fn withdrawn(&self) -> bool {
        self.state.borrow().publisher(self.index).withdrawn
    }

    /// The topic's number on this session.
    #[must_use]
    pub fn handle(&self) -> u32 {
        self.state.borrow().publisher(self.index).handle
    }

    /// Takes the ring's writer out, for a publisher that writes the ring some other way than
    /// [`Publisher::publish`]: a topic of shared buffers. `None` off the board, where there
    /// is no ring to write. Afterwards [`Publisher::wake`] is how subscribers are woken.
    pub fn take_writer(&mut self) -> Option<shm_ring::Writer> {
        let mut state = self.state.borrow_mut();
        let publisher = state.publisher_mut(self.index);
        publisher.taken = true;
        publisher.writer.take()
    }

    /// Rings every subscriber that is asleep. No system call for one that is awake.
    pub fn wake(&mut self) {
        for (_, bell) in &mut self.state.borrow_mut().publisher_mut(self.index).bells {
            bell.ring();
        }
    }

    /// How many subscribers the topic has. A publisher with none may as well not sample.
    #[must_use]
    pub fn subscribers(&self) -> u32 {
        let state = self.state.borrow();
        let publisher = state.publisher(self.index);
        match &publisher.writer {
            Some(writer) => writer.subscribers(),
            None => publisher.subscribers,
        }
    }

    /// What the newest subscriber asked for, as it sent it.
    pub fn with_request<R>(&self, with: impl FnOnce(&[u8]) -> R) -> R {
        with(&self.state.borrow().publisher(self.index).request)
    }

    /// Looks at what the comm core has sent: subscribers that came or went. A publisher that
    /// only publishes should call this now and then; it does not wait.
    ///
    /// # Errors
    ///
    /// If the connection broke.
    pub fn poll(&mut self) -> Result<(), TopicError> {
        Ok(self.client.drain()?)
    }
}

/// A subscriber to one topic.
pub struct Subscriber {
    client: Client,
    state: Rc<RefCell<State>>,
    index: usize,
    reader: Option<RingReader>,
}

impl fmt::Debug for Subscriber {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Subscriber({})", self.index)
    }
}

impl Subscriber {
    /// Takes the ring the comm core sent, if it sent one: the first, or another after the
    /// topic was declared differently.
    fn refresh(&mut self) {
        if let Some(reader) = self
            .state
            .borrow_mut()
            .subscriber_mut(self.index)
            .arrived
            .take()
        {
            self.reader = Some(reader);
        }
    }

    /// The topic's number on this session, once the comm core has given it one.
    #[must_use]
    pub fn handle(&self) -> Option<u32> {
        self.state.borrow().subscriber(self.index).handle()
    }

    /// Takes the ring's reader out, for a subscriber that reads the ring some other way than
    /// [`Subscriber::next`]: a topic of shared buffers. `None` until the topic exists. A
    /// reader that can be taken again later is a new ring: the topic was declared
    /// differently.
    pub fn take_reader(&mut self) -> Option<RingReader> {
        self.refresh();
        self.reader.take()
    }

    /// Where the topic is.
    #[must_use]
    pub fn status(&self) -> Status {
        match self.state.borrow().subscriber(self.index).status {
            TopicStatus::Live => Status::Live,
            TopicStatus::PublisherGone => Status::PublisherGone,
            _ => Status::Waiting,
        }
    }

    /// The topic's declaration, as its publisher made it, once the topic exists.
    pub fn with_declaration<R>(&self, with: impl FnOnce(Option<&[u8]>) -> R) -> R {
        let state = self.state.borrow();
        let declaration = &state.subscriber(self.index).declaration;
        with((!declaration.is_empty()).then_some(declaration.as_slice()))
    }

    /// The next sample in the order they were published, or how many were lost on the way to
    /// it. A memory read: no system call, no allocation.
    // Not an iterator: a sample borrows the subscriber's copy of it.
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Next<'_> {
        self.refresh();
        match &mut self.reader {
            Some(reader) => reader.next(),
            None => Next::Empty,
        }
    }

    /// The newest sample of `field`.
    pub fn latest(&mut self, field: FieldId) -> Option<Sample<'_>> {
        self.refresh();
        self.reader.as_mut()?.latest(field)
    }

    /// The sample of `field` that belongs to `time`: the newest one at or before it.
    pub fn at_or_before(&mut self, field: FieldId, time: SampleTime) -> Lookup<'_> {
        self.refresh();
        match &mut self.reader {
            Some(reader) => shm_ring_time::at_or_before(reader, field, time),
            None => Lookup::Missing,
        }
    }

    /// How many samples have been lost to this subscriber for reading too slowly.
    #[must_use]
    pub fn missed(&self) -> u64 {
        self.reader.as_ref().map_or(0, RingReader::missed)
    }

    /// Whether there is something [`Subscriber::next`] has not returned yet.
    fn pending(&mut self) -> bool {
        self.refresh();
        self.reader
            .as_ref()
            .is_some_and(|reader| reader.pending() > 0)
    }

    /// Sleeps until there is something to read, for up to `wait`. `true` if there is.
    ///
    /// While it sleeps the session is kept: pings are answered, and what the comm core sends
    /// about this program's topics is taken in. Nothing is polled on a timer: the program
    /// sleeps on its doorbell and its session, and wakes when either has something.
    ///
    /// # Errors
    ///
    /// If the connection broke.
    pub fn wait(&mut self, wait: Duration) -> Result<bool, TopicError> {
        self.wait_until(wait, Self::pending)
    }

    /// As [`Subscriber::wait`], for a caller that reads the ring itself: sleeps until `ready`
    /// says there is something, asking it again after the doorbell is armed.
    ///
    /// # Errors
    ///
    /// If the connection broke.
    pub fn wait_until(
        &mut self,
        wait: Duration,
        mut ready: impl FnMut(&mut Self) -> bool,
    ) -> Result<bool, TopicError> {
        let deadline = Instant::now() + wait;
        loop {
            self.client.drain()?;
            if ready(self) {
                return Ok(true);
            }
            // Say "about to sleep", then look once more: a sample published in between is
            // either seen here or rings the doorbell.
            self.arm(true);
            let found = ready(self);
            let now = Instant::now();
            if !found && now < deadline {
                let heartbeat = self.client.session(|session| session.welcomed().heartbeat);
                let sleep = deadline.saturating_duration_since(now).min(heartbeat / 2);
                self.sleep(sleep)?;
            }
            self.arm(false);
            if found {
                return Ok(true);
            }
            if now >= deadline {
                return Ok(false);
            }
        }
    }
}
