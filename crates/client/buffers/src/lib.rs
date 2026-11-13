//! The client's buffer topics.
//!
//! A frame is too large to copy into a ring. A buffer topic's samples only name buffers, and
//! the buffers are shared memory the publisher hands over once. [`Buffers::declare`] gives a
//! [`BufferPublisher`] over a pool of buffers; [`Buffers::subscribe`] gives a
//! [`BufferSubscriber`] that maps the same memory.
//!
//! - The publisher takes a free buffer, fills it, and publishes its number. Nothing is
//!   copied, by it or by anyone after it.
//! - A subscriber that wants to keep a frame holds it: [`Held`] is the frame's buffers, kept
//!   out of the publisher's hands until it is dropped. The publisher sees who holds what and
//!   never reuses a held buffer.
//! - A subscriber that holds too many, or one for too long, is cut off by the publisher, so a
//!   stuck reader cannot stop the camera.
//!
//! Buffer topics are between programs on one board: there is no memory to share across a
//! network. The comm core sets the two sides up and then sees no frame.

#![cfg(target_os = "linux")]

use std::cell::RefCell;
use std::fmt;
use std::os::fd::{AsFd, OwnedFd};
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, Instant};

use orion_connect::{Client, ClientError, Cx, Handler, Incoming};
use orion_msg_topics::{BufferInfo, BufferOffer, Field, FieldType, Holds};
use orion_topics::{Holding, Publisher, Status, Subscriber, TopicDecl, TopicError, Topics};
use orion_wire::{List, kind};
use shm_ring::{FieldId, SampleTime};
use shm_ring_buffers::{BufferReader, BufferWriter};
use shm_ring_holds::Limits;

mod adopt;

pub use adopt::AdoptedBuffers;
pub use shm_ring_buffers::{
    Batch, BufferDesc, BufferError, BufferIndex, Dropped, Held, NextBatch, Pool, PoolView,
};
pub use shm_ring_holds::{HoldTable, Marks};

/// The most buffers a subscriber's page of marks covers.
const MARKS_BUFFERS: u32 = 1024;
/// The bytes of a ring entry that are not the publisher's metadata.
const ENTRY_OVERHEAD: u32 = 72;
/// The field of a buffer topic that holds the frames.
const FRAMES: FieldId = FieldId(0);

/// One reader the publisher is told about: its slot, and its marks if it has come.
struct Change {
    slot: u32,
    page: Option<OwnedFd>,
    limits: Limits,
    max_hold_ms: u32,
}

struct PublisherSide {
    handle: u32,
    changes: Vec<Change>,
}

struct SubscriberSide {
    handle: u32,
    marks: Arc<Marks>,
    /// A pool that has arrived and the subscriber has not taken yet.
    pool: Option<PoolView>,
}

#[derive(Default)]
struct State {
    publishers: Vec<PublisherSide>,
    subscribers: Vec<SubscriberSide>,
}

impl Handler for State {
    fn kinds(&self) -> &'static [u8] {
        &[kind::TOPIC_HOLDS, kind::TOPIC_BUFFER_OFFER]
    }

    fn handle(&mut self, frame: &Incoming<'_>, cx: &mut Cx<'_>) -> Result<(), ClientError> {
        if frame.kind() == kind::TOPIC_HOLDS {
            let holds: Holds = frame.body()?;
            let page = cx.take_fd(0);
            if let Some(publisher) = self
                .publishers
                .iter_mut()
                .find(|side| side.handle == holds.handle)
            {
                publisher.changes.push(Change {
                    slot: holds.subscriber,
                    page: page.filter(|_| holds.present),
                    limits: Limits {
                        max_held: holds.max_held,
                        max_hold: (holds.max_hold_ms > 0)
                            .then(|| Duration::from_millis(u64::from(holds.max_hold_ms))),
                    },
                    max_hold_ms: holds.max_hold_ms,
                });
            } else if let Some(subscriber) = self
                .subscribers
                .iter_mut()
                .find(|side| side.handle == holds.handle)
            {
                // The reader slot the publisher knows this subscriber by.
                subscriber.marks.set_slot(holds.subscriber);
            }
            return Ok(());
        }
        let offer: BufferOffer<'_> = frame.body()?;
        let Some(subscriber) = self
            .subscribers
            .iter_mut()
            .find(|side| side.handle == offer.handle)
        else {
            return Ok(());
        };
        let page = cx
            .take_fd(0)
            .ok_or(ClientError::Protocol("a pool with no page"))?;
        let fds: Vec<OwnedFd> = (1..cx.fds())
            .filter_map(|index| cx.take_fd(index))
            .collect();
        let buffers: Vec<BufferDesc> = offer
            .buffers
            .iter()
            .flatten()
            .map(|buffer| BufferDesc {
                descriptor: buffer.descriptor,
                offset: buffer.offset,
                size: buffer.size,
            })
            .collect();
        // Nothing in an offer is trusted: every buffer is checked against its descriptor.
        let view = PoolView::attach(page, fds, &buffers)
            .map_err(|_| ClientError::Protocol("a pool that does not check"))?;
        subscriber.pool = Some(view);
        Ok(())
    }
}

/// The buffer topics of one session.
#[derive(Clone)]
pub struct Buffers {
    client: Client,
    topics: Topics,
    state: Rc<RefCell<State>>,
}

impl fmt::Debug for Buffers {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Buffers")
    }
}

/// What a subscriber asks to be held to.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct HoldLimits {
    /// How many buffers it may hold at once. Zero for no limit.
    pub max_held: u32,
    /// For how long it may hold one, in milliseconds. Zero for no limit.
    pub max_hold_ms: u32,
}

fn shared_only(client: &Client) -> Result<(), TopicError> {
    if client.session(|session| session.passes_descriptors()) {
        Ok(())
    } else {
        Err(TopicError::Invalid(
            "a buffer topic is between programs on one board".into(),
        ))
    }
}

/// Hands the comm core `pool` for topic `handle`: its page and its memory, for every
/// subscriber now and later, and the reader slots its subscribers may have (zero for all).
/// Returns the connection it went on.
fn offer(client: &Client, handle: u32, pool: &Pool, reader_slots: u64) -> Result<u64, TopicError> {
    let buffers: Vec<BufferInfo> = pool
        .buffers()
        .iter()
        .enumerate()
        .map(|(index, buffer)| BufferInfo {
            index: index as u32,
            size: buffer.size,
            descriptor: buffer.descriptor,
            offset: buffer.offset,
        })
        .collect();
    let page = pool.page_fd()?;
    let mut fds = vec![page.as_fd()];
    fds.extend(pool.descriptors());
    let offer = BufferOffer {
        handle,
        generation: pool.generation(),
        buffers: List::new(&buffers),
        reader_slots,
    };
    client.send_with_fds(&offer, &fds)?;
    Ok(client.session(|session| session.reconnects()))
}

impl Buffers {
    /// The buffer topics of `client`'s session, beside its other `topics`.
    #[must_use]
    pub fn new(client: &Client, topics: &Topics) -> Self {
        let state = Rc::new(RefCell::new(State::default()));
        client.add(state.clone());
        Self {
            client: client.clone(),
            topics: topics.clone(),
            state,
        }
    }

    /// Declares a buffer topic over `pool` and returns its publisher. `history` is how many
    /// frames a slow subscriber can fall behind before it loses some; `max_meta` the most
    /// bytes of the publisher's own metadata a frame carries.
    ///
    /// # Errors
    ///
    /// If the session is not on the board, or the comm core refuses.
    pub fn declare(
        &self,
        name: &str,
        history: u32,
        max_meta: u32,
        pool: Pool,
    ) -> Result<BufferPublisher, TopicError> {
        shared_only(&self.client)?;
        let fields = [Field {
            name: "frame",
            ty: FieldType::Buffers,
            unit: "",
            type_name: "",
        }];
        let mut publisher = self.topics.declare(&TopicDecl {
            name,
            fields: &fields,
            history,
            expiry_ms: 0,
            max_payload: ENTRY_OVERHEAD + max_meta,
        })?;
        let writer = publisher
            .take_writer()
            .ok_or(TopicError::Invalid("no ring was handed over".into()))?;
        let handle = publisher.handle();
        self.state.borrow_mut().publishers.push(PublisherSide {
            handle,
            changes: Vec::new(),
        });
        let table = HoldTable::new(pool.buffers().len() as u32);
        let mut publisher = BufferPublisher {
            client: self.client.clone(),
            state: self.state.clone(),
            publisher,
            handle,
            offered_on: 0,
            writer: BufferWriter::new(writer, FRAMES, pool),
            table,
            readers: Vec::new(),
            started: Instant::now(),
        };
        publisher.offer()?;
        Ok(publisher)
    }

    /// Subscribes to a buffer topic. `request` goes to the publisher unread: the size or rate
    /// this subscriber would like.
    ///
    /// # Errors
    ///
    /// If the session is not on the board, or the comm core refuses. A topic nobody has
    /// declared is not an error: the subscriber waits for it.
    pub fn subscribe(
        &self,
        topic: &str,
        request: &[u8],
        limits: HoldLimits,
    ) -> Result<BufferSubscriber, TopicError> {
        shared_only(&self.client)?;
        let marks = Arc::new(Marks::create(MARKS_BUFFERS, 0, limits.max_held)?);
        let page = marks.page_fd()?;
        let subscriber = self.topics.subscribe_holding(
            topic,
            request,
            &Holding {
                marks: Some(page.as_fd()),
                max_held: limits.max_held,
                max_hold_ms: limits.max_hold_ms,
            },
        )?;
        let handle = subscriber
            .handle()
            .ok_or(TopicError::Invalid("the subscription has no handle".into()))?;
        self.state.borrow_mut().subscribers.push(SubscriberSide {
            handle,
            marks: marks.clone(),
            pool: None,
        });
        Ok(BufferSubscriber {
            state: self.state.clone(),
            subscriber,
            handle,
            marks,
            frames: None,
        })
    }
}

/// The publisher of one buffer topic.
pub struct BufferPublisher {
    client: Client,
    state: Rc<RefCell<State>>,
    publisher: Publisher,
    handle: u32,
    /// The connection the pool was last offered on: the session's count of reconnects then.
    offered_on: u64,
    writer: BufferWriter,
    table: HoldTable,
    /// Every reader's marks, kept so a new pool's table can be told of them again.
    readers: Vec<(u32, OwnedFd, Limits)>,
    started: Instant,
}

impl fmt::Debug for BufferPublisher {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "BufferPublisher({})", self.handle)
    }
}

impl BufferPublisher {
    /// Hands the comm core the pool: its page and its memory, for every subscriber now and
    /// later.
    fn offer(&mut self) -> Result<(), TopicError> {
        self.offered_on = offer(&self.client, self.handle, self.writer.pool(), 0)?;
        Ok(())
    }

    /// Looks at what the comm core has sent: subscribers that came, with their marks, or went.
    /// A subscriber gets no frames until the publisher has looked, so call this once a frame
    /// or whenever the program is idle. It does not wait.
    ///
    /// # Errors
    ///
    /// If the connection broke.
    pub fn poll(&mut self) -> Result<(), TopicError> {
        self.client.drain()?;
        let now = self.publisher.handle();
        let connection = self.client.session(|session| session.reconnects());
        if now != self.handle || connection != self.offered_on {
            // The topic was declared again, to a comm core that came back with no memory of
            // it, and has not been given the pool. It may go by a new number there, or by the
            // same one: a comm core that starts again numbers from the start again.
            if let Some(side) = self
                .state
                .borrow_mut()
                .publishers
                .iter_mut()
                .find(|side| side.handle == self.handle)
            {
                side.handle = now;
            }
            self.handle = now;
            self.offer()?;
        }
        let changes = {
            let mut state = self.state.borrow_mut();
            match state
                .publishers
                .iter_mut()
                .find(|side| side.handle == self.handle)
            {
                Some(side) if !side.changes.is_empty() => std::mem::take(&mut side.changes),
                _ => return Ok(()),
            }
        };
        for change in changes {
            self.table.remove(change.slot, self.writer.pool().page());
            self.readers.retain(|(slot, _, _)| *slot != change.slot);
            let Some(page) = change.page else {
                continue;
            };
            let keep = page
                .try_clone()
                .map_err(|error| TopicError::Client(error.into()))?;
            self.table.add(change.slot, page, change.limits)?;
            self.readers.push((change.slot, keep, change.limits));
            // Only now is the subscriber given the pool: it never reads a frame this
            // publisher is not yet protecting.
            self.client.send(&Holds {
                handle: self.handle,
                subscriber: change.slot,
                max_held: change.limits.max_held,
                max_hold_ms: change.max_hold_ms,
                present: true,
            })?;
        }
        Ok(())
    }

    /// A free buffer to fill, or `Dropped` when every buffer is published or held: then this
    /// frame is the one to drop. A subscriber over its limits is cut off here, so it cannot
    /// keep the pool from the publisher.
    ///
    /// # Errors
    ///
    /// [`Dropped`].
    pub fn take(&mut self) -> Result<BufferIndex, Dropped> {
        let now_ns = self.started.elapsed().as_nanos() as u64;
        self.table.enforce(now_ns, self.writer.pool().page());
        self.writer.take(&self.table)
    }

    /// The memory of a buffer from [`BufferPublisher::take`], to fill. `None` for a pool whose
    /// memory something else fills, such as a camera driver's.
    pub fn buffer_mut(&mut self, buffer: BufferIndex) -> Option<&mut [u8]> {
        self.writer.buffer_mut(buffer)
    }

    /// Publishes a frame made of `buffers`, belonging to `time`, with the publisher's own
    /// `meta`, and wakes the subscribers that are asleep. The frame's bytes are not touched.
    ///
    /// # Errors
    ///
    /// If a buffer was not taken, or the metadata is longer than the topic allows.
    pub fn publish(
        &mut self,
        buffers: &[BufferIndex],
        time: SampleTime,
        meta: &[u8],
    ) -> Result<u64, BufferError> {
        let seq = self.writer.publish(buffers, time, meta)?;
        self.publisher.wake();
        Ok(seq)
    }

    /// Replaces the pool, as a camera changing mode does. Its generation must be higher than
    /// the last. Subscribers are handed the new pool and keep the old mapped for frames they
    /// still hold.
    ///
    /// # Errors
    ///
    /// If the connection broke, or a reader's marks cannot be mapped again.
    pub fn replace_pool(&mut self, pool: Pool) -> Result<(), TopicError> {
        let buffers = pool.buffers().len() as u32;
        drop(self.writer.replace_pool(pool));
        // The same readers hold from the new pool: the same pages, in a table of its size.
        self.table = HoldTable::new(buffers);
        for (slot, page, limits) in &self.readers {
            let page = page
                .try_clone()
                .map_err(|error| TopicError::Client(error.into()))?;
            self.table.add(*slot, page, *limits)?;
        }
        self.offer()
    }

    /// Withdraws the topic: its subscribers are told the publisher is gone, and it is not
    /// declared or offered again after a reconnect. Subscribers keep what they hold until they
    /// let it go.
    ///
    /// # Errors
    ///
    /// As [`Publisher::withdraw`].
    pub fn withdraw(self) -> Result<(), ClientError> {
        let handle = self.handle;
        self.state
            .borrow_mut()
            .publishers
            .retain(|side| side.handle != handle);
        self.publisher.withdraw()
    }

    /// How many subscribers the topic has. With none, a publisher may as well not capture.
    #[must_use]
    pub fn subscribers(&self) -> u32 {
        self.writer.subscribers()
    }

    /// What the newest subscriber asked for, as it sent it.
    pub fn with_request<R>(&self, with: impl FnOnce(&[u8]) -> R) -> R {
        self.publisher.with_request(with)
    }
}

/// A subscriber to one buffer topic.
pub struct BufferSubscriber {
    state: Rc<RefCell<State>>,
    subscriber: Subscriber,
    handle: u32,
    marks: Arc<Marks>,
    frames: Option<BufferReader<shm_ring::ReadMap, Arc<Marks>>>,
}

impl fmt::Debug for BufferSubscriber {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "BufferSubscriber({})", self.handle)
    }
}

/// Takes in a pool the comm core has sent, if it sent one.
fn refresh(
    state: &RefCell<State>,
    handle: &mut u32,
    marks: &Arc<Marks>,
    subscriber: &mut Subscriber,
    frames: &mut Option<BufferReader<shm_ring::ReadMap, Arc<Marks>>>,
) {
    let mut state = state.borrow_mut();
    let Some(side) = state
        .subscribers
        .iter_mut()
        .find(|side| side.handle == *handle)
    else {
        return;
    };
    // Subscribed again, to a comm core that came back: the topic goes by a new number.
    if let Some(now) = subscriber.handle()
        && now != *handle
    {
        side.handle = now;
        *handle = now;
    }
    let Some(pool) = side.pool.take() else {
        return;
    };
    match frames {
        Some(frames) => frames.set_pool(pool, marks.clone()),
        None => match subscriber.take_reader() {
            Some(ring) => *frames = Some(BufferReader::new(ring, FRAMES, pool, marks.clone())),
            // The pool came before the ring: keep it for when the ring does.
            None => side.pool = Some(pool),
        },
    }
}

impl BufferSubscriber {
    /// Where the topic is.
    #[must_use]
    pub fn status(&self) -> Status {
        self.subscriber.status()
    }

    /// The publisher's pool as it was handed over: each buffer's memory descriptor and where in
    /// it the buffer is ([`PoolView::descriptor`]), for a program that maps or passes on the
    /// buffers itself. `None` until the pool has arrived. A frame's buffers are also reached
    /// through its `held` ([`Held::bytes`], [`Held::descriptor`]).
    pub fn pool(&mut self) -> Option<&PoolView> {
        refresh(
            &self.state,
            &mut self.handle,
            &self.marks,
            &mut self.subscriber,
            &mut self.frames,
        );
        self.frames.as_ref().map(BufferReader::pool)
    }

    /// How many buffers this subscriber is holding.
    #[must_use]
    pub fn held(&self) -> u32 {
        self.marks.held()
    }

    /// The next frame, or what stands in the way of one. Take `held` out of the frame to keep
    /// it past the next call; dropping it gives the buffers back to the publisher. A memory
    /// read: no system call, no allocation, and the frame's bytes are not copied.
    // Not an iterator: a frame borrows the subscriber's copy of its metadata.
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> NextBatch<'_, Arc<Marks>> {
        refresh(
            &self.state,
            &mut self.handle,
            &self.marks,
            &mut self.subscriber,
            &mut self.frames,
        );
        match &mut self.frames {
            Some(frames) => frames.next(),
            None => NextBatch::Empty,
        }
    }

    /// Sleeps until there is a frame to look at, for up to `wait`. `true` if there is. The
    /// session is kept while it sleeps, and nothing is polled on a timer.
    ///
    /// # Errors
    ///
    /// If the connection broke.
    pub fn wait(&mut self, wait: Duration) -> Result<bool, TopicError> {
        let (state, handle, marks, frames) =
            (&self.state, &mut self.handle, &self.marks, &mut self.frames);
        self.subscriber.wait_until(wait, |subscriber| {
            refresh(state, handle, marks, subscriber, frames);
            frames.as_ref().is_some_and(|frames| frames.pending() > 0)
        })
    }
}
