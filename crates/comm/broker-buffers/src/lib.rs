//! Buffer topics in the broker: frames are topics.
//!
//! A frame topic is an ordinary topic whose ring entries name buffers. What it adds to set-up
//! is the buffers themselves and the hold marks:
//!
//! - **The publisher offers its pool once** (`BufferOffer`: the pool page and the buffers'
//!   descriptors). The broker keeps the offer and hands it to every subscriber, now and later.
//!   A subscriber that comes late gets the current pool with its ring.
//! - **A subscriber sends its page of hold marks with its subscribe.** The broker gives it a
//!   reader slot and passes the page to the publisher (`Holds`). Only when the publisher
//!   answers that it is honouring those marks is the subscriber given the pool. So a
//!   subscriber never reads a frame the publisher is not yet protecting.
//! - **A replaced pool** (a camera changing mode) is a new offer with a higher generation,
//!   handed to every subscriber. They keep the old buffers mapped for frames they still hold.
//! - **A subscriber's request** reaches the publisher unread, through the broker's `Demand`.
//!
//! The broker reads none of this. It never maps a buffer and never sees a frame: per frame it
//! does nothing at all. A comm core that restarts interrupts no frames, because the ring, the
//! pool and the marks all belong to the publisher and its subscribers; when they reconnect
//! they offer the same things again.

#![cfg(target_os = "linux")]

use std::cell::RefCell;
use std::os::fd::OwnedFd;
use std::rc::Rc;

use orion_broker::{Broker, Topic, TopicId, TopicWatch, Topics};
use orion_msg_topics::{BufferOffer, Holds};
use orion_server::{
    Checked, Cx, Delivery, Error, Incoming, Policy, Service, SessionId, Sessions, What,
};
use orion_wire::{FrameView, kind};

/// How many reader slots a pool has.
const SLOTS: u32 = 64;

/// Where a subscriber's hold marks are with the publisher.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Marks {
    /// It sent no page: it holds nothing and is handed the pool as it is.
    None,
    /// Its page has not been sent to the publisher yet.
    Unsent,
    /// The publisher has been sent the page and has not answered.
    Sent,
    /// The publisher is honouring its marks.
    Honoured,
}

#[derive(Debug)]
struct Reader {
    session: SessionId,
    slot: u32,
    marks: Marks,
}

/// A publisher's pool, as it offered it.
#[derive(Debug)]
struct Offer {
    generation: u32,
    payload: Vec<u8>,
    fds: Vec<OwnedFd>,
}

#[derive(Debug)]
struct Frames {
    topic: TopicId,
    offer: Option<Offer>,
    readers: Vec<Reader>,
}

#[derive(Debug, Default)]
struct State {
    frames: Vec<Frames>,
}

impl State {
    fn frames(&mut self, topic: TopicId) -> &mut Frames {
        let at = match self.frames.iter().position(|frames| frames.topic == topic) {
            Some(at) => at,
            None => {
                self.frames.push(Frames {
                    topic,
                    offer: None,
                    readers: Vec::new(),
                });
                self.frames.len() - 1
            }
        };
        &mut self.frames[at]
    }
}

/// Sends `session` the pool.
fn hand_pool(sessions: &mut Sessions, session: SessionId, offer: &Offer) {
    let Ok(fds) = offer
        .fds
        .iter()
        .map(OwnedFd::try_clone)
        .collect::<Result<Vec<_>, _>>()
    else {
        tracing::warn!("a pool's descriptors could not be duplicated");
        return;
    };
    let _ = sessions.send_frame(
        session,
        kind::TOPIC_BUFFER_OFFER,
        &offer.payload,
        fds,
        Delivery::Must,
    );
}

/// Moves one reader as far along as it can go: its marks to the publisher, or the pool to it.
fn advance(frames: &mut Frames, at: usize, topic: &Topic, sessions: &mut Sessions) {
    let reader = &mut frames.readers[at];
    let Some(subscription) = topic
        .subscribers
        .iter()
        .find(|sub| sub.session == reader.session)
    else {
        return;
    };
    let (Some(offer), Some(publisher)) = (&frames.offer, topic.publisher) else {
        return;
    };
    if !subscription.shared || !topic.publisher_shared {
        // Buffers only mean something to programs on this board.
        return;
    }
    match reader.marks {
        Marks::None | Marks::Honoured => hand_pool(sessions, reader.session, offer),
        Marks::Sent => {}
        Marks::Unsent => {
            let Some(Ok(page)) = subscription.marks.as_ref().map(OwnedFd::try_clone) else {
                return;
            };
            let holds = Holds {
                handle: topic.id.0,
                subscriber: reader.slot,
                max_held: subscription.max_held,
                max_hold_ms: subscription.max_hold_ms,
                present: true,
            };
            if sessions
                .send_with_fds(publisher, &holds, [page], Delivery::Must)
                .is_ok()
            {
                reader.marks = Marks::Sent;
            }
        }
    }
}

/// Gives `session` a reader slot on `topic`, if it has none, and returns where it is.
fn enroll(frames: &mut Frames, topic: &Topic, session: SessionId) -> Option<usize> {
    if let Some(at) = frames
        .readers
        .iter()
        .position(|reader| reader.session == session)
    {
        return Some(at);
    }
    let taken = |slot: &u32| frames.readers.iter().any(|reader| reader.slot == *slot);
    let Some(slot) = (0..SLOTS).find(|slot| !taken(slot)) else {
        tracing::warn!(topic = %topic.name, "a frame topic has no reader slot left");
        return None;
    };
    let has_marks = topic
        .subscribers
        .iter()
        .any(|sub| sub.session == session && sub.marks.is_some());
    frames.readers.push(Reader {
        session,
        slot,
        marks: if has_marks {
            Marks::Unsent
        } else {
            Marks::None
        },
    });
    Some(frames.readers.len() - 1)
}

/// Told by the broker when topics change hands.
struct Watch(Rc<RefCell<State>>);

impl TopicWatch for Watch {
    fn declared(&mut self, topic: &Topic, _sessions: &mut Sessions) {
        // A publisher that came back, or a new declaration: it has to offer its pool again,
        // and it knows nobody's marks. Its subscribers include any that were waiting for the
        // topic, which were never told about a pool at all.
        let mut state = self.0.borrow_mut();
        let frames = state.frames(topic.id);
        frames.offer = None;
        for reader in &mut frames.readers {
            if reader.marks != Marks::None {
                reader.marks = Marks::Unsent;
            }
        }
        for subscription in &topic.subscribers {
            enroll(frames, topic, subscription.session);
        }
    }

    fn subscribed(&mut self, topic: &Topic, session: SessionId, sessions: &mut Sessions) {
        let mut state = self.0.borrow_mut();
        let frames = state.frames(topic.id);
        // Subscribing again starts over: the subscriber may have new marks.
        frames.readers.retain(|reader| reader.session != session);
        if let Some(at) = enroll(frames, topic, session) {
            advance(frames, at, topic, sessions);
        }
    }

    fn left(&mut self, topic: &Topic, session: SessionId, sessions: &mut Sessions) {
        let mut state = self.0.borrow_mut();
        let frames = state.frames(topic.id);
        let Some(at) = frames
            .readers
            .iter()
            .position(|reader| reader.session == session)
        else {
            return;
        };
        let reader = frames.readers.remove(at);
        if let (Marks::Sent | Marks::Honoured, Some(publisher)) = (reader.marks, topic.publisher) {
            let holds = Holds {
                handle: topic.id.0,
                subscriber: reader.slot,
                max_held: 0,
                max_hold_ms: 0,
                present: false,
            };
            let _ = sessions.send(publisher, &holds, Delivery::Must);
        }
    }
}

/// The service. Add it to the server next to the [`Broker`] it was made from.
pub struct BrokerBuffers {
    topics: Rc<RefCell<Topics>>,
    state: Rc<RefCell<State>>,
    policy: Policy,
}

impl std::fmt::Debug for BrokerBuffers {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "BrokerBuffers({} topics)",
            self.state.borrow().frames.len()
        )
    }
}

impl BrokerBuffers {
    /// Buffer topics for `broker`. `policy` is who may offer a pool at all; the broker's own
    /// declare policy is the usual choice.
    #[must_use]
    pub fn new(broker: &Broker, policy: Policy) -> Self {
        let state = Rc::new(RefCell::new(State::default()));
        let topics = broker.topics();
        topics.borrow_mut().watch(Watch(state.clone()));
        Self {
            topics,
            state,
            policy,
        }
    }
}

impl Service for BrokerBuffers {
    fn kinds(&self) -> &'static [u8] {
        &[kind::TOPIC_BUFFER_OFFER, kind::TOPIC_HOLDS]
    }

    fn asks<'a>(&self, _frame: &FrameView<'a>) -> Result<(What<'a>, Policy), Error> {
        // Which topic is checked in `handle`, against who declared it.
        Ok((What::Write("topic"), self.policy))
    }

    fn handle(&mut self, request: Checked<Incoming<'_>>, cx: &mut Cx<'_>) -> Result<(), Error> {
        let mut incoming = request.into_inner();
        let topics = self.topics.borrow();
        let mut state = self.state.borrow_mut();
        let from = cx.from();
        let own = |handle: u32| {
            // Both kinds come from a topic's publisher, on this board.
            let topic = topics.get(TopicId(handle)).ok_or(Error::NotFound)?;
            if topic.publisher != Some(from) || !topic.publisher_shared {
                return Err(Error::Denied);
            }
            Ok(topic)
        };
        if incoming.kind() == kind::TOPIC_HOLDS {
            // The publisher is honouring a subscriber's marks: now that one may have the pool.
            let holds: Holds = incoming.body()?;
            let topic = own(holds.handle)?;
            let frames = state.frames(topic.id);
            let at = frames
                .readers
                .iter()
                .position(|reader| reader.slot == holds.subscriber && reader.marks == Marks::Sent)
                .ok_or(Error::NotFound)?;
            let reader = &mut frames.readers[at];
            reader.marks = Marks::Honoured;
            let limits = topic
                .subscribers
                .iter()
                .find(|sub| sub.session == reader.session);
            let told = Holds {
                handle: topic.id.0,
                subscriber: reader.slot,
                max_held: limits.map_or(0, |sub| sub.max_held),
                max_hold_ms: limits.map_or(0, |sub| sub.max_hold_ms),
                present: true,
            };
            let _ = cx.sessions().send(reader.session, &told, Delivery::Must);
            advance(frames, at, topic, cx.sessions());
            return Ok(());
        }
        let offer: BufferOffer<'_> = incoming.body()?;
        let topic = own(offer.handle)?;
        if incoming.fds() < 2 || offer.buffers.is_empty() {
            // A pool is its page and at least one descriptor of buffers.
            return Err(Error::Malformed);
        }
        let frames = state.frames(topic.id);
        if frames
            .offer
            .as_ref()
            .is_some_and(|had| offer.generation < had.generation)
        {
            return Err(Error::OutOfOrder);
        }
        frames.offer = Some(Offer {
            generation: offer.generation,
            payload: incoming.frame().payload().to_vec(),
            fds: (0..incoming.fds())
                .filter_map(|index| incoming.take_fd(index))
                .collect(),
        });
        for at in 0..frames.readers.len() {
            advance(frames, at, topic, cx.sessions());
        }
        Ok(())
    }

    fn closed(&mut self, _session: SessionId, _sessions: &mut Sessions) {
        // Forget topics that are gone.
        let topics = self.topics.borrow();
        self.state
            .borrow_mut()
            .frames
            .retain(|frames| topics.get(frames.topic).is_some());
    }
}
