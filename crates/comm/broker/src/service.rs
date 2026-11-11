//! The broker as a service: declare, open, subscribe, unsubscribe.

use std::cell::RefCell;
use std::os::fd::OwnedFd;
use std::rc::Rc;

use orion_msg_topics::{
    Bell, Declare, Demand, Open, Opened, Subscribe, TopicState, TopicStatus, Unsubscribe,
};
use orion_ring::{Ring, Spec};
use orion_server::{
    Checked, Cx, Delivery, Error, Incoming, Policy, Service, SessionId, Sessions, What,
};
use orion_types::NodeName;
use orion_wire::{Bytes, FrameView, body, kind};

use crate::topics::{BrokerConfig, Subscription, TopicId, Topics};

/// The broker. Add it to the server; share [`Broker::topics`] with what builds on it.
#[derive(Debug)]
pub struct Broker {
    topics: Rc<RefCell<Topics>>,
}

impl Broker {
    /// A broker for the board `node`.
    #[must_use]
    pub fn new(node: NodeName, config: BrokerConfig) -> Self {
        Self {
            topics: Rc::new(RefCell::new(Topics::new(node, config))),
        }
    }

    /// The table of topics.
    #[must_use]
    pub fn topics(&self) -> Rc<RefCell<Topics>> {
        self.topics.clone()
    }
}

/// The name a topic request is about, read from the front of its payload without decoding the
/// rest.
fn asked_name<'a>(frame: &FrameView<'a>) -> Result<&'a str, Error> {
    let payload = frame.payload();
    match frame.kind() {
        kind::TOPIC_DECLARE => Ok(body::take::<&str>(payload)?.0),
        kind::TOPIC_OPEN | kind::TOPIC_SUBSCRIBE => {
            let (_node, rest) = body::take::<Option<&str>>(payload)?;
            Ok(body::take::<&str>(rest)?.0)
        }
        _ => Ok(""),
    }
}

impl Service for Broker {
    fn kinds(&self) -> &'static [u8] {
        &[
            kind::TOPIC_DECLARE,
            kind::TOPIC_OPEN,
            kind::TOPIC_SUBSCRIBE,
            kind::TOPIC_UNSUBSCRIBE,
        ]
    }

    fn asks<'a>(&self, frame: &FrameView<'a>) -> Result<(What<'a>, Policy), Error> {
        let name = asked_name(frame)?;
        let config = self.topics.borrow().config;
        Ok(match frame.kind() {
            kind::TOPIC_DECLARE => (What::Write(name), config.declare),
            _ => (What::Open(name), config.subscribe),
        })
    }

    fn handle(&mut self, request: Checked<Incoming<'_>>, cx: &mut Cx<'_>) -> Result<(), Error> {
        let mut incoming = request.into_inner();
        let topics = &mut *self.topics.borrow_mut();
        match incoming.kind() {
            kind::TOPIC_DECLARE => {
                let declare: Declare<'_> = incoming.body()?;
                let offered = incoming.take_fd(0);
                let raw = incoming.frame().payload();
                self::declare(topics, cx, &declare, raw, offered)
            }
            kind::TOPIC_SUBSCRIBE => {
                let subscribe: Subscribe<'_> = incoming.body()?;
                let bell = match (incoming.take_fd(0), incoming.take_fd(1)) {
                    (Some(eventfd), Some(page)) => Some((eventfd, page)),
                    _ => None,
                };
                let marks = incoming.take_fd(2);
                self::subscribe(topics, cx, &subscribe, bell, marks)
            }
            kind::TOPIC_UNSUBSCRIBE => {
                let unsubscribe: Unsubscribe = incoming.body()?;
                let id = TopicId(unsubscribe.handle);
                let from = cx.from();
                leave(topics, cx.sessions(), id, from)
            }
            _ => {
                let open: Open = incoming.body()?;
                self::open(topics, cx, &open)
            }
        }
    }

    fn closed(&mut self, session: SessionId, sessions: &mut Sessions) {
        let topics = &mut *self.topics.borrow_mut();
        let ids: Vec<TopicId> = topics.iter().map(|topic| topic.id).collect();
        for id in ids {
            let Some(topic) = topics.get_mut(id) else {
                continue;
            };
            if topic.publisher == Some(session) {
                topic.publisher = None;
                topic.epoch += 1;
                for subscription in &topic.subscribers {
                    let state = TopicState {
                        request_seq: 0,
                        handle: id.0,
                        status: TopicStatus::PublisherGone,
                    };
                    let _ = sessions.send(subscription.session, &state, Delivery::Must);
                }
            }
            if leave(topics, sessions, id, session).is_err() {
                topics.remove_if_unused(id);
            }
        }
    }
}

fn io<T>(result: Result<T, orion_ring::RingError>) -> Result<T, Error> {
    result.map_err(|error| {
        tracing::warn!(%error, "a ring's descriptor could not be handed out");
        Error::Internal
    })
}

fn clone_bell(bell: &(OwnedFd, OwnedFd)) -> Result<[OwnedFd; 2], Error> {
    let clone = |fd: &OwnedFd| fd.try_clone().map_err(|_| Error::Internal);
    Ok([clone(&bell.0)?, clone(&bell.1)?])
}

/// Whether the caller of `cx` is on this board, with descriptors: the only kind of program a
/// ring can be handed to.
fn shares(cx: &Cx<'_>) -> bool {
    cx.caller().passes_descriptors
}

/// Sends a subscriber the topic's declaration and its end of the ring.
fn hand_ring(
    sessions: &mut Sessions,
    id: TopicId,
    declare: &[u8],
    ring: &Ring,
    subscription: &Subscription,
) -> Result<(), Error> {
    let to = subscription.session;
    sessions.send_frame(to, kind::TOPIC_DECLARE, declare, [], Delivery::Must)?;
    let opened = Opened {
        request_seq: subscription.request_seq,
        handle: id.0,
        shared_memory: subscription.shared,
        ring_bytes: ring.mapped_bytes() as u64,
    };
    if subscription.shared {
        sessions.send_with_fds(to, &opened, [io(ring.read_fd())?], Delivery::Must)
    } else {
        sessions.send(to, &opened, Delivery::Must)
    }
}

/// Tells a topic's publisher about one subscriber: its doorbell, and what it asked for.
fn tell_publisher(
    sessions: &mut Sessions,
    publisher: SessionId,
    shared: bool,
    id: TopicId,
    count: u32,
    subscription: &Subscription,
) {
    if shared && let Some(Ok(fds)) = subscription.bell.as_ref().map(clone_bell) {
        let bell = Bell {
            handle: id.0,
            subscriber: subscription.session.0,
            present: true,
        };
        let _ = sessions.send_with_fds(publisher, &bell, fds, Delivery::Must);
    }
    let demand = Demand {
        handle: id.0,
        subscribers: count,
        request: Bytes(&subscription.request),
    };
    let _ = sessions.send(publisher, &demand, Delivery::Must);
}

fn declare(
    topics: &mut Topics,
    cx: &mut Cx<'_>,
    declare: &Declare<'_>,
    raw: &[u8],
    offered: Option<OwnedFd>,
) -> Result<(), Error> {
    let spec = Spec {
        fields: declare.fields.len() as u32,
        history: declare.history,
        max_payload: declare.max_payload,
        expiry_ms: declare.expiry_ms,
    };
    let from = cx.from();
    let shared = shares(cx);
    let id = topics.find_or_add(&declare.topic)?;
    let topic = topics.get(id).ok_or(Error::NotFound)?;
    if topic.internal || topic.publisher.is_some_and(|publisher| publisher != from) {
        return Err(Error::AlreadyExists);
    }
    // The same declaration again carries on in the ring that is there. That is a publisher
    // coming back, or one saying it twice.
    let resumed = topic.ring.is_some() && topic.declare == raw;
    if !resumed && let Err(error) = topics.set_ring(id, &spec, offered) {
        topics.remove_if_unused(id);
        return Err(error);
    }
    let topic = topics.get_mut(id).ok_or(Error::NotFound)?;
    topic.declare.clear();
    topic.declare.extend_from_slice(raw);
    topic.publisher = Some(from);
    topic.publisher_shared = shared;
    topic.epoch += 1;
    let ring = topic.ring.as_ref().ok_or(Error::Internal)?;
    let count = topic.subscriber_count();
    ring.set_subscribers(count);

    let opened = Opened {
        request_seq: cx.request_seq(),
        handle: id.0,
        shared_memory: shared,
        ring_bytes: ring.mapped_bytes() as u64,
    };
    if shared {
        cx.reply_with_fds(&opened, [io(ring.write_fd())?])?;
    } else {
        cx.reply(&opened)?;
    }
    // Subscribers that were here first: waiting for the topic, or left by a publisher that
    // went away.
    let sessions = cx.sessions();
    for subscription in &topic.subscribers {
        if resumed {
            let state = TopicState {
                request_seq: 0,
                handle: id.0,
                status: TopicStatus::Live,
            };
            let _ = sessions.send(subscription.session, &state, Delivery::Must);
        } else {
            let _ = hand_ring(sessions, id, &topic.declare, ring, subscription);
        }
        tell_publisher(sessions, from, shared, id, count, subscription);
    }
    topics.tell(id, |watcher, topic| watcher.declared(topic, sessions));
    Ok(())
}

fn subscribe(
    topics: &mut Topics,
    cx: &mut Cx<'_>,
    subscribe: &Subscribe<'_>,
    bell: Option<(OwnedFd, OwnedFd)>,
    marks: Option<OwnedFd>,
) -> Result<(), Error> {
    if subscribe
        .topic
        .node
        .as_ref()
        .is_some_and(|node| *node != topics.node)
    {
        // Another board's topic. What carries those is not this crate.
        return Err(Error::Unsupported);
    }
    let from = cx.from();
    // A subscriber that cannot be handed the ring cannot be rung either. What carries its
    // samples to it is rung in its place.
    let bell = if shares(cx) {
        bell
    } else {
        topics.offboard_bell()
    };
    let id = topics.find_or_add(&subscribe.topic.topic)?;
    let topic = topics.get_mut(id).ok_or(Error::NotFound)?;
    // Subscribing again replaces the subscription: a client that reconnected its doorbell.
    topic
        .subscribers
        .retain(|subscription| subscription.session != from);
    topic.subscribers.push(Subscription {
        session: from,
        request_seq: cx.request_seq(),
        bell,
        marks,
        request: subscribe.request.to_vec(),
        max_held: subscribe.max_held,
        max_hold_ms: subscribe.max_hold_ms,
        shared: shares(cx),
    });
    topic.epoch += 1;
    let count = topic.subscriber_count();
    let subscription = topic.subscribers.last().ok_or(Error::Internal)?;
    let sessions = cx.sessions();
    let Some(ring) = &topic.ring else {
        return sessions.send(
            from,
            &TopicState {
                request_seq: subscription.request_seq,
                handle: id.0,
                status: TopicStatus::Waiting,
            },
            Delivery::Must,
        );
    };
    ring.set_subscribers(count);
    hand_ring(sessions, id, &topic.declare, ring, subscription)?;
    match topic.publisher {
        Some(publisher) => {
            let shared = topic.publisher_shared;
            tell_publisher(sessions, publisher, shared, id, count, subscription);
        }
        // The comm core publishes this one itself: there is no session, and nobody is gone.
        None if topic.internal => {}
        None => sessions.send(
            from,
            &TopicState {
                request_seq: 0,
                handle: id.0,
                status: TopicStatus::PublisherGone,
            },
            Delivery::Must,
        )?,
    }
    topics.tell(id, |watcher, topic| {
        watcher.subscribed(topic, from, sessions)
    });
    Ok(())
}

/// Ends `session`'s subscription to `id`.
fn leave(
    topics: &mut Topics,
    sessions: &mut Sessions,
    id: TopicId,
    session: SessionId,
) -> Result<(), Error> {
    let topic = topics.get_mut(id).ok_or(Error::NotFound)?;
    let before = topic.subscribers.len();
    topic
        .subscribers
        .retain(|subscription| subscription.session != session);
    if topic.subscribers.len() == before {
        return Err(Error::NotFound);
    }
    topic.epoch += 1;
    let count = topic.subscriber_count();
    if let Some(ring) = &topic.ring {
        ring.set_subscribers(count);
    }
    if let Some(publisher) = topic.publisher {
        if topic.publisher_shared {
            let bell = Bell {
                handle: id.0,
                subscriber: session.0,
                present: false,
            };
            let _ = sessions.send(publisher, &bell, Delivery::Must);
        }
        let demand = Demand {
            handle: id.0,
            subscribers: count,
            request: Bytes(&[]),
        };
        let _ = sessions.send(publisher, &demand, Delivery::Must);
    }
    topics.tell(id, |watcher, topic| watcher.left(topic, session, sessions));
    topics.remove_if_unused(id);
    Ok(())
}

fn open(topics: &mut Topics, cx: &mut Cx<'_>, open: &Open) -> Result<(), Error> {
    if open
        .topic
        .node
        .as_ref()
        .is_some_and(|node| *node != topics.node)
    {
        return Err(Error::Unsupported);
    }
    let id = topics.find(&open.topic.topic).ok_or(Error::NotFound)?;
    let topic = topics.get(id).ok_or(Error::NotFound)?;
    let ring = topic.ring.as_ref().ok_or(Error::NotFound)?;
    let shared = shares(cx);
    let opened = Opened {
        request_seq: cx.request_seq(),
        handle: id.0,
        shared_memory: shared,
        ring_bytes: ring.mapped_bytes() as u64,
    };
    if open.write {
        // Only the topic's publisher has the write end, and only it can ask for it again.
        if topic.publisher != Some(cx.from()) {
            return Err(Error::Denied);
        }
        return if shared {
            cx.reply_with_fds(&opened, [io(ring.write_fd())?])
        } else {
            cx.reply(&opened)
        };
    }
    // A look without subscribing: the declaration and a read end, and no count, no doorbell.
    let from = cx.from();
    let sessions = cx.sessions();
    sessions.send_frame(
        from,
        kind::TOPIC_DECLARE,
        &topic.declare,
        [],
        Delivery::Must,
    )?;
    if shared {
        sessions.send_with_fds(from, &opened, [io(ring.read_fd())?], Delivery::Must)
    } else {
        sessions.send(from, &opened, Delivery::Must)
    }
}
