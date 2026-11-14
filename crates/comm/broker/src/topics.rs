//! The table of topics.

use std::os::fd::OwnedFd;

use orion_config::{ConfigError, Source};
use orion_server::{Admits, Policy, SessionId, Sessions};
use orion_types::{Error, NodeName, TopicName};
use shm_ring::{Ring, Spec};

/// A topic's number: the handle its publisher and subscribers know it by.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TopicId(pub u32);

impl TopicId {
    const fn new(slot: usize, generation: u16) -> Self {
        Self((slot as u32 + 1) | (generation as u32) << 16)
    }

    const fn slot(self) -> usize {
        ((self.0 & 0xFFFF) as usize).wrapping_sub(1)
    }

    const fn generation(self) -> u16 {
        (self.0 >> 16) as u16
    }
}

/// What the broker allows.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BrokerConfig {
    /// `topics.max_bytes`: all ring memory on the board, together.
    pub max_bytes: usize,
    /// `topics.max`: how many topics there can be.
    pub max_topics: u32,
    /// Who may declare a topic.
    pub declare: Policy,
    /// Who may subscribe to one.
    pub subscribe: Policy,
}

impl Default for BrokerConfig {
    fn default() -> Self {
        Self {
            max_bytes: 16 << 20,
            max_topics: 1024,
            // Programs on the board, other nodes and devices publish. People's tools read.
            declare: Policy::new(Admits::LOCAL.and(Admits::PEER).and(Admits::DEVICE)),
            subscribe: Policy::new(Admits::LOCAL.and(Admits::PEER).and(Admits::OPERATOR)),
        }
    }
}

impl BrokerConfig {
    /// Takes the broker's settings out of `source`.
    ///
    /// # Errors
    ///
    /// A value that doesn't parse.
    pub fn from_source(source: &mut Source) -> Result<Self, ConfigError> {
        let default = Self::default();
        Ok(Self {
            max_bytes: source.get("topics.max_bytes", default.max_bytes)?,
            max_topics: source.get("topics.max", default.max_topics)?,
            ..default
        })
    }
}

/// One subscriber of a topic.
#[derive(Debug)]
pub struct Subscription {
    /// Its session.
    pub session: SessionId,
    /// The `seq` of its subscribe, which its `Opened` names.
    pub request_seq: u16,
    /// Its doorbell: the eventfd and the page, read-only. Kept so a publisher that comes back
    /// can be given it again.
    pub bell: Option<(OwnedFd, OwnedFd)>,
    /// For a frame topic: its page of hold marks, read-only. Kept for the same reason.
    pub marks: Option<OwnedFd>,
    /// What it asked of the publisher, passed on unread.
    pub request: Vec<u8>,
    /// For a frame topic: how many buffers it may hold. Zero for no limit.
    pub max_held: u32,
    /// For a frame topic: how long it may hold one, in milliseconds. Zero for no limit.
    pub max_hold_ms: u32,
    /// Whether it was given the ring. If not, samples must be sent to it as messages.
    pub shared: bool,
}

/// One topic.
#[derive(Debug)]
pub struct Topic {
    /// Its number.
    pub id: TopicId,
    /// Its name.
    pub name: TopicName,
    /// The node it is published on, for another node's topic that is asked for here. Such a
    /// topic has no publisher on this board: what carries it across declares it and writes
    /// its ring. `None` for a topic of this board.
    pub node: Option<NodeName>,
    /// Its declaration, as the publisher sent it. Empty until someone declares it.
    pub declare: Vec<u8>,
    /// Its ring. `None` while subscribers wait for a publisher.
    pub ring: Option<Ring>,
    /// The session publishing it, if one is.
    pub publisher: Option<SessionId>,
    /// Whether the publisher was given the ring. If not, it sends samples as messages.
    pub publisher_shared: bool,
    /// Its subscribers.
    pub subscribers: Vec<Subscription>,
    /// Whether the comm core itself publishes it: its own state. Such a topic has no publisher
    /// session, is never removed, and cannot be declared by anyone else.
    pub internal: bool,
    /// For another node's topic: whether what carries it across has it now.
    pub mirror_live: bool,
    /// Goes up whenever the ring, the publisher or the subscribers change. What builds on the
    /// broker keeps its own state per topic and rebuilds it when this moves.
    pub epoch: u64,
}

impl Topic {
    /// How many subscribers it has.
    #[must_use]
    pub fn subscriber_count(&self) -> u32 {
        self.subscribers.len() as u32
    }

    fn bytes(&self) -> usize {
        self.ring.as_ref().map_or(0, Ring::mapped_bytes)
    }
}

/// Something that is told when a topic changes hands, for what builds on the broker and has
/// its own things to hand over: a frame topic's buffers.
pub trait TopicWatch {
    /// `topic` has a publisher and a ring: it was declared, declared again, or its publisher
    /// came back. Every subscriber it has was just sent the ring or told the topic is live.
    fn declared(&mut self, topic: &Topic, sessions: &mut Sessions);
    /// `session` subscribed to `topic` and was just sent its ring.
    fn subscribed(&mut self, topic: &Topic, session: SessionId, sessions: &mut Sessions);
    /// `session` no longer subscribes to `topic`.
    fn left(&mut self, topic: &Topic, session: SessionId, sessions: &mut Sessions);
    /// `session` subscribed to `topic`, which nobody has declared: it waits. For what can
    /// do something about that, as what carries another node's topics across can.
    fn waiting(&mut self, topic: &Topic, session: SessionId, sessions: &mut Sessions) {
        let _ = (topic, session, sessions);
    }
}

/// Every topic on the board. The broker's services share one of these.
pub struct Topics {
    pub(crate) node: NodeName,
    pub(crate) config: BrokerConfig,
    slots: Vec<Option<Topic>>,
    generations: Vec<u16>,
    bytes: usize,
    pub(crate) watchers: Vec<Box<dyn TopicWatch>>,
    /// The doorbell of whatever carries samples to subscribers that were not given the
    /// ring: its eventfd and its page, read-only.
    offboard_bell: Option<(OwnedFd, OwnedFd)>,
    /// Whether something here carries other nodes' topics across. Without it, a subscription
    /// to one is refused.
    carries_remote: bool,
}

impl std::fmt::Debug for Topics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Topics({} topics, {} bytes)", self.len(), self.bytes)
    }
}

impl Topics {
    /// An empty table for the board `node`.
    #[must_use]
    pub fn new(node: NodeName, config: BrokerConfig) -> Self {
        Self {
            node,
            config,
            slots: Vec::new(),
            generations: Vec::new(),
            bytes: 0,
            watchers: Vec::new(),
            offboard_bell: None,
            carries_remote: false,
        }
    }

    /// Sets the doorbell that stands in for every subscriber that was not given the ring. A
    /// program on another board cannot be rung; what carries its samples to it can, and
    /// publishers ring this one as they would the subscriber's own.
    pub fn set_offboard_bell(&mut self, eventfd: OwnedFd, page: OwnedFd) {
        self.offboard_bell = Some((eventfd, page));
    }

    /// A copy of the doorbell for a subscriber that was not given the ring, if one is set.
    pub(crate) fn offboard_bell(&self) -> Option<(OwnedFd, OwnedFd)> {
        let (eventfd, page) = self.offboard_bell.as_ref()?;
        Some((eventfd.try_clone().ok()?, page.try_clone().ok()?))
    }

    /// Adds something to be told when topics change hands.
    pub fn watch(&mut self, watcher: impl TopicWatch + 'static) {
        self.watchers.push(Box::new(watcher));
    }

    /// Tells every watcher about `id`.
    pub(crate) fn tell(&mut self, id: TopicId, mut tell: impl FnMut(&mut dyn TopicWatch, &Topic)) {
        let mut watchers = std::mem::take(&mut self.watchers);
        if let Some(topic) = self.get(id) {
            for watcher in &mut watchers {
                tell(&mut **watcher, topic);
            }
        }
        self.watchers = watchers;
    }

    /// How many topics there are, declared or waited for.
    #[must_use]
    pub fn len(&self) -> usize {
        self.slots.iter().flatten().count()
    }

    /// Whether there are none.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// All ring memory in use, in bytes.
    #[must_use]
    pub const fn bytes(&self) -> usize {
        self.bytes
    }

    /// Every topic.
    pub fn iter(&self) -> impl Iterator<Item = &Topic> {
        self.slots.iter().flatten()
    }

    /// Every topic, to change.
    pub fn iter_mut(&mut self) -> impl Iterator<Item = &mut Topic> {
        self.slots.iter_mut().flatten()
    }

    /// The topic with this number.
    #[must_use]
    pub fn get(&self, id: TopicId) -> Option<&Topic> {
        self.slots
            .get(id.slot())?
            .as_ref()
            .filter(|topic| topic.id == id)
    }

    /// Whether this table ever gave out `id`. One it did not came from before the comm core
    /// started, or was made up.
    #[must_use]
    pub fn issued(&self, id: TopicId) -> bool {
        self.generations
            .get(id.slot())
            .is_some_and(|newest| (1..=*newest).contains(&id.generation()))
    }

    /// The topic with this number, to change.
    pub fn get_mut(&mut self, id: TopicId) -> Option<&mut Topic> {
        self.slots
            .get_mut(id.slot())?
            .as_mut()
            .filter(|topic| topic.id == id)
    }

    /// This board's topic with this name.
    #[must_use]
    pub fn find(&self, name: &TopicName) -> Option<TopicId> {
        self.find_on(None, name)
    }

    /// The topic `name` of `node`, or of this board for `None`.
    #[must_use]
    pub fn find_on(&self, node: Option<&NodeName>, name: &TopicName) -> Option<TopicId> {
        self.iter()
            .find(|topic| topic.name == *name && topic.node.as_ref() == node)
            .map(|topic| topic.id)
    }

    /// Says that something here carries other nodes' topics across: a subscription to
    /// `<node>/<topic>` is then taken, and waits for that to declare it.
    pub fn carry_remote(&mut self) {
        self.carries_remote = true;
    }

    /// Whether another node's topics can be subscribed to here.
    #[must_use]
    pub const fn carries_remote(&self) -> bool {
        self.carries_remote
    }

    /// This board's name.
    #[must_use]
    pub const fn node(&self) -> &NodeName {
        &self.node
    }

    /// This board's topic with this name, made if it isn't there.
    pub(crate) fn find_or_add(&mut self, name: &TopicName) -> Result<TopicId, Error> {
        self.find_or_add_on(None, name)
    }

    /// The topic `name` of `node`, made if it isn't there.
    pub(crate) fn find_or_add_on(
        &mut self,
        node: Option<&NodeName>,
        name: &TopicName,
    ) -> Result<TopicId, Error> {
        if let Some(id) = self.find_on(node, name) {
            return Ok(id);
        }
        if self.len() >= self.config.max_topics as usize {
            return Err(Error::Busy);
        }
        let slot = match self.slots.iter().position(Option::is_none) {
            Some(slot) => slot,
            None => {
                self.slots.push(None);
                self.generations.push(0);
                self.slots.len() - 1
            }
        };
        self.generations[slot] = self.generations[slot].wrapping_add(1) & 0x7FFF;
        let id = TopicId::new(slot, self.generations[slot]);
        self.slots[slot] = Some(Topic {
            id,
            name: name.clone(),
            node: node.cloned(),
            declare: Vec::new(),
            ring: None,
            publisher: None,
            publisher_shared: false,
            subscribers: Vec::new(),
            internal: false,
            mirror_live: false,
            epoch: 0,
        });
        Ok(id)
    }

    /// Gives `id` a ring: a new one of shape `spec`, or `offered`, a ring its publisher kept.
    /// Any ring it had is dropped.
    pub(crate) fn set_ring(
        &mut self,
        id: TopicId,
        spec: &Spec,
        offered: Option<OwnedFd>,
    ) -> Result<(), Error> {
        let had = self.get(id).ok_or(Error::NotFound)?.bytes();
        let ring = match offered {
            Some(fd) => {
                let ring = Ring::adopt(fd).map_err(|_| Error::Malformed)?;
                let wanted = shm_ring::Geometry::for_spec(spec).map_err(|_| Error::Unsupported)?;
                if *ring.geometry() != wanted {
                    return Err(Error::Malformed);
                }
                ring
            }
            None => {
                let wanted = shm_ring::Geometry::for_spec(spec).map_err(|_| Error::Unsupported)?;
                // Rounded to pages, as it will be mapped: checked before it is made.
                let bytes = wanted.bytes().next_multiple_of(4096);
                if self.bytes - had + bytes > self.config.max_bytes {
                    return Err(Error::Busy);
                }
                Ring::create(spec).map_err(|_| Error::Internal)?
            }
        };
        if self.bytes - had + ring.mapped_bytes() > self.config.max_bytes {
            return Err(Error::Busy);
        }
        self.bytes = self.bytes - had + ring.mapped_bytes();
        if let Some(topic) = self.get_mut(id) {
            topic.ring = Some(ring);
            topic.epoch += 1;
        }
        Ok(())
    }

    /// Declares a topic that the comm core itself publishes. `declare` is its declaration as it
    /// would come off the wire, which subscribers are sent.
    ///
    /// # Errors
    ///
    /// [`Error::AlreadyExists`] if someone has declared the name, [`Error::Busy`] if there is no
    /// room for the topic or its ring.
    pub fn declare_internal(
        &mut self,
        name: &TopicName,
        declare: &[u8],
        spec: &Spec,
    ) -> Result<TopicId, Error> {
        let id = self.find_or_add(name)?;
        let taken = self
            .get(id)
            .is_some_and(|topic| topic.ring.is_some() || topic.publisher.is_some());
        if taken {
            return Err(Error::AlreadyExists);
        }
        if let Err(error) = self.set_ring(id, spec, None) {
            self.remove_if_unused(id);
            return Err(error);
        }
        let topic = self.get_mut(id).ok_or(Error::NotFound)?;
        topic.declare = declare.to_vec();
        topic.internal = true;
        topic.epoch += 1;
        Ok(id)
    }

    /// Declares the local copy of another node's topic: gives it a ring of the shape the
    /// declaration says, and hands that ring to everyone here who was waiting for it. Called
    /// by what carries the topic across, with the declaration as the other node sent it.
    ///
    /// # Errors
    ///
    /// [`Error::NotFound`] if nobody asked for the topic, [`Error::Busy`] if there is no room
    /// for its ring.
    pub fn declare_mirror(
        &mut self,
        id: TopicId,
        declare: &[u8],
        spec: &Spec,
        sessions: &mut Sessions,
    ) -> Result<(), Error> {
        let topic = self.get(id).ok_or(Error::NotFound)?;
        if topic.node.is_none() {
            return Err(Error::Denied);
        }
        // The same declaration again carries on in the ring that is there: the link to the
        // other node came back.
        let resumed = topic.ring.is_some() && topic.declare == declare;
        if !resumed {
            self.set_ring(id, spec, None)?;
        }
        let topic = self.get_mut(id).ok_or(Error::NotFound)?;
        topic.declare.clear();
        topic.declare.extend_from_slice(declare);
        topic.mirror_live = true;
        topic.epoch += 1;
        crate::service::mirror_declared(topic, resumed, sessions);
        self.tell(id, |watcher, topic| watcher.declared(topic, sessions));
        Ok(())
    }

    /// The other node, or the way to it, is gone: everyone here who subscribes to its topic
    /// is told its publisher is gone. The ring stays, with what it has.
    pub fn mirror_lost(&mut self, id: TopicId, sessions: &mut Sessions) {
        let Some(topic) = self.get_mut(id) else {
            return;
        };
        if !std::mem::take(&mut topic.mirror_live) {
            return;
        }
        topic.epoch += 1;
        crate::service::mirror_lost(topic, sessions);
    }

    /// Removes `id` if nobody publishes it and nobody subscribes to it.
    pub(crate) fn remove_if_unused(&mut self, id: TopicId) {
        let unused = self.get(id).is_some_and(|topic| {
            topic.publisher.is_none() && topic.subscribers.is_empty() && !topic.internal
        });
        if unused && let Some(topic) = self.slots[id.slot()].take() {
            self.bytes -= topic.bytes();
        }
    }
}
