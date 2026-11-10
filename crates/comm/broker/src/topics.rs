//! The table of topics.

use std::os::fd::OwnedFd;

use orion_config::{ConfigError, Source};
use orion_ring::{Ring, Spec};
use orion_server::{Admits, Policy, SessionId};
use orion_types::{Error, NodeName, TopicName};

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

/// Every topic on the board. The broker's services share one of these.
#[derive(Debug)]
pub struct Topics {
    pub(crate) node: NodeName,
    pub(crate) config: BrokerConfig,
    slots: Vec<Option<Topic>>,
    generations: Vec<u16>,
    bytes: usize,
}

impl Topics {
    /// An empty table for the board `node`.
    #[must_use]
    pub const fn new(node: NodeName, config: BrokerConfig) -> Self {
        Self {
            node,
            config,
            slots: Vec::new(),
            generations: Vec::new(),
            bytes: 0,
        }
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

    /// The topic with this number, to change.
    pub fn get_mut(&mut self, id: TopicId) -> Option<&mut Topic> {
        self.slots
            .get_mut(id.slot())?
            .as_mut()
            .filter(|topic| topic.id == id)
    }

    /// The topic with this name.
    #[must_use]
    pub fn find(&self, name: &TopicName) -> Option<TopicId> {
        self.iter()
            .find(|topic| topic.name == *name)
            .map(|topic| topic.id)
    }

    /// The topic with this name, made if it isn't there.
    pub(crate) fn find_or_add(&mut self, name: &TopicName) -> Result<TopicId, Error> {
        if let Some(id) = self.find(name) {
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
            declare: Vec::new(),
            ring: None,
            publisher: None,
            publisher_shared: false,
            subscribers: Vec::new(),
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
                let wanted =
                    orion_ring::Geometry::for_spec(spec).map_err(|_| Error::Unsupported)?;
                if *ring.geometry() != wanted {
                    return Err(Error::Malformed);
                }
                ring
            }
            None => {
                let wanted =
                    orion_ring::Geometry::for_spec(spec).map_err(|_| Error::Unsupported)?;
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
        }
        Ok(())
    }

    /// Removes `id` if nobody publishes it and nobody subscribes to it.
    pub(crate) fn remove_if_unused(&mut self, id: TopicId) {
        let unused = self
            .get(id)
            .is_some_and(|topic| topic.publisher.is_none() && topic.subscribers.is_empty());
        if unused && let Some(topic) = self.slots[id.slot()].take() {
            self.bytes -= topic.bytes();
        }
    }
}
