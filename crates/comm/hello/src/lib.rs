//! The hello document: who this board is.
//!
//! Anyone who connects may ask (`Identify`) and is answered (`Identity`) with the node's name,
//! its cluster, its public key, its clock, and whatever facts other programs have published
//! about the board. It is how a tool that has just found a board on the network learns what
//! it found, before it is trusted with anything.
//!
//! **Signed.** The answer ends with the node's signature over everything before it, including
//! the asker's nonce, so an answer cannot be made up by something in between or played back
//! from an earlier question. A tool that already knows the board's key checks the answer
//! against it; one that does not learns the key here, and pins it when the board is enrolled.
//! [`verify`] is the check.
//!
//! **Orion invents no board facts.** It does not know a model or a serial number. A program
//! that does (the device's own package, a driver) publishes each fact as a topic named
//! `hello.<fact>`, and the document carries the newest value of each. Remove the program and
//! the fact is gone from the document; Orion's own part is unchanged.
//!
//! **Orion's own facts.** Two names are Orion's and a program's fact by either name is left
//! out: `state`, what the board is doing (`running`, `recovery`, `flasher` or `updating`), and,
//! in recovery, `recovery_why`, why. Both are text, and signed with the rest, so a tool knows a
//! board in recovery from one that is up before it asks for anything else.
//!
//! **Always answers.** This lives in the comm core and needs nothing else to be running, and
//! in recovery too.

#![cfg(target_os = "linux")]

use std::cell::{Ref, RefCell};
use std::rc::Rc;
use std::time::{SystemTime, UNIX_EPOCH};

use orion_broker::{TopicId, Topics};
use orion_keys::SharedKeys;
use orion_msg_session::{Identify, Identity, IdentityField};
use orion_msg_topics::Declare;
use orion_server::{
    Admits, Checked, Cx, Error, Incoming, Policy, Service, SessionId, Sessions, What,
};
use orion_types::NodeName;
use orion_wire::body::encode_fields;
use orion_wire::{Bytes, Decode, FrameView, INTERFACE_REVISION, List, kind};
use shm_ring::{FieldId, Reader};

/// What a board fact's topic name starts with.
pub const FACT_PREFIX: &str = "hello.";
/// The most facts a document carries.
pub const MAX_FACTS: usize = 64;
/// The longest fact value, in bytes. Longer ones are left out.
pub const MAX_FACT_BYTES: usize = 256;
use orion_keys::hello::signed_bytes;
pub use orion_keys::hello::{MAX_NONCE, verify};

/// Orion's fact: what the board is doing.
pub const STATE_FACT: &str = "state";
/// Orion's fact, in recovery: why.
pub const RECOVERY_WHY_FACT: &str = "recovery_why";
/// The number of the text field type, as facts carry it.
const TEXT: u8 = 4;

/// What the board is doing, as the hello document says.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BoardState {
    /// Up as it was told to be.
    Running,
    /// In recovery, and why: the comm core could not start as it was told to.
    Recovery(String),
    /// A flasher image: up to be written to, not to run anything (`hello.state = flasher`).
    Flasher,
    /// Being updated: what runs now is about to be replaced.
    Updating,
}

impl BoardState {
    /// The word the document carries.
    #[must_use]
    pub const fn word(&self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Recovery(_) => "recovery",
            Self::Flasher => "flasher",
            Self::Updating => "updating",
        }
    }
}

/// Where the hello service's [`BoardState`] is kept, to be changed while it runs.
#[derive(Clone, Debug)]
pub struct SharedState(Rc<RefCell<BoardState>>);

impl SharedState {
    /// Says the board is now `state`. The next answer carries it.
    pub fn set(&self, state: BoardState) {
        *self.0.borrow_mut() = state;
    }

    /// What the board is doing.
    #[must_use]
    pub fn get(&self) -> Ref<'_, BoardState> {
        self.0.borrow()
    }
}

/// One fact, read from its topic.
struct Fact {
    topic: TopicId,
    epoch: u64,
    name: String,
    ty: u8,
    reader: Reader,
    value: Vec<u8>,
}

/// The hello service.
pub struct Hello {
    node: NodeName,
    keys: SharedKeys,
    topics: Option<Rc<RefCell<Topics>>>,
    facts: Vec<Fact>,
    state: SharedState,
    payload: Vec<u8>,
}

impl std::fmt::Debug for Hello {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Hello({})", self.node)
    }
}

impl Hello {
    /// The hello service for `node`. With `topics`, the document carries the board facts
    /// published there; without, only Orion's own part.
    #[must_use]
    pub fn new(node: NodeName, keys: SharedKeys, topics: Option<Rc<RefCell<Topics>>>) -> Self {
        Self {
            node,
            keys,
            topics,
            facts: Vec::new(),
            state: SharedState(Rc::new(RefCell::new(BoardState::Running))),
            payload: Vec::new(),
        }
    }

    /// The same, saying the board is `state` from the first answer.
    #[must_use]
    pub fn in_state(self, state: BoardState) -> Self {
        self.state.set(state);
        self
    }

    /// Where the board's state is kept: set it there and the next answer says it.
    #[must_use]
    pub fn state(&self) -> SharedState {
        self.state.clone()
    }

    /// Brings the facts up to date with the topics that are there now, and reads each one's
    /// newest value.
    fn refresh(&mut self) {
        let Some(topics) = &self.topics else {
            return;
        };
        let topics = topics.borrow();
        // Keep the readers of topics that have not changed; drop the rest.
        self.facts.retain(|fact| {
            topics
                .get(fact.topic)
                .is_some_and(|topic| topic.epoch == fact.epoch && topic.ring.is_some())
        });
        for topic in topics.iter() {
            let Some(name) = topic.name.as_str().strip_prefix(FACT_PREFIX) else {
                continue;
            };
            if name == STATE_FACT || name == RECOVERY_WHY_FACT {
                continue;
            }
            let known = self.facts.iter().any(|fact| fact.topic == topic.id);
            if known || self.facts.len() >= MAX_FACTS {
                continue;
            }
            // A fact is the first field of its topic.
            let (Some(ring), Ok(declare)) = (&topic.ring, Declare::decode(&topic.declare)) else {
                continue;
            };
            let (Some(Ok(field)), Ok(reader)) = (declare.fields.iter().next(), ring.reader())
            else {
                continue;
            };
            self.facts.push(Fact {
                topic: topic.id,
                epoch: topic.epoch,
                name: name.to_owned(),
                ty: field.ty.code(),
                reader,
                value: Vec::new(),
            });
        }
        drop(topics);
        for fact in &mut self.facts {
            fact.value.clear();
            if let Some(sample) = fact.reader.latest(FieldId(0))
                && sample.payload.len() <= MAX_FACT_BYTES
            {
                fact.value.extend_from_slice(sample.payload);
            }
        }
        // In name order, so the same board gives the same document whatever started first.
        self.facts.sort_by(|a, b| a.name.cmp(&b.name));
    }
}

impl Service for Hello {
    fn kinds(&self) -> &'static [u8] {
        &[kind::IDENTIFY]
    }

    fn asks<'a>(&self, _frame: &FrameView<'a>) -> Result<(What<'a>, Policy), Error> {
        // Anyone, and an operator needs no grant: this is how a board is known at all.
        Ok((
            What::Read("hello"),
            Policy::new(Admits::ANYONE).without_grant(),
        ))
    }

    fn handle(&mut self, request: Checked<Incoming<'_>>, cx: &mut Cx<'_>) -> Result<(), Error> {
        let identify: Identify<'_> = request.get().body()?;
        if identify.nonce.len() > MAX_NONCE {
            return Err(Error::Malformed);
        }
        self.refresh();
        let state = self.state.get();
        let mut fields: Vec<IdentityField<'_>> = vec![IdentityField {
            name: STATE_FACT,
            ty: TEXT,
            value: Bytes(state.word().as_bytes()),
        }];
        if let BoardState::Recovery(why) = &*state {
            let mut end = why.len().min(MAX_FACT_BYTES);
            while !why.is_char_boundary(end) {
                end -= 1;
            }
            fields.push(IdentityField {
                name: RECOVERY_WHY_FACT,
                ty: TEXT,
                value: Bytes(&why.as_bytes()[..end]),
            });
        }
        fields.extend(
            self.facts
                .iter()
                .filter(|fact| !fact.value.is_empty())
                .map(|fact| IdentityField {
                    name: &fact.name,
                    ty: fact.ty,
                    value: Bytes(&fact.value),
                }),
        );
        // In name order, Orion's with the rest.
        fields.sort_by(|a, b| a.name.cmp(b.name));
        let keys = self.keys.lock();
        let public = keys.identity().public();
        let time_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |since| since.as_millis() as u64);
        let mut identity = Identity {
            request_seq: cx.request_seq(),
            node: self.node.clone(),
            cluster: keys.cluster(),
            public_key: Bytes(&public),
            time_ms,
            revision: INTERFACE_REVISION,
            fields: List::new(&fields),
            nonce: identify.nonce,
            signature: Bytes(&[]),
        };
        // Someone on the board who gave no nonce is not checking a signature: skip the cost.
        let wanted = !identify.nonce.is_empty() || !cx.caller().passes_descriptors;
        let signature;
        if wanted {
            self.payload.resize(cx.caller().max_frame as usize, 0);
            // Encoded once with no signature: all but its last byte, the empty signature's
            // length, is what gets signed.
            let len = encode_fields(&identity, &mut self.payload)?;
            signature = keys
                .identity()
                .sign(&signed_bytes(&self.payload[..len - 1]));
            identity.signature = Bytes(&signature);
        }
        cx.reply(&identity)
    }

    fn closed(&mut self, _session: SessionId, _sessions: &mut Sessions) {}
}
