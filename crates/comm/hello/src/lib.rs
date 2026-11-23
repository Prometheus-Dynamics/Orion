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
//! **Orion's own facts.** Four names are Orion's and a program's fact by any of them is left
//! out: `state`, what the board is doing (`running`, `recovery` or `flasher`); in recovery,
//! `recovery_why`, why; `security`, the board's switch as its record says (`on`, `off`, or
//! `damaged` for a record that cannot be read or is not one, which is on); and
//! `security_generation`, the record's generation, when it was read. All are text, and signed
//! with the rest, so a tool knows a board in recovery from one that is up, and a secured board
//! from an open one, before it asks for anything else. The broker refuses a program that
//! declares any of them ([`orion_broker::NODE_FACTS`]).
//!
//! **Always answers.** This lives in the comm core and needs nothing else to be running, and
//! in recovery too.

#![cfg_attr(not(test), deny(clippy::unwrap_used))]
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
use orion_wire::frame::frame_len;
use orion_wire::{Bytes, Decode, FrameView, INTERFACE_REVISION, List, kind};
use shm_ring::{FieldId, Reader};
use tracing::warn;

/// What a board fact's topic name starts with.
pub const FACT_PREFIX: &str = "hello.";
/// The most facts a document carries.
pub const MAX_FACTS: usize = 64;
/// The most a document can be, encoded: every fact at its longest, with room for its name,
/// and the rest of the document.
const MAX_DOCUMENT: usize = MAX_FACTS * (MAX_FACT_BYTES + 128) + 1024;
/// What the signature adds to a document encoded without one: an ed25519 signature's bytes.
const SIGNATURE_LEN: usize = 64;
/// The longest fact value, in bytes. Longer ones are left out, and the log says so once for
/// each fact.
pub const MAX_FACT_BYTES: usize = 1024;
use orion_keys::hello::signed_bytes;
pub use orion_keys::hello::{MAX_NONCE, verify};

/// Orion's fact: what the board is doing.
pub const STATE_FACT: &str = "state";
/// Orion's fact, in recovery: why.
pub const RECOVERY_WHY_FACT: &str = "recovery_why";
/// Orion's fact: the board's security switch, `on`, `off` or `damaged`.
pub const SECURITY_FACT: &str = "security";
/// Orion's fact: the generation of the board's security record, when it was read.
pub const SECURITY_GENERATION_FACT: &str = "security_generation";
/// Orion's fact, when the board's time sync measures: `synced` or `unset`.
pub const CLOCK_FACT: &str = "clock";
/// Orion's fact, when settings were given that nothing reads: their names, `, ` between.
pub const SETTINGS_UNREAD_FACT: &str = "settings_unread";
/// Orion's own facts: a program's fact by one of these names is left out.
const OWN_FACTS: [&str; 6] = [
    STATE_FACT,
    RECOVERY_WHY_FACT,
    SECURITY_FACT,
    SECURITY_GENERATION_FACT,
    CLOCK_FACT,
    SETTINGS_UNREAD_FACT,
];
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
}

impl BoardState {
    /// The word the document carries.
    #[must_use]
    pub const fn word(&self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Recovery(_) => "recovery",
            Self::Flasher => "flasher",
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
    /// The log has said this fact is too long to carry.
    told_too_long: bool,
}

/// The hello service.
pub struct Hello {
    node: NodeName,
    keys: SharedKeys,
    topics: Option<Rc<RefCell<Topics>>>,
    facts: Vec<Fact>,
    state: SharedState,
    /// The `security` fact, and `security_generation`, if told.
    security: Option<(&'static str, Option<String>)>,
    /// The `settings_unread` fact, if there is something to say.
    unread: Option<String>,
    /// The `clock` fact as it is when asked, if told.
    clock: Option<Box<dyn Fn() -> Option<&'static str>>>,
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
            security: None,
            unread: None,
            clock: None,
            payload: Vec::new(),
        }
    }

    /// The same, saying the board is `state` from the first answer.
    #[must_use]
    pub fn in_state(self, state: BoardState) -> Self {
        self.state.set(state);
        self
    }

    /// The same, saying the board's security switch as `view`, its record, has it. Without
    /// this the document says nothing of it, and a tool reads "not told".
    #[must_use]
    pub fn with_security(mut self, view: &orion_config::SecurityView) -> Self {
        let word = match (view.fault.is_some(), view.security) {
            (true, _) => "damaged",
            (false, orion_config::Security::On) => "on",
            (false, orion_config::Security::Off) => "off",
        };
        let generation = view
            .generation
            .filter(|_| view.fault.is_none())
            .map(|generation| generation.to_string());
        self.security = Some((word, generation));
        self
    }

    /// The same, saying which settings were given that nothing reads, each as it was written.
    /// With none, the document says nothing of it. Cut to what a fact carries.
    #[must_use]
    pub fn with_unread(mut self, names: &[String]) -> Self {
        let mut said = names.join(", ");
        let mut end = said.len().min(MAX_FACT_BYTES);
        while !said.is_char_boundary(end) {
            end -= 1;
        }
        said.truncate(end);
        self.unread = Some(said).filter(|said| !said.is_empty());
        self
    }

    /// The same, saying the board's clock as `word` has it when asked: `synced` or `unset`, or
    /// nothing. Without this the document says nothing of it.
    #[must_use]
    pub fn with_clock(mut self, word: impl Fn() -> Option<&'static str> + 'static) -> Self {
        self.clock = Some(Box::new(word));
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
            if OWN_FACTS.contains(&name) {
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
                told_too_long: false,
            });
        }
        drop(topics);
        for fact in &mut self.facts {
            fact.value.clear();
            let Some(sample) = fact.reader.latest(FieldId(0)) else {
                continue;
            };
            let bytes = sample.payload.len();
            if bytes <= MAX_FACT_BYTES {
                fact.value.extend_from_slice(sample.payload);
            } else if !fact.told_too_long {
                fact.told_too_long = true;
                warn!(
                    "hello.{}: {bytes} bytes is over the {MAX_FACT_BYTES} bytes a fact may be: \
                     left out of the hello",
                    fact.name
                );
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
        if let Some((word, generation)) = &self.security {
            fields.push(IdentityField {
                name: SECURITY_FACT,
                ty: TEXT,
                value: Bytes(word.as_bytes()),
            });
            if let Some(generation) = generation {
                fields.push(IdentityField {
                    name: SECURITY_GENERATION_FACT,
                    ty: TEXT,
                    value: Bytes(generation.as_bytes()),
                });
            }
        }
        if let Some(unread) = &self.unread {
            fields.push(IdentityField {
                name: SETTINGS_UNREAD_FACT,
                ty: TEXT,
                value: Bytes(unread.as_bytes()),
            });
        }
        if let Some(word) = self.clock.as_ref().and_then(|clock| clock()) {
            fields.push(IdentityField {
                name: CLOCK_FACT,
                ty: TEXT,
                value: Bytes(word.as_bytes()),
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
        // Encoded once with no signature: all but its last byte, the empty signature's length,
        // is what gets signed.
        self.payload.resize(MAX_DOCUMENT, 0);
        let len = encode_fields(&identity, &mut self.payload)?;
        // Too large for the caller's frames, a link's among them: refused, saying so, where it
        // would otherwise be dropped on the way or end the session.
        let frame = frame_len(len + if wanted { SIGNATURE_LEN } else { 0 });
        let limit = cx.caller().max_frame as usize;
        if frame > limit {
            drop(keys);
            return cx.refuse(
                Error::BufferTooSmall,
                &format!(
                    "the hello is {frame} bytes, over the {limit} bytes a frame to this \
                     session may be"
                ),
            );
        }
        let signature;
        if wanted {
            signature = keys
                .identity()
                .sign(&signed_bytes(&self.payload[..len - 1]));
            identity.signature = Bytes(&signature);
        }
        cx.reply(&identity)
    }

    fn closed(&mut self, _session: SessionId, _sessions: &mut Sessions) {}

    fn quiet(&mut self, _session: SessionId, _quiet_ms: u64, _sessions: &mut Sessions) {}
}
