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
//! **Always answers.** This lives in the comm core and needs nothing else to be running.

#![cfg(target_os = "linux")]

use std::cell::RefCell;
use std::rc::Rc;
use std::time::{SystemTime, UNIX_EPOCH};

use orion_broker::{TopicId, Topics};
use orion_keys::SharedKeys;
use orion_msg_session::{Identify, Identity, IdentityField};
use orion_msg_topics::Declare;
use orion_ring::{FieldId, Reader};
use orion_server::{
    Admits, Checked, Cx, Error, Incoming, Policy, Service, SessionId, Sessions, What,
};
use orion_types::NodeName;
use orion_wire::body::encode_fields;
use orion_wire::{Bytes, Decode, FrameView, INTERFACE_REVISION, List, kind};

/// What a board fact's topic name starts with.
pub const FACT_PREFIX: &str = "hello.";
/// The most facts a document carries.
pub const MAX_FACTS: usize = 64;
/// The longest fact value, in bytes. Longer ones are left out.
pub const MAX_FACT_BYTES: usize = 256;
/// The longest nonce, in bytes.
pub const MAX_NONCE: usize = 32;

const DOMAIN: &[u8] = b"orion-hello-v1";
const SIGNATURE_LEN: usize = 64;

/// The bytes a hello document's signature is over: a fixed label, then the document up to its
/// signature.
fn signed_bytes(unsigned: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(DOMAIN.len() + unsigned.len());
    out.extend_from_slice(DOMAIN);
    out.extend_from_slice(unsigned);
    out
}

/// Checks a hello document as it came off the wire: that it is signed by the key it names, and
/// that it answers `nonce`. Returns the document if so.
///
/// The caller still decides whether that key is the board it meant to reach.
#[must_use]
pub fn verify<'a>(payload: &'a [u8], nonce: &[u8]) -> Option<Identity<'a>> {
    let identity = Identity::decode(payload).ok()?;
    let key: [u8; 32] = identity.public_key.0.try_into().ok()?;
    // The signature is the last field: its length, then its 64 bytes.
    let unsigned = payload.len().checked_sub(SIGNATURE_LEN + 1)?;
    let signature_is_last =
        identity.signature.len() == SIGNATURE_LEN && payload[unsigned] == SIGNATURE_LEN as u8;
    let genuine = signature_is_last
        && identity.nonce.0 == nonce
        && orion_keys::verify(
            &key,
            &signed_bytes(&payload[..unsigned]),
            &identity.signature,
        );
    genuine.then_some(identity)
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
            payload: Vec::new(),
        }
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
        let fields: Vec<IdentityField<'_>> = self
            .facts
            .iter()
            .filter(|fact| !fact.value.is_empty())
            .map(|fact| IdentityField {
                name: &fact.name,
                ty: fact.ty,
                value: Bytes(&fact.value),
            })
            .collect();
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
