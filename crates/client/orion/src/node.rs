//! Who a board is, and what time it has.

use std::cell::RefCell;
use std::rc::Rc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use orion_connect::{Client, ClientError, Cx, Handler, Incoming};
use orion_identity::{fingerprint, hello, random};
use orion_msg_peers::{TimeReply, TimeRequest};
use orion_msg_session::Identify;
use orion_wire::{Bytes, kind};

/// One fact about a board, as some program on it published it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Fact {
    /// The fact's name.
    pub name: String,
    /// The number of its field type.
    pub ty: u8,
    /// Its value, as that type's payload.
    pub value: Vec<u8>,
}

/// Who a board says it is, checked against its own signature.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Node {
    /// Its name.
    pub name: String,
    /// The cluster it belongs to.
    pub cluster: String,
    /// Its public key.
    pub public_key: [u8; 32],
    /// That key's fingerprint, for people to compare.
    pub fingerprint: String,
    /// The highest interface revision it speaks.
    pub revision: u16,
    /// Its clock when it answered, in milliseconds since 1970.
    pub time_ms: u64,
    /// Facts about it that programs on it published.
    pub facts: Vec<Fact>,
}

/// How a board's clock compares with this program's.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Clock {
    /// The board's clock minus this program's, in nanoseconds.
    pub offset_ns: i64,
    /// How long the question and its answer took on the way, in nanoseconds.
    pub delay_ns: u64,
}

#[derive(Default)]
pub(crate) struct State {
    identity: Option<Vec<u8>>,
    time: Option<(TimeReply, Instant, i128)>,
}

impl Handler for State {
    fn kinds(&self) -> &'static [u8] {
        &[kind::IDENTITY, kind::TIME_REPLY]
    }

    fn handle(&mut self, frame: &Incoming<'_>, _cx: &mut Cx<'_>) -> Result<(), ClientError> {
        if frame.kind() == kind::IDENTITY {
            self.identity = Some(frame.payload().to_vec());
        } else {
            self.time = Some((frame.body()?, Instant::now(), wall_ns()));
        }
        Ok(())
    }
}

fn wall_ns() -> i128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| since.as_nanos() as i128)
}

pub(crate) fn node(client: &Client, state: &Rc<RefCell<State>>) -> Result<Node, ClientError> {
    // A fresh question: the answer is signed over it, so it was made for this asker now.
    let nonce: [u8; hello::MAX_NONCE] = random();
    state.borrow_mut().identity = None;
    let seq = client.send(&Identify {
        nonce: Bytes(&nonce),
    })?;
    client.answered(seq, || state.borrow().identity.is_some())?;
    let payload = state.borrow_mut().identity.take().unwrap_or_default();
    let document = hello::verify(&payload, &nonce).ok_or(ClientError::Denied(
        "the board's answer is not signed by the key it names",
    ))?;
    let mut public_key = [0_u8; 32];
    public_key.copy_from_slice(document.public_key.0);
    Ok(Node {
        name: document.node.to_string(),
        cluster: document.cluster.to_owned(),
        fingerprint: fingerprint(&public_key),
        public_key,
        revision: document.revision,
        time_ms: document.time_ms,
        facts: document
            .fields
            .iter()
            .flatten()
            .map(|field| Fact {
                name: field.name.to_owned(),
                ty: field.ty,
                value: field.value.0.to_vec(),
            })
            .collect(),
    })
}

pub(crate) fn clock(client: &Client, state: &Rc<RefCell<State>>) -> Result<Clock, ClientError> {
    state.borrow_mut().time = None;
    let (asked, asked_wall) = (Instant::now(), wall_ns());
    // The token only has to come back unchanged.
    let token = (asked_wall as u64).max(1);
    let seq = client.send(&TimeRequest { sent_ns: token })?;
    client.answered(seq, || state.borrow().time.is_some())?;
    let (reply, heard, _) = state.borrow_mut().time.take().ok_or(ClientError::Lost)?;
    if reply.sent_ns != token {
        return Err(ClientError::Protocol("an answer to another question"));
    }
    let round = heard.duration_since(asked).as_nanos() as i128;
    let held = (i128::from(reply.replied_ns) - i128::from(reply.received_ns)).clamp(0, round);
    // The four times of a clock comparison: asked and heard here, received and replied there.
    let heard_wall = asked_wall + round;
    let offset = ((i128::from(reply.received_ns) - asked_wall)
        + (i128::from(reply.replied_ns) - heard_wall))
        / 2;
    Ok(Clock {
        offset_ns: offset.clamp(i128::from(i64::MIN), i128::from(i64::MAX)) as i64,
        delay_ns: (round - held).max(0) as u64,
    })
}
