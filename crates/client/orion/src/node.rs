//! Who a board is, what time it has, where its sync with other boards is, and enrolling an
//! operator with it.

use std::cell::RefCell;
use std::rc::Rc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use orion_connect::{Client, ClientError, Cx, Handler, Incoming};
use orion_identity::{fingerprint, hello, proof, random};
use orion_msg_peers::{
    CLOCK_REALTIME, OperatorEnroll, OperatorWelcome, PeerForgive, PeerStates, PeerStatus,
    SecurityPrecheck, SecurityPrechecked, TimeOffer, TimeReply, TimeRequest, TimeTurn,
};
pub use orion_msg_peers::{OperatorState, SecurityTo, SyncState};
use orion_msg_session::Identify;
use orion_types::NodeName;
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

/// A board's security switch, as its hello document says.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Security {
    /// A board too old to say.
    NotTold,
    /// On, with the generation of the record that says so.
    On {
        /// The record's generation, if the board gave it.
        generation: Option<u64>,
    },
    /// Off: its record says so, or it has none (then no generation).
    Off {
        /// The record's generation, if there is a record.
        generation: Option<u64>,
    },
    /// Its record cannot be read or is not one, so security is on.
    Damaged,
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

impl Node {
    /// What the board says it is doing: `running`, `recovery` or `flasher`.
    /// `None` from a board too old to say.
    #[must_use]
    pub fn state(&self) -> Option<&str> {
        self.fact_text("state")
    }

    /// Why the board is in recovery, if it is.
    #[must_use]
    pub fn recovery_why(&self) -> Option<&str> {
        self.fact_text("recovery_why")
    }

    /// The board's security switch, from its record: [`Security::NotTold`] from a board too
    /// old to say, and for a word this build does not know.
    #[must_use]
    pub fn security(&self) -> Security {
        let generation = self
            .fact_text("security_generation")
            .and_then(|text| text.parse().ok());
        match self.fact_text("security") {
            Some("on") => Security::On { generation },
            Some("off") => Security::Off { generation },
            Some("damaged") => Security::Damaged,
            _ => Security::NotTold,
        }
    }

    fn fact_text(&self, name: &str) -> Option<&str> {
        self.facts
            .iter()
            .find(|fact| fact.name == name)
            .and_then(|fact| std::str::from_utf8(&fact.value).ok())
    }
}

/// How a board's clock compares with this program's.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Clock {
    /// The board's clock minus this program's, in nanoseconds.
    pub offset_ns: i64,
    /// How long the question and its answer took on the way, in nanoseconds.
    pub delay_ns: u64,
}

/// Where a board's sync with one other board is.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SyncPeer {
    /// The other board.
    pub node: String,
    /// Where sync with it is.
    pub state: SyncState,
    /// When the two last agreed, in milliseconds since 1970, if they ever have.
    pub agreed_ms: Option<u64>,
}

/// A node's answer to enrolling an operator.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Enrollment {
    /// The node that answered.
    pub node: String,
    /// Its public key, and that key's fingerprint for people to compare.
    pub node_key: [u8; 32],
    /// The fingerprint of `node_key`.
    pub fingerprint: String,
    /// The cluster it belongs to.
    pub cluster: String,
    /// Where the operator is now: `Enrolled`, `Pending` (until someone on the board approves
    /// it), or `Revoked` (refused before, and it stays so).
    pub state: OperatorState,
    /// Whether the operator may read.
    pub read: bool,
    /// Whether it may run every action.
    pub all_actions: bool,
    /// The actions it may run, when not all.
    pub actions: Vec<String>,
}

#[derive(Default)]
pub(crate) struct State {
    identity: Option<Vec<u8>>,
    time: Option<(TimeReply, Instant, i128)>,
    peers: Option<(u16, Vec<SyncPeer>)>,
    welcome: Option<Enrollment>,
    prechecked: Option<(u16, Vec<String>)>,
    /// Whether this program offers its clock: it answers the board's `TimeRequest`.
    offering: bool,
    /// The board's answer to the offer: its `seq`, and how often it will ask.
    turned: Option<(u16, u32)>,
    /// How many times the board has asked for this program's clock.
    asked: u64,
}

impl Handler for State {
    fn kinds(&self) -> &'static [u8] {
        &[
            kind::IDENTITY,
            kind::TIME_REPLY,
            kind::PEER_STATES,
            kind::OPERATOR_WELCOME,
            kind::SECURITY_PRECHECKED,
            kind::TIME_REQUEST,
            kind::TIME_TURN,
        ]
    }

    fn handle(&mut self, frame: &Incoming<'_>, cx: &mut Cx<'_>) -> Result<(), ClientError> {
        if frame.kind() == kind::TIME_REQUEST {
            // Read as near the arrival as can be. Only an offer is asked, and only for the
            // wall clock: anything else is left unanswered, as a board's own question is.
            let received_ns = wall_ns();
            let asked: TimeRequest = frame.body()?;
            if self.offering && asked.clock == CLOCK_REALTIME {
                self.asked += 1;
                let as_u64 = |ns: i128| u64::try_from(ns).unwrap_or(0);
                cx.send(&TimeReply {
                    sent_ns: asked.sent_ns,
                    received_ns: as_u64(received_ns),
                    replied_ns: as_u64(wall_ns()),
                })?;
            }
        } else if frame.kind() == kind::TIME_TURN {
            let turn: TimeTurn = frame.body()?;
            self.turned = Some((turn.request_seq, turn.poll_ms));
        } else if frame.kind() == kind::SECURITY_PRECHECKED {
            let answer: SecurityPrechecked<'_> = frame.body()?;
            let lines = answer.lines.iter().flatten();
            let lines = lines.map(|line| line.line.to_owned()).collect();
            self.prechecked = Some((answer.request_seq, lines));
        } else if frame.kind() == kind::PEER_STATES {
            let states: PeerStates<'_> = frame.body()?;
            let peers = states
                .peers
                .iter()
                .flatten()
                .map(|peer| SyncPeer {
                    node: peer.node.to_string(),
                    state: peer.state,
                    agreed_ms: (peer.agreed_ms != 0).then_some(peer.agreed_ms),
                })
                .collect();
            self.peers = Some((states.request_seq, peers));
        } else if frame.kind() == kind::OPERATOR_WELCOME {
            let welcome: OperatorWelcome<'_> = frame.body()?;
            let operator = welcome
                .operator
                .iter()
                .flatten()
                .next()
                .ok_or(ClientError::Protocol("a welcome that names no operator"))?;
            let node_key: [u8; 32] = welcome
                .public_key
                .0
                .try_into()
                .map_err(|_| ClientError::Protocol("a node key that is not 32 bytes"))?;
            self.welcome = Some(Enrollment {
                node: welcome.node.to_string(),
                fingerprint: fingerprint(&node_key),
                node_key,
                cluster: welcome.cluster.to_owned(),
                state: operator.state,
                read: operator.read,
                all_actions: operator.all_actions,
                actions: operator
                    .actions
                    .iter()
                    .flatten()
                    .map(|action| action.name.to_owned())
                    .collect(),
            });
        } else if frame.kind() == kind::IDENTITY {
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
    let seq = client.send(&TimeRequest {
        sent_ns: token,
        clock: CLOCK_REALTIME,
    })?;
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

/// Offers this program's clock to the board at `stratum`: how often the board will ask.
pub(crate) fn offer_time(
    client: &Client,
    state: &Rc<RefCell<State>>,
    stratum: u8,
) -> Result<Duration, ClientError> {
    {
        let mut state = state.borrow_mut();
        state.offering = true;
        state.turned = None;
    }
    let seq = client.send(&TimeOffer { stratum })?;
    let turned = client.answered(seq, || {
        state.borrow().turned.is_some_and(|(asked, _)| asked == seq)
    });
    if turned.is_err() {
        state.borrow_mut().offering = false;
    }
    turned?;
    let poll_ms = state.borrow().turned.map_or(0, |(_, poll_ms)| poll_ms);
    Ok(Duration::from_millis(u64::from(poll_ms)))
}

/// How many times the board has asked for this program's clock.
pub(crate) fn times_asked(state: &Rc<RefCell<State>>) -> u64 {
    state.borrow().asked
}

/// Where sync is with each peer, after forgiving `forgive` if it is given.
pub(crate) fn peers(
    client: &Client,
    state: &Rc<RefCell<State>>,
    forgive: Option<&str>,
) -> Result<Vec<SyncPeer>, ClientError> {
    state.borrow_mut().peers = None;
    let seq = match forgive {
        Some(node) => client.send(&PeerForgive {
            node: node
                .parse::<NodeName>()
                .map_err(|_| ClientError::Protocol("a node name that is not one"))?,
        })?,
        None => client.send(&PeerStatus {})?,
    };
    client.answered(seq, || {
        state
            .borrow()
            .peers
            .as_ref()
            .is_some_and(|(answered, _)| *answered == seq)
    })?;
    let (_, peers) = state.borrow_mut().peers.take().ok_or(ClientError::Lost)?;
    Ok(peers)
}

/// What the comm core has against turning the security switch `to`: no line when nothing.
pub(crate) fn precheck(
    client: &Client,
    state: &Rc<RefCell<State>>,
    to: SecurityTo,
) -> Result<Vec<String>, ClientError> {
    client.needs(orion_connect::Feature::SecurityPrecheck)?;
    state.borrow_mut().prechecked = None;
    let seq = client.send(&SecurityPrecheck { to })?;
    client.answered(seq, || {
        let answered = state.borrow().prechecked.as_ref().map(|(seq, _)| *seq);
        answered == Some(seq)
    })?;
    let (_, lines) = state
        .borrow_mut()
        .prechecked
        .take()
        .ok_or(ClientError::Lost)?;
    Ok(lines)
}

/// Asks the node, `node` by name, to enrol `public_key` as an operator called `name`. With
/// `enrollment_key` the proof that the asker knows the cluster's enrollment key goes with it,
/// made here; the key itself is not sent.
pub(crate) fn enroll(
    client: &Client,
    state: &Rc<RefCell<State>>,
    node: &str,
    public_key: [u8; 32],
    name: &str,
    enrollment_key: Option<&[u8]>,
) -> Result<Enrollment, ClientError> {
    state.borrow_mut().welcome = None;
    let made =
        enrollment_key.map(|key| proof::proof(key, &proof::operator_proof(node, &public_key)));
    let seq = client.send(&OperatorEnroll {
        public_key: Bytes(&public_key),
        name,
        proof: Bytes(made.as_ref().map_or(&[][..], |made| &made[..])),
    })?;
    client.answered(seq, || state.borrow().welcome.is_some())?;
    state.borrow_mut().welcome.take().ok_or(ClientError::Lost)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(facts: &[(&str, &str)]) -> Node {
        Node {
            name: "board".into(),
            cluster: "lab".into(),
            public_key: [0; 32],
            fingerprint: String::new(),
            revision: 1,
            time_ms: 0,
            facts: facts
                .iter()
                .map(|(name, value)| Fact {
                    name: (*name).into(),
                    ty: 4,
                    value: value.as_bytes().to_vec(),
                })
                .collect(),
        }
    }

    #[test]
    fn the_security_switch_is_read_from_the_boards_facts() {
        let both =
            |word, generation| node(&[("security", word), ("security_generation", generation)]);
        assert_eq!(
            both("on", "7").security(),
            Security::On {
                generation: Some(7)
            }
        );
        assert_eq!(
            both("off", "2").security(),
            Security::Off {
                generation: Some(2)
            }
        );
        assert_eq!(
            node(&[("security", "off")]).security(),
            Security::Off { generation: None }
        );
        assert_eq!(
            node(&[("security", "damaged")]).security(),
            Security::Damaged
        );
        // An older board, or a word this build does not know: not told.
        assert_eq!(node(&[("state", "running")]).security(), Security::NotTold);
        assert_eq!(node(&[("security", "maybe")]).security(), Security::NotTold);
    }
}
