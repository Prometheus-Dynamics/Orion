//! Enrollment and the operator list, as a service.

use orion_config::Security;
use orion_msg_peers::{
    EnrollChallenge, EnrollConfirm, EnrollHello, EnrollRole, GrantedAction, OperatorEnroll,
    OperatorEntry, OperatorList, OperatorState, OperatorWelcome, Operators, PeerRevoke,
};
use orion_server::{
    Admits, Checked, Cx, Error, Incoming, Policy, Principal, Service, SessionId, Sessions, What,
};
use orion_types::NodeName;
use orion_wire::{Bytes, FrameView, List, kind};

use crate::enroll_impl::{NONCE_LEN, Side, Transcript, proof, verify_proof};
use crate::identity::{random, verify};
use crate::store::{Operator, SharedKeys};

/// A handshake this node has answered and is waiting to see confirmed.
struct Waiting {
    session: SessionId,
    role: EnrollRole,
    initiator: NodeName,
    initiator_key: [u8; 32],
    initiator_nonce: [u8; NONCE_LEN],
    nonce: [u8; NONCE_LEN],
}

/// The most handshakes left half done at once. One per session, and a session's goes with it.
const MAX_WAITING: usize = 64;

/// Enrollment, revocation and the operator list.
pub struct KeysService {
    node: NodeName,
    keys: SharedKeys,
    waiting: Vec<Waiting>,
}

impl std::fmt::Debug for KeysService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "KeysService({})", self.node)
    }
}

impl KeysService {
    /// The service for `node`'s trust store.
    #[must_use]
    pub const fn new(node: NodeName, keys: SharedKeys) -> Self {
        Self {
            node,
            keys,
            waiting: Vec::new(),
        }
    }
}

fn entry<'a>(operator: &'a Operator, actions: &'a [GrantedAction<'a>]) -> OperatorEntry<'a> {
    OperatorEntry {
        key: operator.key,
        state: operator.state,
        read: operator.read,
        actions: List::new(actions),
        all_actions: operator.all_actions,
    }
}

fn granted(operator: &Operator) -> Vec<GrantedAction<'_>> {
    operator
        .actions
        .iter()
        .map(|name| GrantedAction { name })
        .collect()
}

fn key_of(bytes: &[u8]) -> Result<[u8; 32], Error> {
    bytes.try_into().map_err(|_| Error::Malformed)
}

impl Service for KeysService {
    fn kinds(&self) -> &'static [u8] {
        &[
            kind::ENROLL_HELLO,
            kind::ENROLL_CONFIRM,
            kind::PEER_REVOKE,
            kind::OPERATOR_ENROLL,
            kind::OPERATOR_LIST,
        ]
    }

    fn asks<'a>(&self, frame: &FrameView<'a>) -> Result<(What<'a>, Policy), Error> {
        Ok(match frame.kind() {
            // Asking to be trusted is open to anyone, a key granted nothing included: what is
            // asked proves itself, or waits for an administrator.
            kind::ENROLL_HELLO | kind::ENROLL_CONFIRM | kind::OPERATOR_ENROLL => (
                What::Open("enroll"),
                Policy::new(Admits::ANYONE).without_grant(),
            ),
            kind::PEER_REVOKE => (What::Write("trust"), Policy::new(Admits::ROOT)),
            _ => (
                What::Read("operators"),
                Policy::new(Admits::ROOT.and(Admits::OPERATOR)),
            ),
        })
    }

    fn handle(&mut self, request: Checked<Incoming<'_>>, cx: &mut Cx<'_>) -> Result<(), Error> {
        let who = request.who().clone();
        let incoming = request.into_inner();
        let store = |_| Error::Internal;
        match incoming.kind() {
            kind::OPERATOR_ENROLL => {
                let enroll: OperatorEnroll<'_> = incoming.body()?;
                let key = key_of(&enroll.public_key)?;
                let mut keys = self.keys.lock();
                let cluster = keys.cluster().to_owned();
                let public = keys.identity().public();
                let operator = if keys.security() == Security::Off {
                    // The open mode: everyone may do everything, and nothing is recorded.
                    Operator {
                        key: orion_types::KeyId(key),
                        name: enroll.name.to_owned(),
                        state: OperatorState::Enrolled,
                        read: true,
                        actions: Vec::new(),
                        all_actions: true,
                    }
                } else {
                    match who {
                        // An administrator on the board says so.
                        Principal::Local { uid: 0, .. } => keys
                            .approve_operator(key, enroll.name)
                            .map_err(store)?
                            .clone(),
                        // The operator itself, on a connection that proved it holds the key.
                        Principal::Operator(own) if own.0 == key => keys
                            .operator_asks(&self.node, key, enroll.name, &enroll.proof)
                            .map_err(store)?,
                        // Anyone else may not speak for a key.
                        _ => return Err(Error::Denied),
                    }
                };
                drop(keys);
                let actions = granted(&operator);
                let entries = [entry(&operator, &actions)];
                cx.reply(&OperatorWelcome {
                    node: self.node.clone(),
                    public_key: Bytes(&public),
                    cluster: &cluster,
                    operator: List::new(&entries),
                })
            }
            kind::OPERATOR_LIST => {
                let _: OperatorList = incoming.body()?;
                let keys = self.keys.lock();
                let operators: Vec<&Operator> = keys.operators().collect();
                let actions: Vec<Vec<GrantedAction<'_>>> =
                    operators.iter().map(|op| granted(op)).collect();
                let entries: Vec<OperatorEntry<'_>> = operators
                    .iter()
                    .zip(&actions)
                    .map(|(operator, actions)| entry(operator, actions))
                    .collect();
                cx.reply(&Operators {
                    request_seq: cx.request_seq(),
                    operators: List::new(&entries),
                })
            }
            kind::PEER_REVOKE => {
                let revoke: PeerRevoke = incoming.body()?;
                self.keys.lock().revoke(revoke.key.0).map_err(store)
            }
            kind::ENROLL_HELLO => {
                let hello: EnrollHello<'_> = incoming.body()?;
                let keys = self.keys.lock();
                let secret = keys.enrollment_key().ok_or(Error::Unsupported)?;
                if hello.responder != self.node || hello.cluster != keys.cluster() {
                    return Err(Error::NotFound);
                }
                let initiator_key = key_of(&hello.public_key)?;
                let initiator_nonce: [u8; NONCE_LEN] =
                    hello.nonce.0.try_into().map_err(|_| Error::Malformed)?;
                if keys.is_revoked(&initiator_key) {
                    return Err(Error::Denied);
                }
                let nonce: [u8; NONCE_LEN] = random();
                let public = keys.identity().public();
                let message = Transcript {
                    role: hello.role.code(),
                    cluster: hello.cluster,
                    initiator: hello.initiator.as_str(),
                    initiator_key: &initiator_key,
                    initiator_nonce: &initiator_nonce,
                    responder: self.node.as_str(),
                    responder_key: &public,
                    responder_nonce: &nonce,
                }
                .message(Side::Responder);
                let proof = proof(secret, &message);
                let signature = keys.identity().sign(&message);
                drop(keys);
                // One handshake per session; a new hello replaces the last.
                self.waiting.retain(|waiting| waiting.session != cx.from());
                if self.waiting.len() >= MAX_WAITING {
                    return Err(Error::Busy);
                }
                self.waiting.push(Waiting {
                    session: cx.from(),
                    role: hello.role,
                    initiator: hello.initiator.clone(),
                    initiator_key,
                    initiator_nonce,
                    nonce,
                });
                cx.reply(&EnrollChallenge {
                    responder: self.node.clone(),
                    public_key: Bytes(&public),
                    nonce: Bytes(&nonce),
                    proof: Bytes(&proof),
                    signature: Bytes(&signature),
                })
            }
            _ => {
                let confirm: EnrollConfirm<'_> = incoming.body()?;
                // The handshake is used up whether or not it is confirmed: no second try
                // against the same nonce.
                let at = self
                    .waiting
                    .iter()
                    .position(|waiting| waiting.session == cx.from())
                    .ok_or(Error::OutOfOrder)?;
                let waiting = self.waiting.swap_remove(at);
                let mut keys = self.keys.lock();
                let secret = keys.enrollment_key().ok_or(Error::Unsupported)?;
                let public = keys.identity().public();
                let message = Transcript {
                    role: waiting.role.code(),
                    cluster: keys.cluster(),
                    initiator: waiting.initiator.as_str(),
                    initiator_key: &waiting.initiator_key,
                    initiator_nonce: &waiting.initiator_nonce,
                    responder: self.node.as_str(),
                    responder_key: &public,
                    responder_nonce: &waiting.nonce,
                }
                .message(Side::Initiator);
                let genuine = confirm.initiator == waiting.initiator
                    && confirm.responder == self.node
                    && confirm.nonce.0 == waiting.nonce
                    && verify_proof(secret, &message, &confirm.proof)
                    && verify(&waiting.initiator_key, &message, &confirm.signature);
                if !genuine {
                    return Err(Error::Denied);
                }
                let trusted = match waiting.role {
                    EnrollRole::Node => keys
                        .trust_peer(&waiting.initiator, waiting.initiator_key)
                        .map_err(store)?,
                    EnrollRole::Operator => {
                        let operator = keys
                            .approve_if_new(waiting.initiator_key, waiting.initiator.as_str())
                            .map_err(store)?;
                        operator.state == OperatorState::Enrolled
                    }
                    EnrollRole::Unknown(_) => false,
                };
                if !trusted {
                    return Err(Error::Denied);
                }
                // Answered with the same body: both sides now hold each other's key.
                cx.reply(&confirm)
            }
        }
    }

    fn closed(&mut self, session: SessionId, _sessions: &mut Sessions) {
        self.waiting.retain(|waiting| waiting.session != session);
    }
}
