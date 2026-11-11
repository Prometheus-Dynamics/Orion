//! The relay: how node logic is the comm core's first client.
//!
//! Records, workloads, actions and files are node logic's to answer, and node logic is another
//! process. It connects like any program, with the node role, and says which kinds it answers
//! ([`NodeAttach`]). From then on a request of one of those kinds is checked here, at the
//! door, and passed on inside a [`NodeRequest`] that names the session and the caller. Node
//! logic answers with [`NodeAnswer`], which is sent to that session as it is.
//!
//! When node logic is away, the same requests are refused with [`Error::Unavailable`]:
//! "restarting". Nothing else in the comm core notices. When it comes back, the requests that
//! stand (a watch) are passed to it again, so a program that was watching goes on being told.
//!
//! The relay keeps no record and no workload. What it knows is the table of [`Route`]s: which
//! kind asks for what, and who may ask. For an action that is whoever the action was
//! registered for: a `Register` stands, so the relay has it, and a `Run` is checked against
//! the policy in it. That keeps the one check at the door for actions too, though the list of
//! actions is node logic's.

use std::collections::{HashSet, VecDeque};
use std::fmt::Write as _;

use orion_msg_actions::{Register, Target};
use orion_msg_session::{NodeAnswer, NodeAttach, NodeGone, NodeRequest, Refusal};
use orion_server::{
    Admits, Checked, Cx, Delivery, Error, Incoming, Policy, Principal, Roles, Service, SessionId,
    Sessions, What,
};
use orion_wire::{Bytes, Decode, FrameView, body, kind};

/// How many standing requests are kept for one session. One more is refused as busy.
pub const MAX_STANDING: usize = 1024;

/// What a kind asks for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Access {
    /// Reads the named thing.
    Read(&'static str),
    /// Changes the named thing.
    Write(&'static str),
}

/// Who may send a kind.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Who {
    /// These classes. An operator also needs the grant.
    Granted(Admits),
    /// These classes, and an operator needs no grant: what is asked is already bound to the
    /// session that asks, like a word about a run it started.
    Any(Admits),
    /// Whoever the action being run was registered for: the policy in the standing
    /// `Register` with the same name and target. With none here, anyone this node knows: the
    /// action may be another node's, and that node's door decides.
    ActionPolicy,
}

/// What ends a request that was passed on, so that the relay knows which callers are still
/// waiting if node logic dies: they are told `unavailable` instead of waiting for ever.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Ends {
    /// Nothing is waited for: a word about a run, which has no answer.
    Nothing,
    /// An answer that names the request's `seq` first: `Changed`, `Registered`, a refusal.
    Answer,
    /// The `ActionResult` of the run the request names, or a refusal.
    ActionResult,
}

/// One kind that is node logic's to answer.
#[derive(Clone, Copy, Debug)]
pub struct Route {
    /// The kind.
    pub kind: u8,
    /// What it asks for.
    pub access: Access,
    /// Who may ask.
    pub who: Who,
    /// Whether the request stands until its session ends, and is passed again to a node
    /// logic that comes back: a watch.
    pub standing: bool,
    /// What ends it, if it does not stand.
    pub ends: Ends,
}

/// Programs on this board, operators and peers.
const KNOWN: Admits = Admits::LOCAL.and(Admits::OPERATOR).and(Admits::PEER);

/// The record kinds.
pub const RECORDS: &[Route] = &[
    Route {
        kind: kind::PUT,
        access: Access::Write("records"),
        who: Who::Granted(KNOWN),
        standing: false,
        ends: Ends::Answer,
    },
    Route {
        kind: kind::GET,
        access: Access::Read("records"),
        who: Who::Granted(KNOWN),
        standing: false,
        ends: Ends::Answer,
    },
    Route {
        kind: kind::WATCH,
        access: Access::Read("records"),
        who: Who::Granted(KNOWN),
        standing: true,
        ends: Ends::Nothing,
    },
];

/// The action kinds. A run is decided by the policy its action was registered with.
pub const ACTIONS: &[Route] = &[
    Route {
        kind: kind::ACTION_REGISTER,
        access: Access::Write("actions"),
        who: Who::Granted(Admits::LOCAL),
        standing: true,
        ends: Ends::Nothing,
    },
    Route {
        kind: kind::ACTION_RUN,
        access: Access::Read("actions"),
        who: Who::ActionPolicy,
        standing: false,
        ends: Ends::ActionResult,
    },
    Route {
        kind: kind::ACTION_PROGRESS,
        access: Access::Write("actions"),
        who: Who::Any(Admits::LOCAL),
        standing: false,
        ends: Ends::Nothing,
    },
    Route {
        kind: kind::ACTION_RESULT,
        access: Access::Write("actions"),
        who: Who::Any(Admits::LOCAL),
        standing: false,
        ends: Ends::Nothing,
    },
    Route {
        kind: kind::ACTION_CANCEL,
        access: Access::Read("actions"),
        who: Who::Any(KNOWN),
        standing: false,
        ends: Ends::Nothing,
    },
    Route {
        kind: kind::ACTION_ATTACH,
        access: Access::Read("actions"),
        who: Who::Any(KNOWN),
        standing: false,
        ends: Ends::ActionResult,
    },
];

/// The kinds of a file being sent to this node. Their answers name the transfer, not the
/// request, and a sender that hears nothing begins again where it was.
pub const FILES: &[Route] = &[
    Route {
        kind: kind::FILE_BEGIN,
        access: Access::Write("files"),
        who: Who::Granted(KNOWN),
        standing: false,
        ends: Ends::Nothing,
    },
    Route {
        kind: kind::FILE_CHUNK,
        access: Access::Write("files"),
        who: Who::Granted(KNOWN),
        standing: false,
        ends: Ends::Nothing,
    },
    Route {
        kind: kind::FILE_COMMIT,
        access: Access::Write("files"),
        who: Who::Granted(KNOWN),
        standing: false,
        ends: Ends::Nothing,
    },
];

/// Every kind that is node logic's.
#[must_use]
pub fn routes() -> Vec<Route> {
    [RECORDS, ACTIONS, FILES].concat()
}

/// How many requests are remembered as waiting on node logic. The oldest goes first.
pub const MAX_WAITING: usize = 8192;

/// A request that was passed on and has not been seen to end.
#[derive(Debug)]
struct Waiting {
    session: SessionId,
    seq: u16,
    /// The run it names, for one that ends with an `ActionResult`.
    call: Option<u64>,
}

#[derive(Debug)]
struct Standing {
    session: SessionId,
    seq: u16,
    kind: u8,
    payload: Vec<u8>,
}

/// The relay service.
#[derive(Debug)]
pub struct Relay {
    routes: Vec<Route>,
    kinds: &'static [u8],
    /// The session that is node logic, once it has attached.
    node: Option<SessionId>,
    /// The kinds it said it answers, one bit each.
    answers: [u64; 4],
    /// Sessions node logic has been passed a request from, to tell it when they end.
    passed: HashSet<SessionId>,
    standing: Vec<Standing>,
    waiting: VecDeque<Waiting>,
    caller: String,
}

impl Relay {
    /// A relay for `routes`.
    ///
    /// # Panics
    ///
    /// If a route names one of the relay's own kinds.
    #[must_use]
    pub fn new(routes: &[Route]) -> Self {
        let mut kinds = vec![kind::NODE_ATTACH, kind::NODE_ANSWER];
        for route in routes {
            assert!(!kinds.contains(&route.kind), "a kind is routed once");
            kinds.push(route.kind);
        }
        Self {
            routes: routes.to_vec(),
            // A service's kinds live as long as the program: the relay is made once.
            kinds: Box::leak(kinds.into_boxed_slice()),
            node: None,
            answers: [0; 4],
            passed: HashSet::new(),
            standing: Vec::new(),
            waiting: VecDeque::new(),
            caller: String::new(),
        }
    }

    /// Whether node logic is attached.
    #[must_use]
    pub const fn attached(&self) -> bool {
        self.node.is_some()
    }

    fn route(&self, kind: u8) -> Option<&Route> {
        self.routes.iter().find(|route| route.kind == kind)
    }

    const fn answered(&self, kind: u8) -> bool {
        self.answers[(kind >> 6) as usize] & (1 << (kind & 63)) != 0
    }

    /// Passes one request to node logic. `false` if node logic could not be sent it.
    fn pass(
        &mut self,
        sessions: &mut Sessions,
        node: SessionId,
        from: SessionId,
        seq: u16,
        kind: u8,
        payload: &[u8],
    ) -> bool {
        let Some(info) = sessions.get(from) else {
            return true;
        };
        self.caller.clear();
        let mut uid = 0;
        match &info.principal {
            Principal::Anonymous => {}
            Principal::Local { uid: user, .. } => uid = *user,
            Principal::Operator(key) => {
                for byte in key.0 {
                    let _ = write!(self.caller, "{byte:02x}");
                }
            }
            Principal::Peer(name) => self.caller.push_str(name.as_str()),
            Principal::Device(name) => self.caller.push_str(name.as_str()),
        }
        let request = NodeRequest {
            session: from.0,
            seq,
            kind,
            class: info.principal.class() as u8,
            uid,
            caller: &self.caller,
            payload: Bytes(payload),
        };
        if sessions.send(node, &request, Delivery::Must).is_err() {
            return false;
        }
        self.passed.insert(from);
        true
    }

    fn keep(
        &mut self,
        session: SessionId,
        seq: u16,
        kind: u8,
        payload: &[u8],
    ) -> Result<(), Error> {
        if let Some(same) = self.standing.iter_mut().find(|standing| {
            standing.session == session && standing.kind == kind && standing.payload == payload
        }) {
            same.seq = seq;
            return Ok(());
        }
        let held = self
            .standing
            .iter()
            .filter(|standing| standing.session == session)
            .count();
        if held >= MAX_STANDING {
            return Err(Error::Busy);
        }
        self.standing.push(Standing {
            session,
            seq,
            kind,
            payload: payload.to_vec(),
        });
        Ok(())
    }

    /// The policy the action a `Run` names was registered with here. An action that is not
    /// registered here may live on another node: such a run is let through for anyone this
    /// node knows, and node logic carries it to the node that has the action, whose door
    /// decides by the policy it was registered with there, or says there is none.
    fn action_policy<'a>(&self, run: &'a [u8]) -> Result<(&'a str, Policy), Error> {
        let (_call, rest) = body::take::<u64>(run)?;
        let (name, rest) = body::take::<&str>(rest)?;
        let (target, _) = body::take::<Target>(rest)?;
        let policy = self
            .standing
            .iter()
            .filter(|standing| standing.kind == kind::ACTION_REGISTER)
            .filter_map(|standing| Register::decode(&standing.payload).ok())
            .find(|register| register.name.as_str() == name && register.target == target)
            .map_or(Policy::new(KNOWN), |register| register.policy);
        Ok((name, policy))
    }

    /// Notes that an answer ends a request that was waiting.
    fn ended(&mut self, answer: &NodeAnswer<'_>) {
        if self.waiting.is_empty() {
            return;
        }
        let to = answer.session;
        match answer.kind {
            kind::REFUSAL | kind::CHANGED | kind::ACTION_REGISTERED => {
                if let Ok((seq, _)) = body::take::<u16>(answer.payload.0) {
                    self.waiting
                        .retain(|waiting| !(waiting.session.0 == to && waiting.seq == seq));
                }
            }
            kind::ACTION_RESULT => {
                if let Ok((call, _)) = body::take::<u64>(answer.payload.0) {
                    self.waiting
                        .retain(|waiting| !(waiting.session.0 == to && waiting.call == Some(call)));
                }
            }
            _ => {}
        }
    }

    /// Node logic is gone: whoever was waiting on it is told, instead of waiting for ever.
    fn fail_waiting(&mut self, sessions: &mut Sessions) {
        for waiting in self.waiting.drain(..) {
            let refusal = Refusal {
                request_seq: waiting.seq,
                code: Error::Unavailable.code(),
                detail: "node logic went away",
            };
            let _ = sessions.send(waiting.session, &refusal, Delivery::Must);
        }
    }

    fn attach(&mut self, attach: &NodeAttach<'_>, cx: &mut Cx<'_>) -> Result<(), Error> {
        let node = cx.from();
        if let Some(old) = self.node.filter(|old| *old != node) {
            // One node logic at a time. The one that just came is the one that is alive.
            cx.sessions().close(old);
            self.fail_waiting(cx.sessions());
        }
        self.answers = [0; 4];
        for kind in attach.kinds.0 {
            if self.route(*kind).is_some() {
                self.answers[(kind >> 6) as usize] |= 1 << (kind & 63);
            }
        }
        self.node = Some(node);
        self.passed.clear();
        let standing = std::mem::take(&mut self.standing);
        for request in &standing {
            if self.answered(request.kind)
                && !self.pass(
                    cx.sessions(),
                    node,
                    request.session,
                    request.seq,
                    request.kind,
                    &request.payload,
                )
            {
                break;
            }
        }
        self.standing = standing;
        Ok(())
    }
}

impl Service for Relay {
    fn kinds(&self) -> &'static [u8] {
        self.kinds
    }

    fn asks<'a>(&self, frame: &FrameView<'a>) -> Result<(What<'a>, Policy), Error> {
        let Some(route) = self.route(frame.kind()) else {
            // Node logic's own frames: a program on this board, and then only the one with
            // the node role, which the socket gives to root and to the users it is told.
            return Ok((
                What::Write("node"),
                Policy::new(Admits::LOCAL).without_grant(),
            ));
        };
        let what = match route.access {
            Access::Read(thing) => What::Read(thing),
            Access::Write(thing) => What::Write(thing),
        };
        match route.who {
            Who::Granted(admits) => Ok((what, Policy::new(admits))),
            Who::Any(admits) => Ok((what, Policy::new(admits).without_grant())),
            Who::ActionPolicy => {
                let (name, policy) = self.action_policy(frame.payload())?;
                Ok((What::Run(name), policy))
            }
        }
    }

    fn handle(&mut self, request: Checked<Incoming<'_>>, cx: &mut Cx<'_>) -> Result<(), Error> {
        let incoming = request.get();
        let (from, seq, kind) = (cx.from(), cx.request_seq(), incoming.kind());
        if kind == kind::NODE_ATTACH || kind == kind::NODE_ANSWER {
            if !cx.caller().roles.has(Roles::NODE) {
                return Err(Error::Denied);
            }
            if kind == kind::NODE_ATTACH {
                return self.attach(&incoming.body()?, cx);
            }
            if self.node != Some(from) {
                return Err(Error::Denied);
            }
            let answer: NodeAnswer<'_> = incoming.body()?;
            self.ended(&answer);
            if answer.kind == kind::REFUSAL
                && let Ok(refusal) = Refusal::decode(answer.payload.0)
            {
                // Node logic turned a standing request down: it does not stand.
                self.standing.retain(|standing| {
                    !(standing.session.0 == answer.session && standing.seq == refusal.request_seq)
                });
            }
            let delivery = if answer.may_drop {
                Delivery::MayDrop
            } else {
                Delivery::Must
            };
            // A session that is gone, or was not reading and is now closed, is not node
            // logic's failure. It is told the session ended.
            let _ = cx.sessions().send_frame(
                SessionId(answer.session),
                answer.kind,
                answer.payload.0,
                [],
                delivery,
            );
            return Ok(());
        }
        let standing = self.route(kind).is_some_and(|route| route.standing);
        let payload = incoming.frame().payload();
        if standing {
            self.keep(from, seq, kind, payload)?;
        }
        let Some(node) = self.node.filter(|_| self.answered(kind)) else {
            return if standing {
                // Kept, and passed on when node logic comes.
                Ok(())
            } else {
                Err(Error::Unavailable)
            };
        };
        if self.pass(cx.sessions(), node, from, seq, kind, payload) {
            let ends = self.route(kind).map_or(Ends::Nothing, |route| route.ends);
            if ends != Ends::Nothing && !standing {
                if self.waiting.len() >= MAX_WAITING {
                    self.waiting.pop_front();
                }
                self.waiting.push_back(Waiting {
                    session: from,
                    seq,
                    call: (ends == Ends::ActionResult)
                        .then(|| body::take::<u64>(payload).ok().map(|(call, _)| call))
                        .flatten(),
                });
            }
            Ok(())
        } else {
            self.node = None;
            if standing {
                Ok(())
            } else {
                Err(Error::Unavailable)
            }
        }
    }

    fn closed(&mut self, session: SessionId, sessions: &mut Sessions) {
        if self.node == Some(session) {
            self.node = None;
            self.answers = [0; 4];
            self.passed.clear();
            self.fail_waiting(sessions);
            return;
        }
        self.waiting.retain(|waiting| waiting.session != session);
        self.standing.retain(|standing| standing.session != session);
        if self.passed.remove(&session)
            && let Some(node) = self.node
        {
            let _ = sessions.send(node, &NodeGone { session: session.0 }, Delivery::Must);
        }
    }
}
