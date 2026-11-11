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
//! The relay keeps no record, no workload and no policy of its own beyond the table of
//! [`Route`]s: which kind asks for what, and who may ask.

use std::collections::HashSet;
use std::fmt::Write as _;

use orion_msg_session::{NodeAnswer, NodeAttach, NodeGone, NodeRequest};
use orion_server::{
    Admits, Checked, Cx, Delivery, Error, Incoming, Policy, Principal, Roles, Service, SessionId,
    Sessions, What,
};
use orion_wire::{Bytes, FrameView, kind};

/// How many standing requests are kept for one session. The oldest goes first.
pub const MAX_STANDING: usize = 64;

/// What a kind asks for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Access {
    /// Reads the named thing.
    Read(&'static str),
    /// Changes the named thing.
    Write(&'static str),
}

/// One kind that is node logic's to answer.
#[derive(Clone, Copy, Debug)]
pub struct Route {
    /// The kind.
    pub kind: u8,
    /// What it asks for.
    pub access: Access,
    /// Who may ask.
    pub admits: Admits,
    /// Whether the request stands until its session ends, and is passed again to a node
    /// logic that comes back: a watch.
    pub standing: bool,
}

/// Programs on this board, operators and peers.
const KNOWN: Admits = Admits::LOCAL.and(Admits::OPERATOR).and(Admits::PEER);

/// The record kinds.
pub const RECORDS: &[Route] = &[
    Route {
        kind: kind::PUT,
        access: Access::Write("records"),
        admits: KNOWN,
        standing: false,
    },
    Route {
        kind: kind::GET,
        access: Access::Read("records"),
        admits: KNOWN,
        standing: false,
    },
    Route {
        kind: kind::WATCH,
        access: Access::Read("records"),
        admits: KNOWN,
        standing: true,
    },
];

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

    fn keep(&mut self, session: SessionId, seq: u16, kind: u8, payload: &[u8]) {
        if let Some(same) = self.standing.iter_mut().find(|standing| {
            standing.session == session && standing.kind == kind && standing.payload == payload
        }) {
            same.seq = seq;
            return;
        }
        let held = self
            .standing
            .iter()
            .filter(|standing| standing.session == session)
            .count();
        if held >= MAX_STANDING
            && let Some(oldest) = self
                .standing
                .iter()
                .position(|standing| standing.session == session)
        {
            self.standing.remove(oldest);
        }
        self.standing.push(Standing {
            session,
            seq,
            kind,
            payload: payload.to_vec(),
        });
    }

    fn attach(&mut self, attach: &NodeAttach<'_>, cx: &mut Cx<'_>) -> Result<(), Error> {
        let node = cx.from();
        if let Some(old) = self.node.filter(|old| *old != node) {
            // One node logic at a time. The one that just came is the one that is alive.
            cx.sessions().close(old);
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
        Ok((what, Policy::new(route.admits)))
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
            self.keep(from, seq, kind, payload);
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
            return;
        }
        self.standing.retain(|standing| standing.session != session);
        if self.passed.remove(&session)
            && let Some(node) = self.node
        {
            let _ = sessions.send(node, &NodeGone { session: session.0 }, Delivery::Must);
        }
    }
}
