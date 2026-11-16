//! The action router: node logic's own action list, `orion-action-list`, behind the comm
//! core's door.
//!
//! **The stub and the node share the action list.** Nothing here decides anything about a run.
//! Each action frame is passed to the list as node logic is passed it on a board, and what the
//! list says is sent on. The list is told when a session ends, and told the time so that
//! deadlines pass.
//!
//! The only thing kept here is what the comm core keeps on a board too: each registration as it
//! came. Its policy is what the door decides a `Run` by, and it is handed to the list again when
//! node logic starts again ([`Router::restart`]). Run handles, attach, cancel, deadlines,
//! callers gone and refusals are all the list's.
//!
//! A stand-in with rules of its own drifts from the node and hides what the node does, so
//! `tests/one_list.rs` fails if one appears anywhere in the workspace.

use std::cell::RefCell;
use std::rc::Rc;
use std::time::Instant;

use orion_action_list::{Actions, Event};
use orion_msg_actions::Register;
use orion_reply::{Asked, Out};
use orion_server::{
    Admits, Checked, Cx, Delivery, Error, Incoming, Policy, Principal, Service, SessionId,
    Sessions, What,
};
use orion_types::NodeName;
use orion_wire::{Decode, FrameView, body, kind};

/// Who may run an action nobody registered here: what a board's comm core lets through to
/// node logic, which then says it has no such action.
const KNOWN: Admits = Admits::LOCAL.and(Admits::OPERATOR).and(Admits::PEER);

/// Room to encode an answer in, as node logic keeps.
const ROOM: usize = 64 * 1024;

/// A registration as it came, as a board's comm core keeps it.
struct Standing {
    session: SessionId,
    seq: u16,
    class: u8,
    uid: u32,
    caller: String,
    name: String,
    policy: Policy,
    payload: Vec<u8>,
}

impl Standing {
    fn asked(&self) -> Asked<'_> {
        Asked {
            session: self.session.0,
            seq: self.seq,
            kind: kind::ACTION_REGISTER,
            class: self.class,
            uid: self.uid,
            caller: &self.caller,
            payload: &self.payload,
        }
    }
}

struct Inner {
    me: NodeName,
    actions: Actions,
    standing: Vec<Standing>,
    started: Instant,
    scratch: Vec<u8>,
    /// What happened to runs, for a test to look at.
    events: Vec<Event>,
}

impl Inner {
    fn now_ms(&self) -> u64 {
        self.started.elapsed().as_millis() as u64
    }

    fn keep_events(&mut self) {
        let events = self.actions.take_events();
        self.events.extend(events);
    }
}

/// What the list sends, sent on to the sessions.
struct Through<'a> {
    sessions: &'a mut Sessions,
    scratch: &'a mut Vec<u8>,
}

impl Out for Through<'_> {
    fn frame(&mut self, session: u32, kind: u8, payload: &[u8], may_drop: bool) {
        let delivery = if may_drop {
            Delivery::MayDrop
        } else {
            Delivery::Must
        };
        let _ = self
            .sessions
            .send_frame(SessionId(session), kind, payload, [], delivery);
    }

    fn scratch(&mut self) -> &mut Vec<u8> {
        self.scratch
    }
}

/// What the list says to nobody: its answers to registrations handed to it again.
struct Nowhere(Vec<u8>);

impl Out for Nowhere {
    fn frame(&mut self, _: u32, _: u8, _: &[u8], _: bool) {}

    fn scratch(&mut self) -> &mut Vec<u8> {
        &mut self.0
    }
}

/// The caller as node logic is told of it: its class, user id and name.
fn who(principal: &Principal) -> (u8, u32, String) {
    let class = principal.class() as u8;
    match principal {
        Principal::Anonymous => (class, 0, String::new()),
        Principal::Local { uid, .. } => (class, *uid, String::new()),
        Principal::Operator(key) => (
            class,
            0,
            key.0.iter().map(|byte| format!("{byte:02x}")).collect(),
        ),
        Principal::Peer(name) => (class, 0, name.to_string()),
        Principal::Device(name) => (class, 0, name.to_string()),
    }
}

/// The router: one list, shared by the service and whoever ticks or restarts it.
#[derive(Clone)]
pub struct Router(Rc<RefCell<Inner>>);

impl std::fmt::Debug for Router {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let inner = self.0.borrow();
        write!(
            f,
            "Router({} listed, {} running)",
            inner.actions.listed(),
            inner.actions.running()
        )
    }
}

impl Router {
    /// The list of the node `me`.
    #[must_use]
    pub fn new(me: NodeName) -> Self {
        Self(Rc::new(RefCell::new(Inner {
            actions: Actions::new(me.clone()),
            me,
            standing: Vec::new(),
            started: Instant::now(),
            scratch: vec![0; ROOM],
            events: Vec::new(),
        })))
    }

    /// Node logic starts again: a list with an instance of its own, so no handle from before
    /// names anything in it, and nothing running. As on a board, the comm core hands it every
    /// registration it holds again, so actions stay listed.
    pub fn restart(&self) {
        let mut inner = self.0.borrow_mut();
        let now = inner.now_ms();
        let me = inner.me.clone();
        inner.actions = Actions::new(me);
        let Inner {
            actions, standing, ..
        } = &mut *inner;
        let mut nowhere = Nowhere(vec![0; ROOM]);
        for standing in standing.iter() {
            let _ = actions.request(&standing.asked(), now, &mut nowhere);
        }
        let _ = actions.take_said();
    }

    /// This start of node logic's instance: what every handle it gives starts with.
    #[must_use]
    pub fn instance(&self) -> u64 {
        self.0.borrow().actions.instance()
    }

    /// Lets deadlines pass: due runs end as timed out, and their programs are told.
    pub fn tick(&self, sessions: &mut Sessions) {
        let mut inner = self.0.borrow_mut();
        let now = inner.now_ms();
        if inner.actions.next_deadline().is_some_and(|due| due <= now) {
            let Inner {
                actions, scratch, ..
            } = &mut *inner;
            actions.tick(now, &mut Through { sessions, scratch });
            inner.keep_events();
        }
    }

    /// What happened to runs since the last call, in order.
    pub fn take_events(&self) -> Vec<Event> {
        let mut inner = self.0.borrow_mut();
        inner.keep_events();
        std::mem::take(&mut inner.events)
    }
}

impl Service for Router {
    fn kinds(&self) -> &'static [u8] {
        Actions::KINDS
    }

    fn asks<'a>(&self, frame: &FrameView<'a>) -> Result<(What<'a>, Policy), Error> {
        let local = Policy::new(Admits::LOCAL);
        match frame.kind() {
            kind::ACTION_REGISTER => Ok((What::Write("actions"), local)),
            kind::ACTION_RUN => {
                let (_call, rest) = body::take::<u64>(frame.payload())?;
                let (name, _) = body::take::<&str>(rest)?;
                let policy = self
                    .0
                    .borrow()
                    .standing
                    .iter()
                    .find(|standing| standing.name == name)
                    .map_or(Policy::new(KNOWN), |standing| standing.policy);
                Ok((What::Run(name), policy))
            }
            _ => Ok((
                What::Read("actions"),
                Policy::new(Admits::ANYONE).without_grant(),
            )),
        }
    }

    fn handle(&mut self, request: Checked<Incoming<'_>>, cx: &mut Cx<'_>) -> Result<(), Error> {
        let incoming = request.get();
        let frame = incoming.frame();
        let (class, uid, caller) = who(&cx.caller().principal);
        let asked = Asked {
            session: cx.from().0,
            seq: cx.request_seq(),
            kind: frame.kind(),
            class,
            uid,
            caller: &caller,
            payload: frame.payload(),
        };
        let mut inner = self.0.borrow_mut();
        let now = inner.now_ms();
        let answered = {
            let Inner {
                actions, scratch, ..
            } = &mut *inner;
            actions.request(
                &asked,
                now,
                &mut Through {
                    sessions: cx.sessions(),
                    scratch,
                },
            )
        };
        inner.keep_events();
        if answered.is_ok() && asked.kind == kind::ACTION_REGISTER {
            let register = Register::decode(asked.payload)?;
            let name = register.name.to_string();
            inner
                .standing
                .retain(|standing| !(standing.session == cx.from() && standing.name == name));
            inner.standing.push(Standing {
                session: cx.from(),
                seq: asked.seq,
                class,
                uid,
                caller: caller.clone(),
                name,
                policy: register.policy,
                payload: asked.payload.to_vec(),
            });
        }
        match answered {
            Ok(()) => Ok(()),
            Err(error) => match inner.actions.take_said() {
                Some(words) => {
                    drop(inner);
                    cx.refuse(error, &words)
                }
                None => Err(error),
            },
        }
    }

    fn closed(&mut self, session: SessionId, sessions: &mut Sessions) {
        let mut inner = self.0.borrow_mut();
        let now = inner.now_ms();
        {
            let Inner {
                actions, scratch, ..
            } = &mut *inner;
            actions.gone(session.0, now, &mut Through { sessions, scratch });
        }
        inner.keep_events();
        inner
            .standing
            .retain(|standing| standing.session != session);
    }

    fn quiet(&mut self, _session: SessionId, _quiet_ms: u64, _sessions: &mut Sessions) {}
}
