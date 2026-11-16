//! The bench the list's tests share: a list, what it sent, and a clock.

#![allow(dead_code)]

use orion_action_list::Actions;
use orion_check::{Admits, Policy};
use orion_msg_actions::{
    ActionResult, Attach, CallerGone, Claim, Outcome, Register, Run, RunHandle, Started, Target,
};
use orion_reply::{Asked, Out};
use orion_types::Error;
use orion_wire::body::encode_fields;
use orion_wire::{Body, Bytes, Decode, kind};

pub const OWNER: u32 = 10;
pub const CALLER: u32 = 20;
pub const OTHER: u32 = 30;

/// The longest deadline a detached run may have unless the node says otherwise.
pub const HOUR: u64 = orion_action_list::MAX_DETACHED_MS;

/// Keeps what was sent, to be read back.
#[derive(Default)]
pub struct Sent {
    pub frames: Vec<(u32, u8, Vec<u8>, bool)>,
    pub scratch: Vec<u8>,
}

impl Out for Sent {
    fn frame(&mut self, session: u32, kind: u8, payload: &[u8], may_drop: bool) {
        self.frames
            .push((session, kind, payload.to_vec(), may_drop));
    }

    fn scratch(&mut self) -> &mut Vec<u8> {
        if self.scratch.is_empty() {
            self.scratch = vec![0; 4096];
        }
        &mut self.scratch
    }
}

pub struct Bench {
    pub actions: Actions,
    pub sent: Sent,
    pub now: u64,
    pub seq: u16,
}

pub fn fan() -> Target {
    Target::Resource("fan.left".parse().expect("name"))
}

impl Bench {
    pub fn new() -> Self {
        Self {
            actions: Actions::new("board".parse().expect("name")),
            sent: Sent::default(),
            now: 1_000,
            seq: 0,
        }
    }

    /// With `set` registered for the left fan by `OWNER`.
    pub fn with_action() -> Self {
        let mut bench = Self::new();
        bench.register(OWNER, "set", fan()).expect("register");
        bench.sent.frames.clear();
        bench
    }

    pub fn ask<B: Body>(&mut self, from: u32, body: &B) -> Result<(), Error> {
        self.ask_class(from, 4, body)
    }

    /// As [`Bench::ask`], from a caller of `class`.
    pub fn ask_class<B: Body>(&mut self, from: u32, class: u8, body: &B) -> Result<(), Error> {
        self.seq += 1;
        let mut payload = [0_u8; 1024];
        let len = encode_fields(body, &mut payload).expect("encode");
        let asked = Asked {
            session: from,
            seq: self.seq,
            kind: B::KIND,
            class,
            uid: 1000,
            caller: "",
            payload: &payload[..len],
        };
        self.actions.request(&asked, self.now, &mut self.sent)
    }

    pub fn register(&mut self, from: u32, name: &str, target: Target) -> Result<(), Error> {
        self.ask(
            from,
            &Register {
                name: name.parse().expect("name"),
                target,
                policy: Policy::new(Admits::LOCAL),
                description: "sets the speed",
            },
        )
    }

    pub fn run(
        &mut self,
        from: u32,
        call: u64,
        deadline_ms: u64,
        detached: bool,
    ) -> Result<(), Error> {
        self.ask(
            from,
            &Run {
                call,
                name: "set".parse().expect("name"),
                target: fan(),
                args: Bytes(b"1500"),
                deadline_ms,
                detached,
                on_behalf: 0,
            },
        )
    }

    /// Session `session` ends. Who was told it ended, and its token, is taken out of what
    /// was sent and returned, in order; the rest stays to be taken.
    pub fn gone(&mut self, session: u32) -> Vec<(u32, u64)> {
        self.actions.gone(session, self.now, &mut self.sent);
        self.gone_frames()
    }

    /// The `CallerGone` frames sent so far, taken out of what was sent, in order.
    pub fn gone_frames(&mut self) -> Vec<(u32, u64)> {
        let mut told = Vec::new();
        self.sent.frames.retain(|(to, kind, payload, _)| {
            if *kind != kind::ACTION_CALLER_GONE {
                return true;
            }
            let gone = CallerGone::decode(payload).expect("decode");
            told.push((*to, gone.session));
            false
        });
        told
    }

    /// The next frame sent, which must be a `B` for `to`. The handles node logic gives are
    /// passed over unless a [`Started`] is asked for: [`Bench::handle`] reads those.
    #[track_caller]
    pub fn take<B: Body + Decode<'static>>(&mut self, to: u32) -> B {
        while B::KIND != kind::ACTION_STARTED
            && self
                .sent
                .frames
                .first()
                .is_some_and(|(_, kind, _, _)| *kind == kind::ACTION_STARTED)
        {
            self.sent.frames.remove(0);
        }
        assert!(!self.sent.frames.is_empty(), "nothing was sent");
        let (session, kind, payload, _) = self.sent.frames.remove(0);
        assert_eq!((session, kind), (to, B::KIND));
        // The body is owned by the test from here on.
        let leaked: &'static [u8] = Box::leak(payload.into_boxed_slice());
        B::decode(leaked).expect("decode")
    }

    /// As [`Bench::ask`], from a user `uid`.
    pub fn ask_as<B: Body>(&mut self, from: u32, uid: u32, body: &B) -> Result<(), Error> {
        self.seq += 1;
        let mut payload = [0_u8; 1024];
        let len = encode_fields(body, &mut payload).expect("encode");
        let asked = Asked {
            session: from,
            seq: self.seq,
            kind: B::KIND,
            class: 4,
            uid,
            caller: "",
            payload: &payload[..len],
        };
        self.actions.request(&asked, self.now, &mut self.sent)
    }

    /// `OWNER` says run `id` ended as `outcome`.
    pub fn result(&mut self, id: u64, outcome: Outcome) {
        self.ask(
            OWNER,
            &ActionResult {
                call: id,
                outcome,
                output: Bytes(b"out"),
                detail: "",
                // The program's own code, passed on as it is.
                code: 11,
                instance: 0,
                run: 0,
                action: "",
            },
        )
        .expect("result");
    }

    /// The handle node logic gave `to` next, for its `call`.
    #[track_caller]
    pub fn handle(&mut self, to: u32, call: u64) -> RunHandle {
        let at = self
            .sent
            .frames
            .iter()
            .position(|(_, kind, _, _)| *kind == kind::ACTION_STARTED)
            .expect("no handle was given");
        let (session, _, payload, _) = self.sent.frames.remove(at);
        let started = Started::decode(&payload).expect("decode");
        assert_eq!((session, started.call), (to, call));
        assert_eq!(started.action, "set");
        assert_eq!(started.instance, self.actions.instance());
        RunHandle::said(started.instance, started.run).expect("a handle")
    }

    /// A run under way: the caller's call 7 is the list's `id`.
    pub fn running(detached: bool) -> (Self, u64) {
        let mut bench = Self::with_action();
        bench
            .run(CALLER, 7, if detached { HOUR } else { 0 }, detached)
            .expect("run");
        let claim: Claim<'_> = bench.take(OWNER);
        assert_eq!(bench.handle(CALLER, 7).run, claim.call);
        bench.actions.take_events();
        (bench, claim.call)
    }
}

/// Coming back to the starter's run `call` by its number alone, as a client before handles
/// does.
pub const fn bare(call: u64) -> Attach {
    Attach {
        call,
        instance: 0,
        run: 0,
    }
}

/// Coming back to the run `handle` names, as call `call`.
pub const fn by(call: u64, handle: RunHandle) -> Attach {
    Attach {
        call,
        instance: handle.instance,
        run: handle.run,
    }
}
