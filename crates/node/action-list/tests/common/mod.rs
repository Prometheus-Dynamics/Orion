//! The bench the list's tests share: a list, what it sent, and a clock.

#![allow(dead_code)]

use orion_action_list::Actions;
use orion_check::{Admits, Policy};
use orion_msg_actions::{CallerGone, Claim, Register, Run, Target};
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
            },
        )
    }

    /// Session `session` ends. Who was told it ended, and its token, is taken out of what
    /// was sent and returned, in order; the rest stays to be taken.
    pub fn gone(&mut self, session: u32) -> Vec<(u32, u64)> {
        self.actions.gone(session, self.now, &mut self.sent);
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

    /// The next frame sent, which must be a `B` for `to`.
    #[track_caller]
    pub fn take<B: Body + Decode<'static>>(&mut self, to: u32) -> B {
        assert!(!self.sent.frames.is_empty(), "nothing was sent");
        let (session, kind, payload, _) = self.sent.frames.remove(0);
        assert_eq!((session, kind), (to, B::KIND));
        // The body is owned by the test from here on.
        let leaked: &'static [u8] = Box::leak(payload.into_boxed_slice());
        B::decode(leaked).expect("decode")
    }

    /// A run under way: the caller's call 7 is the list's `id`.
    pub fn running(detached: bool) -> (Self, u64) {
        let mut bench = Self::with_action();
        bench
            .run(CALLER, 7, if detached { HOUR } else { 0 }, detached)
            .expect("run");
        let claim: Claim<'_> = bench.take(OWNER);
        bench.actions.take_events();
        (bench, claim.call)
    }
}
