//! The list: registering, routing a run to whoever registered it, and what happens to a run
//! when either end goes away or it runs out of time.

use orion_action_list::{Actions, ENDED_FOR_MS, Event, MAX_ENDED, MAX_RUNS};
use orion_check::{Admits, Policy};
use orion_msg_actions::{
    ActionResult, Attach, Cancel, Claim, Outcome, Progress, Register, Registered, Run, Target,
};
use orion_reply::{Asked, Out};
use orion_types::Error;
use orion_wire::body::encode_fields;
use orion_wire::{Body, Bytes, Decode, kind};

const OWNER: u32 = 10;
const CALLER: u32 = 20;
const OTHER: u32 = 30;

/// Keeps what was sent, to be read back.
#[derive(Default)]
struct Sent {
    frames: Vec<(u32, u8, Vec<u8>, bool)>,
    scratch: Vec<u8>,
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

struct Bench {
    actions: Actions,
    sent: Sent,
    now: u64,
    seq: u16,
}

fn fan() -> Target {
    Target::Resource("fan.left".parse().expect("name"))
}

impl Bench {
    fn new() -> Self {
        Self {
            actions: Actions::new("board".parse().expect("name")),
            sent: Sent::default(),
            now: 1_000,
            seq: 0,
        }
    }

    /// With `set` registered for the left fan by `OWNER`.
    fn with_action() -> Self {
        let mut bench = Self::new();
        bench.register(OWNER, "set", fan()).expect("register");
        bench.sent.frames.clear();
        bench
    }

    fn ask<B: Body>(&mut self, from: u32, body: &B) -> Result<(), Error> {
        self.seq += 1;
        let mut payload = [0_u8; 1024];
        let len = encode_fields(body, &mut payload).expect("encode");
        let asked = Asked {
            session: from,
            seq: self.seq,
            kind: B::KIND,
            class: 4,
            uid: 1000,
            caller: "",
            payload: &payload[..len],
        };
        self.actions.request(&asked, self.now, &mut self.sent)
    }

    fn register(&mut self, from: u32, name: &str, target: Target) -> Result<(), Error> {
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

    fn run(&mut self, from: u32, call: u64, deadline_ms: u64, detached: bool) -> Result<(), Error> {
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

    /// The next frame sent, which must be a `B` for `to`.
    #[track_caller]
    fn take<B: Body + Decode<'static>>(&mut self, to: u32) -> B {
        assert!(!self.sent.frames.is_empty(), "nothing was sent");
        let (session, kind, payload, _) = self.sent.frames.remove(0);
        assert_eq!((session, kind), (to, B::KIND));
        // The body is owned by the test from here on.
        let leaked: &'static [u8] = Box::leak(payload.into_boxed_slice());
        B::decode(leaked).expect("decode")
    }

    /// A run under way: the caller's call 7 is the list's `id`.
    fn running(detached: bool) -> (Self, u64) {
        let mut bench = Self::with_action();
        bench.run(CALLER, 7, 0, detached).expect("run");
        let claim: Claim<'_> = bench.take(OWNER);
        bench.actions.take_events();
        (bench, claim.call)
    }
}

#[test]
fn an_action_is_registered_once() {
    let mut bench = Bench::new();
    bench.register(OWNER, "set", fan()).expect("register");
    let registered: Registered = bench.take(OWNER);
    assert_eq!(registered.request_seq, bench.seq);
    assert_eq!(bench.actions.listed(), 1);
    // The same again from the same program is the same action: node logic was passed it
    // twice.
    bench.register(OWNER, "set", fan()).expect("the same again");
    let _: Registered = bench.take(OWNER);
    assert_eq!(bench.actions.listed(), 1);
    // Another program cannot take the name.
    assert_eq!(
        bench.register(OTHER, "set", fan()),
        Err(Error::AlreadyExists)
    );
    // The same name for another target is another action.
    let right = Target::Resource("fan.right".parse().expect("name"));
    bench.register(OTHER, "set", right).expect("another target");
    assert_eq!(bench.actions.listed(), 2);
    // An action for another node cannot be registered here.
    let elsewhere = Target::Node("other".parse().expect("name"));
    assert_eq!(
        bench.register(OWNER, "reboot", elsewhere),
        Err(Error::Unsupported)
    );
    let here = Target::Node("board".parse().expect("name"));
    bench.register(OWNER, "reboot", here).expect("this node");
}

#[test]
fn the_same_name_with_another_policy_from_its_owner_is_refused() {
    let mut bench = Bench::with_action();
    let changed = bench.ask(
        OWNER,
        &Register {
            name: "set".parse().expect("name"),
            target: fan(),
            policy: Policy::new(Admits::ANYONE),
            description: "sets the speed",
        },
    );
    assert_eq!(changed, Err(Error::AlreadyExists));
}

#[test]
fn a_run_is_handed_to_whoever_registered_it_and_its_result_comes_back() {
    let mut bench = Bench::with_action();
    bench.run(CALLER, 7, 0, false).expect("run");
    let claim: Claim<'_> = bench.take(OWNER);
    assert_eq!(claim.name.as_str(), "set");
    assert_eq!(claim.target, fan());
    assert_eq!(claim.args.0, b"1500");
    assert_ne!(claim.call, 7, "the program sees the list's number");
    assert_eq!(bench.actions.running(), 1);

    bench
        .ask(
            OWNER,
            &Progress {
                call: claim.call,
                percent: 40,
                message: "spinning up",
                data: Bytes(b"d"),
            },
        )
        .expect("progress");
    let (to, _, _, may_drop) = bench.sent.frames[0].clone();
    assert!(may_drop && to == CALLER, "progress may be dropped");
    let progress: Progress<'_> = bench.take(CALLER);
    assert_eq!((progress.call, progress.percent), (7, 40));
    assert_eq!(
        (progress.message, progress.data.0),
        ("spinning up", &b"d"[..])
    );

    bench
        .ask(
            OWNER,
            &ActionResult {
                call: claim.call,
                outcome: Outcome::Succeeded,
                output: Bytes(b"ok"),
                detail: "",
            },
        )
        .expect("result");
    let result: ActionResult<'_> = bench.take(CALLER);
    assert_eq!((result.call, result.outcome), (7, Outcome::Succeeded));
    assert_eq!(result.output.0, b"ok");
    assert_eq!(bench.actions.running(), 0);

    let events = bench.actions.take_events();
    let brief: Vec<(u8, &str, Option<Outcome>)> = events
        .iter()
        .map(|event: &Event| (event.percent, event.message.as_str(), event.outcome))
        .collect();
    assert_eq!(
        brief,
        [
            (255, "claimed", None),
            (40, "spinning up", None),
            (255, "", Some(Outcome::Succeeded))
        ]
    );
    assert!(events.iter().all(|event| event.run == claim.call));
}

#[test]
fn a_run_of_what_is_not_listed_is_not_found() {
    let mut bench = Bench::new();
    assert_eq!(bench.run(CALLER, 1, 0, false), Err(Error::NotFound));
    // Listed for another target is not listed for this one.
    let right = Target::Resource("fan.right".parse().expect("name"));
    bench.register(OWNER, "set", right).expect("register");
    assert_eq!(bench.run(CALLER, 1, 0, false), Err(Error::NotFound));
}

#[test]
fn a_callers_number_is_used_once_at_a_time() {
    let (mut bench, _) = Bench::running(false);
    assert_eq!(bench.run(CALLER, 7, 0, false), Err(Error::AlreadyExists));
    // Another session's 7 is another run.
    bench.run(OTHER, 7, 0, false).expect("another caller");
}

#[test]
fn cancel_reaches_the_program_under_the_lists_number() {
    let (mut bench, id) = Bench::running(false);
    bench.ask(CALLER, &Cancel { call: 7 }).expect("cancel");
    let cancel: Cancel = bench.take(OWNER);
    assert_eq!(cancel.call, id);
    // The run goes on until the program says how it ended.
    assert_eq!(bench.actions.running(), 1);
    bench
        .ask(
            OWNER,
            &ActionResult {
                call: id,
                outcome: Outcome::Cancelled,
                output: Bytes(&[]),
                detail: "stopped",
            },
        )
        .expect("result");
    let result: ActionResult<'_> = bench.take(CALLER);
    assert_eq!(
        (result.outcome, result.detail),
        (Outcome::Cancelled, "stopped")
    );
    // Someone else's cancel is of a run they do not have.
    assert_eq!(bench.ask(OTHER, &Cancel { call: 7 }), Err(Error::NotFound));
}

#[test]
fn only_the_program_that_runs_it_speaks_for_a_run() {
    let (mut bench, id) = Bench::running(false);
    let result = ActionResult {
        call: id,
        outcome: Outcome::Succeeded,
        output: Bytes(&[]),
        detail: "",
    };
    assert_eq!(bench.ask(OTHER, &result), Err(Error::Denied));
    assert_eq!(bench.ask(CALLER, &result), Err(Error::Denied));
    assert_eq!(bench.actions.running(), 1);
    // A run that never was.
    let never = ActionResult {
        call: id + 100,
        ..result
    };
    assert_eq!(bench.ask(OWNER, &never), Err(Error::NotFound));
}

#[test]
fn a_caller_that_goes_has_its_run_cancelled_unless_it_is_detached() {
    let (mut bench, id) = Bench::running(false);
    bench.actions.gone(CALLER, bench.now, &mut bench.sent);
    let cancel: Cancel = bench.take(OWNER);
    assert_eq!(cancel.call, id);
    // The program's last word about it goes nowhere, and is not an error.
    bench
        .ask(
            OWNER,
            &ActionResult {
                call: id,
                outcome: Outcome::Cancelled,
                output: Bytes(&[]),
                detail: "",
            },
        )
        .expect("result");
    assert!(bench.sent.frames.is_empty());
    assert_eq!(bench.actions.running(), 0);

    let (mut bench, id) = Bench::running(true);
    bench.actions.gone(CALLER, bench.now, &mut bench.sent);
    assert!(bench.sent.frames.is_empty(), "a detached run carries on");
    assert_eq!(bench.actions.running(), 1);
    bench
        .ask(
            OWNER,
            &Progress {
                call: id,
                percent: 10,
                message: "",
                data: Bytes(&[]),
            },
        )
        .expect("progress");
    assert!(bench.sent.frames.is_empty());
}

#[test]
fn a_program_that_goes_takes_its_actions_and_fails_its_runs() {
    let (mut bench, _) = Bench::running(false);
    bench.actions.gone(OWNER, bench.now, &mut bench.sent);
    let result: ActionResult<'_> = bench.take(CALLER);
    assert_eq!((result.call, result.outcome), (7, Outcome::Failed));
    assert_eq!(result.detail, "the program that runs it went away");
    assert_eq!((bench.actions.listed(), bench.actions.running()), (0, 0));
    assert_eq!(bench.run(CALLER, 8, 0, false), Err(Error::NotFound));
    // The name is free for the next program.
    bench.register(OTHER, "set", fan()).expect("register");
}

#[test]
fn a_run_past_its_deadline_times_out_and_the_program_is_told_to_stop() {
    let mut bench = Bench::with_action();
    bench.run(CALLER, 7, 500, false).expect("run");
    let claim: Claim<'_> = bench.take(OWNER);
    assert_eq!(claim.deadline_ms, 500);
    assert_eq!(bench.actions.next_deadline(), Some(bench.now + 500));
    bench.actions.tick(bench.now + 499, &mut bench.sent);
    assert!(bench.sent.frames.is_empty());
    bench.actions.tick(bench.now + 500, &mut bench.sent);
    let cancel: Cancel = bench.take(OWNER);
    assert_eq!(cancel.call, claim.call);
    let result: ActionResult<'_> = bench.take(CALLER);
    assert_eq!((result.call, result.outcome), (7, Outcome::TimedOut));
    assert_eq!(bench.actions.next_deadline(), None);
    // What the program says a moment later is of a run that ended: nothing, and no error.
    bench
        .ask(
            OWNER,
            &ActionResult {
                call: claim.call,
                outcome: Outcome::Succeeded,
                output: Bytes(&[]),
                detail: "",
            },
        )
        .expect("late");
    assert!(bench.sent.frames.is_empty());
    // A run with no deadline has none.
    bench.run(CALLER, 8, 0, false).expect("run");
    assert_eq!(bench.actions.next_deadline(), None);
}

#[test]
fn more_runs_than_there_is_room_for_are_refused_as_busy() {
    let mut bench = Bench::with_action();
    for call in 0..MAX_RUNS as u64 {
        bench.run(CALLER, call, 0, false).expect("run");
    }
    assert_eq!(bench.run(CALLER, u64::MAX, 0, false), Err(Error::Busy));
    bench.actions.forget_sessions();
    assert_eq!((bench.actions.listed(), bench.actions.running()), (0, 0));
}

impl Bench {
    fn ask_as<B: Body>(&mut self, from: u32, uid: u32, body: &B) -> Result<(), Error> {
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

    fn result(&mut self, id: u64, outcome: Outcome) {
        self.ask(
            OWNER,
            &ActionResult {
                call: id,
                outcome,
                output: Bytes(b"out"),
                detail: "",
            },
        )
        .expect("result");
    }
}

/// The caller's next session.
const BACK: u32 = 21;

#[test]
fn whoever_started_a_detached_run_comes_back_to_its_progress() {
    let (mut bench, id) = Bench::running(true);
    bench
        .ask(
            OWNER,
            &Progress {
                call: id,
                percent: 30,
                message: "working",
                data: Bytes(b"d"),
            },
        )
        .expect("progress");
    let _: Progress<'_> = bench.take(CALLER);
    bench.actions.gone(CALLER, bench.now, &mut bench.sent);
    // It goes on while nobody listens.
    bench
        .ask(
            OWNER,
            &Progress {
                call: id,
                percent: 60,
                message: "still working",
                data: Bytes(&[]),
            },
        )
        .expect("progress");
    assert!(bench.sent.frames.is_empty());

    // Back, on another session: the latest progress, at once.
    bench.ask(BACK, &Attach { call: 7 }).expect("attach");
    let progress: Progress<'_> = bench.take(BACK);
    assert_eq!((progress.call, progress.percent), (7, 60));
    assert_eq!(progress.message, "still working");
    // And what follows, and the end.
    bench
        .ask(
            OWNER,
            &Progress {
                call: id,
                percent: 90,
                message: "nearly",
                data: Bytes(&[]),
            },
        )
        .expect("progress");
    let progress: Progress<'_> = bench.take(BACK);
    assert_eq!(progress.percent, 90);
    bench.result(id, Outcome::Succeeded);
    let result: ActionResult<'_> = bench.take(BACK);
    assert_eq!((result.call, result.outcome), (7, Outcome::Succeeded));
    assert_eq!(bench.actions.ended(), 0, "it was told: nothing is kept");
}

#[test]
fn a_detached_run_that_ended_with_nobody_there_is_kept_for_its_starter() {
    let (mut bench, id) = Bench::running(true);
    bench.actions.gone(CALLER, bench.now, &mut bench.sent);
    bench.result(id, Outcome::Failed);
    assert!(bench.sent.frames.is_empty());
    assert_eq!(bench.actions.ended(), 1);
    bench.now += 60_000;
    bench.ask(BACK, &Attach { call: 7 }).expect("attach");
    let result: ActionResult<'_> = bench.take(BACK);
    assert_eq!((result.call, result.outcome), (7, Outcome::Failed));
    assert_eq!(result.output.0, b"out");
    // Told once.
    assert_eq!(bench.ask(BACK, &Attach { call: 7 }), Err(Error::NotFound));

    // Kept for ten minutes and no longer.
    let (mut bench, id) = Bench::running(true);
    bench.actions.gone(CALLER, bench.now, &mut bench.sent);
    bench.result(id, Outcome::Succeeded);
    bench.now += ENDED_FOR_MS + 1;
    assert_eq!(bench.ask(BACK, &Attach { call: 7 }), Err(Error::NotFound));
    assert_eq!(bench.actions.ended(), 0);
}

#[test]
fn only_whoever_started_it_comes_back_to_a_run() {
    let (mut bench, id) = Bench::running(true);
    bench.actions.gone(CALLER, bench.now, &mut bench.sent);
    // Another user on the board, with the right number.
    assert_eq!(
        bench.ask_as(OTHER, 1001, &Attach { call: 7 }),
        Err(Error::NotFound)
    );
    bench.result(id, Outcome::Succeeded);
    assert_eq!(
        bench.ask_as(OTHER, 1001, &Attach { call: 7 }),
        Err(Error::NotFound)
    );
    assert_eq!(
        bench.actions.ended(),
        1,
        "it is still there for its starter"
    );
    // A run that was not detached is not come back to, even by its starter.
    let (mut bench, _) = Bench::running(false);
    assert_eq!(bench.ask(BACK, &Attach { call: 7 }), Err(Error::NotFound));
}

#[test]
fn a_detached_run_is_cancelled_from_the_session_that_came_back() {
    let (mut bench, id) = Bench::running(true);
    bench.actions.gone(CALLER, bench.now, &mut bench.sent);
    // Nobody is attached: there is nobody whose cancel it would be.
    assert_eq!(bench.ask(BACK, &Cancel { call: 7 }), Err(Error::NotFound));
    bench.ask(BACK, &Attach { call: 7 }).expect("attach");
    bench.ask(BACK, &Cancel { call: 7 }).expect("cancel");
    let cancel: Cancel = bench.take(OWNER);
    assert_eq!(cancel.call, id);
}

#[test]
fn a_detached_run_still_has_its_deadline_and_its_program() {
    let mut bench = Bench::with_action();
    bench.run(CALLER, 7, 500, true).expect("run");
    let claim: Claim<'_> = bench.take(OWNER);
    bench.actions.gone(CALLER, bench.now, &mut bench.sent);
    bench.now += 500;
    bench.actions.tick(bench.now, &mut bench.sent);
    let cancel: Cancel = bench.take(OWNER);
    assert_eq!(cancel.call, claim.call);
    bench.ask(BACK, &Attach { call: 7 }).expect("attach");
    let result: ActionResult<'_> = bench.take(BACK);
    assert_eq!(result.outcome, Outcome::TimedOut);

    let (mut bench, _) = Bench::running(true);
    bench.actions.gone(CALLER, bench.now, &mut bench.sent);
    bench.actions.gone(OWNER, bench.now, &mut bench.sent);
    bench.ask(BACK, &Attach { call: 7 }).expect("attach");
    let result: ActionResult<'_> = bench.take(BACK);
    assert_eq!(result.outcome, Outcome::Failed);
}

#[test]
fn only_so_many_ended_runs_are_kept() {
    let mut bench = Bench::with_action();
    for call in 0..(MAX_ENDED as u64 + 10) {
        bench.run(CALLER, call, 0, true).expect("run");
        let claim: Claim<'_> = bench.take(OWNER);
        bench.actions.gone(CALLER, bench.now, &mut bench.sent);
        bench.result(claim.call, Outcome::Succeeded);
    }
    assert_eq!(bench.actions.ended(), MAX_ENDED);
    // The oldest went first.
    assert_eq!(bench.ask(BACK, &Attach { call: 0 }), Err(Error::NotFound));
    bench.ask(BACK, &Attach { call: 20 }).expect("attach");
}

#[test]
fn a_kind_that_is_not_the_lists_is_unsupported() {
    let mut bench = Bench::new();
    let asked = Asked {
        session: CALLER,
        seq: 1,
        kind: kind::PUT,
        class: 4,
        uid: 0,
        caller: "",
        payload: &[],
    };
    assert_eq!(
        bench.actions.request(&asked, 0, &mut bench.sent),
        Err(Error::Unsupported)
    );
}
