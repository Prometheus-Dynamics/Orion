//! A caller's session that ends: each program that was given a run from it is told once, by a
//! token that names that session alone. And a detached run must end by itself.

mod common;

use common::{Bench, CALLER, HOUR, OTHER, OWNER, fan};
use orion_msg_actions::{CallerGone, Cancel, CancelReason, Claim, Run};
use orion_types::Error;
use orion_wire::Bytes;

/// A second program, with an action of its own.
const KEEPER: u32 = 40;

impl Bench {
    /// `get` registered for the left fan by `KEEPER`, besides `set` by `OWNER`.
    fn with_two() -> Self {
        let mut bench = Self::with_action();
        bench.register(KEEPER, "get", fan()).expect("register");
        bench.sent.frames.clear();
        bench
    }

    fn run_get(&mut self, from: u32, call: u64) {
        self.ask(
            from,
            &Run {
                call,
                name: "get".parse().expect("name"),
                target: fan(),
                args: Bytes(&[]),
                deadline_ms: 0,
                detached: false,
                on_behalf: 0,
            },
        )
        .expect("run");
    }
}

#[test]
fn a_detached_run_needs_a_deadline_no_longer_than_the_node_takes() {
    let mut bench = Bench::with_action();
    assert_eq!(bench.run(CALLER, 1, 0, true), Err(Error::NoDeadline));
    assert_eq!(
        bench.run(CALLER, 2, HOUR + 1, true),
        Err(Error::DeadlineTooLong)
    );
    assert!(
        bench.sent.frames.is_empty(),
        "a refused run reached its program"
    );
    bench.run(CALLER, 3, HOUR, true).expect("an hour is taken");
    let _: Claim<'_> = bench.take(OWNER);
    // A run that is not detached ends with its caller: it needs no deadline.
    bench.run(CALLER, 4, 0, false).expect("run");
    let _: Claim<'_> = bench.take(OWNER);

    // The node says how long it takes.
    bench.actions.set_max_detached(1_000);
    assert_eq!(
        bench.run(CALLER, 5, 1_001, true),
        Err(Error::DeadlineTooLong)
    );
    bench.run(CALLER, 6, 1_000, true).expect("run");
    let _: Claim<'_> = bench.take(OWNER);
    // Not detached, it may be as long as it likes.
    bench.run(CALLER, 7, HOUR * 24, false).expect("run");
}

#[test]
fn each_program_given_a_run_from_a_session_is_told_once_when_it_ends() {
    let mut bench = Bench::with_two();
    bench.run(CALLER, 1, 0, false).expect("run");
    let first: Claim<'_> = bench.take(OWNER);
    bench.run(CALLER, 2, 0, false).expect("run");
    let second: Claim<'_> = bench.take(OWNER);
    bench.run_get(CALLER, 3);
    let third: Claim<'_> = bench.take(KEEPER);
    // One token for the session, in every run it asked for.
    let token = first.session;
    assert_ne!(token, 0);
    assert_eq!((second.session, third.session), (token, token));

    // A run that already ended still means its program is told.
    bench
        .ask(
            KEEPER,
            &orion_msg_actions::ActionResult {
                call: third.call,
                outcome: orion_msg_actions::Outcome::Succeeded,
                output: Bytes(&[]),
                detail: "",
            },
        )
        .expect("result");
    bench.sent.frames.clear();

    let told = bench.gone(CALLER);
    assert_eq!(told, [(OWNER, token), (KEEPER, token)]);
    // Its runs that were going are cancelled first, as before.
    for id in [first.call, second.call] {
        let cancel: Cancel = bench.take(OWNER);
        assert_eq!((cancel.call, cancel.reason), (id, CancelReason::CallerGone));
    }
    assert!(bench.sent.frames.is_empty());
    // Once.
    assert!(bench.gone(CALLER).is_empty());
}

#[test]
fn a_program_given_nothing_from_a_session_is_not_told() {
    let mut bench = Bench::with_two();
    bench.run(CALLER, 1, 0, false).expect("run");
    let claim: Claim<'_> = bench.take(OWNER);
    assert_eq!(bench.gone(CALLER), [(OWNER, claim.session)]);
    // A session that never asked for a run is nobody's caller.
    assert!(bench.gone(OTHER).is_empty());
}

#[test]
fn two_sessions_of_one_identity_have_tokens_of_their_own_never_reused() {
    let mut bench = Bench::with_action();
    // `ask` gives every session the same class and user: one identity, twice.
    bench.run(CALLER, 1, 0, false).expect("run");
    let one: Claim<'_> = bench.take(OWNER);
    bench.run(OTHER, 1, 0, false).expect("run");
    let two: Claim<'_> = bench.take(OWNER);
    assert_ne!(one.session, two.session);

    assert_eq!(bench.gone(CALLER), [(OWNER, one.session)]);
    let _: Cancel = bench.take(OWNER);
    // The comm core's number comes round again: the token does not.
    bench.run(CALLER, 1, 0, false).expect("run");
    let again: Claim<'_> = bench.take(OWNER);
    assert!(again.session != one.session && again.session != two.session);

    // Nor after the comm core is gone and every session with it.
    bench.actions.forget_sessions();
    assert!(
        bench.gone(OTHER).is_empty(),
        "a forgotten session was told of"
    );
    bench.register(OWNER, "set", fan()).expect("register");
    bench.sent.frames.clear();
    bench.run(CALLER, 1, 0, false).expect("run");
    let after: Claim<'_> = bench.take(OWNER);
    assert!(after.session > again.session);
}

#[test]
fn a_detached_run_goes_on_and_its_program_is_told_its_caller_is_gone() {
    let (mut bench, id) = Bench::running(true);
    bench.run(CALLER, 8, 0, false).expect("run");
    let claim: Claim<'_> = bench.take(OWNER);
    assert_eq!(bench.gone(CALLER), [(OWNER, claim.session)]);
    // Only the run that was not detached is cancelled.
    let cancel: Cancel = bench.take(OWNER);
    assert_eq!(cancel.call, claim.call);
    assert!(bench.sent.frames.is_empty());
    // Both are still listed: the cancelled one ends when its program says so.
    assert_eq!(bench.actions.running(), 2);
    assert_ne!(id, claim.call);
}

#[test]
fn a_program_is_not_told_of_its_own_session() {
    let mut bench = Bench::with_two();
    // The program behind `get` asks for a run of `set`, and `set`'s program for one of `get`.
    bench.run(KEEPER, 1, 0, false).expect("run");
    let to_owner: Claim<'_> = bench.take(OWNER);
    bench.run_get(OWNER, 1);
    let _: Claim<'_> = bench.take(KEEPER);
    // `KEEPER` goes: `OWNER` is told, and is not told of `KEEPER` again when it goes itself.
    assert_eq!(bench.gone(KEEPER), [(OWNER, to_owner.session)]);
    let _: Cancel = bench.take(OWNER);
    // `OWNER` goes: `KEEPER`, gone, is told nothing.
    assert!(bench.gone(OWNER).is_empty());
}

/// A peer's session, carrying runs for sessions of its own.
const PEER: u32 = 50;
/// The class bit of a peer.
const AS_PEER: u8 = orion_check::PrincipalClass::Peer as u8;

impl Bench {
    /// A run of `set` asked by `from`, of `class`, for the caller it names `on_behalf`; the
    /// token its claim carried.
    fn run_for(&mut self, from: u32, class: u8, call: u64, on_behalf: u64) -> u64 {
        self.ask_class(
            from,
            class,
            &Run {
                call,
                name: "set".parse().expect("name"),
                target: fan(),
                args: Bytes(&[]),
                deadline_ms: 0,
                detached: false,
                on_behalf,
            },
        )
        .expect("run");
        let claim: Claim<'_> = self.take(OWNER);
        claim.session
    }
}

#[test]
fn each_caller_a_peer_carries_runs_for_is_a_caller_of_its_own() {
    let mut bench = Bench::with_action();
    let seven = bench.run_for(PEER, AS_PEER, 1, 7);
    let eight = bench.run_for(PEER, AS_PEER, 2, 8);
    let own = bench.run_for(PEER, AS_PEER, 3, 0);
    assert!(seven != eight && seven != own && eight != own);
    assert_eq!(bench.run_for(PEER, AS_PEER, 4, 7), seven);

    // The peer says its caller 7 ended: the program hears it, once, and nothing is cancelled.
    bench
        .ask_class(PEER, AS_PEER, &CallerGone { session: 7 })
        .expect("taken");
    let told = bench.gone_frames();
    assert_eq!(told, [(OWNER, seven)]);
    assert!(bench.sent.frames.is_empty());
    assert_eq!(bench.actions.take_ended(), [seven]);
    // Said again, or of a caller it never carried, it is nothing.
    bench
        .ask_class(PEER, AS_PEER, &CallerGone { session: 7 })
        .expect("taken");
    bench
        .ask_class(PEER, AS_PEER, &CallerGone { session: 99 })
        .expect("taken");
    assert!(bench.sent.frames.is_empty());

    // The link goes: every caller behind it ends, each once.
    let mut told = bench.gone(PEER);
    told.sort_unstable();
    let mut want = [(OWNER, eight), (OWNER, own)];
    want.sort_unstable();
    assert_eq!(told, want);
    let mut ended = bench.actions.take_ended();
    ended.sort_unstable();
    let mut want = [eight, own];
    want.sort_unstable();
    assert_eq!(ended, want);
}

#[test]
fn only_a_peer_speaks_for_another_session() {
    let mut bench = Bench::with_action();
    // A program on the board naming another caller is still itself.
    let own = bench.run_for(CALLER, 4, 1, 0);
    assert_eq!(bench.run_for(CALLER, 4, 2, 7), own);
    // And may not say anyone ended.
    assert_eq!(
        bench.ask_class(CALLER, 4, &CallerGone { session: own }),
        Err(Error::Denied)
    );
    assert!(bench.sent.frames.is_empty());
    // Its own end is handed on, for runs carried elsewhere.
    assert_eq!(bench.gone(CALLER), [(OWNER, own)]);
    assert_eq!(bench.actions.take_ended(), [own]);
}
