//! The list: registering, routing a run to whoever registered it, and what happens to a run
//! when either end goes away or it runs out of time.

mod common;

use common::{Bench, CALLER, HOUR, OTHER, OWNER, bare, by, fan};
use orion_action_list::{ENDED_FOR_MS, Event, MAX_ENDED, MAX_RUNS};
use orion_check::{Admits, Policy};
use orion_msg_actions::{
    ActionResult, Cancel, CancelReason, Claim, DEADLINE_CODE, Outcome, Progress, Register,
    Registered, Target,
};
use orion_reply::Asked;
use orion_types::Error;
use orion_wire::{Bytes, kind};

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
    // The caller is told the run's handle, which names it whatever its number.
    let handle = bench.handle(CALLER, 7);
    assert_eq!(handle.run, claim.call);
    assert_eq!(bench.actions.running(), 1);

    bench
        .ask(
            OWNER,
            &Progress {
                call: claim.call,
                percent: 40,
                message: "spinning up",
                data: Bytes(b"d"),
                instance: 0,
                run: 0,
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
    assert_eq!(
        (progress.instance, progress.run),
        (handle.instance, handle.run)
    );

    bench
        .ask(
            OWNER,
            &ActionResult {
                call: claim.call,
                outcome: Outcome::Succeeded,
                output: Bytes(b"ok"),
                detail: "",
                code: 0,
                instance: 0,
                run: 0,
                action: "",
            },
        )
        .expect("result");
    let result: ActionResult<'_> = bench.take(CALLER);
    assert_eq!((result.call, result.outcome), (7, Outcome::Succeeded));
    assert_eq!(result.output.0, b"ok");
    assert_eq!(
        (result.instance, result.run, result.action),
        (handle.instance, handle.run, "set")
    );
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
    // What a caller says of why is not kept: it asked.
    let cancel = Cancel {
        call: 7,
        reason: CancelReason::Deadline,
        instance: 0,
        run: 0,
    };
    bench.ask(CALLER, &cancel).expect("cancel");
    let cancel: Cancel = bench.take(OWNER);
    assert_eq!((cancel.call, cancel.reason), (id, CancelReason::Asked));
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
                code: 0,
                instance: 0,
                run: 0,
                action: "",
            },
        )
        .expect("result");
    let result: ActionResult<'_> = bench.take(CALLER);
    assert_eq!(
        (result.outcome, result.detail),
        (Outcome::Cancelled, "stopped")
    );
    // Someone else's cancel is of a run they do not have.
    let cancel = Cancel {
        call: 7,
        reason: CancelReason::Asked,
        instance: 0,
        run: 0,
    };
    assert_eq!(bench.ask(OTHER, &cancel), Err(Error::NotFound));
}

#[test]
fn another_nodes_reason_for_a_run_it_carried_is_passed_on() {
    const PEER: u8 = orion_check::PrincipalClass::Peer as u8;
    let (mut bench, id) = Bench::running(false);
    let gone = Cancel {
        call: 7,
        reason: CancelReason::CallerGone,
        instance: 0,
        run: 0,
    };
    bench.ask_class(CALLER, PEER, &gone).expect("cancel");
    let cancel: Cancel = bench.take(OWNER);
    assert_eq!((cancel.call, cancel.reason), (id, CancelReason::CallerGone));
    // A node that does not say is taken to have asked.
    let unsaid = Cancel {
        call: 7,
        reason: CancelReason::Unsaid,
        instance: 0,
        run: 0,
    };
    bench.ask_class(CALLER, PEER, &unsaid).expect("cancel");
    let cancel: Cancel = bench.take(OWNER);
    assert_eq!(cancel.reason, CancelReason::Asked);
}

#[test]
fn node_logic_stopping_cancels_every_run_and_says_why() {
    let (mut bench, first) = Bench::running(false);
    bench.run(CALLER, 8, HOUR, true).expect("run");
    let detached: Claim<'_> = bench.take(OWNER);
    let detached = detached.call;
    bench.actions.stopping(bench.now, &mut bench.sent);
    for id in [first, detached] {
        let cancel: Cancel = bench.take(OWNER);
        assert_eq!((cancel.call, cancel.reason), (id, CancelReason::Stopping));
        let result: ActionResult<'_> = bench.take(CALLER);
        assert_eq!(
            (result.outcome, result.detail, result.code),
            (Outcome::Cancelled, "node logic is stopping", 0)
        );
    }
    assert_eq!((bench.actions.running(), bench.actions.ended()), (0, 0));
    assert!(bench.sent.frames.is_empty());
}

#[test]
fn only_the_program_that_runs_it_speaks_for_a_run() {
    let (mut bench, id) = Bench::running(false);
    let result = ActionResult {
        call: id,
        outcome: Outcome::Succeeded,
        output: Bytes(&[]),
        detail: "",
        code: 0,
        instance: 0,
        run: 0,
        action: "",
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
    bench.gone(CALLER);
    let cancel: Cancel = bench.take(OWNER);
    assert_eq!((cancel.call, cancel.reason), (id, CancelReason::CallerGone));
    // The program's last word about it goes nowhere, and is not an error.
    bench
        .ask(
            OWNER,
            &ActionResult {
                call: id,
                outcome: Outcome::Cancelled,
                output: Bytes(&[]),
                detail: "",
                code: 0,
                instance: 0,
                run: 0,
                action: "",
            },
        )
        .expect("result");
    assert!(bench.sent.frames.is_empty());
    assert_eq!(bench.actions.running(), 0);

    let (mut bench, id) = Bench::running(true);
    bench.gone(CALLER);
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
                instance: 0,
                run: 0,
            },
        )
        .expect("progress");
    assert!(bench.sent.frames.is_empty());
}

#[test]
fn a_program_that_goes_takes_its_actions_and_fails_its_runs() {
    let (mut bench, _) = Bench::running(false);
    bench.gone(OWNER);
    let result: ActionResult<'_> = bench.take(CALLER);
    assert_eq!(
        (result.call, result.outcome, result.code),
        (7, Outcome::Failed, 0)
    );
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
    bench.handle(CALLER, 7);
    assert_eq!((claim.deadline_ms, claim.detached), (500, false));
    assert_eq!(bench.actions.next_deadline(), Some(bench.now + 500));
    bench.actions.tick(bench.now + 499, &mut bench.sent);
    assert!(bench.sent.frames.is_empty());
    bench.actions.tick(bench.now + 500, &mut bench.sent);
    let cancel: Cancel = bench.take(OWNER);
    assert_eq!(
        (cancel.call, cancel.reason),
        (claim.call, CancelReason::Deadline)
    );
    let result: ActionResult<'_> = bench.take(CALLER);
    // Ended by node logic for its deadline, with the code that says so.
    assert_eq!(
        (result.call, result.outcome, result.code),
        (7, Outcome::TimedOut, DEADLINE_CODE)
    );
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
                code: 0,
                instance: 0,
                run: 0,
                action: "",
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
                instance: 0,
                run: 0,
            },
        )
        .expect("progress");
    let _: Progress<'_> = bench.take(CALLER);
    bench.gone(CALLER);
    // It goes on while nobody listens.
    bench
        .ask(
            OWNER,
            &Progress {
                call: id,
                percent: 60,
                message: "still working",
                data: Bytes(&[]),
                instance: 0,
                run: 0,
            },
        )
        .expect("progress");
    assert!(bench.sent.frames.is_empty());

    // Back, on another session: the latest progress, at once.
    bench.ask(BACK, &bare(7)).expect("attach");
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
                instance: 0,
                run: 0,
            },
        )
        .expect("progress");
    let progress: Progress<'_> = bench.take(BACK);
    assert_eq!(progress.percent, 90);
    bench.result(id, Outcome::Succeeded);
    let result: ActionResult<'_> = bench.take(BACK);
    assert_eq!(
        (result.call, result.outcome, result.code),
        (7, Outcome::Succeeded, 11)
    );
    // It is kept all the same, in case the session it was sent to had just ended.
    assert_eq!(bench.actions.ended(), 1);
    bench.ask(BACK, &bare(7)).expect("attach");
    let again: ActionResult<'_> = bench.take(BACK);
    assert_eq!((again.outcome, again.code), (Outcome::Succeeded, 11));
    // Coming back does not take it: it is there for the next time too.
    assert_eq!(bench.actions.ended(), 1);
}

#[test]
fn a_detached_run_that_ended_with_nobody_there_is_kept_for_its_starter() {
    let (mut bench, id) = Bench::running(true);
    bench.gone(CALLER);
    bench.result(id, Outcome::Failed);
    assert!(bench.sent.frames.is_empty());
    assert_eq!(bench.actions.ended(), 1);
    bench.now += 60_000;
    bench.ask(BACK, &bare(7)).expect("attach");
    let result: ActionResult<'_> = bench.take(BACK);
    // The program's own code, kept as it said it.
    assert_eq!(
        (result.call, result.outcome, result.code),
        (7, Outcome::Failed, 11)
    );
    assert_eq!(result.output.0, b"out");
    // Told again, the same, as often as it is asked while it is kept.
    bench.ask(BACK, &bare(7)).expect("attach again");
    let again: ActionResult<'_> = bench.take(BACK);
    assert_eq!((again.outcome, again.code), (Outcome::Failed, 11));

    // Kept for ten minutes and no longer; then it is said to have expired.
    let (mut bench, id) = Bench::running(true);
    bench.gone(CALLER);
    bench.result(id, Outcome::Succeeded);
    bench.now += ENDED_FOR_MS + 1;
    assert_eq!(bench.ask(BACK, &bare(7)), Err(Error::NotFound));
    let said = bench.actions.take_said().expect("words");
    assert!(said.starts_with("expired: "), "{said}");
    assert_eq!(bench.actions.ended(), 0);
}

#[test]
fn a_new_run_under_a_number_takes_it_and_the_earlier_end_is_reached_by_its_handle() {
    // A caller's numbers start over with each session: its second session's run 7 is not
    // its first session's.
    let mut bench = Bench::with_action();
    bench.run(CALLER, 7, HOUR, true).expect("run");
    let first: Claim<'_> = bench.take(OWNER);
    let first = (first.call, bench.handle(CALLER, 7));
    bench.result(first.0, Outcome::Failed);
    let _: ActionResult<'_> = bench.take(CALLER);
    assert_eq!(bench.actions.ended(), 1);
    bench.gone(CALLER);
    bench.run(BACK, 7, HOUR, true).expect("run");
    let claim: Claim<'_> = bench.take(OWNER);
    let second = bench.handle(BACK, 7);
    assert_ne!(second, first.1);
    assert_eq!(bench.actions.ended(), 1, "the earlier end is still kept");
    bench.gone(BACK);
    bench.result(claim.call, Outcome::Succeeded);
    // By its number: the newer run's end.
    bench.ask(30, &bare(7)).expect("attach");
    assert_eq!(bench.handle(30, 7), second);
    let result: ActionResult<'_> = bench.take(30);
    assert_eq!(
        (result.outcome, result.run),
        (Outcome::Succeeded, second.run)
    );
    // The number keeps reaching the newer run's end, never the earlier one...
    bench.ask(30, &bare(7)).expect("attach again");
    let again: ActionResult<'_> = bench.take(30);
    assert_eq!(again.run, second.run);
    // ...which is reached by its handle.
    bench.ask(30, &by(9, first.1)).expect("attach");
    assert_eq!(bench.handle(30, 9), first.1);
    let result: ActionResult<'_> = bench.take(30);
    assert_eq!(
        (result.call, result.outcome, result.run),
        (7, Outcome::Failed, first.0)
    );
    assert_eq!(bench.actions.ended(), 2);
}

#[test]
fn only_whoever_started_it_comes_back_to_a_run() {
    let (mut bench, id) = Bench::running(true);
    bench.gone(CALLER);
    // Another user on the board, with the right number.
    assert_eq!(bench.ask_as(OTHER, 1001, &bare(7)), Err(Error::NotFound));
    bench.result(id, Outcome::Succeeded);
    assert_eq!(bench.ask_as(OTHER, 1001, &bare(7)), Err(Error::NotFound));
    assert_eq!(
        bench.actions.ended(),
        1,
        "it is still there for its starter"
    );
    // A run that was not detached is not come back to, even by its starter.
    let (mut bench, _) = Bench::running(false);
    assert_eq!(bench.ask(BACK, &bare(7)), Err(Error::NotFound));
}

#[test]
fn a_detached_run_is_cancelled_by_its_handle_from_the_session_that_came_back() {
    let (mut bench, id) = Bench::running(true);
    bench.gone(CALLER);
    let bare_cancel = Cancel {
        call: 7,
        reason: CancelReason::Asked,
        instance: 0,
        run: 0,
    };
    // A number alone is only ever the session's own: this one started nothing.
    assert_eq!(bench.ask(BACK, &bare_cancel), Err(Error::NotFound));
    bench.ask(BACK, &bare(7)).expect("attach");
    let handle = bench.handle(BACK, 7);
    // Coming back does not make the number its own.
    assert_eq!(bench.ask(BACK, &bare_cancel), Err(Error::NotFound));
    let by_handle = Cancel {
        instance: handle.instance,
        run: handle.run,
        ..bare_cancel
    };
    bench.ask(BACK, &by_handle).expect("cancel");
    let cancel: Cancel = bench.take(OWNER);
    assert_eq!((cancel.call, cancel.reason), (id, CancelReason::Asked));
    // Someone else with the handle is not its starter.
    assert_eq!(bench.ask_as(OTHER, 1001, &by_handle), Err(Error::NotFound));
}

#[test]
fn a_detached_run_still_has_its_deadline_and_its_program() {
    let mut bench = Bench::with_action();
    bench.run(CALLER, 7, 500, true).expect("run");
    let claim: Claim<'_> = bench.take(OWNER);
    // The program is told the run goes on without its caller.
    assert!(claim.detached);
    bench.gone(CALLER);
    bench.now += 500;
    bench.actions.tick(bench.now, &mut bench.sent);
    let cancel: Cancel = bench.take(OWNER);
    assert_eq!(cancel.call, claim.call);
    bench.ask(BACK, &bare(7)).expect("attach");
    let result: ActionResult<'_> = bench.take(BACK);
    assert_eq!(
        (result.outcome, result.code),
        (Outcome::TimedOut, DEADLINE_CODE)
    );

    let (mut bench, _) = Bench::running(true);
    bench.gone(CALLER);
    bench.gone(OWNER);
    bench.ask(BACK, &bare(7)).expect("attach");
    let result: ActionResult<'_> = bench.take(BACK);
    // Ended by node logic another way: no code.
    assert_eq!((result.outcome, result.code), (Outcome::Failed, 0));
}

#[test]
fn only_so_many_ended_runs_are_kept() {
    let mut bench = Bench::with_action();
    for call in 0..(MAX_ENDED as u64 + 10) {
        bench.run(CALLER, call, HOUR, true).expect("run");
        let claim: Claim<'_> = bench.take(OWNER);
        bench.gone(CALLER);
        bench.result(claim.call, Outcome::Succeeded);
    }
    assert_eq!(bench.actions.ended(), MAX_ENDED);
    // The oldest went first.
    assert_eq!(bench.ask(BACK, &bare(0)), Err(Error::NotFound));
    bench.ask(BACK, &bare(20)).expect("attach");
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
