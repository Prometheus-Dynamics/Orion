//! Runs node logic stops itself, at a deadline or when node logic stops: each ends as its
//! program says, and as `Indeterminate` if it says nothing within the bound. Never as cancelled
//! or timed out when the program may have finished it.

use crate::common;

use common::{Bench, CALLER, HOUR, OWNER};
use orion_action_list::CONFIRM_MS;
use orion_msg_actions::{ActionResult, Cancel, CancelReason, Claim, Outcome};
use orion_types::Error;

/// A run with a deadline of 500 ms whose deadline has just passed (501 ms on, on a clock of whole
/// ms): its program was told to stop, and nothing else was sent.
fn past_its_deadline() -> (Bench, u64) {
    let mut bench = Bench::with_action();
    bench.run(CALLER, 7, 500, false).expect("run");
    let claim: Claim<'_> = bench.take(OWNER);
    let id = claim.call;
    bench.handle(CALLER, 7);
    bench.now += 501;
    bench.actions.tick(bench.now, &mut bench.sent);
    let cancel: Cancel = bench.take(OWNER);
    assert_eq!((cancel.call, cancel.reason), (id, CancelReason::Deadline));
    assert!(
        bench.sent.frames.is_empty(),
        "the caller was told before the program said"
    );
    (bench, id)
}

#[test]
fn a_run_its_program_finished_as_its_deadline_passed_ends_as_the_program_says() {
    let (mut bench, id) = past_its_deadline();
    bench.result(id, Outcome::Succeeded);
    let result: ActionResult<'_> = bench.take(CALLER);
    assert_eq!(
        (result.outcome, result.output.0, result.code),
        (Outcome::Succeeded, &b"out"[..], 11)
    );
    assert_eq!(bench.actions.running(), 0);
}

#[test]
fn a_run_whose_program_never_says_ends_indeterminate_at_the_bound() {
    let (mut bench, id) = past_its_deadline();
    bench.actions.tick(bench.now + CONFIRM_MS, &mut bench.sent);
    // The bound counts from the end of the ms the program was told in: not short.
    assert!(bench.sent.frames.is_empty());
    assert_eq!(bench.actions.running(), 1);
    bench
        .actions
        .tick(bench.now + CONFIRM_MS + 1, &mut bench.sent);
    let result: ActionResult<'_> = bench.take(CALLER);
    assert_eq!((result.outcome, result.code), (Outcome::Indeterminate, 0));
    assert!(result.detail.contains("not known"), "{}", result.detail);
    assert_eq!(
        (bench.actions.running(), bench.actions.next_deadline()),
        (0, None)
    );
    // What the program says after is of a run that ended.
    bench.result(id, Outcome::Succeeded);
    assert!(bench.sent.frames.is_empty());
}

#[test]
fn the_bound_is_the_nodes_to_set() {
    let mut bench = Bench::with_action();
    bench.actions.set_confirm(300);
    bench.run(CALLER, 7, 500, false).expect("run");
    let _: Claim<'_> = bench.take(OWNER);
    bench.handle(CALLER, 7);
    bench.actions.tick(bench.now + 501, &mut bench.sent);
    let _: Cancel = bench.take(OWNER);
    assert_eq!(bench.actions.next_deadline(), Some(bench.now + 802));
    bench.actions.tick(bench.now + 802, &mut bench.sent);
    let result: ActionResult<'_> = bench.take(CALLER);
    assert_eq!(result.outcome, Outcome::Indeterminate);
}

#[test]
fn stopping_waits_for_each_program_up_to_the_bound_and_takes_no_new_run() {
    let (mut bench, finished) = Bench::running(false);
    bench.run(CALLER, 8, HOUR, true).expect("run");
    let silent: Claim<'_> = bench.take(OWNER);
    let silent = silent.call;
    bench.actions.take_events();
    bench.actions.stopping(bench.now, &mut bench.sent);
    for id in [finished, silent] {
        let cancel: Cancel = bench.take(OWNER);
        assert_eq!((cancel.call, cancel.reason), (id, CancelReason::Stopping));
    }
    assert!(
        bench.sent.frames.is_empty(),
        "a caller was told before its program said"
    );
    assert!(bench.actions.closing());
    // No new run while stopping.
    assert_eq!(bench.run(CALLER, 9, 0, false), Err(Error::Unavailable));
    // Its stop is bounded: by the bound at the latest.
    assert_eq!(
        bench.actions.next_deadline(),
        Some(bench.now + CONFIRM_MS + 1)
    );
    // One program had finished its run: it ends as it finished.
    bench.result(finished, Outcome::Succeeded);
    let result: ActionResult<'_> = bench.take(CALLER);
    assert_eq!((result.call, result.outcome), (7, Outcome::Succeeded));
    // The other says nothing: not known, at the bound.
    bench
        .actions
        .tick(bench.now + CONFIRM_MS + 1, &mut bench.sent);
    let result: ActionResult<'_> = bench.take(CALLER);
    assert_eq!((result.call, result.outcome), (8, Outcome::Indeterminate));
    // Every run has ended, and nothing is kept for a starter to come back to.
    assert_eq!((bench.actions.running(), bench.actions.ended()), (0, 0));
}
