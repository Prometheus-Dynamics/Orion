//! A detached run's end, come back to: the same answer every time while it is kept, and
//! "expired" once it is not, never "no such run".

mod common;

use common::{Bench, CALLER, HOUR, OTHER, OWNER, bare, by};
use orion_action_list::{ENDED_FOR_MS, MAX_ENDED};
use orion_msg_actions::{ActionResult, Cancel, CancelReason, Claim, Outcome};
use orion_types::Error;

/// The reported sequence: started detached, attached by handle while it runs, cancelled, ended as
/// cancelled with the caller attached, then attached by handle twice. Both attaches after the
/// end hear the same end, and it is still kept.
#[test]
fn every_attach_to_an_ended_run_hears_the_same_end_until_it_expires() {
    let mut bench = Bench::with_action();
    bench.run(CALLER, 2, HOUR, true).expect("run");
    let claim: Claim<'_> = bench.take(OWNER);
    let handle = bench.handle(CALLER, 2);
    bench
        .ask(CALLER, &by(2, handle))
        .expect("attach while running");
    let cancel = Cancel {
        call: 2,
        reason: CancelReason::Asked,
        instance: handle.instance,
        run: handle.run,
    };
    bench.ask(CALLER, &cancel).expect("cancel");
    bench.sent.frames.clear();
    bench.result(claim.call, Outcome::Cancelled);
    let told: ActionResult<'_> = bench.take(CALLER);
    assert_eq!(told.outcome, Outcome::Cancelled);
    assert_eq!(bench.actions.ended(), 1);

    for attach in 0..3 {
        bench.sent.frames.clear();
        bench
            .ask(CALLER, &by(2, handle))
            .expect("attach after the end");
        let again: ActionResult<'_> = bench.take(CALLER);
        assert_eq!(
            (again.outcome, again.run, again.code),
            (Outcome::Cancelled, claim.call, 11),
            "attach {attach}"
        );
        assert_eq!(bench.actions.ended(), 1, "attach {attach} took the end");
    }
    // By its number too.
    bench.ask(CALLER, &bare(2)).expect("attach by number");
    let by_number: ActionResult<'_> = bench.take(CALLER);
    assert_eq!(by_number.outcome, Outcome::Cancelled);

    // Past the time it is kept: expired, by handle and by number, and said so.
    bench.now += ENDED_FOR_MS + 1;
    assert_eq!(bench.ask(CALLER, &by(2, handle)), Err(Error::NotFound));
    let said = bench.actions.take_said().expect("words");
    assert!(said.starts_with("expired: "), "{said}");
    assert_eq!(bench.ask(CALLER, &bare(2)), Err(Error::NotFound));
    assert!(
        bench
            .actions
            .take_said()
            .is_some_and(|said| said.starts_with("expired: "))
    );
    assert_eq!(bench.actions.ended(), 0);
    // Nor is it said to another starter, for whom the run was never there.
    assert_eq!(
        bench.ask_as(OTHER, 1001, &by(2, handle)),
        Err(Error::NotFound)
    );
    assert_eq!(bench.actions.take_said(), None);
    // A run that never was is not found, with no such words.
    assert_eq!(bench.ask(CALLER, &bare(99)), Err(Error::NotFound));
    assert_eq!(bench.actions.take_said(), None);
}

/// Past the count kept, the oldest end expires too.
#[test]
fn the_oldest_end_expires_when_more_are_kept_than_the_limit() {
    let mut bench = Bench::with_action();
    let mut first = None;
    for call in 1..=(MAX_ENDED as u64 + 1) {
        bench.run(CALLER, call, HOUR, true).expect("run");
        let claim: Claim<'_> = bench.take(OWNER);
        let handle = bench.handle(CALLER, call);
        first.get_or_insert(handle);
        bench.gone(CALLER);
        bench.result(claim.call, Outcome::Succeeded);
        bench.sent.frames.clear();
    }
    assert_eq!(bench.actions.ended(), MAX_ENDED);
    let first = first.expect("a first run");
    assert_eq!(bench.ask(CALLER, &by(1, first)), Err(Error::NotFound));
    let said = bench.actions.take_said().expect("words");
    assert!(said.starts_with("expired: "), "{said}");
}
