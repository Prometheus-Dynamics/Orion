//! A run's handle: what names one run of one start of node logic, when a caller's numbers
//! start over with each of its sessions.

mod common;

use common::{Bench, CALLER, HOUR, OTHER, OWNER, bare, by};
use orion_action_list::Actions;
use orion_msg_actions::{ActionResult, Cancel, CancelReason, Claim, Outcome, RunHandle};
use orion_types::Error;

/// The caller's next session.
const BACK: u32 = 21;

/// A run of `set`, detached, as `from`'s call `call`: the program's number and the handle.
fn detached(bench: &mut Bench, from: u32, call: u64) -> (u64, RunHandle) {
    bench.run(from, call, HOUR, true).expect("run");
    let claim: Claim<'_> = bench.take(OWNER);
    let handle = bench.handle(from, call);
    assert_eq!(handle.run, claim.call);
    (claim.call, handle)
}

fn cancel(call: u64, handle: Option<RunHandle>) -> Cancel {
    Cancel {
        call,
        reason: CancelReason::Asked,
        instance: handle.map_or(0, |handle| handle.instance),
        run: handle.map_or(0, |handle| handle.run),
    }
}

#[test]
fn a_new_session_reusing_a_number_follows_its_own_run_and_the_old_one_is_reached_by_handle() {
    let mut bench = Bench::with_action();
    // Session one starts a detached run as its call 7, and is lost.
    let (old, old_handle) = detached(&mut bench, CALLER, 7);
    bench.gone(CALLER);
    // Session two starts another run, also as its call 7, while the old one still runs.
    let (new, new_handle) = detached(&mut bench, BACK, 7);
    assert_ne!(old_handle, new_handle);
    // The old run ends first: nobody is following it, and session two hears nothing of it.
    bench.result(old, Outcome::Failed);
    assert!(
        bench.sent.frames.is_empty(),
        "the old end went to the new session"
    );
    // Session two's own run ends: its follow of call 7 is its own result.
    bench.result(new, Outcome::Succeeded);
    let result: ActionResult<'_> = bench.take(BACK);
    assert_eq!(
        (result.call, result.outcome, result.run),
        (7, Outcome::Succeeded, new)
    );
    // The number reaches the newer run's end, which is kept in case session two had just
    // ended too...
    bench.ask(OTHER, &bare(7)).expect("attach");
    assert_eq!(bench.handle(OTHER, 7), new_handle);
    let _: ActionResult<'_> = bench.take(OTHER);
    // ...again, the same end, and never the old one.
    bench.ask(OTHER, &bare(7)).expect("attach again");
    let again: ActionResult<'_> = bench.take(OTHER);
    assert_eq!((again.outcome, again.run), (Outcome::Succeeded, new));
    // ...only through its handle, by another session of the same starter.
    bench.ask(OTHER, &by(3, old_handle)).expect("attach");
    assert_eq!(bench.handle(OTHER, 3), old_handle);
    let result: ActionResult<'_> = bench.take(OTHER);
    assert_eq!(
        (result.call, result.outcome, result.run),
        (7, Outcome::Failed, old)
    );
    // A handle whose run has ended has nothing to cancel, but its end is still there to come
    // back to, the same each time, while it is kept.
    assert_eq!(
        bench.ask(OTHER, &cancel(0, Some(old_handle))),
        Err(Error::NotFound)
    );
    bench.ask(OTHER, &by(4, old_handle)).expect("attach again");
    let again: ActionResult<'_> = bench.take(OTHER);
    assert_eq!((again.outcome, again.run), (Outcome::Failed, old));
}

#[test]
fn an_old_run_still_going_is_cancelled_by_its_handle_not_by_the_number() {
    let mut bench = Bench::with_action();
    let (old, old_handle) = detached(&mut bench, CALLER, 7);
    bench.gone(CALLER);
    let (new, _) = detached(&mut bench, BACK, 7);
    // By the number, session two cancels its own run, never the old one.
    bench.ask(BACK, &cancel(7, None)).expect("cancel");
    let told: Cancel = bench.take(OWNER);
    assert_eq!(told.call, new);
    // By the old handle, the old one.
    bench
        .ask(BACK, &cancel(7, Some(old_handle)))
        .expect("cancel");
    let told: Cancel = bench.take(OWNER);
    assert_eq!(told.call, old);
    // Coming back to the number is to the newer run, and gives its handle.
    bench.ask(OTHER, &bare(7)).expect("attach");
    assert_eq!(bench.handle(OTHER, 7).run, new);
}

#[test]
fn a_plain_run_under_a_kept_number_is_its_own_and_the_old_answer_is_never_given_for_it() {
    let mut bench = Bench::with_action();
    // Session one's detached run ends while nobody is there: its answer is kept under 7.
    let (old, old_handle) = detached(&mut bench, CALLER, 7);
    bench.gone(CALLER);
    bench.result(old, Outcome::Failed);
    assert_eq!(bench.actions.ended(), 1);
    // Session two starts a plain run as its 7.
    bench.run(BACK, 7, 0, false).expect("run");
    let claim: Claim<'_> = bench.take(OWNER);
    let plain = bench.handle(BACK, 7);
    // Its result is its own.
    bench.result(claim.call, Outcome::Succeeded);
    let result: ActionResult<'_> = bench.take(BACK);
    assert_eq!(
        (result.call, result.outcome, result.run),
        (7, Outcome::Succeeded, plain.run)
    );
    // A number alone is refused, saying why, never answered with the old run's end.
    assert_eq!(bench.ask(BACK, &bare(7)), Err(Error::NotFound));
    let said = bench.actions.take_said().expect("words");
    assert!(said.contains("by its handle"), "{said}");
    assert!(
        bench.sent.frames.is_empty(),
        "something was given for the number"
    );
    // The old answer is still there, by its handle.
    bench.ask(BACK, &by(8, old_handle)).expect("attach");
    assert_eq!(bench.handle(BACK, 8), old_handle);
    let result: ActionResult<'_> = bench.take(BACK);
    assert_eq!((result.outcome, result.run), (Outcome::Failed, old));
}

#[test]
fn a_cancel_after_coming_back_reaches_the_run_come_back_to() {
    let mut bench = Bench::with_action();
    let (id, handle) = detached(&mut bench, CALLER, 7);
    bench.gone(CALLER);
    // Back by the handle, under a number of the new session's own.
    bench.ask(BACK, &by(1, handle)).expect("attach");
    assert_eq!(bench.handle(BACK, 1), handle);
    bench.ask(BACK, &cancel(1, Some(handle))).expect("cancel");
    let told: Cancel = bench.take(OWNER);
    assert_eq!((told.call, told.reason), (id, CancelReason::Asked));
    // Its end comes to the session that came back.
    bench.result(id, Outcome::Cancelled);
    let result: ActionResult<'_> = bench.take(BACK);
    assert_eq!((result.run, result.outcome), (id, Outcome::Cancelled));
}

#[test]
fn after_node_logic_restarts_a_handle_from_before_reaches_nothing_and_says_why() {
    let mut bench = Bench::with_action();
    let (_, before) = detached(&mut bench, CALLER, 7);
    // Node logic starts again: everything it knew is gone.
    bench.actions = Actions::with_instance("board".parse().expect("name"), before.instance ^ 1);
    bench
        .register(OWNER, "set", common::fan())
        .expect("register");
    bench.sent.frames.clear();
    // The same first number is drawn again, under another instance.
    let (_, after) = detached(&mut bench, BACK, 7);
    assert_eq!(after.run, before.run);
    assert_ne!(after, before);
    assert_eq!(bench.ask(BACK, &by(2, before)), Err(Error::RunGone));
    let said = bench.actions.take_said().expect("words");
    assert!(said.contains("it is gone"), "{said}");
    assert_eq!(
        bench.ask(BACK, &cancel(2, Some(before))),
        Err(Error::RunGone)
    );
    assert!(bench.actions.take_said().is_some());
    // Nothing was sent to the program, and the run of this start goes on.
    assert!(bench.sent.frames.is_empty());
    assert_eq!(bench.actions.running(), 1);
}

#[test]
fn each_start_of_node_logic_draws_its_own_instance() {
    let one = Actions::new("board".parse().expect("name"));
    let two = Actions::new("board".parse().expect("name"));
    assert_ne!(one.instance(), 0);
    assert_ne!(one.instance(), two.instance());
}

#[test]
fn a_handle_reads_back_from_its_bytes_and_its_text() {
    let handle = RunHandle::said(0x0123_4567_89ab_cdef, 6).expect("a handle");
    assert_eq!(RunHandle::from_bytes(&handle.to_bytes()), Some(handle));
    assert_eq!(RunHandle::from_bytes(&[0; 16]), None);
    let text = handle.to_string();
    assert_eq!(text, "0123456789abcdef-0000000000000006");
    assert_eq!(text.parse::<RunHandle>().expect("parse"), handle);
    assert!("0123-6".parse::<RunHandle>().is_err());
    assert_eq!(RunHandle::said(0, 6), None);
    assert_eq!(RunHandle::said(1, 0), None);
}
