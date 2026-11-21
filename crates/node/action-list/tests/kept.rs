//! A detached run of an action registered with `keep_ms` is kept while its program's session
//! is lost: given back when the same identity registers the action again, and ended
//! `Indeterminate` if it does not come back in time. Never failed or cancelled by assumption.

use crate::common;

use common::{Bench, CALLER, HOUR, OTHER, OWNER, fan};
use orion_action_list::MAX_KEEP_MS;
use orion_msg_actions::{ActionResult, Cancel, CancelReason, Claim, Outcome, Progress, Registered};
use orion_types::Error;
use orion_wire::Bytes;

/// The program's new session, after its old one (`OWNER`) was lost.
const BACK: u32 = 11;
const KEEP: u32 = 10_000;

/// A detached run of `set`, registered by `OWNER` to be kept `KEEP` ms, under way: the list's
/// number for it.
fn kept_run() -> (Bench, u64) {
    let mut bench = Bench::new();
    bench
        .register_keeping(OWNER, "set", fan(), KEEP)
        .expect("register");
    bench.sent.frames.clear();
    bench.run(CALLER, 7, HOUR, true).expect("run");
    let claim: Claim<'_> = bench.take(OWNER);
    assert!(!claim.kept);
    bench.handle(CALLER, 7);
    (bench, claim.call)
}

/// `OWNER`'s session is lost: the caller is told the run is restoring, and nothing ends.
fn lost(bench: &mut Bench) {
    bench.gone(OWNER);
    let progress: Progress<'_> = bench.take(CALLER);
    assert!(
        progress.message.starts_with("restoring"),
        "{}",
        progress.message
    );
    assert!(bench.sent.frames.is_empty(), "{:?}", bench.sent.frames);
    assert_eq!(bench.actions.running(), 1);
}

#[test]
fn a_kept_run_is_given_back_to_its_program_when_it_registers_again_and_ends_as_it_says() {
    let (mut bench, id) = kept_run();
    lost(&mut bench);
    bench.now += KEEP as u64 - 1;
    bench.actions.tick(bench.now, &mut bench.sent);
    assert!(bench.sent.frames.is_empty(), "it ended while kept");
    // Back, as the same identity: the run comes before the registration's answer.
    bench
        .register_keeping(BACK, "set", fan(), KEEP)
        .expect("register again");
    let claim: Claim<'_> = bench.take(BACK);
    assert_eq!((claim.call, claim.kept, claim.detached), (id, true, true));
    assert_eq!(claim.args, Bytes(&[]));
    assert!(claim.deadline_ms > 0 && claim.deadline_ms <= HOUR);
    let restored: Progress<'_> = bench.take(CALLER);
    assert!(
        restored.message.starts_with("restored"),
        "{}",
        restored.message
    );
    let _: Registered = bench.take(BACK);
    // It goes on, and ends as the program says, on the original run.
    bench
        .ask(
            BACK,
            &ActionResult {
                call: id,
                outcome: Outcome::Succeeded,
                output: Bytes(b"done"),
                detail: "",
                code: 0,
                instance: 0,
                run: 0,
                action: "",
            },
        )
        .expect("result");
    let result: ActionResult<'_> = bench.take(CALLER);
    assert_eq!((result.outcome, result.run), (Outcome::Succeeded, id));
    assert_eq!(bench.actions.running(), 0);
}

#[test]
fn a_kept_run_whose_program_does_not_come_back_in_time_ends_indeterminate_and_a_late_word_is_refused()
 {
    let (mut bench, id) = kept_run();
    lost(&mut bench);
    assert_eq!(bench.actions.next_deadline(), Some(bench.now + KEEP as u64));
    bench.now += KEEP as u64;
    bench.actions.tick(bench.now, &mut bench.sent);
    let result: ActionResult<'_> = bench.take(CALLER);
    assert_eq!((result.outcome, result.run), (Outcome::Indeterminate, id));
    assert!(
        result.detail.contains("did not come back within 10000 ms"),
        "{}",
        result.detail
    );
    // Back too late: nothing is given back, and its word on the run is refused by name.
    bench
        .register_keeping(BACK, "set", fan(), KEEP)
        .expect("register again");
    let _: Registered = bench.take(BACK);
    let late = bench.ask(
        BACK,
        &ActionResult {
            call: id,
            outcome: Outcome::Succeeded,
            output: Bytes(b""),
            detail: "",
            code: 0,
            instance: 0,
            run: 0,
            action: "",
        },
    );
    assert_eq!(late, Err(Error::RunGone));
    let said = bench.actions.take_said().expect("words");
    assert!(said.contains("already ended as indeterminate"), "{said}");
}

#[test]
fn another_identity_registering_the_name_is_not_given_a_kept_run() {
    let (mut bench, _) = kept_run();
    lost(&mut bench);
    bench
        .ask_as(
            OTHER,
            4242,
            &orion_msg_actions::Register {
                name: "set".parse().expect("name"),
                target: fan(),
                policy: orion_check::Policy::new(orion_check::Admits::LOCAL),
                description: "sets the speed",
                keep_ms: KEEP,
            },
        )
        .expect("register");
    let _: Registered = bench.take(OTHER);
    assert!(bench.sent.frames.is_empty(), "{:?}", bench.sent.frames);
    // Nor may its session, which may have the old one's number, speak for the run.
    assert_eq!(bench.actions.running(), 1);
}

#[test]
fn a_cancel_asked_while_kept_reaches_the_program_when_it_is_back() {
    let (mut bench, id) = kept_run();
    lost(&mut bench);
    bench
        .ask(
            CALLER,
            &Cancel {
                call: 7,
                reason: CancelReason::Asked,
                instance: 0,
                run: 0,
            },
        )
        .expect("cancel");
    assert!(bench.sent.frames.is_empty());
    bench
        .register_keeping(BACK, "set", fan(), KEEP)
        .expect("register again");
    let claim: Claim<'_> = bench.take(BACK);
    assert_eq!(claim.call, id);
    let _: Progress<'_> = bench.take(CALLER);
    let cancel: Cancel = bench.take(BACK);
    assert_eq!((cancel.call, cancel.reason), (id, CancelReason::Asked));
}

#[test]
fn a_run_whose_deadline_passed_while_kept_is_told_to_stop_when_its_program_is_back() {
    let mut bench = Bench::new();
    bench
        .register_keeping(OWNER, "set", fan(), KEEP)
        .expect("register");
    bench.sent.frames.clear();
    bench.run(CALLER, 7, 500, true).expect("run");
    let claim: Claim<'_> = bench.take(OWNER);
    bench.handle(CALLER, 7);
    lost(&mut bench);
    bench.now += 600;
    bench.actions.tick(bench.now, &mut bench.sent);
    assert!(bench.sent.frames.is_empty(), "nobody to tell while kept");
    bench
        .register_keeping(BACK, "set", fan(), KEEP)
        .expect("register again");
    let _: Claim<'_> = bench.take(BACK);
    let _: Progress<'_> = bench.take(CALLER);
    let cancel: Cancel = bench.take(BACK);
    assert_eq!(
        (cancel.call, cancel.reason),
        (claim.call, CancelReason::Deadline)
    );
}

#[test]
fn node_logic_stopping_ends_a_kept_run_indeterminate_at_once() {
    let (mut bench, id) = kept_run();
    lost(&mut bench);
    bench.actions.stopping(bench.now, &mut bench.sent);
    let result: ActionResult<'_> = bench.take(CALLER);
    assert_eq!((result.outcome, result.run), (Outcome::Indeterminate, id));
    assert_eq!(bench.actions.running(), 0);
}

#[test]
fn a_run_not_asked_to_be_kept_still_ends_failed_at_once_and_the_keep_is_capped() {
    let (mut bench, id) = Bench::running(true);
    bench.gone(OWNER);
    let result: ActionResult<'_> = bench.take(CALLER);
    assert_eq!((result.outcome, result.run), (Outcome::Failed, id));
    // A keep longer than the node's limit is held to it.
    let mut bench = Bench::new();
    bench
        .register_keeping(OWNER, "set", fan(), u32::MAX)
        .expect("register");
    bench.sent.frames.clear();
    bench.run(CALLER, 7, HOUR, true).expect("run");
    let _: Claim<'_> = bench.take(OWNER);
    bench.handle(CALLER, 7);
    lost(&mut bench);
    assert_eq!(bench.actions.next_deadline(), Some(bench.now + MAX_KEEP_MS));
}
