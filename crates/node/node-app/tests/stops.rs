//! Runs node logic stops itself, through a real caller and a real program: at a deadline, and
//! when node logic stops. Each ends as the program says, or `Indeterminate` if it says nothing
//! within `actions.confirm_ms`; never cancelled or timed out when the program may have
//! finished it.

use std::time::{Duration, Instant};

use orion::actions::{ActionCall, Outcome, Update};
use orion::{Address, Orion};
use orion_stubs::Board;

use crate::actions::{Logic, Provider, call, dir, settings};

/// The bound these tests give a program: short, so a test does not wait the default 10 s.
const CONFIRM_MS: u64 = 300;

/// A board, node logic with the short bound, the provider, and a caller.
fn board(name: &str) -> (Board, Logic, Provider, Orion) {
    let dir = dir(name);
    let board = Board::start_for_node(&dir);
    let mut settings = settings(&dir);
    settings.confirm_ms = CONFIRM_MS;
    let logic = Logic::start(settings);
    let provider = Provider::start(board.socket());
    let caller =
        Orion::connect(&Address::Local(board.socket().to_owned()), "dashboard").expect("connect");
    (board, logic, provider, caller)
}

fn with_deadline(args: &'static [u8], deadline_ms: u64) -> ActionCall<'static> {
    ActionCall {
        deadline_ms,
        ..call("open", args)
    }
}

#[test]
fn a_job_that_finished_as_its_deadline_passed_ends_as_it_finished() {
    let (_board, _logic, _provider, caller) = board("stops-first");
    let ended = caller
        .run(&with_deadline(b"first", 200))
        .expect("run")
        .finish(Duration::from_secs(5))
        .expect("it ends");
    assert_eq!(
        (ended.outcome, ended.output.as_slice()),
        (Outcome::Succeeded, &b"finished first"[..])
    );
}

#[test]
fn a_program_that_never_says_ends_indeterminate_within_the_bound() {
    let (_board, _logic, _provider, caller) = board("stops-deaf");
    let started = Instant::now();
    let ended = caller
        .run(&with_deadline(b"deaf", 200))
        .expect("run")
        .finish(Duration::from_secs(5))
        .expect("it ends");
    let took = started.elapsed();
    assert_eq!((ended.outcome, ended.code), (Outcome::Indeterminate, 0));
    assert!(ended.detail.contains("not known"), "{}", ended.detail);
    // Not before the program had its bound, and not long after.
    assert!(
        took >= Duration::from_millis(200 + CONFIRM_MS) && took < Duration::from_secs(3),
        "{took:?}"
    );
}

#[test]
fn orionctl_says_it_is_not_known_with_an_exit_code_of_its_own() {
    let (board, _logic, _provider, _caller) = board("stops-ctl");
    let words = [
        "--address",
        &board.socket().display().to_string(),
        "action",
        "run",
        "open",
        "--resource",
        "fan.left",
        "--args",
        "deaf",
        "--deadline",
        "200",
    ]
    .map(str::to_owned);
    let (mut out, mut err) = (Vec::new(), Vec::new());
    let code = orion_ctl::run(&words, &mut out, &mut err);
    let out = String::from_utf8(out).expect("text");
    // 3: not known whether it was done; 1 is not done, 2 not understood.
    assert_eq!(code, 3, "{out}");
    assert!(out.contains("Indeterminate: it ran out of time"), "{out}");
    assert!(
        out.ends_with("it is not known whether it was done: look at what the action changes\n"),
        "{out}"
    );
}

#[test]
fn stopping_with_jobs_under_way_is_bounded_and_each_ends_as_its_program_says() {
    let (_board, logic, _provider, caller) = board("stops-stopping");
    let mut answers = caller.run(&call("open", b"long")).expect("run");
    let mut finished = caller.run(&call("open", b"first")).expect("run");
    let mut silent = caller.run(&call("open", b"deaf")).expect("run");
    for run in [&mut answers, &mut finished, &mut silent] {
        assert!(matches!(
            run.wait(Duration::from_secs(5)).expect("run"),
            Some(Update::Doing(_))
        ));
    }
    let stopping = Instant::now();
    drop(logic);
    let took = stopping.elapsed();
    // Node logic's stop waited for the silent one's bound, and no longer.
    assert!(
        took >= Duration::from_millis(CONFIRM_MS) && took < Duration::from_secs(3),
        "{took:?}"
    );
    let ended = |run: &mut orion::actions::Run| run.finish(Duration::from_secs(5)).expect("ends");
    let answered = ended(&mut answers);
    assert_eq!(
        (answered.outcome, answered.detail.as_str()),
        (Outcome::Cancelled, "stopped")
    );
    assert_eq!(ended(&mut finished).outcome, Outcome::Succeeded);
    assert_eq!(ended(&mut silent).outcome, Outcome::Indeterminate);
}
