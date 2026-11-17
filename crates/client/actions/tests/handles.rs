//! Run handles against node logic's own action list, which the stub board runs behind its
//! door: what a caller that lost its session can and cannot reach by a handle, and what a
//! restart of node logic leaves.

#![cfg(target_os = "linux")]

use std::time::Duration;

use orion_actions::{
    ActionCall, ActionDecl, Actions, Admits, Outcome, Policy, Registration, Request, Run,
    RunHandle, Target,
};
use orion_connect::{Address, Client, ClientError, Options, Session};
use orion_server::Error;
use orion_stubs::Board;

/// How long anything here may take. Each wait ends as soon as what it waits for comes.
const WAIT: Duration = Duration::from_secs(10);

fn actions(board: &Board, name: &str) -> Actions {
    let session = Session::connect(
        &Address::Local(board.socket().to_owned()),
        name,
        Options::default(),
    )
    .expect("connect");
    Actions::new(&Client::new(session))
}

fn node() -> Target {
    Target::Node("board".parse().expect("name"))
}

/// The program that owns `arm.move`: its actions, and where its runs arrive.
fn owner(board: &Board) -> (Actions, Registration) {
    let side = actions(board, "arm");
    let runs = side
        .register(
            &ActionDecl {
                name: "arm.move",
                target: node(),
                description: "Moves the arm",
            },
            Policy::new(Admits::LOCAL),
        )
        .expect("register");
    (side, runs)
}

/// What the owner is handed next: a run's number on its side and arguments, or a cancel.
fn handed(owner: &mut Registration) -> Request {
    loop {
        match owner.wait(WAIT).expect("wait") {
            Some(Request::CallerGone { .. }) => {}
            Some(request) => return request,
            None => panic!("the owner was handed nothing"),
        }
    }
}

/// Starts a detached `arm.move` with `args`; the run, its handle, and its number at the owner.
fn start(caller: &Actions, owner: &mut Registration, args: &[u8]) -> (Run, RunHandle, u64) {
    let mut run = caller
        .run(&ActionCall {
            name: "arm.move",
            target: node(),
            args,
            deadline_ms: 60_000,
            detached: true,
        })
        .expect("run");
    let Request::Run {
        call, args: got, ..
    } = handed(owner)
    else {
        panic!("not a run");
    };
    assert_eq!(got, args);
    // A start is no update: wait a little at a time, and look for the handle between.
    let since = std::time::Instant::now();
    while run.handle().is_none() {
        assert!(since.elapsed() < WAIT, "the run never started");
        run.wait(Duration::from_millis(10)).expect("the run starts");
    }
    let handle = run.handle().expect("a handle");
    (run, handle, call)
}

#[test]
fn a_handle_from_a_lost_session_follows_its_own_run_never_the_new_one_under_its_number() {
    let board = Board::start("handles-collision");
    let (_side, mut owner) = owner(&board);
    let lost = actions(&board, "caller");
    let (run, first, first_claim) = start(&lost, &mut owner, b"first");
    let number = run.call();
    // The session is lost; the run is detached, and goes on.
    drop(run);
    drop(lost);

    // The same caller, a new session: its numbers start over, so its next run has the same.
    let caller = actions(&board, "caller");
    let (mut second_run, second, second_claim) = start(&caller, &mut owner, b"second");
    assert_eq!(second_run.call(), number, "the number came round again");
    assert_ne!(second, first, "each run has a handle of its own");
    assert_eq!(first.instance, board.node_logic_instance());
    assert_ne!((first.instance, first.run), (0, 0));

    // Following the old handle gives the old run: how it ended, not the new one.
    owner
        .finish(first_claim, Outcome::Succeeded, b"first done", "")
        .expect("finish");
    let mut back = caller.attach_handle(first).expect("attach");
    assert_eq!(back.finish(WAIT).expect("ended").output, b"first done");
    // And the new one was never touched.
    owner
        .finish(second_claim, Outcome::Succeeded, b"second done", "")
        .expect("finish");
    assert_eq!(
        second_run.finish(WAIT).expect("ended").output,
        b"second done"
    );
}

#[test]
fn a_handle_from_before_node_logic_started_again_is_told_the_run_is_gone() {
    let board = Board::start("handles-restart");
    let (_side, mut owner) = owner(&board);
    let caller = actions(&board, "caller");
    let (_run, before, _) = start(&caller, &mut owner, b"before");
    let first = board.node_logic_instance();
    assert_eq!(before.instance, first);

    let again = board.restart_node_logic();
    assert_ne!(again, first, "a start of its own");
    let mut back = caller.attach_handle(before).expect("sent");
    match back.finish(WAIT) {
        Err(ClientError::Refused {
            error: Error::RunGone,
            ..
        }) => {}
        other => panic!("not told the run is gone: {other:?}"),
    }
    // The action is still listed, as the comm core handed it over again: a new run reaches
    // the owner, with a handle of the new start.
    let (_after, handle, _) = start(&caller, &mut owner, b"after");
    assert_eq!(handle.instance, again);
}

#[test]
fn a_cancel_by_an_ended_runs_handle_never_reaches_the_newer_run_under_its_number() {
    let board = Board::start("handles-cancel");
    let (_side, mut owner) = owner(&board);
    let lost = actions(&board, "caller");
    let (mut old_run, old, old_claim) = start(&lost, &mut owner, b"old");
    owner
        .finish(old_claim, Outcome::Succeeded, b"old done", "")
        .expect("finish");
    assert_eq!(old_run.finish(WAIT).expect("ended").output, b"old done");
    let old_number = old_run.call();
    drop(old_run);
    drop(lost);

    let caller = actions(&board, "caller");
    let (mut newer, _, newer_claim) = start(&caller, &mut owner, b"newer");
    assert_eq!(newer.call(), old_number, "the number came round again");
    // The old run's ending was kept for whoever comes back to it, and is handed out each time
    // the same: by the old handle, it is the old run's, never the newer one's under the same
    // number.
    for _ in 0..2 {
        let mut kept = caller.attach_handle(old).expect("sent");
        assert_eq!(kept.finish(WAIT).expect("ended").output, b"old done");
    }
    // A cancel by the old handle has nothing running to stop, and never reaches the newer run.
    let mut stale = caller.attach_handle(old).expect("sent");
    assert_eq!(stale.finish(WAIT).expect("ended").output, b"old done");
    stale.cancel().expect("sent");
    // Frames reach the owner in order: had the cancel reached the newer run, the owner would
    // be told before it is handed the next run.
    let (_next, _, _) = start(&caller, &mut owner, b"next");
    owner
        .finish(newer_claim, Outcome::Succeeded, b"newer done", "")
        .expect("finish");
    let finished = newer.finish(WAIT).expect("ended");
    assert_eq!(
        (finished.outcome, finished.output),
        (Outcome::Succeeded, b"newer done".to_vec())
    );
}
