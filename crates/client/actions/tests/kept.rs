//! A detached run kept for its program while the program's session is lost, against node
//! logic's own action list behind the stub board's door: the program comes back, is handed the
//! run again, and says how it ended; the caller is told it was restoring, never that it ended.

#![cfg(target_os = "linux")]

use std::time::{Duration, Instant};

use orion_actions::{
    ActionCall, ActionDecl, Actions, Admits, Outcome, Policy, Request, Target, Update,
};
use orion_connect::{Address, Client, Options, Session};
use orion_stubs::Board;

/// How long anything here may take. Each wait ends as soon as what it waits for comes.
const WAIT: Duration = Duration::from_secs(10);

fn connect(board: &Board, name: &str) -> Client {
    let session = Session::connect(
        &Address::Local(board.socket().to_owned()),
        name,
        Options::default(),
    )
    .expect("connect");
    Client::new(session)
}

fn node() -> Target {
    Target::Node("board".parse().expect("name"))
}

/// Waits for the caller to be told progress whose message starts with `starts`.
fn doing_until(run: &mut orion_actions::Run, starts: &str) -> String {
    let since = Instant::now();
    loop {
        assert!(since.elapsed() < WAIT, "never told {starts:?}");
        match run.wait(Duration::from_millis(50)).expect("wait") {
            Some(Update::Doing(doing)) if doing.message.starts_with(starts) => {
                return doing.message;
            }
            Some(Update::Doing(_)) | None => {}
            Some(Update::Finished(finished)) => panic!("the run ended: {finished:?}"),
        }
    }
}

#[test]
fn a_kept_run_is_handed_back_after_the_program_reconnects_and_ends_as_it_says() {
    let board = Board::start("actions-kept");
    let provider = connect(&board, "arm");
    let side = Actions::new(&provider);
    let mut runs = side
        .register_kept(
            &ActionDecl {
                name: "arm.move",
                target: node(),
                description: "Moves the arm",
            },
            Policy::new(Admits::LOCAL),
            Duration::from_secs(10),
        )
        .expect("register");

    let caller = Actions::new(&connect(&board, "caller"));
    let mut run = caller
        .run(&ActionCall {
            name: "arm.move",
            target: node(),
            args: b"far",
            deadline_ms: 60_000,
            detached: true,
        })
        .expect("run");
    let first = match runs.wait(WAIT).expect("wait") {
        Some(Request::Run {
            call, kept, args, ..
        }) => {
            assert!(!kept, "a new run is not a kept one");
            assert_eq!(args, b"far");
            call
        }
        other => panic!("not handed the run: {other:?}"),
    };
    runs.progress(first, 40, "moving", &[]).expect("progress");
    doing_until(&mut run, "moving");

    // The program's session is lost: a new one replaces it. The caller is told the run is
    // restoring, not that it ended.
    provider
        .session(|session| session.reconnect(|_| true))
        .expect("reconnect");
    doing_until(&mut run, "restoring");

    // Back as the same identity: the run is handed over again, by the same number, before the
    // registration is answered.
    side.restore().expect("restore");
    match runs.wait(WAIT).expect("wait") {
        Some(Request::Run {
            call,
            kept,
            detached,
            args,
            ..
        }) => {
            assert!(kept, "handed back as kept");
            assert!(detached);
            assert!(args.is_empty(), "the arguments were handed over before");
            assert_eq!(call, first, "the original run");
        }
        other => panic!("not handed the run again: {other:?}"),
    }
    doing_until(&mut run, "restored");

    // It says how the run ended, and the caller hears that.
    runs.finish(first, Outcome::Succeeded, b"there", "")
        .expect("finish");
    let finished = run.finish(WAIT).expect("ended");
    assert_eq!(
        (finished.outcome, finished.output.as_slice()),
        (Outcome::Succeeded, b"there".as_slice())
    );
    assert!(
        runs.refused(first).expect("asked").is_none(),
        "the finish was taken"
    );
}
