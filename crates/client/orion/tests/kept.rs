//! An action registered kept through the facade: its detached run outlives the program's
//! session, and a new session of the same program registering it again is handed the run.

#![cfg(target_os = "linux")]

use std::time::{Duration, Instant};

use orion::actions::{
    ActionCall, ActionDecl, Admits, Outcome, Policy, Registration, Request, Run, Target, Update,
};
use orion::{Address, Orion};
use orion_stubs::Board;

const WAIT: Duration = Duration::from_secs(10);

fn decl() -> ActionDecl<'static> {
    ActionDecl {
        name: "arm.move",
        target: Target::Node("board".parse().expect("name")),
        description: "Moves the arm",
    }
}

/// The program that runs `arm.move`, a session of its own, the action registered kept.
fn arm(address: &Address) -> (Orion, Registration) {
    let arm = Orion::connect(address, "arm").expect("connect");
    let runs = arm
        .register_action_kept(&decl(), Policy::new(Admits::LOCAL), Duration::from_secs(10))
        .expect("register");
    (arm, runs)
}

fn handed(runs: &mut Registration) -> (u64, bool) {
    match runs.wait(WAIT).expect("wait") {
        Some(Request::Run { call, kept, .. }) => (call, kept),
        other => panic!("not handed a run: {other:?}"),
    }
}

fn told(run: &mut Run, starts: &str) {
    let since = Instant::now();
    loop {
        assert!(since.elapsed() < WAIT, "never told {starts:?}");
        match run.wait(Duration::from_millis(50)).expect("wait") {
            Some(Update::Doing(doing)) if doing.message.starts_with(starts) => return,
            Some(Update::Doing(_)) | None => {}
            Some(Update::Finished(finished)) => panic!("the run ended: {finished:?}"),
        }
    }
}

#[test]
fn a_kept_action_s_run_is_handed_to_a_new_session_of_the_same_program() {
    let board = Board::start("facade-kept");
    let address = Address::Local(board.socket().to_owned());
    let (first, mut runs) = arm(&address);
    let planner = Orion::connect(&address, "planner").expect("connect");
    let mut run = planner
        .run(&ActionCall {
            name: "arm.move",
            target: decl().target,
            args: b"far",
            deadline_ms: 60_000,
            detached: true,
        })
        .expect("run");
    let (call, kept) = handed(&mut runs);
    assert!(!kept);

    drop(runs);
    drop(first);
    told(&mut run, "restoring");

    let (_second, mut runs) = arm(&address);
    let (again, kept) = handed(&mut runs);
    assert!(kept, "handed back as kept");
    assert_eq!(again, call, "the original run");
    runs.finish(call, Outcome::Succeeded, b"there", "")
        .expect("finish");
    assert_eq!(run.finish(WAIT).expect("ended").outcome, Outcome::Succeeded);
}
