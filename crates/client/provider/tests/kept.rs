//! A provider's kept action: its detached run outlives the provider's session, and a new
//! session of the same program, describing the resource again, is handed the run back.

#![cfg(target_os = "linux")]

use std::time::{Duration, Instant};

use orion_actions::{ActionCall, Actions, Admits, Outcome, Policy, Request, Run, Target, Update};
use orion_connect::{Address, Client, Options, Roles, Session};
use orion_provider::{Provider, Resource, ResourceState};
use orion_records::Records;
use orion_stubs::Board;
use orion_topics::Topics;

const WAIT: Duration = Duration::from_secs(10);

/// A provider session of `name`, its resource `arm.left` described with a kept action, `move`.
fn arm(board: &Board) -> Resource {
    let options = Options {
        roles: Roles::CLIENT.and(Roles::PROVIDER),
        ..Options::default()
    };
    let session = Session::connect(
        &Address::Local(board.socket().to_owned()),
        "arm-driver",
        options,
    )
    .expect("connect");
    let client = Client::new(session);
    let provider = Provider::new(
        &client,
        &Records::new(&client),
        &Topics::new(&client),
        &Actions::new(&client),
    );
    let mut describing = provider.resource("arm.left", "arm").expect("name");
    describing
        .action_kept(
            "move",
            "Moves the arm",
            Policy::new(Admits::LOCAL),
            Duration::from_secs(10),
        )
        .expect("action");
    describing.publish(ResourceState::Ready).expect("publish")
}

/// The run the resource is handed next: its number, and whether it was kept for it.
fn handed(resource: &mut Resource) -> (u64, bool) {
    let requests = resource.requests().expect("the arm has an action");
    match requests.wait(WAIT).expect("wait") {
        Some(Request::Run { call, kept, .. }) => (call, kept),
        other => panic!("not handed a run: {other:?}"),
    }
}

/// Waits for the caller to be told progress whose message starts with `starts`.
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
fn a_kept_run_is_handed_to_a_new_session_of_the_same_provider() {
    let board = Board::start("provider-kept");
    let mut first = arm(&board);
    let caller = Actions::new(&Client::new(
        Session::connect(
            &Address::Local(board.socket().to_owned()),
            "planner",
            Options::default(),
        )
        .expect("connect"),
    ));
    let mut run = caller
        .run(&ActionCall {
            name: "arm.left.move",
            target: Target::Resource("arm.left".parse().expect("name")),
            args: b"far",
            deadline_ms: 60_000,
            detached: true,
        })
        .expect("run");
    let (call, kept) = handed(&mut first);
    assert!(!kept);

    // The provider's session is gone, as when its program is restarted: the run is kept.
    drop(first);
    told(&mut run, "restoring");

    // The same program, a new session, describing the arm again: handed the run back.
    let mut second = arm(&board);
    let (again, kept) = handed(&mut second);
    assert!(kept, "handed back as kept");
    assert_eq!(again, call, "the original run");
    told(&mut run, "restored");
    second
        .requests()
        .expect("the arm has an action")
        .finish(call, Outcome::Succeeded, b"there", "")
        .expect("finish");
    let finished = run.finish(WAIT).expect("ended");
    assert_eq!(
        (finished.outcome, finished.output.as_slice()),
        (Outcome::Succeeded, b"there".as_slice())
    );
}
