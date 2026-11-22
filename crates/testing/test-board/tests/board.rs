//! The test board is a board: the real comm core and node logic answer, and refuse, as on one.

use orion::node::Security as Said;
use orion::records::{Record, RecordClass};
use orion::types::Error;
use std::io::{Cursor, ErrorKind};
use std::time::{Duration, Instant};

use orion::files::{FileState, Step, Transfer};
use orion::{Address, ClientError, Identity, Orion, Reply};
use orion_test_board::{Options, Security, State, TestBoard};

/// What node logic answers: records are its to read.
fn records(orion: &Orion) -> Result<Vec<Record>, ClientError> {
    orion.records().get(RecordClass::Resource, "")
}

fn unavailable(result: Result<Vec<Record>, ClientError>) -> bool {
    matches!(
        result,
        Err(ClientError::Refused {
            error: Error::Unavailable,
            ..
        })
    )
}

#[test]
fn a_board_answers_as_a_board_and_node_logic_can_go_and_come_back() {
    let mut board = TestBoard::start("whole");
    assert_eq!(board.state(), &State::Running);
    assert!(board.node_runs());
    let orion = board.connect("tool").expect("connect");
    let node = orion.node().expect("hello");
    assert_eq!(node.name, "board");
    assert_eq!(node.state(), Some("running"));
    assert_eq!(node.security(), Said::Off { generation: None });
    // Node logic answers: records are its to read.
    assert!(records(&orion).expect("node logic answers").is_empty());

    // Without node logic, what is its is refused as unavailable, by the real relay.
    board.stop_node();
    assert!(!board.node_runs());
    let orion = board.connect("tool").expect("connect without node logic");
    assert!(unavailable(records(&orion)));
    board.start_node();
    let orion = board.connect("tool").expect("connect");
    assert!(records(&orion).is_ok());
    let dir = board.dir().to_owned();
    drop(board);
    assert!(!dir.exists(), "the board's directory is removed");
}

#[test]
fn with_its_record_on_the_board_checks_and_refuses_as_a_board_does() {
    let operator = Identity::generate();
    let board = TestBoard::start_with(
        "secured",
        Options {
            security: Security::On,
            network: true,
            operator: Some(operator.public()),
            ..Options::default()
        },
    );
    let orion = board.connect("tool").expect("someone on the board");
    assert_eq!(
        orion.node().expect("hello").security(),
        Said::On {
            generation: Some(1)
        }
    );
    assert!(records(&orion).is_ok());
    // A network session with no key it can prove is not let in with security on.
    let network = Address::Network {
        host: board.network().expect("network way in").to_string(),
        key: None,
    };
    let refused = Orion::connect(&network, "stranger")
        .and_then(|orion| records(&orion).map_err(orion::Error::from));
    assert!(refused.is_err(), "{refused:?}");
    // The operator it was seeded with is let in, over the same way.
    let trusted = Orion::connect_with(
        &network,
        "operator",
        orion::Options {
            identity: Some(operator),
            ..orion::Options::default()
        },
    )
    .expect("the seeded operator");
    assert!(records(&trusted).is_ok());

    // The same with security off lets it in.
    let open = TestBoard::start_with(
        "open",
        Options {
            security: Security::Off,
            network: true,
            ..Options::default()
        },
    );
    let network = Address::Network {
        host: open.network().expect("network way in").to_string(),
        key: None,
    };
    let orion = Orion::connect(&network, "stranger").expect("let in");
    assert!(records(&orion).is_ok());
    assert_eq!(
        orion.node().expect("hello").security(),
        Said::Off {
            generation: Some(1)
        }
    );

    // A damaged record is on, and the hello document says so.
    let damaged = TestBoard::start_with(
        "damaged",
        Options {
            security: Security::Damaged,
            ..Options::default()
        },
    );
    let orion = damaged.connect("tool").expect("someone on the board");
    assert_eq!(orion.node().expect("hello").security(), Said::Damaged);
}

#[test]
fn recovery_is_the_real_recovery_and_a_comm_core_that_cannot_start_goes_there() {
    let board = TestBoard::start_in_recovery("recovery", Options::default(), "a test asked");
    assert_eq!(board.state(), &State::Recovery("a test asked".to_owned()));
    assert!(!board.node_runs());
    assert!(board.recovery_ssh().is_some());
    let orion = board.connect("tool").expect("recovery serves the socket");
    let node = orion.node().expect("hello");
    assert_eq!(node.state(), Some("recovery"));
    assert_eq!(node.recovery_why(), Some("a test asked"));

    // Settings that parse but cannot work: a keys directory that cannot be made. A line of the
    // test's replaces the board's own.
    let broken = TestBoard::start_with(
        "broken",
        Options {
            conf: "keys.dir = /proc/orion-test-board/keys".to_owned(),
            ..Options::default()
        },
    );
    let State::Recovery(why) = broken.state() else {
        panic!("started: {broken:?}");
    };
    assert!(why.starts_with("keys.dir "), "{why}");
    let orion = broken.connect("tool").expect("recovery serves the socket");
    assert_eq!(orion.node().expect("hello").state(), Some("recovery"));
}

#[test]
#[should_panic(expected = "the board's settings do not parse")]
fn settings_that_do_not_parse_are_the_tests_fault_not_a_recovery() {
    let _board = TestBoard::start_with(
        "unparsed",
        Options {
            conf: "comm.max_sessions = many".to_owned(),
            ..Options::default()
        },
    );
}

/// How long the pushing clients wait for each answer: short, so twelve of them pass quickly.
const TIMEOUT: Duration = Duration::from_millis(400);

/// Pushes a small file to `destination` under `reply`. Until every byte is there it waits on a
/// generous bound, so a slow moment on a loaded machine is not what ends it: only the commit's
/// wait is under `reply`.
fn push_late(board: &TestBoard, destination: &str, reply: Reply) -> Result<Transfer, ClientError> {
    let options = orion::Options {
        timeout: TIMEOUT,
        ..orion::Options::default()
    };
    let orion =
        Orion::connect_with(&board.address(), "updater", options).expect("connect the pusher");
    orion.set_reply(Reply::Within(Duration::from_secs(20)));
    let data = vec![7_u8; 50_000];
    let files = orion.files();
    let mut pushing = files.start_push(destination, Cursor::new(&data), None)?;
    loop {
        match pushing.step(Duration::from_millis(50))? {
            Step::Done(transfer) => return Ok(transfer),
            // Every byte is there and the commit is sent: from here the commit's wait is the
            // one under test.
            Step::Progress { received, size } if received == size => orion.set_reply(reply),
            _ => {}
        }
    }
}

#[test]
fn a_commit_answered_after_twelve_timeouts_ends_committed_under_alive_and_timed_out_otherwise() {
    let board = TestBoard::start_with(
        "held",
        Options {
            heartbeat_ms: Some(500),
            ..Options::default()
        },
    );
    // Later than the commit's wait under the default: twelve timeouts.
    let hold = TIMEOUT * 12 + Duration::from_millis(1500);
    let held = board.hold_commits("slots/", hold);
    let began = Instant::now();
    let (timed, alive) = std::thread::scope(|scope| {
        let timed = scope.spawn(|| push_late(&board, "slots/a.img", Reply::Timeout));
        let alive = scope.spawn(|| {
            push_late(
                &board,
                "slots/b.img",
                Reply::Alive {
                    ceiling: Some(Duration::from_secs(30)),
                },
            )
        });
        (timed.join().expect("a push"), alive.join().expect("a push"))
    });
    match timed {
        Err(ClientError::Io(error)) => assert_eq!(error.kind(), ErrorKind::TimedOut, "{error}"),
        other => panic!("under Reply::Timeout the late commit times the push out: {other:?}"),
    }
    let alive = alive.expect("under Reply::Alive the push waits for the late commit");
    assert_eq!(alive.state, FileState::Committed, "{}", alive.detail);
    assert!(
        began.elapsed() >= hold,
        "the commit was answered late: {:?}",
        began.elapsed()
    );
    assert!(held.answered() >= 1);
}
