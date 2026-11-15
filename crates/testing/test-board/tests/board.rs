//! The test board is a board: the real comm core and node logic answer, and refuse, as on one.

use orion::node::Security as Said;
use orion::records::{Record, RecordClass};
use orion::types::Error;
use orion::{Address, ClientError, Identity, Orion};
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
