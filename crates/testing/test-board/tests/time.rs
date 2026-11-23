//! A computer gives the board its clock (`orion.offer_time`): with security off any program's
//! offer is taken, with security on only an operator granted `clock.set`'s, and the one
//! without is told which grant it needs. `orion.time` names whose clock it took.

use std::time::{Duration, Instant};

use orion::topics::FieldId;
use orion::{Address, ClientError, Error, Identity, Orion};
use orion_test_board::{Granted, Options, Security, TestBoard};

/// `orion.time`'s `source`, its third field.
const SOURCE: FieldId = FieldId(2);

/// The board's `orion.time` source, once it names one starting with `starts`, while `offering`
/// answers the board.
fn source_once(board: &TestBoard, offering: &Orion, starts: &str) -> String {
    let watcher = board.connect("watcher").expect("connect");
    let mut time = watcher.subscribe("orion.time").expect("subscribe");
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        offering.pump(Duration::from_millis(20)).expect("pump");
        time.wait(Duration::from_millis(20)).expect("wait");
        if let Some(sample) = time.latest(SOURCE) {
            let source = String::from_utf8_lossy(sample.payload).into_owned();
            if source.starts_with(starts) {
                return source;
            }
        }
        assert!(
            Instant::now() < deadline,
            "the board never took the offered clock"
        );
    }
}

const CONF: &str = "time.sync = report\ntime.servers = operator\n";

#[test]
fn with_security_off_any_programs_clock_is_taken_and_named() {
    let board = TestBoard::start_with(
        "time-off",
        Options {
            conf: CONF.to_owned(),
            ..Options::default()
        },
    );
    let pc = board.connect("bench-pc").expect("connect");
    let poll = pc.offer_time(2).expect("taken");
    assert!(poll >= Duration::from_secs(1), "{poll:?}");
    assert_eq!(source_once(&board, &pc, "operator"), "operator bench-pc");
    assert!(pc.times_asked() >= 1);
}

#[test]
fn a_board_that_takes_no_time_from_operators_says_so() {
    let board = TestBoard::start("time-none");
    let pc = board.connect("bench-pc").expect("connect");
    let refused = pc.offer_time(2).expect_err("not taken");
    assert!(
        matches!(
            &refused,
            Error::Client(ClientError::Refused { error, .. })
                if *error == orion::types::Error::Unsupported
        ),
        "{refused}"
    );
}

#[test]
fn with_security_on_only_an_operator_granted_clock_set_gives_the_time() {
    let (granted, other) = (Identity::generate(), Identity::generate());
    let board = TestBoard::start_with(
        "time-on",
        Options {
            security: Security::On,
            network: true,
            conf: CONF.to_owned(),
            granted: vec![
                Granted {
                    key: granted.public(),
                    read: true,
                    actions: vec!["clock.set".to_owned()],
                },
                Granted {
                    key: other.public(),
                    read: true,
                    actions: Vec::new(),
                },
            ],
            ..Options::default()
        },
    );
    let network = Address::Network {
        host: board.network().expect("network way in").to_string(),
        key: None,
    };
    let connect = |identity: Identity, name: &str| {
        Orion::connect_with(
            &network,
            name,
            orion::Options {
                identity: Some(identity),
                ..orion::Options::default()
            },
        )
        .expect("an operator")
    };
    let start = |key: [u8; 32]| -> String { key[..4].iter().map(|b| format!("{b:02x}")).collect() };
    let granted_key = start(granted.public());
    let refused = connect(other, "laptop")
        .offer_time(2)
        .expect_err("not granted");
    assert!(
        refused.to_string().contains("needs the grant clock.set"),
        "{refused}"
    );
    let pc = connect(granted, "bench-pc");
    pc.offer_time(2).expect("granted");
    assert_eq!(
        source_once(&board, &pc, "operator"),
        format!("operator bench-pc (key {granted_key}…)")
    );
}
