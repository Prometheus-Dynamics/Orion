//! A first connect keeps trying until it is answered: a program may start before the comm core,
//! and nobody answering is not an answer. A refusal is one, and ends the trying at once.

#![cfg(target_os = "linux")]

use crate::common;

use common::*;
use orion_connect::{Address, ClientError, Options, Patience, Roles, Session};
use orion_msg_session::RejectReason;
use orion_scratch::Scratch;
use orion_server::Security;

/// Longer than any one try, so a connect that gives up early is seen to.
const BEFORE: Duration = Duration::from_millis(1500);

fn dir(name: &str) -> Scratch {
    Scratch::new(&format!("patience-{name}"))
}

#[test]
fn a_program_started_before_the_comm_core_is_welcomed_when_it_comes() {
    let dir = dir("early");
    let address = Address::Local(dir.join("orion.sock"));
    let short = Options {
        // Each try is short; the trying is not.
        timeout: Duration::from_millis(200),
        ..Options::default()
    };
    let waiting = std::thread::spawn(move || {
        let started = Instant::now();
        (
            Session::connect(&address, "early", short),
            started.elapsed(),
        )
    });
    std::thread::sleep(BEFORE);
    assert!(!waiting.is_finished(), "it gave up while nothing was there");
    let board = Board::start_in(dir.to_path_buf(), Security::Off, None);
    let (session, waited) = waiting.join().expect("thread");
    let mut session = session.expect("welcomed once the comm core came");
    assert!(waited >= BEFORE, "{waited:?}");
    assert_eq!(who(&mut session, &[1]), "board");
    drop(board);
}

#[test]
fn a_refusal_ends_the_trying_at_once() {
    let board = Board::start("refused", Security::Off);
    let address = Address::Local(board.socket.clone());
    let node_role = Options {
        roles: Roles::NODE,
        ..Options::default()
    };
    let started = Instant::now();
    let error = Session::connect(&address, "tool", node_role).expect_err("role");
    assert!(
        matches!(error, ClientError::Rejected(RejectReason::RoleNotOffered)),
        "{error}"
    );
    assert!(!error.may_retry());
    assert!(
        started.elapsed() < Duration::from_millis(500),
        "tried again after an answer: {:?}",
        started.elapsed()
    );
}

#[test]
fn a_bounded_wait_and_a_single_try_end_when_they_say() {
    let dir = dir("bounded");
    let nowhere = Address::Local(dir.join("orion.sock"));
    let within = Options {
        patience: Patience::Within(Duration::from_millis(700)),
        ..Options::default()
    };
    let started = Instant::now();
    let error = Session::connect(&nowhere, "tool", within).expect_err("nothing came");
    let took = started.elapsed();
    assert!(error.is_lost(), "{error}");
    assert!(
        took >= Duration::from_millis(400) && took < Duration::from_millis(1500),
        "{took:?}"
    );

    let started = Instant::now();
    let error = Session::connect_once(&nowhere, "tool", Options::default()).expect_err("once");
    assert!(error.is_lost(), "{error}");
    assert!(
        started.elapsed() < Duration::from_millis(200),
        "tried again"
    );
}

#[test]
fn the_trying_is_told_how_it_goes_and_may_be_stopped() {
    let dir = dir("watched");
    let nowhere = Address::Local(dir.join("orion.sock"));
    let mut seen = Vec::new();
    let error = Session::connect_watching(&nowhere, "tool", Options::default(), |status| {
        seen.push((status.failures, status.retry_at_ms.is_some()));
        seen.len() < 3
    })
    .expect_err("stopped");
    assert!(error.is_lost(), "{error}");
    assert_eq!(seen, [(1, true), (2, true), (3, true)]);
}

#[test]
fn over_a_link_a_hello_is_said_again_past_one_timeout_until_it_is_welcomed() {
    let mut board = Board::start("link-patient", Security::Off);
    let link = board.link.take().expect("link");
    // Three hellos lost: 1.75 s before the fourth, far past one try's timeout.
    let (read, write) = plug_with(
        &link,
        Faults {
            lose: 3,
            ..Faults::default()
        },
    );
    let short = Options {
        timeout: Duration::from_millis(300),
        ..Options::default()
    };
    let mut session = Session::over(read, write, None, "tool", short).expect("welcomed");
    assert_eq!(who(&mut session, &[1]), "board");
}

#[test]
fn over_a_link_one_try_ends_with_its_timeout() {
    let mut board = Board::start("link-once", Security::Off);
    let link = board.link.take().expect("link");
    let (read, write) = plug_with(
        &link,
        Faults {
            lose: 3,
            ..Faults::default()
        },
    );
    let once = Options {
        timeout: Duration::from_millis(300),
        patience: Patience::Once,
        ..Options::default()
    };
    let error = Session::over(read, write, None, "tool", once).expect_err("one try");
    assert!(error.is_lost(), "{error}");
}
