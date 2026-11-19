//! A program's own link: halves plugged in again, a hello said again until it is welcomed, and
//! a welcome that comes late.

#![cfg(target_os = "linux")]

use crate::common;

use common::*;
use orion_connect::{FIRST_AGAIN, MOST_BETWEEN};

#[test]
fn a_link_plugged_in_again_says_hello_again_as_the_same_program() {
    let mut board = Board::start("replug", Security::Off);
    let link = board.link.take().expect("link");
    let (read, write) = plug(&link);
    let old = write.unplugged();
    let mut session = Session::over(read, write, None, "tool", options(None)).expect("halves");
    assert_eq!(who(&mut session, &[1]), "board");
    let first = session.welcomed().session;

    // The cable comes out and goes in again: new halves on the same link. They read nothing
    // until the old ones are let go, which the session does before it says hello over them.
    let (read, write) = plug_with(
        &link,
        Faults {
            after: Some(old),
            ..Faults::default()
        },
    );
    session.reconnect_over(read, write).expect("again");
    assert_eq!(session.reconnects(), 1);
    assert_ne!(session.welcomed().session, first, "a new session");
    assert_eq!(who(&mut session, &[2]), "board");
    // And again: the halves before are let go each time.
    let (read, write) = plug(&link);
    session.reconnect_over(read, write).expect("again");
    assert_eq!(session.reconnects(), 2);
    assert_eq!(who(&mut session, &[3]), "board");
}

/// Options with room for a few hellos said again.
fn patient(timeout: Duration) -> Options {
    Options {
        timeout,
        ..options(None)
    }
}

#[test]
fn a_hello_lost_on_the_link_is_said_again_until_it_is_welcomed() {
    let mut board = Board::start("lost-once", Security::Off);
    let link = board.link.take().expect("link");
    let (read, write) = plug_with(
        &link,
        Faults {
            lose: 1,
            ..Faults::default()
        },
    );
    let mut session = Session::over(read, write, None, "tool", patient(Duration::from_secs(10)))
        .expect("welcomed after the hello was said again");
    assert_eq!(who(&mut session, &[1]), "board");
}

#[test]
fn hellos_lost_again_and_again_are_said_again_on_a_capped_backoff() {
    let mut board = Board::start("lost-often", Security::Off);
    let link = board.link.take().expect("link");
    // Four lost: said again after 250 ms, then 500, 1 s and 2 s, the most between two.
    let (read, write) = plug_with(
        &link,
        Faults {
            lose: 4,
            ..Faults::default()
        },
    );
    let started = Instant::now();
    let mut session = Session::over(read, write, None, "tool", patient(Duration::from_secs(20)))
        .expect("welcomed in the end");
    let took = started.elapsed();
    assert!(
        took >= FIRST_AGAIN * (1 + 2 + 4) + MOST_BETWEEN,
        "said again sooner than the backoff: {took:?}"
    );
    assert_eq!(who(&mut session, &[1]), "board");
}

#[test]
fn a_welcome_that_comes_late_is_followed_by_the_session_the_board_now_has() {
    let mut board = Board::start("late", Security::Off);
    let link = board.link.take().expect("link");
    // The first hello is answered, but the answer is not read until the hello was said again:
    // the board took the second as a start over, and welcomes that too.
    let (read, write) = plug_with(
        &link,
        Faults {
            hold: 2,
            ..Faults::default()
        },
    );
    let mut session = Session::over(read, write, None, "tool", patient(Duration::from_secs(10)))
        .expect("welcomed");
    let first = session.welcomed().session;
    assert_eq!(who(&mut session, &[1]), "board");
    assert_ne!(
        session.welcomed().session,
        first,
        "the later welcome is the session"
    );
    assert_eq!(who(&mut session, &[2]), "board");
}

#[test]
fn a_link_that_stays_silent_is_tried_again_with_new_halves_until_it_answers() {
    let mut board = Board::start("again-with", Security::Off);
    let link = board.link.take().expect("link");
    let (read, write) = plug(&link);
    let mut session = Session::over(
        read,
        write,
        None,
        "tool",
        patient(Duration::from_millis(600)),
    )
    .expect("halves");
    let first = session.welcomed().session;
    // The first halves lose everything; the next ones work.
    let mut opened = 0;
    let mut waits = 0;
    session
        .reconnect_over_with(
            || {
                opened += 1;
                let lose = if opened == 1 { usize::MAX } else { 0 };
                Ok(plug_with(
                    &link,
                    Faults {
                        lose,
                        ..Faults::default()
                    },
                ))
            },
            |_| {
                waits += 1;
                true
            },
        )
        .expect("back");
    assert_eq!((opened, waits), (2, 1));
    assert_eq!(session.reconnects(), 1);
    assert_ne!(session.welcomed().session, first);
    assert_eq!(who(&mut session, &[3]), "board");
}
