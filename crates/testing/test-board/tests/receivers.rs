//! Prefixes the image says are receivers' (`files.receivers`): a file pushed before its
//! receiver is there waits for it and is never stored by the node, and one whose receiver
//! never comes ends saying so. And a root pushed to while node logic is still starting.

use std::io::Cursor;
use std::path::Path;
use std::time::Duration;

use orion::files::FileState;
use orion::types::Error;
use orion::{ClientError, Orion};
use orion_scratch::Scratch;
use orion_test_board::{Options, TestBoard};

fn board(name: &str) -> TestBoard {
    TestBoard::start_with(
        name,
        Options {
            name: name.to_owned(),
            conf: "files.receivers = held/\n".to_owned(),
            ..Options::default()
        },
    )
}

/// Whether a file whose path has `part` in it is anywhere under `dir`.
fn in_store(dir: &Path, part: &str) -> bool {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return false;
    };
    entries.flatten().any(|entry| {
        let path = entry.path();
        path.to_string_lossy().contains(part) || (path.is_dir() && in_store(&path, part))
    })
}

#[test]
fn a_file_pushed_before_its_receiver_is_there_waits_for_it_and_is_never_stored() {
    let board = board("early");
    let address = board.address();
    let pushing = std::thread::spawn(move || {
        let orion = Orion::connect(&address, "pusher").expect("connect");
        orion.files().push(
            "held/a.img",
            &mut Cursor::new(vec![5_u8; 70_000]),
            |_, _| {},
        )
    });
    // The receiver starts after the push: a bridge that comes up a moment after the board.
    std::thread::sleep(Duration::from_millis(600));
    assert!(!pushing.is_finished(), "the push waits for the receiver");
    assert!(!in_store(&board.dir().join("files"), "a.img"), "not stored");
    let held = board.hold_commits("held/", Duration::ZERO);
    let pushed = pushing.join().expect("joined").expect("pushed");
    assert_eq!(pushed.state, FileState::Committed, "{}", pushed.detail);
    assert_eq!(held.answered(), 1, "the receiver took it");
    assert!(!in_store(&board.dir().join("files"), "a.img"), "not stored");

    // What the image did not name is the store's, as ever.
    let orion = board.connect("pusher").expect("connect");
    let stored = orion
        .files()
        .push("other/b", &mut Cursor::new(b"data".to_vec()), |_, _| {})
        .expect("pushed");
    assert_eq!(stored.state, FileState::Committed, "{}", stored.detail);
    assert!(in_store(&board.dir().join("files"), "other"), "stored");
}

#[test]
fn a_push_whose_receiver_never_comes_ends_saying_nothing_received_it() {
    let board = board("never");
    // Twelve of these is how long the push asks again: 3 s. Long enough that an answer late
    // on a loaded machine is not taken for none.
    let options = orion::Options {
        timeout: Duration::from_millis(250),
        ..orion::Options::default()
    };
    let orion = Orion::connect_with(&board.address(), "pusher", options).expect("connect");
    // Node logic is there: a push it stores is taken.
    let stored = orion
        .files()
        .push("other/b", &mut Cursor::new(b"data".to_vec()), |_, _| {})
        .expect("pushed");
    assert_eq!(stored.state, FileState::Committed, "{}", stored.detail);
    let began = std::time::Instant::now();
    let refused = orion
        .files()
        .push("held/a.img", &mut Cursor::new(b"data".to_vec()), |_, _| {})
        .expect_err("nothing receives it");
    assert!(
        began.elapsed() >= Duration::from_secs(3),
        "{:?}: {refused}",
        began.elapsed()
    );
    // The board's receiver never came up: said so, and not as a lost connection.
    assert!(
        matches!(
            &refused,
            ClientError::Refused { error: Error::Unavailable, detail }
                if detail == "nothing received held/ within 3s"
        ),
        "{refused}"
    );
    assert!(!refused.is_lost() && !refused.is_busy() && !refused.is_held());
    assert!(!in_store(&board.dir().join("files"), "a.img"), "not stored");
}

#[test]
fn a_file_pushed_to_a_root_while_node_logic_is_starting_lands_in_the_root() {
    let app = Scratch::new("starting-app");
    let mut board = TestBoard::start_with(
        "starting",
        Options {
            name: "starting".to_owned(),
            node: false,
            conf: format!("files.root.app = {} rw\n", app.display()),
            ..Options::default()
        },
    );
    let address = board.address();
    let pushing = std::thread::spawn(move || {
        let orion = Orion::connect(&address, "pusher").expect("connect");
        orion.files().push(
            "roots/app/early",
            &mut Cursor::new(b"data".to_vec()),
            |_, _| {},
        )
    });
    std::thread::sleep(Duration::from_millis(400));
    assert!(!pushing.is_finished(), "the push waits for node logic");
    board.start_node();
    let pushed = pushing.join().expect("joined").expect("pushed");
    assert_eq!(pushed.state, FileState::Committed, "{}", pushed.detail);
    assert_eq!(
        std::fs::read(app.join("early")).expect("in the root"),
        b"data"
    );
    assert!(
        !in_store(&board.dir().join("files"), "early"),
        "not in the store"
    );
}
