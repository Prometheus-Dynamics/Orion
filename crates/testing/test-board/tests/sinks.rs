//! Several receivers on one session: each is handed the files under its own prefix, and a
//! prefix another session holds is refused.

use std::collections::BTreeMap;
use std::io::Cursor;
use std::sync::mpsc;
use std::time::{Duration, Instant};

use orion::files::{FileState, Handed, Sink, Step, digest};
use orion::types::Error;
use orion::{ClientError, Orion};
use orion_test_board::TestBoard;

/// What one receiver was handed: each file's destination, and whether it was whole.
type Got = Vec<(String, bool)>;

/// Files begun and not yet committed, by transfer: destination, size, digest, bytes so far.
type Files = BTreeMap<u64, (String, u64, [u8; 32], Vec<u8>)>;

/// `prefix`'s receiver on `orion`, once node logic is there to take it.
fn sink(orion: &Orion, prefix: &str) -> Result<Sink, ClientError> {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        match orion.files().sink(prefix) {
            Err(ClientError::Refused {
                error: Error::Unavailable,
                ..
            }) if Instant::now() < deadline => std::thread::sleep(Duration::from_millis(10)),
            other => return other,
        }
    }
}

/// Hands one thing from `sink` on, into `files` (by transfer) and `got`, and answers it.
fn take(sink: &mut Sink, files: &mut Files, got: &mut Got) {
    let answer = match sink.next(Duration::from_millis(10)).expect("next") {
        None => return,
        Some(Handed::Begin {
            transfer,
            destination,
            size,
            digest,
        }) => {
            files.insert(transfer, (destination, size, digest, Vec::new()));
            sink.status(transfer, 0, FileState::Receiving, "")
        }
        Some(Handed::Chunk {
            transfer,
            offset,
            data,
        }) => {
            // A chunk of a file another receiver began is not this one's.
            let file = files
                .get_mut(&transfer)
                .unwrap_or_else(|| panic!("a chunk of transfer {transfer}, not begun here"));
            assert_eq!(offset, file.3.len() as u64);
            file.3.extend_from_slice(&data);
            sink.status(transfer, file.3.len() as u64, FileState::Receiving, "")
        }
        Some(Handed::Commit { transfer }) => {
            let (destination, size, sum, data) = files
                .remove(&transfer)
                .unwrap_or_else(|| panic!("a commit of transfer {transfer}, not begun here"));
            let whole = digest(&mut Cursor::new(&data)).ok() == Some((sum, size));
            got.push((destination, whole));
            sink.status(transfer, size, FileState::Committed, "")
        }
    };
    answer.expect("answered");
}

fn push(board: &TestBoard, destination: &str, data: &[u8]) -> FileState {
    let orion = board.connect("pusher").expect("connect");
    let files = orion.files();
    let mut pushing = files
        .start_push(destination, Cursor::new(data), None)
        .expect("push");
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Step::Done(transfer) = pushing.step(Duration::from_millis(50)).expect("step") {
            return transfer.state;
        }
        assert!(Instant::now() < deadline, "{destination} not done in time");
    }
}

#[test]
fn two_receivers_on_one_session_are_each_handed_their_own_files() {
    let board = TestBoard::start("sinks");
    // Another program holds `keys/`.
    let other = board.connect("keeper").expect("connect");
    let _keys = sink(&other, "keys/").expect("keys/");

    let socket = board.address();
    let (ready, up) = mpsc::channel();
    let (stop, stopping) = mpsc::channel::<()>();
    let receiver = std::thread::spawn(move || {
        let orion = Orion::connect(&socket, "manager").expect("connect");
        let mut slots = sink(&orion, "slots/").expect("slots/");
        let mut logs = sink(&orion, "logs/").expect("logs/");
        // The prefix the other program holds is refused, and this session's two go on.
        let refused = match orion.files().sink("keys/") {
            Err(ClientError::Refused { error, .. }) => Some(error),
            _ => None,
        };
        ready.send(refused).expect("said");
        let (mut slot_files, mut log_files) = (Files::new(), Files::new());
        let (mut slots_got, mut logs_got) = (Got::new(), Got::new());
        while stopping.try_recv().is_err() {
            take(&mut slots, &mut slot_files, &mut slots_got);
            take(&mut logs, &mut log_files, &mut logs_got);
        }
        (slots_got, logs_got)
    });
    let refused = up
        .recv_timeout(Duration::from_secs(30))
        .expect("the receivers are there");
    assert_eq!(refused, Some(Error::AlreadyExists));

    for (i, destination) in ["slots/a.img", "logs/x.log", "slots/b.img", "logs/y.log"]
        .into_iter()
        .enumerate()
    {
        let data = vec![u8::try_from(i).expect("small"); 300_000 * (i + 1)];
        assert_eq!(
            push(&board, destination, &data),
            FileState::Committed,
            "{destination}"
        );
    }
    stop.send(()).expect("stop");
    let (slots_got, logs_got) = receiver.join().expect("the receivers");
    let whole = |names: &[&str]| -> Got { names.iter().map(|n| ((*n).to_owned(), true)).collect() };
    assert_eq!(slots_got, whole(&["slots/a.img", "slots/b.img"]));
    assert_eq!(logs_got, whole(&["logs/x.log", "logs/y.log"]));
}
