//! A program that pauses mid-gift, busy elsewhere, for longer than its session's timeout,
//! while its client is there: the gift goes on. The client's silence counts from its last
//! word, never from the program's last step.

#![cfg(target_os = "linux")]

use std::io::Cursor;
use std::path::Path;
use std::sync::mpsc;
use std::time::{Duration, Instant};

use orion_connect::{Address, Client, Options, Session};
use orion_files::{Fetching, FileState, Files, Giving, Step};

use crate::fetch::{WAIT, comm_core, content, files};

/// The program's answers are each bounded by this; it pauses three times as long.
const TIMEOUT: Duration = Duration::from_millis(300);

/// A program whose session's timeout is [`TIMEOUT`]; its client, to pump it by hand.
fn impatient(socket: &Path) -> (Files, Client) {
    let options = Options {
        timeout: TIMEOUT,
        ..Options::default()
    };
    let session =
        Session::connect(&Address::Local(socket.to_owned()), "logger", options).expect("connect");
    let client = Client::new(session);
    (Files::new(&client), client)
}

/// Begins fetching `logs/kernel`.
fn start(files: &Files) -> Fetching<Cursor<Vec<u8>>> {
    files
        .start_fetch("logs/kernel", Cursor::new(Vec::new()), 0)
        .expect("fetch")
}

/// Steps a fetch until it is whole. Tells `here` the first time the program has sent nothing for
/// 200 ms: once its step has sent the window, nothing more comes until it steps again, so
/// then all it sent is here, and the client has said so.
fn fetch_whole(fetching: Fetching<Cursor<Vec<u8>>>, size: u64, here: &mpsc::Sender<()>) {
    let mut fetching = fetching.stalled_after(Duration::from_millis(200));
    let mut told = false;
    let start = Instant::now();
    let ended = loop {
        match fetching.step(Duration::from_millis(20)).expect("step") {
            Step::Done(ended) => break ended,
            Step::Stalled { received, .. } if received > 0 && !told => {
                told = true;
                let _ = here.send(());
            }
            _ => {}
        }
        assert!(start.elapsed() < Duration::from_secs(20), "never over");
    };
    assert_eq!(ended.state, FileState::Committed, "{}", ended.detail);
    assert_eq!(ended.received, size);
}

#[test]
fn a_program_paused_past_its_timeout_with_chunks_out_still_gives_the_file() {
    let (socket, _stop, _dir) = comm_core("paused-out");
    let socket_for = socket.clone();
    let (ready, offered) = mpsc::channel();
    let (paused_tx, paused) = mpsc::channel();
    let size = 4 << 20;
    let program = std::thread::spawn(move || {
        let (files, _client) = impatient(&socket_for);
        let mut offer = files.offer("logs/").expect("offer");
        ready.send(()).expect("ready");
        let asked = offer.next(WAIT).expect("next").expect("asked");
        let mut gift = asked.give(Cursor::new(content(size))).expect("give");
        // The window goes out; then the program is busy elsewhere, nothing pumped, while
        // the client takes the window and says so. The client takes none of it before the
        // pause, so nothing it says can reach the gift before then.
        gift.step(Duration::ZERO).expect("step");
        paused_tx.send(()).expect("paused");
        std::thread::sleep(TIMEOUT * 3);
        gift.finish()
            .expect("the client is there: the gift goes on")
    });
    offered.recv().expect("offered");
    let collector = files(&socket, "collector");
    let fetching = start(&collector);
    paused.recv().expect("paused");
    let (here, _) = mpsc::channel();
    fetch_whole(fetching, size as u64, &here);
    assert_eq!(
        program.join().expect("program"),
        Giving::Done {
            taken: true,
            detail: String::new()
        }
    );
}

#[test]
fn a_program_paused_past_its_timeout_with_nothing_owed_still_gives_the_file() {
    let (socket, _stop, _dir) = comm_core("paused-owed");
    let socket_for = socket.clone();
    let (ready, offered) = mpsc::channel();
    let (here, all_here) = mpsc::channel::<()>();
    let size = 4 << 20;
    let program = std::thread::spawn(move || {
        let (files, client) = impatient(&socket_for);
        let mut offer = files.offer("logs/").expect("offer");
        ready.send(()).expect("ready");
        let asked = offer.next(WAIT).expect("next").expect("asked");
        let mut gift = asked.give(Cursor::new(content(size))).expect("give");
        gift.step(Duration::ZERO).expect("step");
        // The client has the whole window, and its word on it is taken in by the session
        // (the program pumps it for its other work), not by the gift: the client owes
        // nothing more. Then the program pauses, past its timeout.
        all_here.recv().expect("all here");
        let pumped = Instant::now();
        while pumped.elapsed() < Duration::from_millis(200) {
            client.pump(Duration::from_millis(20)).expect("pump");
        }
        std::thread::sleep(TIMEOUT * 3);
        gift.finish()
            .expect("the client owed nothing: the gift goes on")
    });
    offered.recv().expect("offered");
    fetch_whole(start(&files(&socket, "collector")), size as u64, &here);
    assert_eq!(
        program.join().expect("program"),
        Giving::Done {
            taken: true,
            detail: String::new()
        }
    );
}

/// Bytes written slowly, as to a slow card: a client that takes its time, and says so as it
/// goes.
struct Slow(Cursor<Vec<u8>>);

impl std::io::Write for Slow {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        std::thread::sleep(Duration::from_millis(10));
        self.0.write(bytes)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl std::io::Read for Slow {
    fn read(&mut self, bytes: &mut [u8]) -> std::io::Result<usize> {
        self.0.read(bytes)
    }
}

impl std::io::Seek for Slow {
    fn seek(&mut self, to: std::io::SeekFrom) -> std::io::Result<u64> {
        self.0.seek(to)
    }
}

#[test]
fn a_gift_longer_than_its_timeout_lives_on_its_clients_word() {
    let (socket, _stop, _dir) = comm_core("paused-word");
    let socket_for = socket.clone();
    let (ready, offered) = mpsc::channel();
    let size = 2 << 20;
    let program = std::thread::spawn(move || {
        let (files, _client) = impatient(&socket_for);
        let mut offer = files.offer("logs/").expect("offer");
        ready.send(()).expect("ready");
        let asked = offer.next(WAIT).expect("next").expect("asked");
        let gift = asked.give(Cursor::new(content(size))).expect("give");
        gift.finish()
            .expect("the client keeps saying how much it has: the gift goes on")
    });
    offered.recv().expect("offered");
    let start = Instant::now();
    let ended = files(&socket, "collector")
        .start_fetch("logs/kernel", Slow(Cursor::new(Vec::new())), 0)
        .expect("fetch")
        .finish(|_, _| {})
        .expect("fetched");
    assert_eq!(ended.state, FileState::Committed, "{}", ended.detail);
    // Long past the program's timeout, with chunks out all the while.
    assert!(start.elapsed() > TIMEOUT * 2, "{:?}", start.elapsed());
    assert_eq!(
        program.join().expect("program"),
        Giving::Done {
            taken: true,
            detail: String::new()
        }
    );
}
