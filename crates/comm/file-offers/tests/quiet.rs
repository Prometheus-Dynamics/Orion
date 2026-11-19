//! A program that goes quiet mid-gift: the fetch says it is stalled and waits it out, past the
//! session's timeout, and gives up only when its caller set a limit, saying so.

#![cfg(target_os = "linux")]

use std::io::Cursor;
use std::path::Path;
use std::sync::mpsc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use orion_connect::{Address, Client, Options, Session};
use orion_files::{Fetching, FileState, Files, Giving, Step};

use crate::fetch::{WAIT, comm_core, content, files};

/// A session whose answers are each bounded by this: the fetch must not be.
const TIMEOUT: Duration = Duration::from_millis(300);

fn impatient(socket: &Path) -> Files {
    let options = Options {
        timeout: TIMEOUT,
        ..Options::default()
    };
    let session = Session::connect(&Address::Local(socket.to_owned()), "collector", options)
        .expect("connect");
    Files::new(&Client::new(session))
}

/// A program that gives `size` bytes, goes quiet once the client has some until `resume`
/// says, then gives the rest; returns how its gift ended.
fn quiet_giver(
    socket: &Path,
    size: usize,
    resume: mpsc::Receiver<()>,
) -> (JoinHandle<Giving>, mpsc::Receiver<()>) {
    let socket = socket.to_owned();
    let (ready, offered) = mpsc::channel();
    let (quiet_tx, quiet) = mpsc::channel();
    let program = std::thread::spawn(move || {
        let files = files(&socket, "logger");
        let mut offer = files.offer("logs/").expect("offer");
        ready.send(()).expect("ready");
        let asked = offer.next(WAIT).expect("next").expect("asked");
        let mut gift = asked.give(Cursor::new(content(size))).expect("give");
        loop {
            if let Giving::Progress { received, .. } =
                gift.step(Duration::from_millis(5)).expect("step")
                && received > 0
            {
                break;
            }
        }
        // Quiet: nothing given, nothing heard, as a program busy elsewhere.
        quiet_tx.send(()).expect("quiet");
        resume.recv().expect("resume");
        gift.finish().expect("over")
    });
    offered.recv().expect("offered");
    (program, quiet)
}

/// Steps `fetching` until some of the file is here, which the program waits to hear.
fn under_way(fetching: &mut Fetching<Cursor<Vec<u8>>>) {
    loop {
        match fetching.step(Duration::from_millis(20)).expect("step") {
            Step::Progress { received, .. } if received > 0 => return,
            Step::Done(transfer) => panic!("over before it began: {transfer:?}"),
            _ => {}
        }
    }
}

#[test]
fn a_quiet_program_is_said_to_be_stalled_and_the_fetch_waits_it_out() {
    let (socket, _stop) = comm_core("stalled");
    let size = 4 << 20;
    let (resume, resumed) = mpsc::channel();
    let (program, quiet) = quiet_giver(&socket, size, resumed);
    let client = impatient(&socket);
    let mut fetching = client
        .start_fetch("logs/kernel", Cursor::new(Vec::new()), 0)
        .expect("fetch")
        .stalled_after(Duration::from_millis(100));
    under_way(&mut fetching);
    quiet.recv().expect("quiet");
    let start = Instant::now();
    let mut stalls = Vec::new();
    let mut resumed_at = None;
    let ended = loop {
        // Quiet for three times the session's timeout, then the program goes on.
        if resumed_at.is_none() && start.elapsed() >= TIMEOUT * 3 {
            resume.send(()).expect("resume");
            resumed_at = Some(stalls.len());
        }
        match fetching
            .step(Duration::from_millis(20))
            .expect("waits, and is not lost")
        {
            Step::Done(ended) => break ended,
            Step::Stalled {
                received,
                size: said,
                quiet,
            } => stalls.push((received, said, quiet)),
            _ => {}
        }
        assert!(start.elapsed() < Duration::from_secs(20), "never over");
    };
    assert_eq!(ended.state, FileState::Committed, "{}", ended.detail);
    assert_eq!(ended.received, size as u64);
    let before = resumed_at.expect("resumed");
    assert!(
        before >= 3,
        "said stalled each 100 ms of the quiet: {stalls:?}"
    );
    for (received, said, quiet) in &stalls[..before] {
        assert!(*received > 0 && *received < size as u64, "{stalls:?}");
        assert_eq!(*said, Some(size as u64));
        assert!(*quiet >= Duration::from_millis(100), "{stalls:?}");
    }
    assert_eq!(
        program.join().expect("program"),
        Giving::Done {
            taken: true,
            detail: String::new()
        }
    );
}

#[test]
fn a_fetch_given_a_limit_gives_up_on_a_quiet_program_saying_so() {
    let (socket, _stop) = comm_core("give-up");
    let (resume, resumed) = mpsc::channel();
    let (program, quiet) = quiet_giver(&socket, 4 << 20, resumed);
    let client = impatient(&socket);
    let limit = Duration::from_millis(400);
    let mut fetching = client
        .start_fetch("logs/kernel", Cursor::new(Vec::new()), 0)
        .expect("fetch")
        .stalled_after(Duration::from_millis(100));
    under_way(&mut fetching);
    // The limit is for the quiet once the file is under way: set before, it would count the
    // program's start too, which a loaded machine can stretch past it (docs/flakes.md).
    let mut fetching = fetching.give_up_after(limit);
    quiet.recv().expect("quiet");
    let start = Instant::now();
    let mut stalls = 0;
    let ended = loop {
        match fetching.step(Duration::from_millis(20)).expect("step") {
            Step::Done(ended) => break ended,
            Step::Stalled { .. } => stalls += 1,
            _ => {}
        }
        assert!(start.elapsed() < Duration::from_secs(3), "never gave up");
    };
    assert_eq!(ended.state, FileState::Rejected);
    assert_eq!(
        ended.detail,
        "gave up: no progress for 400ms, the limit set for this fetch"
    );
    assert!(stalls >= 1, "said stalled before it gave up");
    resume.send(()).expect("resume");
    // The program is told why, in the same words.
    assert_eq!(
        program.join().expect("program"),
        Giving::Done {
            taken: false,
            detail: "gave up: no progress for 400ms, the limit set for this fetch".into()
        }
    );
}
