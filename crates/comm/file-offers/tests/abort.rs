//! A program that stops giving a file ends its client's fetch at once, saying why: by
//! aborting the gift, or by dropping it, or what it was asked, unanswered, or because the file
//! could not be read. The client is not left to wait for the session's silence limit.

#![cfg(target_os = "linux")]

use std::io::{Cursor, Read, Seek, SeekFrom};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use orion_connect::ClientError;
use orion_file_offers::MAX_FETCHES;
use orion_files::{Fetching, FileState, Giving, Known, Step, Transfer};
use sha2::{Digest, Sha256};

use crate::fetch::{WAIT, comm_core, content, files};

/// Well inside the session's silence limit (5 s by default), which is how long a client
/// waits on a program that says nothing.
const AT_ONCE: Duration = Duration::from_secs(1);

/// Steps `fetching` until it is over, panicking if that takes `bound`.
fn ended(fetching: &mut Fetching<Cursor<Vec<u8>>>, bound: Duration) -> Transfer {
    let start = Instant::now();
    loop {
        if let Step::Done(transfer) = fetching.step(Duration::from_millis(20)).expect("step") {
            return transfer;
        }
        assert!(
            start.elapsed() < bound,
            "the fetch was not over within {bound:?}"
        );
    }
}

/// Steps `fetching` until some of the file is here.
fn under_way(fetching: &mut Fetching<Cursor<Vec<u8>>>) -> u64 {
    loop {
        match fetching.step(Duration::from_millis(20)).expect("step") {
            Step::Progress { received, .. } if received > 0 => return received,
            Step::Done(transfer) => panic!("over before it began: {transfer:?}"),
            _ => {}
        }
    }
}

#[test]
fn a_gift_aborted_mid_transfer_ends_the_fetch_at_once_with_its_reason() {
    let (socket, _stop, _dir) = comm_core("abort");
    let socket_for = socket.clone();
    let (ready, offered) = mpsc::channel();
    let (abort, aborting) = mpsc::channel::<()>();
    let (aborted_tx, aborted) = mpsc::channel();
    let (done, finished) = mpsc::channel::<()>();
    let size = 4 << 20;
    let program = std::thread::spawn(move || {
        let files = files(&socket_for, "logger");
        let mut offer = files.offer("logs/").expect("offer");
        ready.send(()).expect("ready");
        let asked = offer.next(WAIT).expect("next").expect("asked");
        let mut gift = asked.give(Cursor::new(content(size))).expect("give");
        // Gives until told to stop, as the client takes it.
        while aborting.try_recv().is_err() {
            let step = gift.step(Duration::from_millis(5)).expect("step");
            assert!(matches!(step, Giving::Progress { .. }), "{step:?}");
        }
        gift.abort("the log was rotated").expect("abort");
        aborted_tx.send(Instant::now()).expect("aborted");
        // Its session lives on: what the client hears is the abort, not that it went.
        finished.recv().expect("finished");
    });
    offered.recv().expect("offered");
    let client = files(&socket, "collector");
    let mut fetching = client
        .start_fetch("logs/kernel", Cursor::new(Vec::new()), 0)
        .expect("fetch");
    under_way(&mut fetching);
    abort.send(()).expect("abort");
    let transfer = ended(&mut fetching, Duration::from_secs(10));
    let took = aborted.recv().expect("aborted").elapsed();
    assert_eq!(transfer.state, FileState::Aborted, "{}", transfer.detail);
    assert_eq!(
        transfer.detail,
        "the program that offers it stopped giving it: the log was rotated"
    );
    assert!(
        transfer.received > 0 && transfer.received < size as u64,
        "{}",
        transfer.received
    );
    assert!(took < AT_ONCE, "heard {took:?} after the abort");
    done.send(()).expect("done");
    program.join().expect("program");
}

#[test]
fn a_gift_dropped_and_a_file_asked_and_never_answered_end_their_fetches_at_once() {
    let (socket, _stop, _dir) = comm_core("dropped");
    let socket_for = socket.clone();
    let (ready, offered) = mpsc::channel();
    let (drop_gift, dropping) = mpsc::channel::<()>();
    let (done, finished) = mpsc::channel::<()>();
    let program = std::thread::spawn(move || {
        let files = files(&socket_for, "logger");
        let mut offer = files.offer("logs/").expect("offer");
        ready.send(()).expect("ready");
        let asked = offer.next(WAIT).expect("next").expect("asked");
        let mut gift = asked.give(Cursor::new(content(4 << 20))).expect("give");
        while dropping.try_recv().is_err() {
            gift.step(Duration::from_millis(5)).expect("step");
        }
        drop(gift);
        let asked = offer.next(WAIT).expect("next").expect("asked again");
        drop(asked);
        finished.recv().expect("finished");
    });
    offered.recv().expect("offered");
    let client = files(&socket, "collector");
    let mut fetching = client
        .start_fetch("logs/kernel", Cursor::new(Vec::new()), 0)
        .expect("fetch");
    under_way(&mut fetching);
    drop_gift.send(()).expect("drop");
    let transfer = ended(&mut fetching, AT_ONCE);
    assert_eq!(transfer.state, FileState::Aborted, "{}", transfer.detail);
    assert_eq!(
        transfer.detail,
        "the program that offers it stopped giving it: it was dropped before it was given"
    );

    let mut fetching = client
        .start_fetch("logs/boot", Cursor::new(Vec::new()), 0)
        .expect("fetch");
    let transfer = ended(&mut fetching, AT_ONCE);
    assert_eq!(transfer.state, FileState::Rejected, "{}", transfer.detail);
    assert_eq!(
        transfer.detail,
        "the program that offers it did not give it"
    );
    done.send(()).expect("done");
    program.join().expect("program");
}

#[test]
fn a_fetch_aborted_is_over_in_the_comm_core_so_aborts_do_not_use_up_its_fetches() {
    let (socket, _stop, _dir) = comm_core("aborts");
    let socket_for = socket.clone();
    let (ready, offered) = mpsc::channel();
    let bytes = content(1000);
    let known = Known {
        digest: Sha256::digest(&bytes).into(),
        size: bytes.len() as u64,
    };
    let given = bytes.clone();
    let program = std::thread::spawn(move || {
        let files = files(&socket_for, "logger");
        let mut offer = files.offer("logs/").expect("offer");
        ready.send(()).expect("ready");
        // More aborts than the comm core has room for fetches under way, then one given.
        for _ in 0..=MAX_FETCHES {
            let asked = offer.next(WAIT).expect("next").expect("asked");
            let gift = asked
                .give_known(Cursor::new(given.clone()), known)
                .expect("give");
            gift.abort("not now").expect("abort");
        }
        let asked = offer.next(WAIT).expect("next").expect("asked");
        asked
            .give_known(Cursor::new(given), known)
            .expect("give")
            .finish()
            .expect("given")
    });
    offered.recv().expect("offered");
    let client = files(&socket, "collector");
    for _ in 0..=MAX_FETCHES {
        let mut fetching = client
            .start_fetch("logs/kernel", Cursor::new(Vec::new()), 0)
            .expect("fetch");
        let transfer = ended(&mut fetching, AT_ONCE);
        assert_eq!(transfer.state, FileState::Aborted, "{}", transfer.detail);
    }
    let mut into = Cursor::new(Vec::new());
    let transfer = client
        .fetch("logs/kernel", &mut into, 0, |_, _| {})
        .expect("the comm core still takes a fetch");
    assert_eq!(transfer.state, FileState::Committed, "{}", transfer.detail);
    assert_eq!(into.into_inner(), bytes);
    assert_eq!(
        program.join().expect("program"),
        Giving::Done {
            taken: true,
            detail: String::new()
        }
    );
}

/// A file whose `fail_on`th read fails, as a disk that went away would; every read counted.
struct Failing {
    inner: Cursor<Vec<u8>>,
    reads: Arc<AtomicUsize>,
    fail_on: usize,
}

impl Read for Failing {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.reads.fetch_add(1, Ordering::SeqCst) + 1 == self.fail_on {
            return Err(std::io::Error::other("the disk went away"));
        }
        self.inner.read(buf)
    }
}

impl Seek for Failing {
    fn seek(&mut self, to: SeekFrom) -> std::io::Result<u64> {
        self.inner.seek(to)
    }
}

/// `size` bytes of [`content`], failing on read `fail_on`, and what to give them as.
fn failing(size: usize, fail_on: usize) -> (Failing, Known, Arc<AtomicUsize>) {
    let bytes = content(size);
    let known = Known {
        digest: Sha256::digest(&bytes).into(),
        size: bytes.len() as u64,
    };
    let reads = Arc::new(AtomicUsize::new(0));
    let source = Failing {
        inner: Cursor::new(bytes),
        reads: reads.clone(),
        fail_on,
    };
    (source, known, reads)
}

#[test]
fn a_gift_whose_file_cannot_be_read_ends_the_fetch_at_once_saying_so() {
    let (socket, _stop, _dir) = comm_core("unreadable");
    let socket_for = socket.clone();
    let (ready, offered) = mpsc::channel();
    let (failed_tx, failed) = mpsc::channel();
    let (done, finished) = mpsc::channel::<()>();
    let program = std::thread::spawn(move || {
        let files = files(&socket_for, "logger");
        let mut offer = files.offer("logs/").expect("offer");
        ready.send(()).expect("ready");
        let asked = offer.next(WAIT).expect("next").expect("asked");
        let (source, known, _) = failing(4 << 20, 3);
        let mut gift = asked.give_known(source, known).expect("give");
        let error = loop {
            match gift.step(Duration::from_millis(5)) {
                Ok(Giving::Progress { .. }) => {}
                Ok(done @ Giving::Done { .. }) => {
                    panic!("given, though it cannot be read: {done:?}")
                }
                Err(error) => break error,
            }
        };
        failed_tx
            .send((error.to_string(), Instant::now()))
            .expect("failed");
        // The program's session lives on: what the client hears is why, not that it went.
        finished.recv().expect("finished");
    });
    offered.recv().expect("offered");
    let client = files(&socket, "collector");
    let mut fetching = client
        .start_fetch("logs/kernel", Cursor::new(Vec::new()), 0)
        .expect("fetch");
    let transfer = ended(&mut fetching, Duration::from_secs(10));
    let (error, at) = failed.recv().expect("the program's step failed");
    let took = at.elapsed();
    assert!(error.contains("the disk went away"), "{error}");
    assert_eq!(transfer.state, FileState::Aborted, "{}", transfer.detail);
    assert_eq!(
        transfer.detail,
        "the program that offers it stopped giving it: it could not be read: the disk went away"
    );
    assert!(took < AT_ONCE, "heard {took:?} after the read failed");
    done.send(()).expect("done");
    program.join().expect("program");
}

#[test]
fn a_gift_whose_connection_broke_says_so_and_never_reads_on() {
    let (socket, stop, _dir) = comm_core("broken");
    let socket_for = socket.clone();
    let (ready, offered) = mpsc::channel();
    let (giving_tx, giving) = mpsc::channel::<()>();
    let (broken_tx, broken) = mpsc::channel::<()>();
    let program = std::thread::spawn(move || {
        let files = files(&socket_for, "logger");
        let mut offer = files.offer("logs/").expect("offer");
        ready.send(()).expect("ready");
        let asked = offer.next(WAIT).expect("next").expect("asked");
        let (source, known, reads) = failing(4 << 20, usize::MAX);
        let mut gift = asked.give_known(source, known).expect("give");
        gift.step(Duration::from_millis(5)).expect("the first step");
        giving_tx.send(()).expect("giving");
        broken.recv().expect("broken");
        let start = Instant::now();
        let (error, before, after) = loop {
            let before = reads.load(Ordering::SeqCst);
            match gift.step(Duration::from_millis(5)) {
                Ok(_) => assert!(start.elapsed() < WAIT, "the break was never seen"),
                Err(error) => break (error, before, reads.load(Ordering::SeqCst)),
            }
        };
        // The connection's error, not the file's: the source was never what failed.
        assert!(
            matches!(error, ClientError::Io(_) | ClientError::Lost),
            "{error:?}"
        );
        assert!(!error.to_string().contains("the disk"), "{error}");
        assert!(
            after <= before + 1,
            "read on after the break: {before} then {after}"
        );
        // The gift is over: nothing more is read or sent, by a step or by its drop.
        let read = reads.load(Ordering::SeqCst);
        assert!(matches!(
            gift.step(Duration::from_millis(5)),
            Err(ClientError::Protocol("a gift that is over"))
        ));
        drop(gift);
        assert_eq!(reads.load(Ordering::SeqCst), read);
    });
    offered.recv().expect("offered");
    let client = files(&socket, "collector");
    let _fetching = client
        .start_fetch("logs/kernel", Cursor::new(Vec::new()), 0)
        .expect("fetch");
    giving.recv().expect("giving");
    stop.send(()).expect("stop the comm core");
    broken_tx.send(()).expect("broken");
    program.join().expect("program");
}
