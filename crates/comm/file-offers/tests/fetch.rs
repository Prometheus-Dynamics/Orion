//! Files from a board: a client fetches what a program there offers, through the comm core,
//! whole or from where it broke off, and each side is told when the other goes.

#![cfg(target_os = "linux")]

use std::io::Cursor;
use std::net::Shutdown;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use orion_connect::{Address, Client, ClientError, Options, Session};
use orion_file_offers::FileOffersService;
use orion_files::{FileState, Files, Giving, Known, OfferedBy, Step};
use orion_scratch::Scratch;
use orion_server::{Comm, Error, Limits, Security};
use orion_unix::UnixCarrier;
use sha2::{Digest, Sha256};

pub(crate) const WAIT: Duration = Duration::from_secs(10);

/// A comm core with the offers service, in a thread of its own, on a socket of its own.
pub(crate) fn comm_core(name: &str) -> (PathBuf, tokio::sync::oneshot::Sender<()>, Scratch) {
    let dir = Scratch::new(&format!("offers-{name}"));
    let socket = dir.join("orion.sock");
    let (ready, started) = mpsc::channel();
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let path = socket.clone();
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        tokio::task::LocalSet::new().block_on(&runtime, async move {
            let comm = Comm::new(
                "board".parse().expect("name"),
                Security::Off,
                None,
                Limits::default(),
            );
            comm.add_service(FileOffersService::standard());
            let unix = UnixCarrier::bind(&path, Some(0o600), &[]).expect("socket");
            tokio::task::spawn_local(async move { comm.serve(unix).await });
            ready.send(()).expect("ready");
            let _ = stopped.await;
        });
    });
    started.recv().expect("the comm core starts");
    (socket, stop, dir)
}

pub(crate) fn files(socket: &Path, name: &str) -> Files {
    let session = Session::connect(&Address::Local(socket.to_owned()), name, Options::default())
        .expect("connect");
    Files::new(&Client::new(session))
}

/// Bytes that differ from place to place, so a chunk in the wrong place is seen.
pub(crate) fn content(size: usize) -> Vec<u8> {
    (0..size).map(|at| (at * 31 % 251) as u8).collect()
}

/// What the offering program saw of the one request it was handed, and how its gift ended.
#[derive(Debug)]
struct Served {
    name: String,
    from: u64,
    caller: String,
    ended: Giving,
}

/// A program that offers `prefix` and serves `files` (name, bytes) to `count` requests, in a
/// thread; refuses any other name in its own words. Its offer is in place when this returns.
fn offerer(
    socket: &Path,
    prefix: &'static str,
    files_held: Vec<(&'static str, Vec<u8>)>,
    count: usize,
) -> JoinHandle<Vec<Served>> {
    let socket = socket.to_owned();
    let (ready, offered) = mpsc::channel();
    let handle = std::thread::spawn(move || {
        let files = files(&socket, "logger");
        let mut offer = files.offer(prefix).expect("offer");
        ready.send(()).expect("ready");
        let mut served = Vec::new();
        while served.len() < count {
            let Some(asked) = offer.next(WAIT).expect("next") else {
                panic!("asked for nothing");
            };
            let (name, from, caller) = (
                asked.name().to_owned(),
                asked.from(),
                asked.caller().to_owned(),
            );
            let Some((_, bytes)) = files_held.iter().find(|(held, _)| *held == name) else {
                asked
                    .refuse(&format!("{name}: no such file"))
                    .expect("refuse");
                served.push(Served {
                    name,
                    from,
                    caller,
                    ended: Giving::Done {
                        taken: false,
                        detail: "refused".into(),
                    },
                });
                continue;
            };
            let ended = asked
                .give(Cursor::new(bytes.clone()))
                .expect("give")
                .finish()
                .expect("given");
            served.push(Served {
                name,
                from,
                caller,
                ended,
            });
        }
        served
    });
    offered.recv().expect("offered");
    handle
}

#[test]
fn a_file_offered_on_the_board_is_fetched_whole_and_checked() {
    let (socket, _stop, _dir) = comm_core("whole");
    // Larger than the window, so the client's answers pace the program.
    let kernel = content(600 * 1024);
    let program = offerer(&socket, "logs/", vec![("logs/kernel", kernel.clone())], 1);

    let client = files(&socket, "collector");
    let mut into = Cursor::new(Vec::new());
    let mut seen = Vec::new();
    let transfer = client
        .fetch("logs/kernel", &mut into, 0, |received, size| {
            seen.push((received, size));
        })
        .expect("fetch");
    assert_eq!(transfer.state, FileState::Committed, "{}", transfer.detail);
    assert_eq!(transfer.received, kernel.len() as u64);
    assert_eq!(into.into_inner(), kernel);
    let digest: [u8; 32] = Sha256::digest(&kernel).into();
    assert_eq!(transfer.digest, digest);
    assert!(seen.len() > 2, "progress was told as it went: {seen:?}");

    let served = program.join().expect("program");
    assert_eq!(served[0].name, "logs/kernel");
    assert_eq!(served[0].from, 0);
    // The comm core says who asked, as the audit log names them.
    assert!(
        served[0].caller.starts_with("local:"),
        "{}",
        served[0].caller
    );
    assert_eq!(
        served[0].ended,
        Giving::Done {
            taken: true,
            detail: String::new()
        }
    );
}

#[test]
fn a_fetch_that_broke_off_goes_on_from_what_is_here() {
    let (socket, _stop, _dir) = comm_core("resume");
    let kernel = content(300 * 1024);
    let program = offerer(&socket, "logs/", vec![("logs/kernel", kernel.clone())], 1);

    let client = files(&socket, "collector");
    let have = 70_000;
    let mut into = Cursor::new(kernel[..have].to_vec());
    let transfer = client
        .fetch("logs/kernel", &mut into, have as u64, |_, _| {})
        .expect("fetch");
    assert_eq!(transfer.state, FileState::Committed, "{}", transfer.detail);
    assert_eq!(into.into_inner(), kernel);
    let served = program.join().expect("program");
    assert_eq!(served[0].from, have as u64, "only the rest was asked for");
}

#[test]
fn a_name_nothing_offers_and_one_the_program_lacks_are_refused_by_name() {
    let (socket, _stop, _dir) = comm_core("refused");
    let program = offerer(&socket, "logs/", vec![], 1);
    let client = files(&socket, "collector");

    let error = client
        .fetch("other/x", &mut Cursor::new(Vec::new()), 0, |_, _| {})
        .expect_err("nothing offers it");
    assert!(
        matches!(&error, ClientError::Refused { error: Error::NotFound, detail } if detail == "nothing offers other/x"),
        "{error}"
    );

    let transfer = client
        .fetch("logs/missing", &mut Cursor::new(Vec::new()), 0, |_, _| {})
        .expect("answered");
    assert_eq!(transfer.state, FileState::Rejected);
    assert_eq!(transfer.detail, "logs/missing: no such file");
    program.join().expect("program");
}

#[test]
fn a_second_program_offering_a_prefix_is_refused_naming_the_holder_and_counted() {
    let (socket, _stop, _dir) = comm_core("clash");
    let holder = files(&socket, "logger");
    let offer = holder.offer("logs/").expect("offer");
    let other = files(&socket, "other-logger");
    let error = other.offer("logs/").expect_err("held");
    assert!(
        matches!(&error, ClientError::Refused { error: Error::Busy, detail } if detail.starts_with("logs/ is offered by logger (local:")),
        "{error}"
    );
    assert!(matches!(
        other.offer("logs").expect_err("not a prefix"),
        ClientError::Refused {
            error: Error::InvalidName,
            ..
        }
    ));

    let (offers, clashes) = other.offers().expect("offers");
    assert_eq!(clashes, 1);
    assert_eq!(offers.len(), 1);
    assert_eq!(offers[0].prefix, "logs/");
    assert_eq!(offers[0].by, OfferedBy::Program);
    assert_eq!(offers[0].name, "logger");
    assert!(offers[0].principal.starts_with("local:"));

    // Its session ends: the prefix is free.
    drop(offer);
    drop(holder);
    let mut taken = None;
    for _ in 0..200 {
        match other.offer("logs/") {
            Ok(offer) => {
                taken = Some(offer);
                break;
            }
            Err(_) => std::thread::sleep(Duration::from_millis(10)),
        }
    }
    assert!(taken.is_some(), "the prefix was never freed");
    let (offers, _) = other.offers().expect("offers");
    assert_eq!(offers[0].name, "other-logger");
}

#[test]
fn the_longest_prefix_offered_is_handed_the_fetch() {
    let (socket, _stop, _dir) = comm_core("longest");
    let broad = offerer(&socket, "logs/", vec![("logs/boot", content(10))], 1);
    let narrow = offerer(
        &socket,
        "logs/kernel/",
        vec![("logs/kernel/ring", content(20))],
        1,
    );
    let client = files(&socket, "collector");
    let mut ring = Cursor::new(Vec::new());
    let mut boot = Cursor::new(Vec::new());
    let one = client.fetch("logs/kernel/ring", &mut ring, 0, |_, _| {});
    let two = client.fetch("logs/boot", &mut boot, 0, |_, _| {});
    assert_eq!(one.expect("ring").state, FileState::Committed);
    assert_eq!(two.expect("boot").state, FileState::Committed);
    assert_eq!(ring.into_inner(), content(20));
    assert_eq!(boot.into_inner(), content(10));
    assert_eq!(narrow.join().expect("narrow")[0].name, "logs/kernel/ring");
    assert_eq!(broad.join().expect("broad")[0].name, "logs/boot");
}

#[test]
fn a_file_whose_bytes_are_not_its_digest_is_not_taken() {
    let (socket, _stop, _dir) = comm_core("digest");
    let socket_for = socket.clone();
    let (ready, offered) = mpsc::channel();
    let program = std::thread::spawn(move || {
        let files = files(&socket_for, "logger");
        let mut offer = files.offer("logs/").expect("offer");
        ready.send(()).expect("ready");
        let asked = offer.next(WAIT).expect("next").expect("asked");
        let bytes = content(5000);
        let wrong = Known {
            digest: [7; 32],
            size: bytes.len() as u64,
        };
        asked
            .give_known(Cursor::new(bytes), wrong)
            .expect("give")
            .finish()
            .expect("over")
    });
    offered.recv().expect("offered");
    let client = files(&socket, "collector");
    let transfer = client
        .fetch("logs/kernel", &mut Cursor::new(Vec::new()), 0, |_, _| {})
        .expect("answered");
    assert_eq!(transfer.state, FileState::Rejected);
    assert!(transfer.detail.contains("digest"), "{}", transfer.detail);
    let Giving::Done { taken, detail } = program.join().expect("program") else {
        panic!("not over");
    };
    assert!(!taken);
    assert!(detail.contains("digest"), "{detail}");
}

/// A relay in front of the comm core's socket, for one program: cutting it ends that
/// program's session as a program that dies does, with nothing of the program run after.
fn relay(socket: &Path) -> (PathBuf, impl FnOnce()) {
    let (path, cut, _held) = relay_holding(socket, false);
    (path, cut)
}

/// [`relay`], and when `hold`, the program's side stops one byte short of the end of the
/// first frame that starts past the first chunk, and says so on the receiver: cut then, the
/// program dies inside a frame.
fn relay_holding(socket: &Path, hold: bool) -> (PathBuf, impl FnOnce(), mpsc::Receiver<()>) {
    let path = socket.with_file_name("relay.sock");
    let _ = std::fs::remove_file(&path);
    let listener = UnixListener::bind(&path).expect("relay");
    let socket = socket.to_owned();
    let (cut_tx, cut_rx) = mpsc::channel::<Vec<UnixStream>>();
    let (held_tx, held) = mpsc::channel();
    std::thread::spawn(move || {
        let (program, _) = listener.accept().expect("accept");
        let comm = UnixStream::connect(&socket).expect("comm core");
        let ends = vec![
            program.try_clone().expect("clone"),
            comm.try_clone().expect("clone"),
        ];
        cut_tx.send(ends).expect("ends");
        let copy = |mut from: UnixStream, mut to: UnixStream| {
            std::thread::spawn(move || {
                let _ = std::io::copy(&mut from, &mut to);
                let _ = to.shutdown(Shutdown::Both);
            });
        };
        let (from, to) = (
            program.try_clone().expect("clone"),
            comm.try_clone().expect("clone"),
        );
        if hold {
            std::thread::spawn(move || hold_inside_a_frame(from, to, held_tx));
        } else {
            copy(from, to);
        }
        copy(comm, program);
    });
    let cut = move || {
        for end in cut_rx.recv().expect("ends") {
            let _ = end.shutdown(Shutdown::Both);
        }
    };
    (path, cut, held)
}

/// Copies the program's side to the comm core's, frame by frame as the carrier frames them
/// (`[u32 length][frame]`), up to one byte short of the end of the first frame that starts past
/// 40,000 bytes (past the first chunk of a gift); then says so and copies nothing more.
fn hold_inside_a_frame(mut from: UnixStream, mut to: UnixStream, held: mpsc::Sender<()>) {
    use std::io::{Read, Write};
    let mut stream = Vec::new();
    let mut buf = vec![0_u8; 64 * 1024];
    let (mut sent, mut next, mut stop) = (0, 0, None);
    loop {
        let n = match from.read(&mut buf) {
            Ok(0) | Err(_) => return,
            Ok(n) => n,
        };
        stream.extend_from_slice(&buf[..n]);
        while stop.is_none() && stream.len() >= next + 4 {
            let len = u32::from_le_bytes(stream[next..next + 4].try_into().expect("4 bytes"));
            let end = next + 4 + len as usize;
            if next > 40_000 {
                stop = Some(end - 1);
            }
            next = end;
        }
        let upto = stop.unwrap_or(stream.len()).min(stream.len());
        if upto > sent {
            to.write_all(&stream[sent..upto]).expect("to the comm core");
            sent = upto;
            if Some(sent) == stop {
                held.send(()).expect("held");
            }
        }
    }
}

#[test]
fn a_fetch_whose_program_goes_is_rejected_to_its_client() {
    let (socket, _stop, _dir) = comm_core("gone");
    // The program dies mid-gift: no drop of its gift is run, so it says nothing itself.
    let (socket_for, cut) = relay(&socket);
    let (ready, offered) = mpsc::channel();
    let (asked_tx, asked_rx) = mpsc::channel();
    let (go, gone) = mpsc::channel::<()>();
    let program = std::thread::spawn(move || {
        let files = files(&socket_for, "logger");
        let mut offer = files.offer("logs/").expect("offer");
        ready.send(()).expect("ready");
        let asked = offer.next(WAIT).expect("next").expect("asked");
        // Gives the start, then its session is cut before the rest.
        let mut gift = asked.give(Cursor::new(content(1 << 20))).expect("give");
        let _ = gift.step(Duration::from_millis(10)).expect("step");
        asked_tx.send(()).expect("asked");
        gone.recv().expect("go");
    });
    offered.recv().expect("offered");
    let client = files(&socket, "collector");
    let mut fetching = client
        .start_fetch("logs/kernel", Cursor::new(Vec::new()), 0)
        .expect("fetch");
    loop {
        if let Step::Progress { received, .. } =
            fetching.step(Duration::from_millis(20)).expect("step")
            && received > 0
        {
            break;
        }
    }
    asked_rx.recv().expect("asked");
    let cut_at = Instant::now();
    cut();
    // As soon as the comm core sees the program's link close: well inside the client's own
    // silence limit (5 s), which is all it would have without being told.
    let ended = loop {
        if let Step::Done(transfer) = fetching.step(Duration::from_millis(50)).expect("step") {
            break transfer;
        }
        let waited = cut_at.elapsed();
        assert!(waited < Duration::from_secs(1), "not told after {waited:?}");
    };
    assert_eq!(ended.state, FileState::Rejected);
    assert_eq!(ended.detail, "the program that offers it went");
    go.send(()).expect("go");
    program.join().expect("program");
}

#[test]
fn a_program_that_dies_inside_a_frame_is_gone_and_the_comm_core_serves_the_others() {
    let (socket, _stop, _dir) = comm_core("inside-a-frame");
    // The program dies with only part of a frame sent: the rest never comes.
    let (socket_for, cut, held) = relay_holding(&socket, true);
    let (ready, offered) = mpsc::channel();
    let (asked_tx, asked_rx) = mpsc::channel();
    let (go, gone) = mpsc::channel::<()>();
    let program = std::thread::spawn(move || {
        let files = files(&socket_for, "logger");
        let mut offer = files.offer("logs/").expect("offer");
        ready.send(()).expect("ready");
        let asked = offer.next(WAIT).expect("next").expect("asked");
        let mut gift = asked.give(Cursor::new(content(1 << 20))).expect("give");
        let _ = gift.step(Duration::from_millis(10)).expect("step");
        asked_tx.send(()).expect("asked");
        gone.recv().expect("go");
    });
    offered.recv().expect("offered");
    let client = files(&socket, "collector");
    let mut fetching = client
        .start_fetch("logs/kernel", Cursor::new(Vec::new()), 0)
        .expect("fetch");
    loop {
        if let Step::Progress { received, .. } =
            fetching.step(Duration::from_millis(20)).expect("step")
            && received > 0
        {
            break;
        }
    }
    asked_rx.recv().expect("asked");
    held.recv_timeout(WAIT)
        .expect("the relay holds a frame's last byte");
    let cut_at = Instant::now();
    cut();
    // Another session, meanwhile: the comm core still answers it.
    let (answered, answer) = mpsc::channel();
    let at = socket.clone();
    std::thread::spawn(move || {
        let _ = answered.send(
            files(&at, "another")
                .offers()
                .map(|(offers, _)| offers.len()),
        );
    });
    // As soon as the comm core sees the program's link close, as in
    // a_fetch_whose_program_goes_is_rejected_to_its_client.
    let ended = loop {
        if let Step::Done(transfer) = fetching.step(Duration::from_millis(50)).expect("step") {
            break transfer;
        }
        let waited = cut_at.elapsed();
        assert!(waited < Duration::from_secs(1), "not told after {waited:?}");
    };
    assert_eq!(ended.state, FileState::Rejected);
    assert_eq!(ended.detail, "the program that offers it went");
    answer
        .recv_timeout(WAIT)
        .expect("another session is answered")
        .expect("offers");
    go.send(()).expect("go");
    program.join().expect("program");
}

#[test]
fn a_fetch_whose_comm_core_goes_ends_as_a_lost_session_at_once() {
    let (socket, stop, _dir) = comm_core("core-gone");
    let (ready, offered) = mpsc::channel();
    let (asked_tx, asked_rx) = mpsc::channel();
    let (go, gone) = mpsc::channel::<()>();
    let socket_for = socket.clone();
    let program = std::thread::spawn(move || {
        let files = files(&socket_for, "logger");
        let mut offer = files.offer("logs/").expect("offer");
        ready.send(()).expect("ready");
        let asked = offer.next(WAIT).expect("next").expect("asked");
        let mut gift = asked.give(Cursor::new(content(1 << 20))).expect("give");
        let _ = gift.step(Duration::from_millis(10)).expect("step");
        asked_tx.send(()).expect("asked");
        gone.recv().expect("go");
    });
    offered.recv().expect("offered");
    let client = files(&socket, "collector");
    let mut fetching = client
        .start_fetch("logs/kernel", Cursor::new(Vec::new()), 0)
        .expect("fetch");
    loop {
        if let Step::Progress { received, .. } =
            fetching.step(Duration::from_millis(20)).expect("step")
            && received > 0
        {
            break;
        }
    }
    asked_rx.recv().expect("asked");
    // The comm core goes mid-gift: its sockets close with it.
    let stopped_at = Instant::now();
    stop.send(()).expect("stop");
    let error = loop {
        match fetching.step(Duration::from_millis(50)) {
            Ok(Step::Done(transfer)) => panic!("ended as {transfer:?}, not as a lost session"),
            Ok(_) => assert!(
                stopped_at.elapsed() < WAIT,
                "the fetch never saw its session go"
            ),
            Err(error) => break error,
        }
    };
    // As soon as its socket closes: well inside its silence limit (5 s).
    let took = stopped_at.elapsed();
    assert!(error.is_lost(), "{error}");
    assert!(took < Duration::from_secs(1), "told after {took:?}");
    go.send(()).expect("go");
    program.join().expect("program");
}

#[test]
fn a_fetch_held_to_a_bound_does_not_take_a_larger_file() {
    let (socket, _stop, _dir) = comm_core("bound");
    let program = offerer(
        &socket,
        "logs/",
        vec![("logs/kernel", content(5000)), ("logs/boot", content(4000))],
        2,
    );
    let client = files(&socket, "collector");
    let mut into = Cursor::new(Vec::new());
    let over = client
        .start_fetch("logs/kernel", &mut into, 0)
        .expect("fetch")
        .within(4000)
        .finish(|_, _| {})
        .expect("answered");
    assert_eq!(over.state, FileState::Rejected);
    assert_eq!(
        over.detail,
        "the file is 5000 bytes, more than its bound of 4000"
    );
    assert!(into.into_inner().is_empty(), "nothing of it was written");
    // At its bound exactly, it is taken.
    let mut into = Cursor::new(Vec::new());
    let at = client
        .start_fetch("logs/boot", &mut into, 0)
        .expect("fetch")
        .within(4000)
        .finish(|_, _| {})
        .expect("answered");
    assert_eq!(at.state, FileState::Committed, "{}", at.detail);
    let served = program.join().expect("program");
    assert!(
        matches!(&served[0].ended, Giving::Done { taken: false, detail } if detail.contains("bound")),
        "{:?}",
        served[0].ended
    );
}
