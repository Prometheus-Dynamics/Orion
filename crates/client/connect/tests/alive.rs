//! Keeping a session alive: a client pings twice a heartbeat while its program pumps it, takes
//! a silent comm core at the far end of a link for gone, and keeps a silent one on the board's
//! own socket.

#![cfg(target_os = "linux")]

use std::io::{Read, Write};
use std::os::fd::AsFd;
use std::os::unix::net::{UnixListener, UnixStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

use orion_connect::{Address, ClientError, Options, Reply, Session};
use orion_msg_session::{Pong, Welcome};
use orion_wire::body::encode_fields;
use orion_wire::frame::{self, FrameHeader};
use orion_wire::{INTERFACE_LEVEL, INTERFACE_REVISION, StreamDecoder, encode_frame, kind};

const HEARTBEAT_MS: u32 = 200;

/// A comm core that welcomes, counts the pings it is sent, and answers them or not.
struct Node {
    pings: Arc<AtomicU32>,
}

impl Node {
    fn pings(&self) -> u32 {
        self.pings.load(Ordering::Relaxed)
    }
}

/// The frame a node answers a frame of `kind` with, if it answers.
fn answer(asked: u8, pongs: bool, pings: &AtomicU32) -> Option<Vec<u8>> {
    let mut payload = [0_u8; 128];
    let (kind, len) = match asked {
        kind::HELLO => {
            let welcome = Welcome {
                node: "board".parse().expect("name"),
                session: 1,
                heartbeat_ms: HEARTBEAT_MS,
                max_frame: 4096,
                revision: INTERFACE_REVISION,
                idle_ms: HEARTBEAT_MS * 3,
                level: INTERFACE_LEVEL,
            };
            (
                kind::WELCOME,
                encode_fields(&welcome, &mut payload).expect("welcome"),
            )
        }
        kind::PING => {
            pings.fetch_add(1, Ordering::Relaxed);
            if !pongs {
                return None;
            }
            (
                kind::PONG,
                encode_fields(&Pong { now_ms: 0 }, &mut payload).expect("pong"),
            )
        }
        _ => return None,
    };
    let mut framed = vec![0_u8; 256];
    let len =
        frame::encode(FrameHeader::new(kind, 1), &payload[..len], &mut framed).expect("frame");
    framed.truncate(len);
    Some(framed)
}

/// A node at the far end of a link, and the client's end of it.
fn on_a_link(pongs: bool) -> (Node, UnixStream) {
    let (theirs, mut ours) = UnixStream::pair().expect("a link");
    let pings = Arc::new(AtomicU32::new(0));
    let counted = Arc::clone(&pings);
    std::thread::spawn(move || {
        let mut decoder = StreamDecoder::<4096>::new();
        let mut chunk = [0_u8; 1024];
        while let Ok(n) = ours.read(&mut chunk) {
            if n == 0 {
                return;
            }
            for byte in &chunk[..n] {
                let Ok(Some(frame)) = decoder.push(*byte) else {
                    continue;
                };
                if let Some(framed) = answer(frame.kind(), pongs, &counted) {
                    let mut wire = [0_u8; 600];
                    let len = encode_frame(&framed, &mut wire).expect("stream");
                    if ours.write_all(&wire[..len]).is_err() {
                        return;
                    }
                }
            }
        }
    });
    (Node { pings }, theirs)
}

/// A node behind a socket on the board, and the socket's path.
fn on_the_board(name: &str, pongs: bool) -> (Node, std::path::PathBuf) {
    let path = std::env::temp_dir().join(format!("orion-alive-{name}-{}", std::process::id()));
    let _ = std::fs::remove_file(&path);
    let listener = UnixListener::bind(&path).expect("socket");
    let pings = Arc::new(AtomicU32::new(0));
    let counted = Arc::clone(&pings);
    // Each connection in turn: a test that gave up on a slow handshake connects again.
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(stream) = stream else {
                return;
            };
            let mut buf = vec![0_u8; 8192];
            while let Ok(Some(received)) = shm_fdpass::recv(stream.as_fd(), &mut buf, 0, false) {
                let Ok(view) = frame::decode(&buf[..received.len]) else {
                    break;
                };
                if let Some(framed) = answer(view.kind(), pongs, &counted)
                    && shm_fdpass::send(stream.as_fd(), &[&framed], &[], false).is_err()
                {
                    break;
                }
            }
        }
    });
    (Node { pings }, path)
}

struct Half(UnixStream);

impl orion_connect::LinkRead for Half {
    fn read(&mut self, into: &mut [u8]) -> std::io::Result<usize> {
        Read::read(&mut self.0, into)
    }
}

impl orion_connect::LinkWrite for Half {
    fn write_all(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        Write::write_all(&mut self.0, bytes)
    }
}

fn over(link: UnixStream) -> Session {
    let write = Half(link.try_clone().expect("the link again"));
    Session::over(Half(link), write, None, "tool", Options::default()).expect("session")
}

/// Pumps `session` for `time`, or until it fails.
fn pump(session: &mut Session, time: Duration) -> Result<(), ClientError> {
    let until = Instant::now() + time;
    while Instant::now() < until {
        session.next(until.saturating_duration_since(Instant::now()))?;
    }
    Ok(())
}

#[test]
fn a_client_pings_twice_a_heartbeat_while_its_program_pumps_it() {
    let (node, link) = on_a_link(true);
    let mut session = over(link);
    pump(
        &mut session,
        Duration::from_millis(u64::from(HEARTBEAT_MS) * 5),
    )
    .expect("alive");
    // Ten in five heartbeats, give or take the ends. Once a heartbeat would be five.
    let pings = node.pings();
    assert!(
        (8..=11).contains(&pings),
        "{pings} pings in five heartbeats"
    );

    // A program that does not pump its session sends nothing: the session is only as alive
    // as its program.
    std::thread::sleep(Duration::from_millis(u64::from(HEARTBEAT_MS) * 2));
    assert_eq!(node.pings(), pings);
}

#[test]
fn a_comm_core_silent_at_the_far_end_of_a_link_is_taken_for_gone() {
    let (_node, link) = on_a_link(false);
    let mut session = over(link);
    let started = Instant::now();
    let lost = pump(&mut session, Duration::from_secs(5)).expect_err("silent");
    assert!(lost.is_lost(), "{lost}");
    // At the idle time the comm core named, three heartbeats.
    let after = started.elapsed().as_millis() as u64;
    let idle = u64::from(HEARTBEAT_MS) * 3;
    assert!((idle..idle + 300).contains(&after), "lost after {after} ms");
}

#[test]
fn a_comm_core_silent_on_the_boards_own_socket_is_kept_and_its_hang_up_is_heard() {
    let (node, path) = on_the_board("silent", false);
    let mut session = Session::connect(&Address::Local(path.clone()), "tool", Options::default())
        .expect("session");
    // Many idle times of silence: its socket is open, so it is busy or stopped, not gone.
    pump(
        &mut session,
        Duration::from_millis(u64::from(HEARTBEAT_MS) * 10),
    )
    .expect("kept");
    assert!(node.pings() >= 16, "{} pings", node.pings());
    let _ = std::fs::remove_file(&path);
}

#[test]
fn a_comm_core_that_closes_the_boards_socket_is_gone_at_once() {
    let path = std::env::temp_dir().join(format!("orion-alive-closed-{}", std::process::id()));
    let _ = std::fs::remove_file(&path);
    let listener = UnixListener::bind(&path).expect("socket");
    let serving = std::thread::spawn(move || {
        let (stream, _) = listener.accept().expect("accept");
        let mut buf = vec![0_u8; 8192];
        let received = shm_fdpass::recv(stream.as_fd(), &mut buf, 0, false);
        let received = received.expect("hello").expect("hello");
        let view = frame::decode(&buf[..received.len]).expect("frame");
        let framed = answer(view.kind(), true, &AtomicU32::new(0)).expect("welcome");
        shm_fdpass::send(stream.as_fd(), &[&framed], &[], false).expect("sent");
        // And it is gone: the stream is dropped here.
    });
    let mut session = Session::connect(&Address::Local(path.clone()), "tool", Options::default())
        .expect("session");
    serving.join().expect("served");
    let started = Instant::now();
    let lost = pump(&mut session, Duration::from_secs(5)).expect_err("closed");
    assert!(lost.is_lost(), "{lost}");
    assert!(
        started.elapsed() < Duration::from_millis(u64::from(HEARTBEAT_MS)),
        "heard late"
    );
    let _ = std::fs::remove_file(&path);
}

/// A session on the board's socket to a node that answers pings or not and nothing else, made
/// with `reply`.
fn asking(name: &str, pongs: bool, reply: Reply) -> (Node, std::path::PathBuf, Session) {
    let (node, path) = on_the_board(name, pongs);
    let options = || Options {
        timeout: Duration::from_millis(300),
        reply,
        ..Options::default()
    };
    // The short timeout is what these tests measure a reply against. It bounds the handshake
    // too, which a loaded machine can take longer over: that is tried again.
    let mut tries = 0;
    let session = loop {
        match Session::connect(&Address::Local(path.clone()), "tool", options()) {
            Ok(session) => break session,
            Err(error) if timed_out(&error) && tries < 20 => tries += 1,
            Err(error) => panic!("session: {error}"),
        }
    };
    (node, path, session)
}

/// Asks something the node never answers, and gives how the wait ended and after how long.
fn unanswered(session: &mut Session) -> (ClientError, Duration) {
    let seq = session.send(&Pong { now_ms: 0 }).expect("sent");
    let started = Instant::now();
    let error = session
        .answer(seq, kind::WELCOME)
        .map(|_| ())
        .expect_err("no answer");
    (error, started.elapsed())
}

fn timed_out(error: &ClientError) -> bool {
    matches!(error, ClientError::Io(io) if io.kind() == std::io::ErrorKind::TimedOut)
}

/// By default an answer has `Options::timeout`, however alive the node is: today's rule.
#[test]
fn by_default_an_answer_has_the_timeout() {
    let (_node, path, mut session) = asking("timeout", true, Reply::default());
    let (error, after) = unanswered(&mut session);
    assert!(timed_out(&error), "{error}");
    assert!((300..600).contains(&after.as_millis()), "after {after:?}");
    let _ = std::fs::remove_file(&path);
}

/// While the node is heard from, an answer may take longer than the timeout and as long as the
/// ceiling; the pings sent while waiting keep it heard from.
#[test]
fn while_the_node_is_heard_from_an_answer_waits_until_the_ceiling() {
    let ceiling = Duration::from_millis(u64::from(HEARTBEAT_MS) * 8);
    let reply = Reply::Alive {
        ceiling: Some(ceiling),
    };
    let (node, path, mut session) = asking("ceiling", true, reply);
    let (error, after) = unanswered(&mut session);
    assert!(timed_out(&error), "{error}");
    assert!(after >= ceiling && after < ceiling * 2, "after {after:?}");
    // Pinged all along, and heard from: never silent for its idle time.
    assert!(node.pings() >= 12, "{} pings", node.pings());
    assert!(session.silent_for() < Duration::from_millis(u64::from(HEARTBEAT_MS) * 3));
    let _ = std::fs::remove_file(&path);
}

/// A node silent for the idle time it declared is gone, before any ceiling, on the board's own
/// socket too; and `silent_for` says how long it has been.
#[test]
fn a_node_silent_for_its_idle_time_is_gone_and_silent_for_says_so() {
    let reply = Reply::Alive {
        ceiling: Some(Duration::from_secs(30)),
    };
    let (_node, path, mut session) = asking("silent", false, reply);
    let (error, after) = unanswered(&mut session);
    assert!(matches!(error, ClientError::Lost), "{error}");
    let idle = u64::from(HEARTBEAT_MS) * 3;
    assert!(
        (idle..idle + 300).contains(&(after.as_millis() as u64)),
        "after {after:?}"
    );
    assert!(session.silent_for() >= Duration::from_millis(idle));
    let _ = std::fs::remove_file(&path);
}

/// The bound is the session's, kept until it is set again.
#[test]
fn the_reply_bound_is_kept_until_it_is_set_again() {
    let (_node, path, mut session) = asking("set", true, Reply::default());
    let longer = Reply::Alive {
        ceiling: Some(Duration::from_millis(900)),
    };
    session.set_reply(longer);
    assert_eq!(session.reply(), longer);
    let (_, after) = unanswered(&mut session);
    assert!(after >= Duration::from_millis(900), "after {after:?}");
    let (_, after) = unanswered(&mut session);
    assert!(after >= Duration::from_millis(900), "after {after:?}");
    session.set_reply(Reply::Timeout);
    let (_, after) = unanswered(&mut session);
    assert!(after < Duration::from_millis(600), "after {after:?}");
    let _ = std::fs::remove_file(&path);
}

/// A node that takes the connection and never says hello: the client says it timed out, not
/// the kernel's "try again" from a timed read.
#[test]
fn a_node_that_never_says_hello_has_timed_out() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("listen");
    let host = listener.local_addr().expect("addr").to_string();
    let held = std::thread::spawn(move || {
        let taken = listener.accept();
        std::thread::sleep(Duration::from_secs(2));
        drop(taken);
    });
    let options = Options {
        timeout: Duration::from_millis(300),
        ..Options::default()
    };
    let address = Address::Network { host, key: None };
    let error = Session::connect(&address, "tool", options)
        .map(|_| ())
        .expect_err("no hello");
    assert!(timed_out(&error), "{error}");
    assert!(!error.to_string().contains("os error 11"), "{error}");
    assert!(error.is_lost(), "a reconnect tries again: {error}");
    held.join().expect("held");
}

/// Inside a tokio runtime the bound is the same: a ceiling past the timeout, while heard from.
#[cfg(feature = "tokio")]
#[test]
fn inside_a_runtime_an_answer_is_bounded_the_same_way() {
    let ceiling = Duration::from_millis(u64::from(HEARTBEAT_MS) * 5);
    let reply = Reply::Alive {
        ceiling: Some(ceiling),
    };
    let (_node, path, mut session) = asking("async", true, reply);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let (error, after) = runtime.block_on(async {
        let seq = session.send(&Pong { now_ms: 0 }).expect("sent");
        let started = Instant::now();
        let error = session
            .answer_async(seq, kind::WELCOME)
            .await
            .map(|_| ())
            .expect_err("no answer");
        (error, started.elapsed())
    });
    assert!(timed_out(&error), "{error}");
    assert!(after >= ceiling && after < ceiling * 2, "after {after:?}");
    let _ = std::fs::remove_file(&path);
}
