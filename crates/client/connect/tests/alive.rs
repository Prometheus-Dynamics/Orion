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

use orion_connect::{Address, ClientError, Options, Session};
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
    std::thread::spawn(move || {
        let Ok((stream, _)) = listener.accept() else {
            return;
        };
        let mut buf = vec![0_u8; 8192];
        while let Ok(Some(received)) = shm_fdpass::recv(stream.as_fd(), &mut buf, 0, false) {
            let Ok(view) = frame::decode(&buf[..received.len]) else {
                return;
            };
            if let Some(framed) = answer(view.kind(), pongs, &counted)
                && shm_fdpass::send(stream.as_fd(), &[&framed], &[], false).is_err()
            {
                return;
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
