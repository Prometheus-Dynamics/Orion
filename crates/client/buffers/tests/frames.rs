//! Two processes pass frames through shared buffers: one fills them, the other reads them
//! where they lie. Nothing is copied.
//!
//! This test is the comm core and the camera. It starts the same test binary again as the
//! viewer, in another process.

use std::os::unix::process::ExitStatusExt;
use std::path::PathBuf;
use std::process::Command;
use std::sync::mpsc;
use std::time::{Duration, Instant};

use orion_broker::{Broker, BrokerConfig};
use orion_broker_buffers::BrokerBuffers;
use orion_buffers::{Buffers, HoldLimits, NextFrame, Pool};
use orion_connect::{Address, Client, Options, Session};
use orion_measure::{CountingAlloc, thread_counts};
use orion_ring::FrameTime;
use orion_server::{Admits, Comm, Limits, Policy, Security};
use orion_topics::{Status, TopicError, Topics};
use orion_unix::UnixCarrier;

#[global_allocator]
static GLOBAL: CountingAlloc = CountingAlloc::system();

const ROLE: &str = "ORION_TEST_ROLE";
const SOCKET: &str = "ORION_TEST_SOCKET";
const TOPIC: &str = "camera.front";
/// One megabyte a frame: far more than a session or a ring carries.
const FRAME: usize = 1 << 20;
const BUFFERS: u32 = 8;
const FRAMES: u64 = 400;

fn buffers(socket: &std::path::Path, name: &str) -> Buffers {
    let session = Session::connect(&Address::Local(socket.to_owned()), name, Options::default())
        .expect("connect");
    let client = Client::new(session);
    Buffers::new(&client, &Topics::new(&client))
}

/// What a frame holds: its number at the start, the end and across the middle.
fn fill(buffer: &mut [u8], number: u64) {
    for chunk in buffer.chunks_mut(4096) {
        chunk[..8].copy_from_slice(&number.to_le_bytes());
    }
    let end = buffer.len() - 8;
    buffer[end..].copy_from_slice(&(!number).to_le_bytes());
}

fn check(bytes: &[u8], number: u64) -> bool {
    bytes.len() == FRAME
        && bytes
            .chunks(4096)
            .all(|chunk| chunk[..8] == number.to_le_bytes())
        && bytes[FRAME - 8..] == (!number).to_le_bytes()
}

/// The viewer process: holds every frame it gets and reads it where it lies.
#[test]
fn viewer_process() {
    if std::env::var(ROLE).as_deref() != Ok("viewer") {
        return;
    }
    let socket = PathBuf::from(std::env::var(SOCKET).expect("socket"));
    let side = buffers(&socket, "viewer");
    let limits = HoldLimits {
        max_held: 2,
        max_hold_ms: 0,
    };
    let mut frames = side.subscribe(TOPIC, b"1080p", limits).expect("subscribe");
    // Where each buffer is in this process: the same place every time it is used, because
    // it is the publisher's memory, mapped, and not a copy made for this frame.
    let mut places = [std::ptr::null::<u8>(); BUFFERS as usize];
    let (mut seen, mut last, mut warm) = (0_u64, 0_u64, None);
    let started = Instant::now();
    while last < FRAMES {
        assert!(
            started.elapsed() < Duration::from_secs(30),
            "saw {seen} frames, last {last}"
        );
        if !frames.wait(Duration::from_millis(500)).expect("wait") {
            continue;
        }
        loop {
            let held = match frames.next() {
                NextFrame::Frame(frame) => {
                    last = u64::from_le_bytes(frame.meta.try_into().expect("the frame's number"));
                    assert_eq!(frame.time, FrameTime::from_nanos(last));
                    frame.held
                }
                NextFrame::Empty => break,
                NextFrame::CutOff => panic!("cut off"),
                NextFrame::Skipped(_) | NextFrame::Limited | NextFrame::PoolChanged { .. } => {
                    continue;
                }
            };
            let index = held.buffer(0).expect("one buffer").0 as usize;
            // The bytes are fetched again for the check: a held frame is only as good as the
            // hold, so nothing is read through a pointer kept from before.
            let bytes = held.bytes(0).expect("mapped");
            assert!(check(bytes, last), "frame {last} is not what was published");
            if places[index].is_null() {
                places[index] = bytes.as_ptr();
            }
            assert_eq!(
                places[index],
                bytes.as_ptr(),
                "buffer {index} moved: it was copied"
            );
            assert!(frames.held() <= 2);
            seen += 1;
            drop(held);
            // Once every buffer has been seen, a frame costs this process no allocation.
            if seen == 50 {
                warm = Some(thread_counts());
            }
        }
    }
    let allocated = thread_counts().since(warm.expect("fifty frames"));
    assert_eq!(
        allocated.allocs,
        0,
        "{allocated:?} over {} frames",
        seen - 50
    );
    assert!(seen >= FRAMES / 2, "only {seen} of {FRAMES} frames");
    assert_eq!(frames.status(), Status::Live);
    println!("viewer: {seen} frames read in place");
}

#[test]
fn two_processes_pass_frames_without_a_copy() {
    if std::env::var(ROLE).is_ok() {
        return;
    }
    let dir = std::env::temp_dir().join(format!("orion-buffers-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("dir");
    let socket = dir.join("orion.sock");

    // The comm core, in its own thread.
    let (ready, started) = mpsc::channel();
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let path = socket.clone();
    let comm_core = std::thread::spawn(move || {
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
            let anyone = Policy::new(Admits::ANYONE).without_grant();
            let config = BrokerConfig {
                declare: anyone,
                subscribe: anyone,
                ..BrokerConfig::default()
            };
            let broker = Broker::new("board".parse().expect("name"), config);
            comm.add_service(BrokerBuffers::new(&broker, anyone));
            comm.add_service(broker);
            let unix = UnixCarrier::bind(&path, Some(0o600), &[]).expect("socket");
            let stats = comm.clone();
            tokio::task::spawn_local(async move { comm.serve(unix).await });
            ready.send(()).expect("ready");
            let _ = stopped.await;
            // What the comm core handled is set-up, not frames.
            stats.stats().requests
        })
    });
    started.recv().expect("the comm core starts");

    // The camera, here.
    let side = buffers(&socket, "camera");
    let pool = Pool::in_memory(1, BUFFERS, FRAME).expect("pool");
    let mut camera = side.declare(TOPIC, 8, 8, pool).expect("declare");

    // The viewer, in another process.
    let mut viewer = Command::new(std::env::current_exe().expect("this test"))
        .args(["--exact", "viewer_process", "--nocapture"])
        .env(ROLE, "viewer")
        .env(SOCKET, &socket)
        .spawn()
        .expect("the viewer starts");

    // No frames until someone is looking.
    let waiting = Instant::now();
    while camera.subscribers() == 0 || camera.with_request(<[u8]>::is_empty) {
        assert!(
            waiting.elapsed() < Duration::from_secs(20),
            "the viewer never subscribed"
        );
        camera.poll().expect("poll");
        std::thread::sleep(Duration::from_millis(2));
    }
    camera.with_request(|request| assert_eq!(request, b"1080p"));

    let (mut published, mut dropped, mut warm) = (0_u64, 0_u64, None);
    for number in 1..=FRAMES {
        camera.poll().expect("poll");
        match camera.take() {
            Ok(buffer) => {
                fill(
                    camera
                        .buffer_mut(buffer)
                        .expect("this pool is the camera's own"),
                    number,
                );
                camera
                    .publish(
                        &[buffer],
                        FrameTime::from_nanos(number),
                        &number.to_le_bytes(),
                    )
                    .expect("publish");
                published += 1;
            }
            // Every buffer published or held: this frame is the one that is lost.
            Err(_) => dropped += 1,
        }
        if number == 50 {
            warm = Some(thread_counts());
        }
        std::thread::sleep(Duration::from_millis(2));
    }
    let allocated = thread_counts().since(warm.expect("fifty frames"));
    assert_eq!(
        allocated.allocs,
        0,
        "{allocated:?} over {} frames",
        FRAMES - 50
    );
    assert_eq!(
        dropped, 0,
        "a viewer holding two of eight buffers starved the camera"
    );
    assert_eq!(published, FRAMES);

    let status = viewer.wait().expect("the viewer ends");
    assert!(
        status.success(),
        "the viewer failed: {:?} (signal {:?})",
        status.code(),
        status.signal()
    );

    // The viewer is gone: the camera is told, and has every buffer back.
    let waiting = Instant::now();
    while camera.subscribers() != 0 {
        assert!(
            waiting.elapsed() < Duration::from_secs(5),
            "never told the viewer left"
        );
        camera.poll().expect("poll");
        std::thread::sleep(Duration::from_millis(2));
    }
    camera.poll().expect("poll");
    let free = (0..BUFFERS).filter(|_| camera.take().is_ok()).count();
    assert!(
        free >= BUFFERS as usize - 1,
        "only {free} of {BUFFERS} buffers came back"
    );

    drop(camera);
    let _ = stop.send(());
    let requests = comm_core.join().expect("the comm core stops");
    // 400 MB of frames went by; the comm core handled a handful of requests.
    assert!(requests < 40, "{requests} requests for {FRAMES} frames");
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn a_buffer_topic_needs_the_boards_own_socket() {
    if std::env::var(ROLE).is_ok() {
        return;
    }
    // A session that cannot pass descriptors: here, one over a link.
    let (ours, theirs) = std::os::unix::net::UnixStream::pair().expect("pair");
    let answering = std::thread::spawn(move || {
        use orion_wire::{StreamDecoder, encode_body, encode_frame, kind};
        use std::io::{Read, Write};
        let mut theirs = theirs;
        let mut decoder = Box::new(StreamDecoder::<1024>::new());
        let mut byte = [0_u8; 1];
        loop {
            theirs.read_exact(&mut byte).expect("read");
            if let Ok(Some(frame)) = decoder.push(byte[0])
                && frame.kind() == kind::HELLO
            {
                break;
            }
        }
        let welcome = orion_msg_session::Welcome {
            node: "board".parse().expect("name"),
            session: 1,
            heartbeat_ms: 2000,
            max_frame: 4096,
            revision: 1,
        };
        let (mut frame, mut wire) = ([0_u8; 128], [0_u8; 160]);
        let len = encode_body(&welcome, 1, &mut frame).expect("encode");
        let len = encode_frame(&frame[..len], &mut wire).expect("cobs");
        theirs.write_all(&wire[..len]).expect("write");
        theirs
    });
    let session = Session::over_link(ours.into(), "tool", Options::default()).expect("session");
    let _kept = answering.join().expect("answered");
    let client = Client::new(session);
    let side = Buffers::new(&client, &Topics::new(&client));
    let pool = Pool::in_memory(1, 2, 4096).expect("pool");
    assert!(
        matches!(side.declare(TOPIC, 8, 8, pool), Err(TopicError::Invalid(why)) if why.contains("one board"))
    );
    let limits = HoldLimits::default();
    assert!(matches!(
        side.subscribe(TOPIC, &[], limits),
        Err(TopicError::Invalid(_))
    ));
}
