//! A job that ended with nothing to show, and a subscriber cut off by hand: an entry that names
//! no buffer reaches a subscriber as its own arm, never as a frame; a subscriber cut off gives
//! its buffers back at once, and a read it was in the middle of is dropped, not taken as good.

#![cfg(target_os = "linux")]

use std::sync::mpsc;
use std::time::{Duration, Instant};

use orion_broker::{Broker, BrokerConfig};
use orion_broker_buffers::BrokerBuffers;
use orion_buffers::{
    BufferPublisher, BufferSubscriber, Buffers, HoldLimits, HoldTable, Marks, NextBatch, Pool, Torn,
};
use orion_connect::{Address, Client, Options, Session};
use orion_server::{Admits, Comm, Limits, Policy, Security};
use orion_topics::Topics;
use orion_unix::UnixCarrier;
use shm_ring::{FieldId, Ring, SampleTime, Spec};
use shm_ring_buffers::BufferWriter;
use shm_ring_holds::Limits as HoldTableLimits;

const TOPIC: &str = "camera.front";
const FRAME: usize = 4096;
const WAIT: Duration = Duration::from_secs(10);

fn buffers(socket: &std::path::Path, name: &str) -> Buffers {
    let session = Session::connect(&Address::Local(socket.to_owned()), name, Options::default())
        .expect("connect");
    let client = Client::new(session);
    Buffers::new(&client, &Topics::new(&client))
}

/// A comm core with the broker and buffer topics, in a thread of its own, on a socket of its
/// own under the temporary directory.
fn comm_core(name: &str) -> (std::path::PathBuf, tokio::sync::oneshot::Sender<()>) {
    let dir = std::env::temp_dir().join(format!("orion-ended-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("dir");
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
            tokio::task::spawn_local(async move { comm.serve(unix).await });
            ready.send(()).expect("ready");
            let _ = stopped.await;
        });
    });
    started.recv().expect("the comm core starts");
    (socket, stop)
}

/// A publisher of `count` buffers and one subscriber of it, the subscriber placed and handed
/// the pool.
fn pair(socket: &std::path::Path, count: u32) -> (BufferPublisher, BufferSubscriber, Buffers) {
    let camera = buffers(socket, "camera");
    let pool = Pool::in_memory(1, count, FRAME).expect("pool");
    let mut publisher = camera.declare(TOPIC, 8, 16, pool).expect("declare");
    let viewer = buffers(socket, "viewer");
    let limits = HoldLimits {
        max_held: count,
        max_hold_ms: 0,
    };
    let mut subscriber = viewer.subscribe(TOPIC, &[], limits).expect("subscribe");
    let since = Instant::now();
    while publisher.subscribers() == 0 || subscriber.pool().is_none() {
        assert!(since.elapsed() < WAIT, "the subscriber was never placed");
        publisher.poll().expect("poll");
        let _ = subscriber.wait(Duration::from_millis(10));
    }
    (publisher, subscriber, viewer)
}

/// Publishes a frame filled with `fill`, in whichever buffer is free; its buffer.
fn frame(publisher: &mut BufferPublisher, fill: u8, time: u64) -> orion_buffers::BufferIndex {
    let buffer = publisher.take().expect("a free buffer");
    publisher.buffer_mut(buffer).expect("memory").fill(fill);
    publisher
        .publish(&[buffer], SampleTime::from_nanos(time), &[fill])
        .expect("publish");
    buffer
}

/// What the subscriber is handed next, waiting for it: frame (its fill), nothing (its time and
/// meta), or cut off.
#[derive(Debug, PartialEq)]
enum Seen {
    Frame(u8),
    Nothing(u64, Vec<u8>),
    CutOff,
}

fn seen(subscriber: &mut BufferSubscriber) -> Seen {
    let since = Instant::now();
    loop {
        assert!(since.elapsed() < WAIT, "the subscriber was handed nothing");
        match subscriber.next() {
            NextBatch::Frame(frame) => {
                let fill = frame
                    .held
                    .read(0, |bytes| bytes[0])
                    .expect("mapped")
                    .expect("a held frame is never reused");
                return Seen::Frame(fill);
            }
            NextBatch::NoBuffers(entry) => {
                return Seen::Nothing(entry.time.as_nanos(), entry.meta.to_vec());
            }
            NextBatch::CutOff => return Seen::CutOff,
            _ => {
                let _ = subscriber.wait(Duration::from_millis(10));
            }
        }
    }
}

#[test]
fn a_job_that_ended_with_nothing_to_show_reaches_a_subscriber_as_its_own_arm() {
    let (socket, _stop) = comm_core("nothing");
    let (mut publisher, mut subscriber, _viewer) = pair(&socket, 2);

    frame(&mut publisher, 1, 10);
    // A job that timed out: no buffer, only when and why, in the publisher's own words.
    publisher
        .publish_empty(SampleTime::from_nanos(20), &[2, 0, 0, 0])
        .expect("publish nothing");
    frame(&mut publisher, 3, 30);

    assert_eq!(seen(&mut subscriber), Seen::Frame(1));
    assert_eq!(seen(&mut subscriber), Seen::Nothing(20, vec![2, 0, 0, 0]));
    assert_eq!(seen(&mut subscriber), Seen::Frame(3));
    // Nothing was taken from the pool for it: both buffers are free again.
    assert_eq!(subscriber.held(), 0);
    let (a, b) = (publisher.take(), publisher.take());
    assert!(a.is_ok() && b.is_ok(), "{a:?} {b:?}");
}

#[test]
fn a_subscriber_cut_off_by_hand_gives_its_buffer_back_and_a_late_read_is_dropped() {
    let (socket, _stop) = comm_core("cut");
    let (mut publisher, mut subscriber, _viewer) = pair(&socket, 2);

    let buffer = frame(&mut publisher, 1, 10);
    let held = loop {
        if let NextBatch::Frame(frame) = subscriber.next() {
            break frame.held;
        }
        let _ = subscriber.wait(Duration::from_millis(10));
    };
    // The other buffer carries the newest frame, which the publisher keeps; the first is held:
    // the publisher has nowhere to put a frame.
    let newest = frame(&mut publisher, 2, 20);
    assert_ne!(newest, buffer);
    assert!(publisher.take().is_err(), "the held buffer was taken");
    let holders = publisher.holders_of(buffer);
    assert_eq!(holders.len(), 1, "one subscriber holds it");

    assert!(publisher.cut_off(holders[0]), "cut off");
    assert!(!publisher.cut_off(holders[0]), "once is enough");
    assert!(
        publisher.holders_of(buffer).is_empty(),
        "a cut-off reader holds nothing"
    );
    // The buffer is the publisher's again, at once, while the subscriber still has it in hand.
    let again = frame(&mut publisher, 3, 30);
    assert_eq!(again, buffer);
    // Read now, the bytes are not the frame the subscriber took: the value is dropped.
    assert_eq!(held.read(0, |bytes| bytes[0]), Some(Err(Torn { buffer })));
    drop(held);
    assert_eq!(seen(&mut subscriber), Seen::CutOff);
}

#[test]
fn an_adopted_topic_cuts_off_only_orions_own_subscribers() {
    let (socket, _stop) = comm_core("adopted");
    let ring = Ring::create(&Spec {
        fields: 1,
        history: 8,
        max_payload: 72 + 8,
        expiry_ms: 0,
    })
    .expect("ring");
    let pool = Pool::in_memory(1, 2, FRAME).expect("pool");
    let mut writer = BufferWriter::new(ring.writer().expect("writer"), FieldId(0), pool);
    let mut table = HoldTable::new(2);
    let own = Marks::create(2, 0, 2).expect("marks");
    let own_limits = HoldTableLimits {
        max_held: 2,
        max_hold: None,
    };
    table
        .add(0, own.page_fd().expect("page"), own_limits)
        .expect("own reader");
    let camera = buffers(&socket, "camera");
    let mut adopted = camera
        .adopt(TOPIC, 8, 8, &ring, writer.pool(), !1)
        .expect("adopt");
    let viewer = buffers(&socket, "viewer");
    let limits = HoldLimits {
        max_held: 2,
        max_hold_ms: 0,
    };
    let mut subscriber = viewer.subscribe(TOPIC, &[], limits).expect("subscribe");
    let since = Instant::now();
    while adopted.subscribers() == 0 || subscriber.pool().is_none() {
        assert!(since.elapsed() < WAIT, "the subscriber was never placed");
        adopted.poll(writer.pool(), &mut table).expect("poll");
        let _ = subscriber.wait(Duration::from_millis(10));
    }

    // The program's job ends with nothing to show: Orion's subscriber gets it as it is.
    writer
        .publish_empty(SampleTime::from_nanos(5), &[7])
        .expect("publish nothing");
    adopted.wake();
    assert_eq!(seen(&mut subscriber), Seen::Nothing(5, vec![7]));

    // Slot 0 is the program's own reader: never Orion's to cut off.
    assert!(!adopted.cut_off(0, writer.pool(), &mut table));
    assert!(!table.is_cut_off(0));
    let orions = (1..64)
        .find(|slot| adopted.cut_off(*slot, writer.pool(), &mut table))
        .expect("Orion's subscriber is in one of Orion's slots");
    assert!(table.is_cut_off(orions));
    assert_eq!(seen(&mut subscriber), Seen::CutOff);
}
