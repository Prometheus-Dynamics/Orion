//! A program that made its own ring, pool and hold table, and serves readers of its own, makes
//! them a buffer topic: Orion's subscribers read the frames it writes with its own writer, in
//! the same memory, from reader slots it left for Orion.

use std::sync::mpsc;
use std::time::{Duration, Instant};

use orion_broker::{Broker, BrokerConfig};
use orion_broker_buffers::BrokerBuffers;
use orion_buffers::{Buffers, HoldLimits, Marks, NextBatch, Pool};
use orion_connect::{Address, Client, ClientError, Options, Session};
use orion_server::{Admits, Comm, Limits, Policy, Security};
use orion_topics::{TopicError, Topics};
use orion_types::Error;
use orion_unix::UnixCarrier;
use shm_ring::{FieldId, Ring, SampleTime, Spec};
use shm_ring_buffers::BufferWriter;
use shm_ring_holds::{HoldTable, Limits as HoldTableLimits};

const TOPIC: &str = "camera.side";
const FRAME: usize = 64 * 1024;
const BUFFERS: u32 = 4;
/// The declaration's sample size: a ring entry's own bytes and eight of the program's.
const MAX_PAYLOAD: u32 = 72 + 8;

fn buffers(socket: &std::path::Path, name: &str) -> Buffers {
    let session = Session::connect(&Address::Local(socket.to_owned()), name, Options::default())
        .expect("connect");
    let client = Client::new(session);
    Buffers::new(&client, &Topics::new(&client))
}

/// A comm core with the broker and buffer topics, in a thread of its own.
fn comm_core(socket: std::path::PathBuf) -> tokio::sync::oneshot::Sender<()> {
    let (ready, started) = mpsc::channel();
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
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
            let unix = UnixCarrier::bind(&socket, Some(0o600), &[]).expect("socket");
            tokio::task::spawn_local(async move { comm.serve(unix).await });
            ready.send(()).expect("ready");
            let _ = stopped.await;
        });
    });
    started.recv().expect("the comm core starts");
    stop
}

#[test]
fn a_program_s_own_ring_and_pool_become_a_buffer_topic() {
    let dir = std::env::temp_dir().join(format!("orion-adopt-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("dir");
    let socket = dir.join("orion.sock");
    let _stop = comm_core(socket.clone());

    // The program's own objects, made before Orion is asked anything.
    let spec = Spec {
        fields: 1,
        history: 8,
        max_payload: MAX_PAYLOAD,
        expiry_ms: 0,
    };
    let ring = Ring::create(&spec).expect("ring");
    let pool = Pool::in_memory(1, BUFFERS, FRAME).expect("pool");
    let mut writer = BufferWriter::new(ring.writer().expect("writer"), FieldId(0), pool);
    let mut table = HoldTable::new(BUFFERS);
    // A reader of its own, outside Orion, in slot 0.
    let own = Marks::create(BUFFERS, 0, 2).expect("marks");
    let own_limits = HoldTableLimits {
        max_held: 2,
        max_hold: None,
    };
    table
        .add(0, own.page_fd().expect("page"), own_limits)
        .expect("own reader");

    let side = buffers(&socket, "camera");
    // A ring of another shape than the declaration is refused.
    let wrong = side.adopt(TOPIC, 4, 8, &ring, writer.pool(), !1);
    assert!(
        matches!(
            wrong,
            Err(TopicError::Client(ClientError::Refused(Error::Malformed)))
        ),
        "{wrong:?}"
    );
    // Orion may use every slot but 0.
    let mut adopted = side
        .adopt(TOPIC, 8, 8, &ring, writer.pool(), !1)
        .expect("adopt");

    let viewer = buffers(&socket, "viewer");
    let limits = HoldLimits {
        max_held: 2,
        max_hold_ms: 0,
    };
    let mut frames = viewer.subscribe(TOPIC, b"720p", limits).expect("subscribe");

    let started = Instant::now();
    let (mut number, mut seen) = (0_u64, Vec::new());
    while seen.len() < 20 {
        assert!(started.elapsed() < Duration::from_secs(10), "saw {seen:?}");
        // Orion's subscriber goes in the program's table; slot 0 is never offered to it.
        adopted.poll(writer.pool(), &mut table).expect("poll");
        if adopted.subscribers() > 0 && table.readers() == 2 {
            number += 1;
            if let Ok(buffer) = writer.take(&table) {
                if let Some(bytes) = writer.buffer_mut(buffer) {
                    bytes[..8].copy_from_slice(&number.to_le_bytes());
                }
                writer
                    .publish(
                        &[buffer],
                        SampleTime::from_nanos(number),
                        &number.to_le_bytes(),
                    )
                    .expect("publish");
                adopted.wake();
            }
        }
        let _ = frames.wait(Duration::from_millis(5)).expect("wait");
        loop {
            match frames.next() {
                NextBatch::Batch(frame) => {
                    let meta = u64::from_le_bytes(frame.meta.try_into().expect("number"));
                    let bytes = frame.held.bytes(0).expect("mapped");
                    assert_eq!(bytes[..8], meta.to_le_bytes(), "the program's own bytes");
                    seen.push(meta);
                }
                // Frames gone before this reader got to them: read on to the ones still
                // there. Stopping here leaves a reader one behind for good, as the publisher
                // takes back each buffer before it is read.
                NextBatch::Skipped(_) => {}
                _ => break,
            }
        }
    }
    adopted.with_request(|request| assert_eq!(request, b"720p"));
    // The subscriber has the pool as it was offered: every buffer, where it is, and a
    // descriptor to map or pass on.
    let offered = writer.pool().buffers().to_vec();
    let view = frames.pool().expect("the pool");
    assert_eq!(view.len() as usize, offered.len());
    for (index, desc) in offered.iter().enumerate() {
        let (_fd, seen) = view
            .descriptor(orion_buffers::BufferIndex(index as u32))
            .expect("a buffer");
        assert_eq!(seen, *desc);
    }
    assert!(seen.windows(2).all(|pair| pair[0] < pair[1]), "{seen:?}");
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn a_program_s_own_value_ring_becomes_a_topic() {
    use orion_msg_topics::{Field, FieldType};
    use orion_topics::TopicDecl;
    use shm_ring::{Flags, Next};

    let dir = std::env::temp_dir().join(format!("orion-adopt-value-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("dir");
    let socket = dir.join("orion.sock");
    let _stop = comm_core(socket.clone());

    let spec = Spec {
        fields: 1,
        history: 16,
        max_payload: 8,
        expiry_ms: 0,
    };
    let ring = Ring::create(&spec).expect("ring");
    let mut writer = ring.writer().expect("writer");
    let fields = [Field {
        name: "count",
        ty: FieldType::Int,
        unit: "",
        type_name: "",
    }];
    let decl = TopicDecl {
        name: "counter.own",
        fields: &fields,
        history: 16,
        expiry_ms: 0,
        max_payload: 8,
        on_subscribe: Default::default(),
    };
    let connect = |name: &str| {
        let session = Session::connect(&Address::Local(socket.clone()), name, Options::default())
            .expect("connect");
        Topics::new(&Client::new(session))
    };
    let counter = connect("counter");
    let mut publisher = counter.adopt(&decl, &ring).expect("adopt");
    let mut reader = connect("reader")
        .subscribe("counter.own", &[])
        .expect("subscribe");

    let started = Instant::now();
    let mut seen = Vec::new();
    let mut count = 0_u64;
    while seen.len() < 10 {
        assert!(started.elapsed() < Duration::from_secs(10), "saw {seen:?}");
        publisher.poll().expect("poll");
        if publisher.subscribers() > 0 {
            count += 1;
            // The program's own writer: Orion makes no second one.
            writer
                .publish(
                    FieldId(0),
                    SampleTime::from_nanos(count),
                    Flags(0),
                    &(count as i64).to_le_bytes(),
                )
                .expect("publish");
            publisher.wake();
        }
        let _ = reader.wait(Duration::from_millis(5)).expect("wait");
        while let Next::Sample(sample) = reader.next() {
            seen.push(i64::from_le_bytes(
                sample.payload.try_into().expect("eight bytes"),
            ));
        }
    }
    assert!(seen.windows(2).all(|pair| pair[0] < pair[1]), "{seen:?}");

    // The same declaration with a ring of another shape is refused, not taken for the ring
    // that is there.
    let other = Ring::create(&Spec {
        history: 32,
        ..spec
    })
    .expect("ring");
    let refused = counter.adopt(&decl, &other).expect_err("another shape");
    assert!(
        matches!(
            refused,
            orion_topics::TopicError::Client(orion_connect::ClientError::Refused(
                orion_types::Error::Malformed
            ))
        ),
        "{refused}"
    );
    // The same ring again is the topic carrying on.
    let _again = counter.adopt(&decl, &ring).expect("the same ring");
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn orion_never_takes_a_slot_that_is_the_programs_own() {
    let dir = std::env::temp_dir().join(format!("orion-adopt-slots-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("dir");
    let socket = dir.join("orion.sock");
    let _stop = comm_core(socket.clone());

    let spec = Spec {
        fields: 1,
        history: 8,
        max_payload: MAX_PAYLOAD,
        expiry_ms: 0,
    };
    let ring = Ring::create(&spec).expect("ring");
    let pool = Pool::in_memory(1, BUFFERS, FRAME).expect("pool");
    let writer = BufferWriter::new(ring.writer().expect("writer"), FieldId(0), pool);
    let side = buffers(&socket, "camera");

    // An adopted ring has readers Orion did not place: Orion has to be told which slots are
    // its own. "Any" is refused.
    let any = side.adopt(TOPIC, 8, 8, &ring, writer.pool(), 0);
    assert!(matches!(any, Err(TopicError::Invalid(_))), "{any:?}");

    // The program says slot 1 is Orion's, and then puts a reader of its own there anyway.
    let mut table = HoldTable::new(BUFFERS);
    let own = Marks::create(BUFFERS, 1, 2).expect("marks");
    let limits = HoldTableLimits {
        max_held: 2,
        max_hold: None,
    };
    table
        .add(1, own.page_fd().expect("page"), limits)
        .expect("own reader");
    let mut adopted = side
        .adopt(TOPIC, 8, 8, &ring, writer.pool(), 0b10)
        .expect("adopt");
    let viewer = buffers(&socket, "viewer");
    let held = HoldLimits {
        max_held: 2,
        max_hold_ms: 0,
    };
    let mut frames = viewer.subscribe(TOPIC, b"", held).expect("subscribe");

    // Orion's subscriber is given slot 1, which is taken: the program hears it, its own
    // reader is left where it was, and the subscriber is not handed the pool.
    let deadline = Instant::now() + Duration::from_secs(5);
    let refused = loop {
        match adopted.poll(writer.pool(), &mut table) {
            Err(error) => break error,
            Ok(()) => {
                assert!(Instant::now() < deadline, "the clash was never said");
                let _ = frames.wait(Duration::from_millis(5)).expect("wait");
            }
        }
    };
    assert!(matches!(refused, TopicError::Ring(_)), "{refused:?}");
    assert_eq!(
        table.readers(),
        1,
        "the program's own reader is still there"
    );
    for _ in 0..20 {
        adopted.poll(writer.pool(), &mut table).expect("poll");
        let _ = frames.wait(Duration::from_millis(5)).expect("wait");
    }
    assert!(frames.pool().is_none(), "never handed the pool");
    assert_eq!(table.readers(), 1);
    let _ = std::fs::remove_dir_all(&dir);
}
