//! Publish and subscribe with the same calls, on the board and across the network.

use std::path::PathBuf;
use std::rc::Rc;
use std::sync::mpsc;
use std::time::{Duration, Instant};

use orion_broker::{Broker, BrokerConfig};
use orion_broker_proxy::BrokerProxy;
use orion_connect::{Address, Client, ClientError, Options, Session};
use orion_keys::{Keys, KeysConfig, SharedKeys};
use orion_measure::{CountingAlloc, thread_counts};
use orion_msg_topics::{Declare, Field, FieldType};
use orion_ring::{FieldId, Next, SampleTime};
use orion_ring_time::Lookup;
use orion_server::{Admits, Comm, Limits, Policy, Security};
use orion_tcp::{TcpCarrier, TcpOptions};
use orion_topics::{Status, TopicDecl, TopicError, Topics};
use orion_types::Error;
use orion_unix::UnixCarrier;
use orion_wire::Decode;

#[global_allocator]
static GLOBAL: CountingAlloc = CountingAlloc::system();

const FIELDS: [Field<'static>; 2] = [
    Field {
        name: "angle",
        ty: FieldType::Float,
        unit: "rad",
        type_name: "",
    },
    Field {
        name: "state",
        ty: FieldType::Int,
        unit: "",
        type_name: "",
    },
];
const ANGLE: FieldId = FieldId(0);
const STATE: FieldId = FieldId(1);

/// A comm core in its own thread, with the topic broker, on a socket and on the network.
struct Board {
    socket: PathBuf,
    network: std::net::SocketAddr,
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    thread: Option<std::thread::JoinHandle<()>>,
    dir: PathBuf,
}

impl Board {
    fn start(name: &str) -> Self {
        let dir = std::env::temp_dir().join(format!("orion-topics-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("dir");
        let socket = dir.join("orion.sock");
        let (ready, started) = mpsc::channel();
        let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
        let path = socket.clone();
        let thread = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("runtime");
            tokio::task::LocalSet::new().block_on(&runtime, async move {
                let keys = SharedKeys::new(
                    Keys::open(KeysConfig::in_memory("lab"), Security::Off).expect("keys"),
                );
                let node: orion_types::NodeName = "board".parse().expect("name");
                let comm = Comm::new(
                    node.clone(),
                    Security::Off,
                    Some(Rc::new(keys.clone())),
                    Limits::default(),
                );
                let anyone = Policy::new(Admits::ANYONE).without_grant();
                let config = BrokerConfig {
                    declare: anyone,
                    subscribe: anyone,
                    ..BrokerConfig::default()
                };
                let broker = Broker::new(node, config);
                comm.add_service(BrokerProxy::new(&broker, anyone));
                comm.add_service(broker);
                let unix = UnixCarrier::bind(&path, Some(0o600), &[]).expect("socket");
                let options = TcpOptions {
                    security: Security::Off,
                    keys,
                    max_frame: 64 * 1024,
                    handshake: Duration::from_secs(5),
                };
                let tcp = TcpCarrier::bind("127.0.0.1:0".parse().expect("addr"), options)
                    .await
                    .expect("listen");
                ready.send(tcp.local_addr().expect("addr")).expect("ready");
                let (on_socket, on_network) = (comm.clone(), comm);
                tokio::task::spawn_local(async move { on_socket.serve(unix).await });
                tokio::task::spawn_local(async move { on_network.serve(tcp).await });
                let _ = stopped.await;
            });
        });
        Self {
            socket,
            network: started.recv().expect("the board starts"),
            stop: Some(stop),
            thread: Some(thread),
            dir,
        }
    }

    fn local(&self) -> Address {
        Address::Local(self.socket.clone())
    }

    fn network(&self) -> Address {
        format!("orion+tcp://{}", self.network)
            .parse()
            .expect("address")
    }
}

impl Drop for Board {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn topics(address: &Address, name: &str) -> Topics {
    let session = Session::connect(address, name, Options::default()).expect("connect");
    Topics::new(&Client::new(session))
}

fn decl(name: &str) -> TopicDecl<'_> {
    TopicDecl {
        name,
        fields: &FIELDS,
        history: 64,
        expiry_ms: 0,
        max_payload: 16,
    }
}

fn angle(sample: orion_ring::Sample<'_>) -> (f64, u64) {
    (
        f64::from_le_bytes(sample.payload.try_into().expect("eight bytes")),
        sample.time.as_nanos(),
    )
}

/// The test a publisher runs wherever it is: `publishes` is on the board or across the
/// network, and a program on the board reads what it publishes.
fn publish_and_read(board: &Board, publishes: &Address, topic: &str) {
    let publisher_side = topics(publishes, "sensor");
    let reader_side = topics(&board.local(), "dashboard");

    // A subscriber may come first. It waits, and nothing is wrong.
    let mut subscriber = reader_side
        .subscribe(topic, b"fast please")
        .expect("subscribe");
    assert_eq!(subscriber.status(), Status::Waiting);
    assert!(matches!(subscriber.next(), Next::Empty));
    assert!(subscriber.latest(ANGLE).is_none());
    assert!(!subscriber.wait(Duration::from_millis(20)).expect("wait"));

    let mut publisher = publisher_side.declare(&decl(topic)).expect("declare");
    // The publisher hears who is listening and what they asked for.
    // The count is in the ring at once; what the subscriber asked for comes as a message.
    let before = Instant::now();
    while publisher.subscribers() == 0 || publisher.with_request(<[u8]>::is_empty) {
        assert!(
            before.elapsed() < Duration::from_secs(2),
            "never heard of the subscriber"
        );
        publisher.poll().expect("poll");
        std::thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(publisher.subscribers(), 1);
    publisher.with_request(|request| assert_eq!(request, b"fast please"));

    // The subscriber is given the topic when it is declared, with its declaration.
    assert!(
        !subscriber.wait(Duration::from_millis(50)).expect("wait"),
        "nothing published yet"
    );
    assert_eq!(subscriber.status(), Status::Live);
    subscriber.with_declaration(|declaration| {
        let declare = Declare::decode(declaration.expect("declared")).expect("declaration");
        assert_eq!(
            (
                declare.topic.as_str(),
                declare.fields.len(),
                declare.history
            ),
            (topic, 2, 64)
        );
    });

    // Samples, each with the time it belongs to.
    for step in 1..=10_u64 {
        publisher
            .publish(
                ANGLE,
                SampleTime::from_nanos(step * 100),
                &(step as f64 * 0.5).to_le_bytes(),
            )
            .expect("publish");
    }
    publisher
        .publish(STATE, SampleTime::from_nanos(1000), &7_i64.to_le_bytes())
        .expect("publish");
    assert!(
        subscriber.wait(Duration::from_secs(2)).expect("wait"),
        "the samples arrive"
    );
    // Across the network they arrive as messages; give the last one time to land.
    let before = Instant::now();
    while subscriber.latest(STATE).is_none() && before.elapsed() < Duration::from_secs(2) {
        std::thread::sleep(Duration::from_millis(1));
    }

    // In the order they were published.
    let mut seen = Vec::new();
    loop {
        match subscriber.next() {
            Next::Sample(sample) if sample.field == ANGLE => seen.push(angle(sample)),
            Next::Sample(_) => {}
            Next::Empty => break,
            Next::Skipped(lost) => panic!("lost {lost}"),
        }
    }
    assert_eq!(seen.len(), 10);
    assert_eq!((seen[0], seen[9]), ((0.5, 100), (5.0, 1000)));
    // The newest of a field, whatever else was published since.
    assert_eq!(subscriber.latest(ANGLE).map(angle), Some((5.0, 1000)));
    assert_eq!(
        subscriber
            .latest(STATE)
            .map(|sample| sample.payload.to_vec()),
        Some(7_i64.to_le_bytes().to_vec())
    );
    // The value that belongs to a time: the newest at or before it.
    let Lookup::Hit(found) = subscriber.at_or_before(ANGLE, SampleTime::from_nanos(400)) else {
        panic!("no sample for 400");
    };
    assert_eq!(angle(found), (2.0, 400));
    let Lookup::Lagged { sample, lag_ns } =
        subscriber.at_or_before(ANGLE, SampleTime::from_nanos(450))
    else {
        panic!("no sample before 450");
    };
    assert_eq!((angle(sample), lag_ns), ((2.0, 400), 50));
    assert!(matches!(
        subscriber.at_or_before(ANGLE, SampleTime::from_nanos(50)),
        Lookup::Missing
    ));

    // A sample that is not the topic's is refused where it is made.
    let too_long = [0_u8; 17];
    match publisher.publish(ANGLE, SampleTime::from_nanos(1), &too_long) {
        Err(TopicError::Publish(_)) => {}
        // Across the network the comm core refuses it, and the refusal is heard later.
        Ok(()) => assert!(!matches!(publishes, Address::Local(_))),
        Err(other) => panic!("{other}"),
    }

    // Publishing allocates nothing, and neither does reading.
    for step in 0..200_u64 {
        publisher
            .publish(
                ANGLE,
                SampleTime::from_nanos(2000 + step),
                &1.0_f64.to_le_bytes(),
            )
            .expect("publish");
    }
    let before = thread_counts();
    for step in 0..10_000_u64 {
        publisher
            .publish(
                ANGLE,
                SampleTime::from_nanos(3000 + step),
                &(step as f64).to_le_bytes(),
            )
            .expect("publish");
    }
    let allocated = thread_counts().since(before);
    assert_eq!(allocated.allocs, 0, "{allocated:?} in 10,000 publishes");
    let deadline = Instant::now() + Duration::from_secs(5);
    while subscriber.latest(ANGLE).map(angle).map(|(value, _)| value) != Some(9999.0) {
        assert!(Instant::now() < deadline, "the last sample never arrived");
        std::thread::sleep(Duration::from_millis(1));
    }
    let before = thread_counts();
    let mut read = 0_u64;
    for _ in 0..10_000 {
        if subscriber.latest(ANGLE).is_some() {
            read += 1;
        }
        if matches!(subscriber.next(), Next::Sample(_) | Next::Skipped(_)) {
            read += 1;
        }
        if subscriber
            .at_or_before(ANGLE, SampleTime::from_nanos(20_000))
            .sample()
            .is_some()
        {
            read += 1;
        }
    }
    let allocated = thread_counts().since(before);
    assert_eq!(allocated.allocs, 0, "{allocated:?} in 30,000 reads");
    assert!(read >= 20_000, "{read}");

    // The publisher goes away: the subscriber is told, and keeps what was published.
    drop(publisher);
    drop(publisher_side);
    let deadline = Instant::now() + Duration::from_secs(2);
    while subscriber.status() != Status::PublisherGone {
        assert!(
            Instant::now() < deadline,
            "never told the publisher is gone"
        );
        let _ = subscriber.wait(Duration::from_millis(10)).expect("wait");
        while !matches!(subscriber.next(), Next::Empty) {}
    }
    assert_eq!(
        subscriber.latest(ANGLE).map(angle).map(|(value, _)| value),
        Some(9999.0)
    );
}

#[test]
fn a_publisher_on_the_board_and_one_across_the_network_use_the_same_calls() {
    let board = Board::start("same");
    publish_and_read(&board, &board.local(), "arm.joint");
    publish_and_read(&board, &board.network(), "arm.wrist");
}

#[test]
fn a_subscriber_sleeps_until_something_is_published() {
    let board = Board::start("sleep");
    let reader_side = topics(&board.local(), "dashboard");
    let mut subscriber = reader_side.subscribe("clock.tick", &[]).expect("subscribe");
    // Another program, in another thread, publishes a little later.
    let address = board.local();
    let publishing = std::thread::spawn(move || {
        let side = topics(&address, "clock");
        let mut publisher = side.declare(&decl("clock.tick")).expect("declare");
        for tick in 1..=5_u64 {
            std::thread::sleep(Duration::from_millis(60));
            publisher.poll().expect("poll");
            publisher
                .publish(
                    ANGLE,
                    SampleTime::from_nanos(tick),
                    &(tick as f64).to_le_bytes(),
                )
                .expect("publish");
        }
        // Stay until the subscriber has read them.
        std::thread::sleep(Duration::from_millis(300));
    });
    let mut woken = Vec::new();
    let started = Instant::now();
    while woken.len() < 5 {
        assert!(started.elapsed() < Duration::from_secs(5), "{woken:?}");
        // The wait is long; the wake is what ends it.
        let waited = Instant::now();
        if subscriber.wait(Duration::from_secs(3)).expect("wait") {
            assert!(
                waited.elapsed() < Duration::from_secs(1),
                "woken by the clock, not the doorbell"
            );
            while let Next::Sample(sample) = subscriber.next() {
                woken.push(angle(sample).0);
            }
        }
    }
    assert_eq!(woken, [1.0, 2.0, 3.0, 4.0, 5.0]);
    // The session was kept all the while, and a wait with nothing to read ends on time.
    let waited = Instant::now();
    assert!(!subscriber.wait(Duration::from_millis(100)).expect("wait"));
    assert!(
        waited.elapsed() >= Duration::from_millis(100)
            && waited.elapsed() < Duration::from_millis(400)
    );
    publishing.join().expect("publisher");
}

#[test]
fn what_the_comm_core_refuses_is_an_error_where_it_was_asked() {
    let board = Board::start("refused");
    let first = topics(&board.local(), "one");
    let second = topics(&board.local(), "two");
    let _publisher = first.declare(&decl("only.one")).expect("declare");
    // A topic has one publisher.
    let refused = second.declare(&decl("only.one")).expect_err("taken");
    assert!(
        matches!(
            refused,
            TopicError::Client(ClientError::Refused(Error::AlreadyExists))
        ),
        "{refused}"
    );
    // The session is fine afterwards.
    let _other = second.declare(&decl("another.one")).expect("declare");
    // Names that are not names never leave the program.
    assert!(matches!(
        first.declare(&decl("not a name")),
        Err(TopicError::Invalid(_))
    ));
    assert!(matches!(
        first.subscribe("not a name", &[]),
        Err(TopicError::Invalid(_))
    ));

    // A subscriber across the network is answered like any other: it has no ring to share,
    // so it is given the declaration and keeps a ring of its own.
    let remote = topics(&board.network(), "remote");
    let mut subscriber = remote.subscribe("only.one", &[]).expect("subscribe");
    assert_eq!(subscriber.status(), Status::Live);
    subscriber.with_declaration(|declaration| assert!(declaration.is_some()));
    assert!(matches!(subscriber.next(), Next::Empty));
}
