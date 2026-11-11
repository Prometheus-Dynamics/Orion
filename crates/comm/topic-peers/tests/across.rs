//! Two comm cores: a program on one subscribes to a topic of the other with the code it
//! would use on its own board, only while it asks, through the other node restarting, and
//! with frame times in its own board's time.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::{Arc, Mutex, mpsc};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use orion::topics::{FieldId, FrameTime, Next, Publisher, Status, Subscriber, TopicDecl};
use orion::{Address, Orion, fields};
use orion_broker::{Broker, BrokerConfig};
use orion_broker_proxy::BrokerProxy;
use orion_keys::{Keys, KeysConfig, SharedKeys};
use orion_outbound::{Outbound, OutboundLimits};
use orion_server::{Admits, Comm, Limits, Policy, Security};
use orion_tcp::{TcpCarrier, TcpOptions};
use orion_topic_peers::{Reach, Resolve, TopicPeers};
use orion_types::NodeName;
use orion_unix::UnixCarrier;

/// Where each node is, filled in as they start.
#[derive(Clone, Default)]
struct Map(Arc<Mutex<BTreeMap<NodeName, SocketAddr>>>);

impl Resolve for Map {
    fn reach(&self, node: &NodeName) -> Option<Reach> {
        self.0.lock().expect("lock").get(node).map(|addr| Reach {
            addr: *addr,
            key: None,
        })
    }
}

fn wall_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("after 1970")
        .as_nanos() as u64
}

/// A comm core in its own thread: the broker with everything a topic needs to leave the
/// board and to arrive on one.
struct Node {
    socket: PathBuf,
    network: SocketAddr,
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Node {
    /// Starts the node `name`, whose clock is `ahead_ns` ahead of the machine's, on `port`
    /// if it is to come back where it was.
    fn start(test: &str, name: &'static str, map: &Map, ahead_ns: u64, port: Option<u16>) -> Self {
        let dir = std::env::temp_dir().join(format!(
            "orion-topic-peers-{test}-{name}-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("dir");
        let socket = dir.join("orion.sock");
        let (ready, started) = mpsc::channel();
        let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
        let (path, resolve) = (socket.clone(), map.clone());
        let thread = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("runtime");
            tokio::task::LocalSet::new().block_on(&runtime, async move {
                let keys = SharedKeys::new(
                    Keys::open(KeysConfig::in_memory("lab"), Security::Off).expect("keys"),
                );
                let node: NodeName = name.parse().expect("name");
                let comm = Comm::new(
                    node.clone(),
                    Security::Off,
                    Some(Rc::new(keys.clone())),
                    Limits::default(),
                );
                let anyone = Policy::new(Admits::ANYONE).without_grant();
                let broker = Broker::new(
                    node,
                    BrokerConfig {
                        declare: anyone,
                        subscribe: anyone,
                        ..BrokerConfig::default()
                    },
                );
                let options = TcpOptions {
                    security: Security::Off,
                    keys,
                    max_frame: 64 * 1024,
                    handshake: Duration::from_secs(5),
                };
                // Out: samples for subscribers without a ring, which another comm core is.
                let _outbound =
                    Outbound::start(&comm, &broker, OutboundLimits::default()).expect("outbound");
                // In: other nodes' topics, into local copies of their rings.
                let _peers = TopicPeers::with_clock(
                    &comm,
                    &broker.topics(),
                    options.clone(),
                    resolve,
                    move || wall_ns() + ahead_ns,
                );
                // The clock is answered for: that is what a link compares with.
                let _time = orion_time::TimeSync::start(
                    &comm,
                    None,
                    orion_time::TimeConfig::default(),
                    None,
                )
                .expect("time");
                comm.add_service(BrokerProxy::new(&broker, anyone));
                comm.add_service(broker);
                let unix = UnixCarrier::bind(&path, Some(0o600), &[]).expect("socket");
                let listen = SocketAddr::from(([127, 0, 0, 1], port.unwrap_or(0)));
                let tcp = TcpCarrier::bind(listen, options).await.expect("listen");
                ready.send(tcp.local_addr().expect("addr")).expect("ready");
                let (on_socket, on_network) = (comm.clone(), comm);
                tokio::task::spawn_local(async move { on_socket.serve(unix).await });
                tokio::task::spawn_local(async move { on_network.serve(tcp).await });
                let _ = stopped.await;
            });
        });
        let network = started.recv().expect("the node starts");
        map.0
            .lock()
            .expect("lock")
            .insert(name.parse().expect("name"), network);
        Self {
            socket,
            network,
            stop: Some(stop),
            thread: Some(thread),
        }
    }

    fn connect(&self, name: &str) -> Orion {
        Orion::connect(&Address::Local(self.socket.clone()), name).expect("connect")
    }

    fn stop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        self.stop();
    }
}

fn level(sensor: &Orion, name: &str) -> Publisher {
    sensor
        .declare(&TopicDecl {
            name,
            fields: &fields![litres: Float "l"],
            history: 256,
            expiry_ms: 0,
            max_payload: 8,
        })
        .expect("declare")
}

fn publish(level: &mut Publisher, time: u64, litres: f64) {
    level
        .publish(
            FieldId(0),
            FrameTime::from_nanos(time),
            &litres.to_le_bytes(),
        )
        .expect("publish");
}

/// Everything a subscriber does, wherever the topic is: wait, and take samples in order.
fn read(subscriber: &mut Subscriber, want: usize) -> Vec<(u64, f64)> {
    let mut seen = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(10);
    while seen.len() < want {
        assert!(
            Instant::now() < deadline,
            "only {} of {want} samples",
            seen.len()
        );
        subscriber.wait(Duration::from_millis(50)).expect("wait");
        loop {
            match subscriber.next() {
                Next::Sample(sample) => seen.push((
                    sample.time.as_nanos(),
                    f64::from_le_bytes(sample.payload.try_into().expect("a float")),
                )),
                Next::Empty => break,
                Next::Skipped(lost) => panic!("lost {lost}"),
            }
        }
    }
    seen
}

fn until(what: &str, mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !done() {
        assert!(Instant::now() < deadline, "{what}");
        std::thread::sleep(Duration::from_millis(5));
    }
}

fn subscribers(level: &mut Publisher) -> u32 {
    level.poll().expect("poll");
    level.subscribers()
}

#[test]
fn a_topic_of_another_node_is_read_with_the_code_for_one_of_this_board() {
    let map = Map::default();
    let left = Node::start("same", "left", &map, 0, None);
    let right = Node::start("same", "right", &map, 0, None);
    let sensor = right.connect("sensor");
    let mut level = level(&sensor, "tank.level");

    // One program on the board that has the topic, one on the other board. The only
    // difference between them is the name they ask for.
    let near = right.connect("panel");
    let far = left.connect("dashboard");
    let mut near_reading = near.subscribe("tank.level").expect("subscribe");
    let mut far_reading = far.subscribe("right/tank.level").expect("subscribe");
    // The far one is handed a ring like any other, once the link has the declaration.
    until("the far subscriber has the topic", || {
        far_reading.wait(Duration::from_millis(10)).expect("wait");
        far_reading.status() == Status::Live
    });
    until("the publisher knows of both", || {
        subscribers(&mut level) == 2
    });

    let expected: Vec<(u64, f64)> = (1..=200_u64)
        .map(|step| (step, step as f64 * 0.25))
        .collect();
    for (time, litres) in &expected {
        publish(&mut level, *time, *litres);
        if time % 25 == 0 {
            std::thread::sleep(Duration::from_millis(1));
        }
    }
    assert_eq!(read(&mut near_reading, 200), expected);
    // The same values with the same times: two boards whose clocks agree.
    assert_eq!(read(&mut far_reading, 200), expected);
    assert_eq!(
        far_reading
            .latest(FieldId(0))
            .expect("a latest value")
            .time
            .as_nanos(),
        200
    );

    // A second program on the far board shares the one link: the publisher sees no more
    // subscribers than before.
    let second = left.connect("logger");
    let mut second_reading = second.subscribe("right/tank.level").expect("subscribe");
    until("the second far subscriber has the topic", || {
        second_reading
            .wait(Duration::from_millis(10))
            .expect("wait");
        second_reading.status() == Status::Live
    });
    publish(&mut level, 301, 1.0);
    assert_eq!(read(&mut second_reading, 1), [(301, 1.0)]);
    assert_eq!(read(&mut far_reading, 1), [(301, 1.0)]);
    assert_eq!(subscribers(&mut level), 2);

    // Only while someone asks: when both have gone, the other node is no longer asked.
    drop(far_reading);
    drop(far);
    std::thread::sleep(Duration::from_millis(100));
    assert_eq!(subscribers(&mut level), 2, "one of them still asks");
    drop(second_reading);
    drop(second);
    until("the link is given up", || subscribers(&mut level) == 1);
    let _ = right.network;
}

#[test]
fn a_subscriber_may_come_before_the_topic_and_outlives_the_other_node_restarting() {
    let map = Map::default();
    let left = Node::start("restart", "left", &map, 0, None);
    let mut right = Node::start("restart", "right", &map, 0, None);
    let port = right.network.port();

    // Asked for before anyone has declared it: it waits, as it would on its own board.
    let far = left.connect("dashboard");
    let mut reading = far.subscribe("right/pump.speed").expect("subscribe");
    assert_eq!(reading.status(), Status::Waiting);
    std::thread::sleep(Duration::from_millis(200));
    assert_eq!(reading.status(), Status::Waiting);
    assert!(matches!(reading.next(), Next::Empty));

    let sensor = right.connect("sensor");
    let mut speed = level(&sensor, "pump.speed");
    until("the subscriber has the topic", || {
        reading.wait(Duration::from_millis(10)).expect("wait");
        reading.status() == Status::Live
    });
    until("the publisher knows of it", || subscribers(&mut speed) == 1);
    publish(&mut speed, 1, 10.0);
    publish(&mut speed, 2, 20.0);
    assert_eq!(read(&mut reading, 2), [(1, 10.0), (2, 20.0)]);

    // The other node goes away, comm core and all.
    drop(speed);
    drop(sensor);
    right.stop();
    until("the subscriber is told the publisher is gone", || {
        reading.wait(Duration::from_millis(10)).expect("wait");
        reading.status() == Status::PublisherGone
    });
    // What it had is still there to read.
    assert_eq!(
        reading.latest(FieldId(0)).expect("latest").time.as_nanos(),
        2
    );

    // It comes back where it was, and its publisher declares the topic again. The program
    // on this board does nothing: it is live again, in the ring it has.
    let right = Node::start("restart", "right", &map, 0, Some(port));
    let sensor = right.connect("sensor");
    let mut speed = level(&sensor, "pump.speed");
    until("the subscriber is live again", || {
        reading.wait(Duration::from_millis(10)).expect("wait");
        reading.status() == Status::Live
    });
    until("the publisher knows of it again", || {
        subscribers(&mut speed) == 1
    });
    publish(&mut speed, 3, 30.0);
    assert_eq!(read(&mut reading, 1), [(3, 30.0)]);
}

#[test]
fn frame_times_arrive_in_this_boards_time() {
    // The left board's clock is five seconds ahead of the right's: they are not synced.
    const AHEAD: u64 = 5_000_000_000;
    let map = Map::default();
    let left = Node::start("clock", "left", &map, AHEAD, None);
    let right = Node::start("clock", "right", &map, 0, None);
    let sensor = right.connect("sensor");
    let mut level = level(&sensor, "tank.level");
    let far = left.connect("dashboard");
    let mut reading = far.subscribe("right/tank.level").expect("subscribe");
    until("the far subscriber has the topic", || {
        reading.wait(Duration::from_millis(10)).expect("wait");
        reading.status() == Status::Live
    });
    until("the publisher knows of it", || subscribers(&mut level) == 1);
    // The clocks are compared as soon as the link is up; give the answer time to arrive.
    std::thread::sleep(Duration::from_millis(300));

    // A frame stamped on the right board with the right board's time.
    let stamped = wall_ns();
    publish(&mut level, stamped, 1.0);
    let (arrived, _) = read(&mut reading, 1)[0];
    // On the left board it carries the left board's time for the same moment.
    let moved = arrived as i128 - stamped as i128;
    let off_by = (moved - i128::from(AHEAD)).abs();
    assert!(
        off_by < 20_000_000,
        "moved by {moved} ns, {off_by} ns from the five seconds between the clocks"
    );
}

#[test]
fn a_node_nobody_knows_leaves_its_subscriber_waiting() {
    let map = Map::default();
    let left = Node::start("nowhere", "left", &map, 0, None);
    let far = left.connect("dashboard");
    let mut reading = far.subscribe("nowhere/tank.level").expect("subscribe");
    std::thread::sleep(Duration::from_millis(300));
    assert_eq!(reading.status(), Status::Waiting);
    assert!(matches!(reading.next(), Next::Empty));
    // And everything else on the board goes on.
    let sensor = left.connect("sensor");
    let mut own = level(&sensor, "own.level");
    let mut own_reading = far.subscribe("own.level").expect("subscribe");
    until("the publisher knows of it", || subscribers(&mut own) == 1);
    publish(&mut own, 1, 2.0);
    assert_eq!(read(&mut own_reading, 1), [(1, 2.0)]);
}
