//! Two processes exchange values through a topic, with the broker idle afterward.
//!
//! This test is the comm core: a server with the broker on a real Unix socket. It starts the
//! same test binary twice more, once as a publisher and once as a subscriber. They find each
//! other through the broker and then exchange 100,000 values, with wake-ups, without it.

mod common;

use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::time::Duration;

use common::{Client, Event, Wire};
use orion_broker::{Broker, BrokerConfig};
use orion_ring::{FieldId, Flags, FrameTime, Next};
use orion_ring_doorbell::{Bell, Doorbell, Wake};
use orion_server::{Comm, Limits, Security};
use orion_unix::{UnixCarrier, UnixConnection};

const ROLE: &str = "ORION_TEST_ROLE";
const SOCKET: &str = "ORION_TEST_SOCKET";
const VALUES: u64 = 100_000;

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime")
}

async fn connect(name: &str) -> Client {
    let path = PathBuf::from(std::env::var(SOCKET).expect("socket path"));
    let connection = UnixConnection::connect(&path).await.expect("connect");
    Client::open(Wire::Unix(connection), name).await
}

/// The publisher process: declares, waits for its subscriber, publishes.
async fn publisher() {
    let mut client = connect("publisher").await;
    let (handle, mut writer, _keep) = client.declare("counter", 256).await;
    let mut bell = loop {
        if let Event::Bell {
            handle: for_topic,
            present: true,
            mut fds,
            ..
        } = client.next().await
        {
            assert_eq!(for_topic, handle);
            let page = fds.pop().expect("page");
            break Bell::attach(fds.pop().expect("eventfd"), page).expect("bell");
        }
    };
    assert_eq!(writer.subscribers(), 1);
    for value in 1..=VALUES {
        writer
            .publish(
                FieldId(0),
                FrameTime::from_nanos(value),
                Flags::NONE,
                &value.to_le_bytes(),
            )
            .expect("publish");
        bell.ring();
        // A pause every half history, so the subscriber is woken many times and is not
        // simply overrun.
        if value % 128 == 0 {
            std::thread::sleep(Duration::from_micros(200));
        }
    }
    // Stay until the subscriber has had time to read the last ones.
    std::thread::sleep(Duration::from_millis(300));
}

/// The subscriber process: subscribes, then reads until it has seen the last value.
async fn subscriber() {
    let mut client = connect("subscriber").await;
    let mut doorbell = Doorbell::create().expect("doorbell");
    let seq = client.subscribe("counter", Some(&doorbell), b"").await;
    let mut reader = loop {
        match client.next().await {
            Event::State { .. } => {}
            Event::Catalog(_) => {}
            Event::Opened {
                request_seq,
                mut fds,
                ..
            } if request_seq == seq => {
                break orion_ring::Reader::attach(fds.pop().expect("read end")).expect("reader");
            }
            other => panic!("{other:?}"),
        }
    };
    let (mut last, mut seen, mut woken) = (0_u64, 0_u64, 0_u64);
    let first = std::cell::Cell::new(0_u64);
    while last < VALUES {
        let mut take = |last: &mut u64, seen: &mut u64| loop {
            match reader.next() {
                Next::Sample(sample) => {
                    let value = u64::from_le_bytes(sample.payload.try_into().expect("eight bytes"));
                    assert!(value > *last, "values arrive in order");
                    assert_eq!(sample.time.as_nanos(), value);
                    if first.get() == 0 {
                        first.set(value);
                    }
                    *last = value;
                    *seen += 1;
                }
                Next::Skipped(_) => {}
                Next::Empty => break,
            }
        };
        take(&mut last, &mut seen);
        if last >= VALUES {
            break;
        }
        let before = seen;
        let wake = doorbell
            .wait(Some(Duration::from_secs(20)), || {
                take(&mut last, &mut seen);
                seen > before
            })
            .expect("wait");
        assert_ne!(wake, Wake::TimedOut, "slept through a value after {last}");
        woken += u64::from(wake == Wake::Rung);
    }
    // Values published before this reader attached are not its to read, and how many that
    // is depends on when this process got to run. Every value from the first one it read is
    // read or counted as missed.
    let first = first.get();
    let accounted = seen + reader.missed();
    assert!(
        first >= 1 && accounted <= VALUES && accounted > VALUES - first,
        "{seen} read, {} missed",
        reader.missed()
    );
    assert!(seen > VALUES / 2, "read {seen} of {VALUES}");
    assert!(
        woken > 0,
        "the doorbell was never needed, so it was never tested"
    );
}

/// The role this process was started for, if it is one of the two children.
#[test]
fn child_process() {
    match std::env::var(ROLE).as_deref() {
        Ok("publisher") => runtime().block_on(publisher()),
        Ok("subscriber") => runtime().block_on(subscriber()),
        _ => {}
    }
}

fn start(role: &str, socket: &Path) -> Child {
    Command::new(std::env::current_exe().expect("test binary"))
        .args(["--exact", "child_process", "--nocapture"])
        .env(ROLE, role)
        .env(SOCKET, socket)
        .spawn()
        .expect("child process")
}

#[test]
fn two_processes_exchange_values_and_the_broker_is_idle_afterward() {
    let dir = std::env::temp_dir().join(format!("orion-broker-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("directory");
    let socket = dir.join("orion.sock");

    let local = tokio::task::LocalSet::new();
    local.block_on(&runtime(), async {
        let comm = Comm::new(
            "board".parse().expect("name"),
            Security::Off,
            None,
            Limits::default(),
        );
        let broker = Broker::new("board".parse().expect("name"), BrokerConfig::default());
        let topics = broker.topics();
        comm.add_service(broker);
        let carrier = UnixCarrier::bind(&socket, None, &[]).expect("bind");
        let serving = comm.clone();
        tokio::task::spawn_local(async move { serving.serve(carrier).await });

        // The subscriber first, so the order of starting does not matter either.
        let mut subscriber = start("subscriber", &socket);
        tokio::time::sleep(Duration::from_millis(100)).await;
        let mut publisher = start("publisher", &socket);

        let mut exits = [None, None];
        for _ in 0..3000 {
            exits = [
                subscriber.try_wait().expect("wait"),
                publisher.try_wait().expect("wait"),
            ];
            if exits.iter().all(Option::is_some) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        for (child, exit) in ["subscriber", "publisher"].iter().zip(exits) {
            let status = exit.unwrap_or_else(|| panic!("the {child} did not finish"));
            assert!(
                status.success(),
                "the {child} failed: {:?} {:?}",
                status.code(),
                status.signal()
            );
        }

        // All the broker ever did: two hellos, one declare, one subscribe. A hundred thousand
        // values and their wake-ups went past it.
        let stats = comm.stats();
        assert_eq!(
            (stats.opened, stats.requests, stats.refused, stats.unknown),
            (2, 2, 0, 0)
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            topics.borrow().is_empty(),
            "both gone, so the topic and its memory are gone"
        );
    });
    let _ = std::fs::remove_dir_all(dir);
}
