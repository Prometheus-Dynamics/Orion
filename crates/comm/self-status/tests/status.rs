//! The board's vital signs and Orion's own state as topics: read only while watched.

#[path = "../../broker/tests/common/mod.rs"]
mod common;

use std::cell::Cell;
use std::rc::Rc;
use std::time::Duration;

use common::{Client, Event, FIELDS, Wire};
use orion_broker::{Broker, BrokerConfig};
use orion_self_status::{COMM_TOPIC, HOST_TOPIC, HostSample, HostSource, SelfStatus};
use orion_server::mem::{self, MemConnector};
use orion_server::{Comm, Error, Limits, Principal, Roles, Security};
use orion_supervise::{State, Status};
use shm_ring::{FieldId, Reader};

const USER: Principal = Principal::Local {
    uid: 1000,
    gid: 1000,
};
const INTERVAL: Duration = Duration::from_millis(100);

fn run(test: impl Future<Output = ()>) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .expect("runtime");
    tokio::task::LocalSet::new().block_on(&runtime, test);
}

/// A host whose uptime goes up by one each reading and whose memory never changes.
struct Fake(Rc<Cell<u64>>);

impl HostSource for Fake {
    fn sample(&mut self) -> HostSample {
        self.0.set(self.0.get() + 1);
        HostSample {
            uptime_s: 1000 + self.0.get(),
            mem_total_kb: 4_000_000,
            mem_available_kb: 3_000_000,
            cpu_percent: (self.0.get() > 1).then_some(12.5),
            temperature_c: None,
        }
    }
}

fn board() -> (SelfStatus, Rc<Cell<u64>>, MemConnector) {
    let node: orion_types::NodeName = "board".parse().expect("name");
    let comm = Comm::new(node.clone(), Security::Off, None, Limits::default());
    let broker = Broker::new(node, BrokerConfig::default());
    let reads = Rc::new(Cell::new(0));
    let status = SelfStatus::start(&comm, &broker, Fake(reads.clone()), INTERVAL).expect("start");
    comm.add_service(broker);
    let (carrier, connector) = mem::carrier(Roles::CLIENT.and(Roles::PROVIDER), 64 * 1024);
    tokio::task::spawn_local(async move { comm.serve(carrier).await });
    (status, reads, connector)
}

/// Subscribes to `topic` and returns its handle and a reader of its ring.
async fn watch(client: &mut Client, topic: &str) -> (u32, Reader) {
    let seq = client.subscribe(topic, None, &[]).await;
    loop {
        match client.next().await {
            Event::Opened {
                request_seq,
                handle,
                shared: true,
                mut fds,
                ..
            } if request_seq == seq => {
                let reader = Reader::attach(fds.pop().expect("the read end")).expect("reader");
                // Let the sampler see that someone is watching.
                tokio::time::sleep(Duration::from_millis(1)).await;
                return (handle, reader);
            }
            Event::Catalog(_) | Event::Other(_) => {}
            other => panic!("subscribe to {topic}: {other:?}"),
        }
    }
}

async fn leave(client: &mut Client, handle: u32) {
    client.unsubscribe(handle).await;
    tokio::time::sleep(Duration::from_millis(1)).await;
}

fn int(reader: &mut Reader, field: u16) -> Option<i64> {
    let sample = reader.latest(FieldId(field))?;
    Some(i64::from_le_bytes(
        sample.payload.try_into().expect("eight bytes"),
    ))
}

fn float(reader: &mut Reader, field: u16) -> Option<f64> {
    let sample = reader.latest(FieldId(field))?;
    Some(f64::from_le_bytes(
        sample.payload.try_into().expect("eight bytes"),
    ))
}

#[test]
fn nothing_is_sampled_while_nobody_watches() {
    run(async {
        let (status, reads, connector) = board();
        // A long time with the comm core up and nobody subscribed: the host is never read.
        tokio::time::sleep(Duration::from_secs(600)).await;
        assert_eq!((reads.get(), status.readings()), (0, 0));

        // A client that is connected but watches something else changes nothing.
        let mut client = Client::open(Wire::Mem(connector.connect(USER)), "dashboard").await;
        let _own = client.declare("other.topic", 4).await;
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert_eq!(reads.get(), 0);

        // The first subscriber gets a reading at once, not an interval later.
        let (handle, mut host) = watch(&mut client, HOST_TOPIC).await;
        assert_eq!(reads.get(), 1);
        assert_eq!(int(&mut host, 0), Some(1001));
        assert_eq!(int(&mut host, 1), Some(4_000_000));
        assert_eq!(int(&mut host, 2), Some(3_000_000));
        assert_eq!(
            float(&mut host, 3),
            None,
            "one reading has no processor share"
        );
        assert_eq!(
            float(&mut host, 4),
            None,
            "this board reports no temperature"
        );

        // Then one every interval, for as long as it is watched.
        tokio::time::sleep(INTERVAL * 5).await;
        assert_eq!(reads.get(), 6);
        assert_eq!(int(&mut host, 0), Some(1006));
        assert_eq!(float(&mut host, 3), Some(12.5));
        // Only what changed was published: uptime six times, memory once, the processor
        // share once.
        let published =
            |reader: &Reader| [0, 1, 2, 3, 4].map(|field| reader.lane_head(FieldId(field)));
        assert_eq!(published(&host), [6, 1, 1, 1, 0]);

        // The last subscriber leaves: the reading stops, and no timer is left behind.
        leave(&mut client, handle).await;
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert_eq!(reads.get(), 6);

        // Someone comes back: a reading at once, published whole, since what is in the ring
        // is from before.
        let (handle, mut host) = watch(&mut client, HOST_TOPIC).await;
        assert_eq!(reads.get(), 7);
        assert_eq!(published(&host), [7, 2, 2, 2, 0]);
        assert_eq!(int(&mut host, 0), Some(1007));

        // Two watchers are read for once, and one leaving does not stop it.
        let mut second = Client::open(Wire::Mem(connector.connect(USER)), "tool").await;
        let (_, mut also) = watch(&mut second, HOST_TOPIC).await;
        let before = reads.get();
        tokio::time::sleep(INTERVAL * 3).await;
        assert_eq!(reads.get(), before + 3);
        assert_eq!(int(&mut also, 0), int(&mut host, 0));
        leave(&mut client, handle).await;
        let before = reads.get();
        tokio::time::sleep(INTERVAL * 3).await;
        assert_eq!(reads.get(), before + 3);

        // A watcher that goes away without a word counts as gone.
        drop(second);
        tokio::time::sleep(Duration::from_millis(5)).await;
        let before = reads.get();
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert_eq!(reads.get(), before);
    });
}

#[test]
fn the_comm_core_counts_itself_without_reading_the_host() {
    run(async {
        let (_status, reads, connector) = board();
        let mut client = Client::open(Wire::Mem(connector.connect(USER)), "dashboard").await;
        let (_, mut comm) = watch(&mut client, COMM_TOPIC).await;
        // sessions, opened, topics: this client, and the two topics the comm core has.
        assert_eq!(
            (int(&mut comm, 0), int(&mut comm, 1), int(&mut comm, 8)),
            (Some(1), Some(1), Some(2))
        );
        let ring_bytes = int(&mut comm, 9).expect("ring bytes");
        assert!(ring_bytes > 0);

        let mut second = Client::open(Wire::Mem(connector.connect(USER)), "camera").await;
        let _topic = second.declare("camera.frames", 4).await;
        tokio::time::sleep(INTERVAL * 2).await;
        assert_eq!(
            (int(&mut comm, 0), int(&mut comm, 1), int(&mut comm, 8)),
            (Some(2), Some(2), Some(3))
        );
        assert!(int(&mut comm, 9).expect("ring bytes") > ring_bytes);
        assert_eq!(reads.get(), 0, "nobody asked about the host");

        // Its topics are its own: nobody else may publish them or take them over.
        let declare = Client::declaration(HOST_TOPIC, &FIELDS, 4);
        let seq = second.send(&declare, &[]).await;
        loop {
            match second.next().await {
                Event::Refused {
                    request_seq, error, ..
                } if request_seq == seq => {
                    assert_eq!(error, Some(Error::AlreadyExists));
                    break;
                }
                Event::Other(_) => {}
                other => panic!("declare over the comm core's topic: {other:?}"),
            }
        }
        let seq = second.open_topic(COMM_TOPIC, true).await;
        loop {
            match second.next().await {
                Event::Refused { request_seq, .. } if request_seq == seq => break,
                Event::Other(_) => {}
                other => panic!("open the comm core's topic to write: {other:?}"),
            }
        }
    });
}

#[test]
fn a_part_says_how_it_is_doing_when_that_changes() {
    run(async {
        let (status, reads, connector) = board();
        let mut link = status.part("link.peer").expect("part");
        let retrying = Status {
            state: State::Retrying,
            off: None,
            restarts: 0,
            failures: 3,
            retry_at_ms: Some(42_000),
        };
        // Reported before anyone watches: it is there when they come.
        link.report(&retrying);
        let mut client = Client::open(Wire::Mem(connector.connect(USER)), "dashboard").await;
        let (_, mut state) = watch(&mut client, "orion.state.link.peer").await;
        let read = |state: &mut Reader| [0, 1, 2, 3].map(|field| int(state, field));
        assert_eq!(read(&mut state), [Some(2), Some(3), Some(0), Some(42_000)]);

        // The same again publishes nothing.
        let published =
            |reader: &Reader| [0, 1, 2, 3].map(|field| reader.lane_head(FieldId(field)));
        link.report(&retrying);
        assert_eq!(published(&state), [1, 1, 1, 1]);

        // It came up: only what changed is published.
        link.report(&Status {
            state: State::Running,
            off: None,
            restarts: 1,
            failures: 0,
            retry_at_ms: None,
        });
        assert_eq!(read(&mut state), [Some(1), Some(0), Some(1), Some(0)]);
        assert_eq!(published(&state), [2, 2, 2, 2]);
        link.report(&Status {
            state: State::Running,
            off: None,
            restarts: 2,
            failures: 0,
            retry_at_ms: None,
        });
        assert_eq!(published(&state), [2, 2, 3, 2]);

        // A name that is taken is refused, and none of this read the host.
        assert_eq!(status.part("link.peer").err(), Some(Error::AlreadyExists));
        assert_eq!(reads.get(), 0);
    });
}
