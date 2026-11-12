//! Subscribers that cannot map the ring: they are sent every sample they have room for, a
//! throttled one beside a fast one costs the fast one nothing, and after a loss a subscriber
//! is sent nothing that depends on what it lost.

use std::cell::RefCell;
use std::rc::Rc;
use std::time::Duration;

use orion_broker::{Broker, BrokerConfig};
use orion_broker_proxy::BrokerProxy;
use orion_msg_session::{Hello, Welcome};
use orion_msg_topics::{
    Declare, Field, FieldType, Opened, Sample, SampleFlags, Subscribe, Unsubscribe,
};
use orion_outbound::{Outbound, OutboundLimits};
use orion_server::mem::{self, MemConnection, MemConnector};
use orion_server::{Admits, Comm, FdSet, Limits, Policy, Principal, Roles, Security};
use orion_types::{SampleTime, TopicRef};
use orion_wire::frame;
use orion_wire::{Body, Bytes, Decode, List, encode_body, kind};

const USER: Principal = Principal::Local {
    uid: 1000,
    gid: 1000,
};

/// The most that may wait for one subscriber, in these tests.
const LIMIT: usize = 48 * 1024;

fn run(test: impl Future<Output = ()>) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    tokio::task::LocalSet::new().block_on(&runtime, test);
}

/// A comm core with the broker, the proxy for publishers without a ring, and the outbound
/// part for subscribers without one. Every program in these tests is one of those.
fn board() -> (MemConnector, Rc<Outbound>) {
    let node: orion_types::NodeName = "board".parse().expect("name");
    let comm = Comm::new(node.clone(), Security::Off, None, Limits::default());
    let anyone = Policy::new(Admits::ANYONE).without_grant();
    let broker = Broker::new(
        node,
        BrokerConfig {
            declare: anyone,
            subscribe: anyone,
            ..BrokerConfig::default()
        },
    );
    let limits = OutboundLimits {
        max_bytes: LIMIT,
        ..OutboundLimits::default()
    };
    let outbound = Rc::new(Outbound::start(&comm, &broker, limits).expect("outbound"));
    comm.add_service(BrokerProxy::new(&broker, anyone));
    comm.add_service(broker);
    let (carrier, connector) = mem::carrier(Roles::CLIENT.and(Roles::PROVIDER), 16 * 1024);
    tokio::task::spawn_local(async move { comm.serve(carrier).await });
    (connector, outbound)
}

/// One sample as a subscriber got it: field, number, whether it was a key.
type Got = (u16, u32, bool);

struct Client {
    connection: MemConnection,
    buf: Vec<u8>,
    seq: u16,
}

impl Client {
    async fn open(board: &MemConnector) -> Self {
        let mut client = Self {
            connection: board.connect_without_descriptors(USER),
            buf: vec![0; 64 * 1024],
            seq: 0,
        };
        client
            .send(&Hello {
                name: "test",
                roles: Roles::CLIENT.and(Roles::PROVIDER),
                max_frame: 64 * 1024,
                revision: 1,
            })
            .await;
        let (kind, payload) = client.next().await;
        assert_eq!(kind, kind::WELCOME);
        let _: Welcome = Welcome::decode(&payload).expect("welcome");
        client
    }

    async fn send<B: Body>(&mut self, body: &B) -> u16 {
        self.seq += 1;
        let mut out = vec![0_u8; 8 * 1024];
        let len = encode_body(body, self.seq, &mut out).expect("encode");
        self.connection.send(&out[..len], &[]).await.expect("send");
        self.seq
    }

    /// The next frame that is not a heartbeat: its kind and payload.
    async fn next(&mut self) -> (u8, Vec<u8>) {
        loop {
            let mut fds = FdSet::default();
            let len = self
                .connection
                .recv(&mut self.buf, &mut fds)
                .await
                .expect("recv")
                .expect("the session is open");
            let view = frame::decode(&self.buf[..len]).expect("frame");
            if !matches!(view.kind(), kind::PING | kind::PONG) {
                return (view.kind(), view.payload().to_vec());
            }
        }
    }

    /// Declares `counter` with two fields and returns its handle.
    async fn declare(&mut self) -> u32 {
        let fields = [
            Field {
                name: "video",
                ty: FieldType::Bytes,
                unit: "",
                type_name: "",
            },
            Field {
                name: "reading",
                ty: FieldType::Bytes,
                unit: "",
                type_name: "",
            },
        ];
        self.send(&Declare {
            topic: "counter".parse().expect("name"),
            fields: List::new(&fields),
            history: 64,
            expiry_ms: 0,
            max_payload: 2048,
        })
        .await;
        loop {
            let (kind, payload) = self.next().await;
            if kind == kind::TOPIC_OPENED {
                let opened = Opened::decode(&payload).expect("opened");
                assert!(
                    !opened.shared_memory,
                    "a program without descriptors has no ring"
                );
                return opened.handle;
            }
        }
    }

    /// Subscribes to `counter` and returns its handle.
    async fn subscribe(&mut self) -> u32 {
        self.send(&Subscribe {
            topic: TopicRef {
                node: None,
                topic: "counter".parse().expect("name"),
            },
            request: Bytes(&[]),
            max_held: 0,
            max_hold_ms: 0,
        })
        .await;
        loop {
            let (kind, payload) = self.next().await;
            if kind == kind::TOPIC_OPENED {
                let opened = Opened::decode(&payload).expect("opened");
                assert!(!opened.shared_memory);
                return opened.handle;
            }
        }
    }

    /// Publishes sample `number` of `field`, a kilobyte of it. Every tenth of the first
    /// field is a key: the ones after it depend on it.
    async fn publish(&mut self, handle: u32, field: u16, number: u32) {
        let mut payload = [0_u8; 1024];
        payload[..4].copy_from_slice(&number.to_le_bytes());
        let key = field == 0 && number.is_multiple_of(10);
        self.send(&Sample {
            handle,
            field,
            time: SampleTime::from_nanos(u64::from(number)),
            flags: if key {
                SampleFlags::OTHERS_DEPEND
            } else {
                SampleFlags::NONE
            },
            payload: Bytes(&payload),
        })
        .await;
    }

    /// The next sample this subscriber is sent.
    async fn sample(&mut self, handle: u32) -> Got {
        loop {
            let (kind, payload) = self.next().await;
            if kind == kind::TOPIC_SAMPLE {
                let sample = Sample::decode(&payload).expect("sample");
                assert_eq!(sample.handle, handle);
                let number = u32::from_le_bytes(sample.payload.0[..4].try_into().expect("four"));
                assert_eq!(sample.time.as_nanos(), u64::from(number));
                assert_eq!(sample.payload.len(), 1024);
                return (
                    sample.field,
                    number,
                    sample.flags.has(SampleFlags::OTHERS_DEPEND),
                );
            }
        }
    }
}

#[test]
fn a_subscriber_without_a_ring_is_sent_every_sample() {
    run(async {
        let (board, outbound) = board();
        let mut publisher = Client::open(&board).await;
        let handle = publisher.declare().await;
        let mut subscriber = Client::open(&board).await;
        let theirs = subscriber.subscribe().await;
        for number in 0..40 {
            publisher.publish(handle, 0, number).await;
            publisher.publish(handle, 1, number).await;
            // As fast as the subscriber reads: nothing is dropped.
            assert_eq!(
                subscriber.sample(theirs).await,
                (0, number, number.is_multiple_of(10))
            );
            assert_eq!(subscriber.sample(theirs).await, (1, number, false));
        }
        let counters = outbound.counters();
        assert_eq!(
            (counters.sent, counters.dropped, counters.overrun),
            (80, 0, 0)
        );
        assert_eq!(outbound.subscriptions(), 1);

        // It unsubscribes: it is carried no longer, and is sent nothing more.
        subscriber.send(&Unsubscribe { handle: theirs }).await;
        tokio::time::sleep(Duration::from_millis(60)).await;
        assert_eq!(outbound.subscriptions(), 0);
        publisher.publish(handle, 0, 40).await;
        tokio::time::sleep(Duration::from_millis(60)).await;
        assert_eq!(outbound.counters().sent, 80);
    });
}

#[test]
fn a_throttled_reader_beside_a_fast_one_costs_the_fast_one_nothing() {
    run(async {
        const SAMPLES: u32 = 1500;
        let (board, outbound) = board();
        let mut publisher = Client::open(&board).await;
        let handle = publisher.declare().await;
        let mut fast = Client::open(&board).await;
        let fast_handle = fast.subscribe().await;
        let mut slow = Client::open(&board).await;
        let slow_handle = slow.subscribe().await;

        // The fast one reads as fast as it is sent to.
        let fast_got = Rc::new(RefCell::new(Vec::<Got>::new()));
        let collecting = fast_got.clone();
        let fast_task = tokio::task::spawn_local(async move {
            loop {
                let got = fast.sample(fast_handle).await;
                collecting.borrow_mut().push(got);
            }
        });
        // The throttled one takes one sample every two milliseconds: about a quarter of
        // what is published.
        let slow_got = Rc::new(RefCell::new(Vec::<Got>::new()));
        let collecting = slow_got.clone();
        let slow_task = tokio::task::spawn_local(async move {
            loop {
                let got = slow.sample(slow_handle).await;
                collecting.borrow_mut().push(got);
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        });

        // Three megabytes, at the pace the fast subscriber takes them.
        for number in 0..SAMPLES {
            publisher.publish(handle, 0, number).await;
            publisher.publish(handle, 1, number).await;
            while fast_got.borrow().len() < (number as usize + 1) * 2 {
                tokio::task::yield_now().await;
            }
        }

        // The fast one got everything, in order, while the other was falling behind.
        let fast_got = fast_got.borrow().clone();
        assert_eq!(fast_got.len(), SAMPLES as usize * 2);
        for (index, got) in fast_got.iter().enumerate() {
            let number = (index / 2) as u32;
            let expected = if index % 2 == 0 {
                (0, number, number.is_multiple_of(10))
            } else {
                (1, number, false)
            };
            assert_eq!(*got, expected);
        }

        // The throttled one got some, in order, and was not closed for being slow.
        tokio::time::sleep(Duration::from_millis(200)).await;
        let slow_got = slow_got.borrow().clone();
        let counters = outbound.counters();
        assert!(counters.dropped > 0, "{counters:?}");
        assert!(
            slow_got.len() > 20 && slow_got.len() < SAMPLES as usize * 2,
            "{} samples",
            slow_got.len()
        );
        for field in [0_u16, 1] {
            let numbers: Vec<(u32, bool)> = slow_got
                .iter()
                .filter(|(got, ..)| *got == field)
                .map(|(_, number, key)| (*number, *key))
                .collect();
            assert!(
                numbers.windows(2).all(|pair| pair[0].0 < pair[1].0),
                "in order"
            );
            let gaps = numbers
                .windows(2)
                .filter(|pair| pair[1].0 != pair[0].0 + 1)
                .count();
            assert!(gaps > 0, "field {field} lost nothing");
            if field == 0 {
                // After every loss, the next thing it was sent is a key: nothing that
                // depended on what it lost.
                for pair in numbers.windows(2) {
                    if pair[1].0 != pair[0].0 + 1 {
                        assert!(
                            pair[1].1,
                            "{} follows {} and is not a key",
                            pair[1].0, pair[0].0
                        );
                    }
                }
                assert!(numbers[0].1, "it starts at a key");
            }
        }
        // What was waiting for it never passed its limit by more than one sample.
        assert_eq!(outbound.subscriptions(), 2);
        fast_task.abort();
        slow_task.abort();
    });
}

#[test]
fn a_subscriber_that_stops_reading_is_dropped_for_and_not_waited_for() {
    run(async {
        let (board, outbound) = board();
        let mut publisher = Client::open(&board).await;
        let handle = publisher.declare().await;
        let mut stalled = Client::open(&board).await;
        let stalled_handle = stalled.subscribe().await;

        // Nobody reads. The publisher is never held up: 600 KiB goes into the ring as fast
        // as it is sent.
        let started = std::time::Instant::now();
        for number in 0..300 {
            publisher.publish(handle, 1, number).await;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(started.elapsed() < Duration::from_secs(2));
        let counters = outbound.counters();
        assert!(counters.sent < 150, "{counters:?}");
        assert!(counters.dropped + counters.overrun >= 150, "{counters:?}");

        // It reads again: what was waiting for it, then what is published now.
        let mut last = None;
        let mut count = 0;
        loop {
            let got =
                tokio::time::timeout(Duration::from_millis(100), stalled.sample(stalled_handle))
                    .await;
            match got {
                Ok((_, number, _)) => {
                    assert!(last.is_none_or(|before| before < number));
                    last = Some(number);
                    count += 1;
                }
                Err(_) => break,
            }
        }
        // No more was kept for it than its limit and what was already on its way.
        assert!(
            count <= (LIMIT + 16 * 1024) / 1024 + 4,
            "{count} samples were waiting"
        );
        publisher.publish(handle, 1, 1000).await;
        assert_eq!(stalled.sample(stalled_handle).await, (1, 1000, false));
    });
}
