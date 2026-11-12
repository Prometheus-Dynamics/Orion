//! The broker over the in-memory carrier.

mod common;

use std::time::Duration;

use common::{Client, Event, FIELDS, Wire};
use orion_broker::{Broker, BrokerConfig};
use orion_msg_topics::{Field, FieldType, TopicStatus};
use orion_ring::{FieldId, Flags, Next, Reader, SampleTime, Writer};
use orion_ring_doorbell::{Bell, Doorbell, Wake};
use orion_server::mem::{self, MemConnector};
use orion_server::{Comm, Error, Limits, Principal, Roles, Security};
use orion_wire::Decode;

const USER: Principal = Principal::Local {
    uid: 1000,
    gid: 1000,
};

fn run(test: impl Future<Output = ()>) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .expect("runtime");
    tokio::task::LocalSet::new().block_on(&runtime, test);
}

struct Board {
    comm: Comm,
    connector: MemConnector,
    topics: std::rc::Rc<std::cell::RefCell<orion_broker::Topics>>,
}

fn board(security: Security, config: BrokerConfig) -> Board {
    let comm = Comm::new(
        "board".parse().expect("name"),
        security,
        None,
        Limits::default(),
    );
    let broker = Broker::new("board".parse().expect("name"), config);
    let topics = broker.topics();
    comm.add_service(broker);
    let (carrier, connector) = mem::carrier(Roles::CLIENT.and(Roles::PROVIDER), 256 * 1024);
    let serving = comm.clone();
    tokio::task::spawn_local(async move { serving.serve(carrier).await });
    Board {
        comm,
        connector,
        topics,
    }
}

impl Board {
    async fn client(&self, name: &str) -> Client {
        Client::open(Wire::Mem(self.connector.connect(USER)), name).await
    }
}

fn publish(writer: &mut Writer, value: u64) {
    writer
        .publish(
            FieldId(0),
            SampleTime::from_nanos(value),
            Flags::NONE,
            &value.to_le_bytes(),
        )
        .expect("publish");
}

fn read(reader: &mut Reader) -> Option<u64> {
    loop {
        match reader.next() {
            Next::Sample(sample) => return Some(sample.time.as_nanos()),
            Next::Skipped(_) => {}
            Next::Empty => return None,
        }
    }
}

/// What the publisher is told when a subscriber joins: its doorbell, then what it asked.
async fn joined(publisher: &mut Client, handle: u32, count: u32, request: &[u8]) -> Bell {
    let Event::Bell {
        handle: for_topic,
        present: true,
        mut fds,
        ..
    } = publisher.next().await
    else {
        panic!("no doorbell for the publisher");
    };
    assert_eq!(for_topic, handle);
    let page = fds.pop().expect("page");
    let bell = Bell::attach(fds.pop().expect("eventfd"), page).expect("bell");
    match publisher.next().await {
        Event::Demand {
            handle: for_topic,
            subscribers,
            request: asked,
        } => assert_eq!(
            (for_topic, subscribers, asked.as_slice()),
            (handle, count, request)
        ),
        other => panic!("{other:?}"),
    }
    bell
}

#[test]
fn values_flow_between_two_programs_and_the_broker_does_nothing_more() {
    run(async {
        let board = board(Security::Off, BrokerConfig::default());
        let mut publisher = board.client("sensor").await;
        let mut subscriber = board.client("display").await;

        let (handle, mut writer, _keep) = publisher.declare("cpu.temp", 8).await;
        assert_eq!(writer.subscribers(), 0);
        let mut doorbell = Doorbell::create().expect("doorbell");
        let seq = subscriber
            .subscribe("cpu.temp", Some(&doorbell), b"10Hz")
            .await;
        let (subscription, mut reader, catalog) = subscriber.opened(seq).await;
        assert_eq!(subscription, handle);

        // The subscriber was sent the declaration: it knows the fields without being told.
        let declared = orion_msg_topics::Declare::decode(&catalog).expect("declaration");
        let names: Vec<&str> = declared
            .fields
            .iter()
            .map(|field| field.expect("field").name)
            .collect();
        assert_eq!(
            (declared.topic.as_str(), names),
            ("cpu.temp", vec!["value", "state"])
        );

        // The publisher was sent the subscriber's doorbell and its request, and sees the count
        // in the ring.
        let mut bell = joined(&mut publisher, handle, 1, b"10Hz").await;
        assert_eq!(writer.subscribers(), 1);

        // From here the broker is not involved. Values go from one program to the other, the
        // doorbell wakes the reader, and the broker handles no request at all.
        let before = board.comm.stats();
        assert_eq!(
            doorbell.wait(Some(Duration::ZERO), || false).expect("wait"),
            Wake::TimedOut
        );
        doorbell.arm();
        publish(&mut writer, 1);
        assert!(bell.ring(), "the subscriber is asleep, so it is rung");
        doorbell.disarm();
        assert_eq!(read(&mut reader), Some(1));
        for value in 2..10_000 {
            publish(&mut writer, value);
            assert!(!bell.ring(), "awake: no wake");
            assert_eq!(read(&mut reader), Some(value));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(board.comm.stats(), before, "the broker saw none of it");
    });
}

#[test]
fn a_subscriber_that_comes_first_waits_and_gets_the_ring_when_it_is_declared() {
    run(async {
        let board = board(Security::Off, BrokerConfig::default());
        let mut early = board.client("early").await;
        let seq = early.subscribe("late.topic", None, b"").await;
        let Event::State {
            request_seq,
            handle,
            status: TopicStatus::Waiting,
        } = early.next().await
        else {
            panic!("not told to wait");
        };
        assert_eq!(request_seq, seq);

        let mut publisher = board.client("publisher").await;
        let (declared, mut writer, _keep) = publisher.declare("late.topic", 4).await;
        assert_eq!(
            declared, handle,
            "the handle the subscriber was given is the topic's"
        );
        // The publisher learns of the subscriber that was already there. It has no doorbell.
        assert!(matches!(
            publisher.next().await,
            Event::Demand { subscribers: 1, .. }
        ));
        assert_eq!(writer.subscribers(), 1);

        let (_, mut reader, _) = early.opened(seq).await;
        publish(&mut writer, 5);
        assert_eq!(read(&mut reader), Some(5));
    });
}

#[test]
fn a_publisher_that_leaves_is_seen_and_one_that_returns_carries_on_in_the_same_ring() {
    run(async {
        let board = board(Security::Off, BrokerConfig::default());
        let mut subscriber = board.client("display").await;
        let mut publisher = board.client("sensor").await;
        let (handle, mut writer, _keep) = publisher.declare("cpu.temp", 8).await;
        let doorbell = Doorbell::create().expect("doorbell");
        let seq = subscriber
            .subscribe("cpu.temp", Some(&doorbell), b"fast")
            .await;
        let (_, mut reader, _) = subscriber.opened(seq).await;
        publish(&mut writer, 1);

        // The publisher's program dies.
        drop(publisher);
        drop(writer);
        assert!(matches!(
            subscriber.next().await,
            Event::State { status: TopicStatus::PublisherGone, handle: gone, .. } if gone == handle
        ));
        // What it published is still there to read.
        assert_eq!(read(&mut reader), Some(1));
        assert_eq!(board.topics.borrow().len(), 1);

        // It comes back with the same declaration. It is given the same ring, and told again
        // about the subscriber that stayed: its doorbell and its request.
        let mut publisher = board.client("sensor").await;
        let (again, mut writer, _keep) = publisher.declare("cpu.temp", 8).await;
        assert_eq!(again, handle);
        assert_eq!(writer.published(), 1, "the ring it left");
        joined(&mut publisher, handle, 1, b"fast").await;
        assert!(matches!(
            subscriber.next().await,
            Event::State {
                status: TopicStatus::Live,
                ..
            }
        ));
        publish(&mut writer, 2);
        assert_eq!(
            read(&mut reader),
            Some(2),
            "the subscriber's reader needed nothing"
        );

        // A second program may not take a topic that has a publisher.
        let mut other = board.client("impostor").await;
        let declare = Client::declaration("cpu.temp", &FIELDS, 8);
        let refused = other.send(&declare, &[]).await;
        assert!(matches!(
            other.next().await,
            Event::Refused { request_seq, error: Some(Error::AlreadyExists) } if request_seq == refused
        ));

        // The same publisher declaring it differently replaces the ring. Subscribers are sent
        // the new declaration and the new ring, under the same handle.
        let wider = [
            FIELDS[0].clone(),
            FIELDS[1].clone(),
            Field {
                name: "extra",
                ty: FieldType::Bool,
                unit: "",
                type_name: "",
            },
        ];
        let seq = publisher
            .send(&Client::declaration("cpu.temp", &wider, 8), &[])
            .await;
        assert!(
            matches!(publisher.next().await, Event::Opened { request_seq, .. } if request_seq == seq)
        );
        let Event::Catalog(catalog) = subscriber.next().await else {
            panic!("no new declaration");
        };
        assert_eq!(
            orion_msg_topics::Declare::decode(&catalog)
                .expect("declaration")
                .fields
                .len(),
            3
        );
        assert!(
            matches!(subscriber.next().await, Event::Opened { handle: same, .. } if same == handle)
        );
    });
}

#[test]
fn leaving_is_told_to_the_publisher_and_an_unused_topic_gives_its_memory_back() {
    run(async {
        let board = board(Security::Off, BrokerConfig::default());
        let mut publisher = board.client("sensor").await;
        let (handle, writer, _keep) = publisher.declare("cpu.temp", 8).await;
        let mut first = board.client("first").await;
        let mut second = board.client("second").await;
        let doorbell = Doorbell::create().expect("doorbell");
        let seq = first.subscribe("cpu.temp", Some(&doorbell), b"").await;
        first.opened(seq).await;
        joined(&mut publisher, handle, 1, b"").await;
        let seq = second.subscribe("cpu.temp", None, b"").await;
        second.opened(seq).await;
        assert!(matches!(
            publisher.next().await,
            Event::Demand { subscribers: 2, .. }
        ));
        assert_eq!(writer.subscribers(), 2);

        // One unsubscribes: the publisher is told to drop its doorbell, and the new count.
        first.unsubscribe(handle).await;
        assert!(matches!(
            publisher.next().await,
            Event::Bell { present: false, .. }
        ));
        assert!(matches!(
            publisher.next().await,
            Event::Demand { subscribers: 1, .. }
        ));
        // The other just disconnects. Same thing.
        drop(second);
        assert!(matches!(
            publisher.next().await,
            Event::Bell { present: false, .. }
        ));
        assert!(matches!(
            publisher.next().await,
            Event::Demand { subscribers: 0, .. }
        ));
        assert_eq!(writer.subscribers(), 0);

        // Unsubscribing from what one is not subscribed to is refused.
        first.unsubscribe(handle).await;
        assert!(matches!(
            first.next().await,
            Event::Refused {
                error: Some(Error::NotFound),
                ..
            }
        ));

        // With the publisher gone too, the topic and its memory are gone.
        assert!(board.topics.borrow().bytes() > 0);
        drop(publisher);
        tokio::time::sleep(Duration::from_millis(1)).await;
        assert_eq!(
            (board.topics.borrow().len(), board.topics.borrow().bytes()),
            (0, 0)
        );
    });
}

#[test]
fn one_cap_bounds_all_ring_memory() {
    run(async {
        let config = BrokerConfig {
            max_bytes: 3 * 4096,
            max_topics: 4,
            ..BrokerConfig::default()
        };
        let board = board(Security::Off, config);
        let mut publisher = board.client("greedy").await;
        // Each of these rings is one page.
        for index in 0..3 {
            publisher.declare(&format!("topic.{index}"), 4).await;
        }
        assert_eq!(board.topics.borrow().bytes(), 3 * 4096);
        let over = publisher
            .send(&Client::declaration("topic.3", &FIELDS, 4), &[])
            .await;
        assert!(matches!(
            publisher.next().await,
            Event::Refused { request_seq, error: Some(Error::Busy) } if request_seq == over
        ));
        // One ring far larger than the cap is refused before any memory is made for it.
        let huge = publisher
            .send(&Client::declaration("huge", &FIELDS, 1 << 20), &[])
            .await;
        assert!(matches!(
            publisher.next().await,
            Event::Refused { request_seq, error: Some(Error::Busy) } if request_seq == huge
        ));
        assert_eq!(
            (board.topics.borrow().len(), board.topics.borrow().bytes()),
            (3, 3 * 4096)
        );
        // A shape the ring does not allow is refused as such.
        let bad = publisher
            .send(&Client::declaration("empty", &[], 4), &[])
            .await;
        assert!(matches!(
            publisher.next().await,
            Event::Refused { request_seq, error: Some(Error::Unsupported) } if request_seq == bad
        ));
        // And the number of topics is bounded too, waited-for ones included.
        let mut waiter = board.client("waiter").await;
        waiter.subscribe("wanted.0", None, b"").await;
        assert!(matches!(
            waiter.next().await,
            Event::State {
                status: TopicStatus::Waiting,
                ..
            }
        ));
        waiter.subscribe("wanted.1", None, b"").await;
        assert!(matches!(
            waiter.next().await,
            Event::Refused {
                error: Some(Error::Busy),
                ..
            }
        ));
    });
}

#[test]
fn a_restarted_comm_core_adopts_the_rings_its_publishers_kept() {
    run(async {
        let first = board(Security::Off, BrokerConfig::default());
        let mut publisher = first.client("sensor").await;
        let mut subscriber = first.client("display").await;
        let (_, mut writer, keep) = publisher.declare("cpu.temp", 8).await;
        let seq = subscriber.subscribe("cpu.temp", None, b"").await;
        let (_, mut reader, _) = subscriber.opened(seq).await;
        publish(&mut writer, 1);
        assert_eq!(read(&mut reader), Some(1));

        // The comm core goes away. Both programs lose their connection and nothing else:
        // values still flow, because the ring is theirs.
        drop(first);
        drop((publisher, subscriber));
        publish(&mut writer, 2);
        assert_eq!(read(&mut reader), Some(2));

        // A new comm core starts with no rings. The publisher declares again and offers the
        // ring it has; the broker takes it instead of making one.
        let second = board(Security::Off, BrokerConfig::default());
        let mut publisher = second.client("sensor").await;
        let declare = Client::declaration("cpu.temp", &FIELDS, 8);
        let seq = publisher.send(&declare, &[keep]).await;
        assert!(
            matches!(publisher.next().await, Event::Opened { request_seq, .. } if request_seq == seq)
        );
        // A subscriber that connects now is given that same ring, with what is already in it.
        let mut late = second.client("late").await;
        let seq = late.subscribe("cpu.temp", None, b"").await;
        let (_, mut late_reader, _) = late.opened(seq).await;
        late_reader.rewind();
        assert_eq!(read(&mut late_reader), Some(1));
        assert_eq!(read(&mut late_reader), Some(2));
        publish(&mut writer, 3);
        assert_eq!(
            (read(&mut reader), read(&mut late_reader)),
            (Some(3), Some(3))
        );
        assert_eq!(
            writer.subscribers(),
            1,
            "the new broker keeps the count in the adopted ring"
        );

        // A ring offered under a declaration it does not fit is refused.
        let mut liar = second.client("liar").await;
        let other = orion_ring::Ring::create(&orion_ring::Spec {
            fields: 1,
            history: 2,
            max_payload: 8,
            expiry_ms: 0,
        })
        .expect("ring");
        let seq = liar
            .send(
                &Client::declaration("other", &FIELDS, 8),
                &[other.write_fd().expect("fd")],
            )
            .await;
        assert!(matches!(
            liar.next().await,
            Event::Refused { request_seq, error: Some(Error::Malformed) } if request_seq == seq
        ));
        assert_eq!(second.topics.borrow().len(), 1);
    });
}

#[test]
fn looking_without_subscribing_and_who_may_have_the_write_end() {
    run(async {
        let board = board(Security::Off, BrokerConfig::default());
        let mut publisher = board.client("sensor").await;
        let (handle, mut writer, _keep) = publisher.declare("cpu.temp", 8).await;
        publish(&mut writer, 9);

        // A look: the declaration and a read end. Not a subscriber: no count, nothing sent to
        // the publisher.
        let mut tool = board.client("tool").await;
        let seq = tool.open_topic("cpu.temp", false).await;
        let (looked, mut reader, _) = tool.opened(seq).await;
        assert_eq!(looked, handle);
        reader.rewind();
        assert_eq!(read(&mut reader), Some(9));
        assert_eq!(writer.subscribers(), 0);

        // The write end again, for its publisher only.
        let seq = publisher.open_topic("cpu.temp", true).await;
        let Event::Opened {
            request_seq,
            mut fds,
            ..
        } = publisher.next().await
        else {
            panic!("not opened");
        };
        assert_eq!(request_seq, seq);
        assert!(Writer::attach(fds.pop().expect("fd")).is_ok());
        tool.open_topic("cpu.temp", true).await;
        assert!(matches!(
            tool.next().await,
            Event::Refused {
                error: Some(Error::Denied),
                ..
            }
        ));
        // The read end cannot be made a write end.
        let seq = tool.open_topic("cpu.temp", false).await;
        let Event::Catalog(_) = tool.next().await else {
            panic!("no declaration");
        };
        let Event::Opened {
            request_seq,
            mut fds,
            ..
        } = tool.next().await
        else {
            panic!("not opened");
        };
        assert_eq!(request_seq, seq);
        assert!(Writer::attach(fds.pop().expect("fd")).is_err());

        // A topic that is not there, and one on another board.
        tool.open_topic("nothing", false).await;
        assert!(matches!(
            tool.next().await,
            Event::Refused {
                error: Some(Error::NotFound),
                ..
            }
        ));
        tool.open_topic("elsewhere/cpu.temp", false).await;
        assert!(matches!(
            tool.next().await,
            Event::Refused {
                error: Some(Error::Unsupported),
                ..
            }
        ));
        tool.subscribe("elsewhere/cpu.temp", None, b"").await;
        assert!(matches!(
            tool.next().await,
            Event::Refused {
                error: Some(Error::Unsupported),
                ..
            }
        ));
        // This board by name is this board.
        tool.open_topic("board/cpu.temp", false).await;
        assert!(matches!(tool.next().await, Event::Catalog(_)));
    });
}

#[test]
fn with_security_on_the_policy_decides_and_a_program_without_descriptors_is_told_so() {
    run(async {
        let board = board(Security::On, BrokerConfig::default());
        // Nothing is known about this caller: it may neither declare nor subscribe.
        let mut stranger = Client::open(
            Wire::Mem(board.connector.connect(Principal::Anonymous)),
            "stranger",
        )
        .await;
        stranger
            .send(&Client::declaration("cpu.temp", &FIELDS, 8), &[])
            .await;
        assert!(matches!(
            stranger.next().await,
            Event::Refused {
                error: Some(Error::Denied),
                ..
            }
        ));
        stranger.subscribe("cpu.temp", None, b"").await;
        assert!(matches!(
            stranger.next().await,
            Event::Refused {
                error: Some(Error::Denied),
                ..
            }
        ));
        assert!(board.topics.borrow().is_empty());

        // A person's tool may subscribe, not declare.
        let operator = Principal::Operator(orion_types::KeyId([3; 32]));
        let wire = Wire::Mem(board.connector.connect_without_descriptors(operator));
        let mut tool = Client::open(wire, "tool").await;
        tool.send(&Client::declaration("cpu.temp", &FIELDS, 8), &[])
            .await;
        assert!(matches!(
            tool.next().await,
            Event::Refused {
                error: Some(Error::Denied),
                ..
            }
        ));

        // It is on a connection that cannot carry descriptors, so it is given no ring and told
        // that samples will come as messages.
        let mut publisher = board.client("sensor").await;
        let (handle, writer, _keep) = publisher.declare("cpu.temp", 8).await;
        let seq = tool.subscribe("cpu.temp", None, b"").await;
        assert!(matches!(tool.next().await, Event::Catalog(_)));
        match tool.next().await {
            Event::Opened {
                request_seq,
                handle: for_topic,
                shared: false,
                fds,
                ..
            } => assert_eq!((request_seq, for_topic, fds.len()), (seq, handle, 0)),
            other => panic!("{other:?}"),
        }
        // It still counts as a subscriber.
        assert_eq!(writer.subscribers(), 1);
        assert!(
            !board
                .topics
                .borrow()
                .iter()
                .next()
                .expect("topic")
                .subscribers[0]
                .shared
        );
    });
}
