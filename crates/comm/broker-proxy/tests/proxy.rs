//! A publisher that cannot map shared memory, and a reader that can: they agree.

use std::os::fd::OwnedFd;
use std::time::Duration;

use orion_broker::{Broker, BrokerConfig};
use orion_broker_proxy::BrokerProxy;
use orion_measure::{CountingAlloc, thread_counts};
use orion_msg_session::{Hello, Refusal, Welcome};
use orion_msg_topics::{
    BatchSample, Declare, Demand, Field, FieldType, Opened, Sample, SampleBatch, SampleFlags,
    Subscribe,
};
use orion_server::mem::{self, MemConnection, MemConnector};
use orion_server::{Comm, Error, FdSet, Limits, Principal, Roles, Security};
use orion_types::TopicRef;
use orion_wire::frame;
use orion_wire::{Body, Bytes, Decode, List, encode_body, kind};
use shm_ring::{FieldId, Flags, Next, Reader, SampleTime};
use shm_ring_doorbell::{Doorbell, Wake};

#[global_allocator]
static GLOBAL: CountingAlloc = CountingAlloc::system();

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

fn board() -> (Comm, MemConnector) {
    let comm = Comm::new(
        "board".parse().expect("name"),
        Security::Off,
        None,
        Limits::default(),
    );
    let config = BrokerConfig::default();
    let broker = Broker::new("board".parse().expect("name"), config);
    comm.add_service(BrokerProxy::new(&broker, config.declare));
    comm.add_service(broker);
    let (carrier, connector) = mem::carrier(Roles::CLIENT.and(Roles::PROVIDER), 256 * 1024);
    let serving = comm.clone();
    tokio::task::spawn_local(async move { serving.serve(carrier).await });
    (comm, connector)
}

struct Client {
    connection: MemConnection,
    out: [u8; 2048],
    buf: Vec<u8>,
    len: usize,
    fds: FdSet,
    seq: u16,
}

impl Client {
    async fn open(connection: MemConnection) -> Self {
        let mut client = Self {
            connection,
            out: [0; 2048],
            buf: vec![0; 16 * 1024],
            len: 0,
            fds: FdSet::default(),
            seq: 0,
        };
        let hello = Hello {
            name: "test",
            roles: Roles::CLIENT.and(Roles::PROVIDER),
            max_frame: 16 * 1024,
            revision: 1,
        };
        client.send(&hello, &[]).await;
        let _: Welcome = client.recv().await;
        client
    }

    async fn send<B: Body>(&mut self, body: &B, fds: &[OwnedFd]) -> u16 {
        self.seq += 1;
        let len = encode_body(body, self.seq, &mut self.out).expect("encode");
        self.connection
            .send(&self.out[..len], fds)
            .await
            .expect("send");
        self.seq
    }

    /// The next frame's kind, with the frame left in the buffer.
    async fn next(&mut self) -> u8 {
        self.fds = FdSet::default();
        self.len = self
            .connection
            .recv(&mut self.buf, &mut self.fds)
            .await
            .expect("recv")
            .expect("open");
        frame::decode(&self.buf[..self.len]).expect("frame").kind()
    }

    fn body<'a, B: Body + Decode<'a>>(&'a self) -> B {
        let frame = frame::decode(&self.buf[..self.len]).expect("frame");
        orion_wire::decode_body(&frame).unwrap_or_else(|_| panic!("kind {:#x}", frame.kind()))
    }

    async fn recv<'a, B: Body + Decode<'a>>(&'a mut self) -> B {
        self.next().await;
        self.body()
    }

    /// Declares a two-field topic and returns its handle and whether a ring came with it.
    async fn declare(&mut self, topic: &str) -> (u32, bool) {
        let fields = [
            Field {
                name: "x",
                ty: FieldType::Float,
                unit: "",
                type_name: "",
            },
            Field {
                name: "y",
                ty: FieldType::Float,
                unit: "",
                type_name: "",
            },
        ];
        let declare = Declare {
            topic: topic.parse().expect("topic"),
            fields: List::new(&fields),
            history: 64,
            expiry_ms: 0,
            max_payload: 16,
        };
        self.send(&declare, &[]).await;
        let opened: Opened = self.recv().await;
        (opened.handle, opened.shared_memory)
    }

    /// Subscribes with a doorbell and returns a reader of the ring.
    async fn subscribe(&mut self, topic: &str, doorbell: &Doorbell, request: &[u8]) -> Reader {
        let subscribe = Subscribe {
            topic: TopicRef::parse(topic).expect("topic"),
            request: Bytes(request),
            max_held: 0,
            max_hold_ms: 0,
        };
        let (eventfd, page) = doorbell.bell_fds().expect("bell");
        self.send(&subscribe, &[eventfd, page]).await;
        assert_eq!(self.next().await, kind::TOPIC_DECLARE);
        let opened: Opened = self.recv().await;
        assert!(opened.shared_memory);
        Reader::attach(self.fds.take(0).expect("read end")).expect("reader")
    }

    async fn sample(&mut self, handle: u32, field: u16, time: u64, value: f64) -> u16 {
        let sample = Sample {
            handle,
            field,
            time: SampleTime::from_nanos(time),
            flags: SampleFlags::OTHERS_DEPEND,
            payload: Bytes(&value.to_le_bytes()),
        };
        self.send(&sample, &[]).await
    }

    async fn refused(&mut self, seq: u16) -> Option<Error> {
        let refusal: Refusal = self.recv().await;
        assert_eq!(refusal.request_seq, seq);
        refusal.error()
    }
}

/// Lets the server handle what was sent.
async fn settle() {
    tokio::time::sleep(Duration::from_millis(1)).await;
}

fn read(reader: &mut Reader) -> Option<(u16, u64, f64, u8)> {
    match reader.next() {
        Next::Sample(sample) => Some((
            sample.field.0,
            sample.time.as_nanos(),
            f64::from_le_bytes(sample.payload.try_into().expect("eight bytes")),
            sample.flags.0,
        )),
        _ => None,
    }
}

#[test]
fn a_publisher_without_shared_memory_and_a_reader_with_it_agree() {
    run(async {
        let (_comm, connector) = board();
        // The publisher is on a connection that cannot carry descriptors, as a remote one is.
        let mut remote = Client::open(connector.connect_without_descriptors(USER)).await;
        let (handle, shared) = remote.declare("pose").await;
        assert!(!shared, "no ring for a program that cannot map one");

        let mut local = Client::open(connector.connect(USER)).await;
        let mut doorbell = Doorbell::create().expect("doorbell");
        let mut reader = local.subscribe("pose", &doorbell, b"30Hz").await;

        // The publisher hears about its subscriber: how many, and what it asked for. That is
        // all it needs to start or stop sampling. It is sent no doorbell: it could not ring it.
        let demand: Demand = remote.recv().await;
        assert_eq!(
            (demand.handle, demand.subscribers, demand.request.0),
            (handle, 1, &b"30Hz"[..])
        );

        // Samples sent as messages land in the ring exactly as a local publisher's would:
        // field, time, flags and value.
        for round in 0..50_u64 {
            remote
                .sample(handle, (round % 2) as u16, 1000 + round, round as f64 * 0.5)
                .await;
        }
        settle().await;
        for round in 0..50_u64 {
            let flags = Flags::OTHERS_DEPEND.0;
            assert_eq!(
                read(&mut reader),
                Some(((round % 2) as u16, 1000 + round, round as f64 * 0.5, flags))
            );
        }
        assert_eq!(read(&mut reader), None);
        let newest = reader.latest(FieldId(1)).expect("latest");
        assert_eq!(newest.time, SampleTime::from_nanos(1049));

        // A sleeping subscriber is woken by a sample published on its behalf.
        doorbell.arm();
        remote.sample(handle, 0, 2000, 9.0).await;
        settle().await;
        doorbell.disarm();
        assert_eq!(
            doorbell.wait(Some(Duration::ZERO), || true).expect("wait"),
            Wake::Ready
        );
        assert_eq!(read(&mut reader), Some((0, 2000, 9.0, 1)));

        // A batch is its samples, in order.
        let values: Vec<[u8; 8]> = (0..5).map(|index| f64::from(index).to_le_bytes()).collect();
        let samples: Vec<BatchSample<'_>> = values
            .iter()
            .enumerate()
            .map(|(index, value)| BatchSample {
                field: 1,
                time: SampleTime::from_nanos(3000 + index as u64),
                flags: SampleFlags::NONE,
                payload: Bytes(value),
            })
            .collect();
        remote
            .send(
                &SampleBatch {
                    handle,
                    samples: List::new(&samples),
                },
                &[],
            )
            .await;
        settle().await;
        for index in 0..5_u64 {
            assert_eq!(read(&mut reader), Some((1, 3000 + index, index as f64, 0)));
        }

        // The subscriber leaves: the publisher is told nobody listens.
        drop(local);
        let demand: Demand = remote.recv().await;
        assert_eq!((demand.subscribers, demand.request.0.len()), (0, 0));
    });
}

#[test]
fn only_a_topics_own_publisher_without_a_ring_may_send_its_samples() {
    run(async {
        let (_comm, connector) = board();
        let mut remote = Client::open(connector.connect_without_descriptors(USER)).await;
        let (handle, _) = remote.declare("pose").await;
        let mut local = Client::open(connector.connect(USER)).await;
        let doorbell = Doorbell::create().expect("doorbell");
        let mut reader = local.subscribe("pose", &doorbell, b"").await;
        let _: Demand = remote.recv().await;

        // Someone else's topic.
        let mut other = Client::open(connector.connect_without_descriptors(USER)).await;
        let seq = other.sample(handle, 0, 1, 1.0).await;
        assert_eq!(other.refused(seq).await, Some(Error::Denied));
        // A topic that is not there.
        let seq = other.sample(9999, 0, 1, 1.0).await;
        assert_eq!(other.refused(seq).await, Some(Error::NotFound));
        // A publisher that was given the ring writes it itself: a ring has one writer.
        let (own, shared) = local.declare("local.topic").await;
        assert!(shared);
        let seq = local.sample(own, 0, 1, 1.0).await;
        assert_eq!(local.refused(seq).await, Some(Error::Denied));

        // A field or a size outside the declaration is refused, and the publisher is told.
        let seq = remote.sample(handle, 2, 1, 1.0).await;
        assert_eq!(remote.refused(seq).await, Some(Error::Malformed));
        let big = Sample {
            handle,
            field: 0,
            time: SampleTime::ZERO,
            flags: SampleFlags::NONE,
            payload: Bytes(&[0; 17]),
        };
        let seq = remote.send(&big, &[]).await;
        assert_eq!(remote.refused(seq).await, Some(Error::Malformed));
        // None of that reached the ring.
        assert_eq!(read(&mut reader), None);

        // The publisher reconnects and declares again: it carries on in the same ring.
        remote.sample(handle, 0, 5, 5.0).await;
        settle().await;
        drop(remote);
        let mut remote = Client::open(connector.connect_without_descriptors(USER)).await;
        let (again, _) = remote.declare("pose").await;
        assert_eq!(again, handle);
        let _: Demand = remote.recv().await;
        remote.sample(handle, 0, 6, 6.0).await;
        settle().await;
        assert_eq!(read(&mut reader), Some((0, 5, 5.0, 1)));
        assert_eq!(read(&mut reader), Some((0, 6, 6.0, 1)));
    });
}

#[test]
fn a_sample_published_on_behalf_allocates_nothing() {
    run(async {
        let (comm, connector) = board();
        let mut remote = Client::open(connector.connect_without_descriptors(USER)).await;
        let (handle, _) = remote.declare("pose").await;
        let mut local = Client::open(connector.connect(USER)).await;
        let doorbell = Doorbell::create().expect("doorbell");
        let mut reader = local.subscribe("pose", &doorbell, b"").await;
        let _: Demand = remote.recv().await;

        let mut round = 0_u64;
        let mut read_back = 0_u64;
        let mut step = async |remote: &mut Client, reader: &mut Reader| {
            round += 1;
            remote.sample(handle, (round % 2) as u16, round, 1.5).await;
            tokio::task::yield_now().await;
            tokio::task::yield_now().await;
            while let Some((_, time, _, _)) = read(reader) {
                read_back += u64::from(time > 0);
            }
        };
        for _ in 0..512 {
            step(&mut remote, &mut reader).await;
        }
        let before = thread_counts();
        for _ in 0..20_000 {
            step(&mut remote, &mut reader).await;
        }
        let allocated = thread_counts().since(before);
        assert_eq!(allocated.allocs, 0, "{allocated:?} in 20,000 samples");
        assert_eq!(read_back, 20_512, "every sample reached the ring");
        assert_eq!(comm.stats().refused, 0);
    });
}
