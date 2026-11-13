//! Buffer topics through the broker: the pool handed over once, hold marks honoured before a
//! subscriber sees a buffer, a replaced pool, and a comm core that restarts under it all.

use std::os::fd::OwnedFd;
use std::sync::Arc;
use std::time::Duration;

use orion_broker::{Broker, BrokerConfig};
use orion_broker_buffers::BrokerBuffers;
use orion_msg_session::{Hello, Refusal, Welcome};
use orion_msg_topics::{
    BufferInfo, BufferOffer, Declare, Field, FieldType, Holds, Opened, Subscribe,
};
use orion_server::mem::{self, MemConnection, MemConnector};
use orion_server::{Comm, Error, FdSet, Limits, Principal, Roles, Security};
use orion_types::TopicRef;
use orion_wire::frame;
use orion_wire::{Body, Bytes, Decode, List, encode_body, kind};
use shm_ring::{FieldId, Reader, SampleTime, Writer};
use shm_ring_buffers::{BufferDesc, BufferReader, BufferWriter, Held, NextBatch, Pool, PoolView};
use shm_ring_doorbell::Doorbell;
use shm_ring_holds::{HoldTable, Limits as HoldLimits, Marks};

const USER: Principal = Principal::Local {
    uid: 1000,
    gid: 1000,
};
const FRAME: usize = 4096;
const TOPIC: &str = "sensor0.data";

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
    comm.add_service(BrokerBuffers::new(&broker, config.declare));
    comm.add_service(broker);
    let (carrier, connector) = mem::carrier(Roles::CLIENT.and(Roles::PROVIDER), 256 * 1024);
    let serving = comm.clone();
    tokio::task::spawn_local(async move { serving.serve(carrier).await });
    (comm, connector)
}

/// A connection, with the last frame received kept for decoding.
struct Client {
    connection: MemConnection,
    buf: Vec<u8>,
    len: usize,
    fds: Vec<OwnedFd>,
    seq: u16,
}

impl Client {
    async fn open(connector: &MemConnector) -> Self {
        let mut client = Self {
            connection: connector.connect(USER),
            buf: vec![0; 16 * 1024],
            len: 0,
            fds: Vec::new(),
            seq: 0,
        };
        let hello = Hello {
            name: "test",
            roles: Roles::CLIENT.and(Roles::PROVIDER),
            max_frame: 16 * 1024,
            revision: 1,
        };
        client.send(&hello, &[]).await;
        assert_eq!(client.next().await, kind::WELCOME);
        let _: Welcome = client.body();
        client
    }

    async fn send<B: Body>(&mut self, body: &B, fds: &[OwnedFd]) -> u16 {
        self.seq += 1;
        let mut out = [0_u8; 2048];
        let len = encode_body(body, self.seq, &mut out).expect("encode");
        self.connection.send(&out[..len], fds).await.expect("send");
        self.seq
    }

    /// Waits for the next frame and returns its kind.
    async fn next(&mut self) -> u8 {
        let mut set = FdSet::default();
        self.len = self
            .connection
            .recv(&mut self.buf, &mut set)
            .await
            .expect("recv")
            .expect("open");
        self.fds = (0..set.len()).filter_map(|index| set.take(index)).collect();
        frame::decode(&self.buf[..self.len]).expect("frame").kind()
    }

    /// The next frame's kind if one comes without waiting.
    async fn poll(&mut self) -> Option<u8> {
        tokio::time::timeout(Duration::from_millis(1), self.next())
            .await
            .ok()
    }

    fn body<'a, B: Body + Decode<'a>>(&'a self) -> B {
        let frame = frame::decode(&self.buf[..self.len]).expect("frame");
        orion_wire::decode_body(&frame).unwrap_or_else(|_| panic!("kind {:#x}", frame.kind()))
    }
}

/// A camera: a frame topic, a pool and the table of who holds what.
struct Camera {
    client: Client,
    handle: u32,
    writer: BufferWriter,
    table: HoldTable,
    ring: OwnedFd,
    acks: bool,
    now_ms: u64,
    /// The reader slots it lets the broker give its subscribers. Zero for all.
    reader_slots: u64,
}

impl Camera {
    async fn start(
        connector: &MemConnector,
        generation: u32,
        buffers: u32,
        ring: Option<OwnedFd>,
    ) -> Self {
        let mut client = Client::open(connector).await;
        let fields = [Field {
            name: "frame",
            ty: FieldType::Buffers,
            unit: "",
            type_name: "",
        }];
        let declare = Declare {
            topic: TOPIC.parse().expect("topic"),
            fields: List::new(&fields),
            history: 8,
            expiry_ms: 0,
            max_payload: 64,
        };
        let offered: Vec<OwnedFd> = ring.into_iter().collect();
        client.send(&declare, &offered).await;
        assert_eq!(client.next().await, kind::TOPIC_OPENED);
        let opened: Opened = client.body();
        let ring = client.fds.pop().expect("write end");
        let writer = Writer::attach(ring.try_clone().expect("clone")).expect("writer");
        let pool = Pool::in_memory(generation, buffers, FRAME).expect("pool");
        let mut camera = Self {
            client,
            handle: opened.handle,
            writer: BufferWriter::new(writer, FieldId(0), pool),
            table: HoldTable::new(buffers),
            ring,
            acks: true,
            now_ms: 0,
            reader_slots: 0,
        };
        camera.offer().await;
        camera
    }

    /// Sends the broker the pool: its page and its memory.
    async fn offer(&mut self) -> u16 {
        let pool = self.writer.pool();
        let buffers: Vec<BufferInfo> = pool
            .buffers()
            .iter()
            .enumerate()
            .map(|(index, buffer)| BufferInfo {
                index: index as u32,
                size: buffer.size,
                descriptor: buffer.descriptor,
                offset: buffer.offset,
            })
            .collect();
        let mut fds = vec![pool.page_fd().expect("pool page")];
        fds.extend(
            pool.descriptors()
                .map(|fd| fd.try_clone_to_owned().expect("clone")),
        );
        let offer = BufferOffer {
            handle: self.handle,
            generation: pool.generation(),
            buffers: List::new(&buffers),
            reader_slots: self.reader_slots,
        };
        self.client.send(&offer, &fds).await
    }

    /// Switches to a new pool, as a camera changing mode does.
    async fn replace_pool(&mut self, generation: u32, buffers: u32) {
        let pool = Pool::in_memory(generation, buffers, FRAME).expect("pool");
        drop(self.writer.replace_pool(pool));
        // The same readers hold from the new pool: same pages, a table of the new size.
        self.table = HoldTable::new(buffers);
        self.offer().await;
    }

    /// Handles what the broker has sent: marks to honour or forget.
    async fn pump(&mut self) {
        while let Some(kind) = self.client.poll().await {
            if kind != kind::TOPIC_HOLDS {
                continue;
            }
            let holds: Holds = self.client.body();
            let page = self.client.fds.pop();
            self.table
                .remove(holds.subscriber, self.writer.pool().page());
            if let (true, Some(page)) = (holds.present, page) {
                let limits = HoldLimits {
                    max_held: holds.max_held,
                    max_hold: None,
                };
                self.table
                    .add(holds.subscriber, page, limits)
                    .expect("add reader");
                if self.acks {
                    self.client.send(&holds, &[]).await;
                }
            }
        }
    }

    /// Publishes one frame filled with `fill`. `false` if it had to be dropped.
    fn frame(&mut self, fill: u8) -> bool {
        self.now_ms += 8;
        self.table
            .enforce(self.now_ms * 1_000_000, self.writer.pool().page());
        let Ok(buffer) = self.writer.take(&self.table) else {
            return false;
        };
        self.writer.buffer_mut(buffer).expect("free").fill(fill);
        self.writer
            .publish(&[buffer], SampleTime::from_nanos(self.now_ms), &[fill])
            .expect("publish");
        true
    }
}

/// A consumer of frames, with its doorbell and its marks.
struct Viewer {
    client: Client,
    doorbell: Doorbell,
    marks: Arc<Marks>,
    ring: Option<Reader>,
    frames: Option<BufferReader<shm_ring::ReadMap, Arc<Marks>>>,
    slot: Option<u32>,
}

impl Viewer {
    async fn start(connector: &MemConnector, max_held: u32, with_marks: bool) -> Self {
        let client = Client::open(connector).await;
        let mut viewer = Self {
            client,
            doorbell: Doorbell::create().expect("doorbell"),
            marks: Arc::new(Marks::create(1024, 0, max_held).expect("marks")),
            ring: None,
            frames: None,
            slot: None,
        };
        viewer.subscribe(max_held, with_marks).await;
        viewer
    }

    async fn subscribe(&mut self, max_held: u32, with_marks: bool) {
        let subscribe = Subscribe {
            topic: TopicRef::parse(TOPIC).expect("topic"),
            request: Bytes(b"640x480"),
            max_held,
            max_hold_ms: 0,
        };
        let (eventfd, page) = self.doorbell.bell_fds().expect("bell");
        let mut fds = vec![eventfd, page];
        if with_marks {
            fds.push(self.marks.page_fd().expect("marks page"));
        }
        self.client.send(&subscribe, &fds).await;
    }

    /// Handles what the broker has sent: the ring, the reader slot, the pool.
    async fn pump(&mut self) {
        while let Some(kind) = self.client.poll().await {
            match kind {
                kind::TOPIC_OPENED => {
                    let ring = self.client.fds.pop().expect("read end");
                    self.ring = Some(Reader::attach(ring).expect("reader"));
                }
                kind::TOPIC_HOLDS => {
                    let holds: Holds = self.client.body();
                    self.marks.set_slot(holds.subscriber);
                    self.slot = Some(holds.subscriber);
                }
                kind::TOPIC_BUFFER_OFFER => {
                    let mut fds = std::mem::take(&mut self.client.fds);
                    let offer: BufferOffer<'_> = self.client.body();
                    let buffers: Vec<BufferDesc> = offer
                        .buffers
                        .iter()
                        .map(|buffer| {
                            let buffer = buffer.expect("buffer");
                            BufferDesc {
                                descriptor: buffer.descriptor,
                                offset: buffer.offset,
                                size: buffer.size,
                            }
                        })
                        .collect();
                    let page = fds.remove(0);
                    let view = PoolView::attach(page, fds, &buffers).expect("pool view");
                    assert_eq!(view.generation(), offer.generation);
                    match (&mut self.frames, self.ring.take()) {
                        (Some(frames), _) => frames.set_pool(view, self.marks.clone()),
                        (None, Some(ring)) => {
                            self.frames = Some(BufferReader::new(
                                ring,
                                FieldId(0),
                                view,
                                self.marks.clone(),
                            ));
                        }
                        (None, None) => panic!("a pool before a ring"),
                    }
                }
                _ => {}
            }
        }
    }

    /// The newest frame there is, held, with its fill byte.
    fn newest(&mut self) -> Option<(u8, Held<Arc<Marks>>)> {
        let frames = self.frames.as_mut()?;
        let mut newest = None;
        for _ in 0..64 {
            match frames.next() {
                NextBatch::Batch(frame) => newest = Some((frame.meta[0], frame.held)),
                NextBatch::Empty => break,
                _ => {}
            }
        }
        newest
    }
}

fn whole(held: &Held<Arc<Marks>>, fill: u8) -> bool {
    held.bytes(0)
        .expect("bytes")
        .iter()
        .all(|byte| *byte == fill)
}

#[test]
fn frames_reach_subscribers_early_and_late_and_holds_are_honoured() {
    run(async {
        let (comm, connector) = board();
        let mut camera = Camera::start(&connector, 1, 3, None).await;
        let mut first = Viewer::start(&connector, 2, true).await;
        camera.pump().await;
        first.pump().await;
        assert_eq!(first.slot, Some(0));

        assert!(camera.frame(1));
        let (fill, held) = first.newest().expect("a frame");
        assert_eq!(fill, 1);
        // The camera goes on. The frame the viewer holds is never written over.
        for fill in 2..40 {
            assert!(camera.frame(fill));
            assert!(whole(&held, 1));
        }

        // A viewer that comes late gets the pool that is current, and the next frame.
        let mut late = Viewer::start(&connector, 2, true).await;
        camera.pump().await;
        late.pump().await;
        assert_eq!(late.slot, Some(1));
        assert!(camera.frame(40));
        let (fill, late_held) = late.newest().expect("a frame");
        assert_eq!(fill, 40);
        assert!(whole(&late_held, 40) && whole(&held, 1));

        // Per frame the broker did nothing: every request it saw was set-up.
        let requests = comm.stats().requests;
        for fill in 41..200 {
            camera.frame(fill);
            late.newest();
        }
        assert_eq!(comm.stats().requests, requests);

        // A viewer that leaves has its marks forgotten: its buffer comes back.
        drop(held);
        drop(first);
        camera.pump().await;
        assert_eq!(camera.table.readers(), 1);
    });
}

#[test]
fn a_subscriber_is_not_given_buffers_until_the_publisher_honours_its_marks() {
    run(async {
        let (_comm, connector) = board();
        let mut camera = Camera::start(&connector, 1, 3, None).await;
        // A publisher that has the marks but has not said so yet.
        camera.acks = false;
        let mut viewer = Viewer::start(&connector, 2, true).await;
        camera.pump().await;
        viewer.pump().await;
        assert!(viewer.ring.is_some(), "it has the ring");
        assert!(
            viewer.frames.is_none() && viewer.slot.is_none(),
            "but no pool yet"
        );
        assert_eq!(camera.table.readers(), 1);

        // Now it says so: the viewer is told its slot and handed the pool.
        let ack = Holds {
            handle: camera.handle,
            subscriber: 0,
            max_held: 0,
            max_hold_ms: 0,
            present: true,
        };
        camera.client.send(&ack, &[]).await;
        viewer.pump().await;
        assert_eq!(viewer.slot, Some(0));
        assert!(camera.frame(7));
        assert_eq!(viewer.newest().expect("a frame").0, 7);

        // A viewer that only passes descriptors on sends no marks. It holds nothing, so it is
        // handed the pool as it is.
        let mut passer = Viewer::start(&connector, 0, false).await;
        camera.pump().await;
        passer.pump().await;
        assert!(passer.frames.is_some() && passer.slot.is_none());
        assert_eq!(camera.table.readers(), 1);
    });
}

#[test]
fn a_publisher_keeps_reader_slots_for_readers_of_its_own() {
    run(async {
        let (_comm, connector) = board();
        let mut camera = Camera::start(&connector, 1, 3, None).await;
        let mut viewer = Viewer::start(&connector, 2, true).await;
        camera.pump().await;
        viewer.pump().await;
        assert_eq!(viewer.slot, Some(0), "any slot, by default");

        // The camera serves readers of its own in slots 0 and 1 and offers again, letting the
        // broker use only 2 and 3. The viewer moves: the camera is told slot 0 is gone and
        // given its marks under slot 2, and the viewer is told its new slot.
        camera.reader_slots = 0b1100;
        camera.offer().await;
        camera.pump().await;
        viewer.pump().await;
        assert_eq!(viewer.slot, Some(2));
        assert_eq!(camera.table.readers(), 1);
        assert!(camera.frame(5));
        assert_eq!(viewer.newest().expect("a frame").0, 5);

        // A second viewer gets the other allowed slot, and a third none at all: it still has
        // the ring, and is never handed buffers the camera does not protect.
        let mut second = Viewer::start(&connector, 2, true).await;
        camera.pump().await;
        second.pump().await;
        assert_eq!(second.slot, Some(3));
        let mut third = Viewer::start(&connector, 2, true).await;
        camera.pump().await;
        third.pump().await;
        assert!(third.ring.is_some() && third.slot.is_none() && third.frames.is_none());
        assert_eq!(camera.table.readers(), 2);
    });
}

#[test]
fn a_subscriber_that_comes_before_the_camera_gets_the_pool_when_it_is_offered() {
    run(async {
        let (_comm, connector) = board();
        let mut viewer = Viewer::start(&connector, 2, true).await;
        viewer.pump().await;
        assert!(viewer.ring.is_none(), "nothing declared yet");

        let mut camera = Camera::start(&connector, 1, 3, None).await;
        camera.pump().await;
        viewer.pump().await;
        assert!(camera.frame(3));
        assert_eq!(viewer.newest().expect("a frame").0, 3);
    });
}

#[test]
fn a_replaced_pool_is_handed_to_every_subscriber() {
    run(async {
        let (_comm, connector) = board();
        let mut camera = Camera::start(&connector, 1, 3, None).await;
        let mut viewer = Viewer::start(&connector, 2, true).await;
        camera.pump().await;
        viewer.pump().await;
        assert!(camera.frame(1));
        let (_, old) = viewer.newest().expect("a frame");

        // The camera changes mode: a new pool, a higher generation.
        camera.replace_pool(2, 5).await;
        // The publisher still has every reader's page. It re-adds them itself.
        let page = viewer.marks.page_fd().expect("marks page");
        camera
            .table
            .add(0, page, HoldLimits::default())
            .expect("add reader");
        viewer.pump().await;
        assert!(camera.frame(2));
        let (fill, new) = viewer.newest().expect("a frame of the new pool");
        assert_eq!(fill, 2);
        // The frame of the old pool is still what it was, for as long as it is held.
        assert!(whole(&old, 1) && whole(&new, 2));

        // An older pool offered again is refused.
        let pool = Pool::in_memory(1, 3, FRAME).expect("pool");
        drop(camera.writer.replace_pool(pool));
        let seq = camera.offer().await;
        assert_eq!(camera.client.next().await, kind::REFUSAL);
        let refusal: Refusal = camera.client.body();
        assert_eq!(
            (refusal.request_seq, refusal.error()),
            (seq, Some(Error::OutOfOrder))
        );
    });
}

#[test]
fn only_the_publisher_may_offer_a_pool() {
    run(async {
        let (_comm, connector) = board();
        let camera = Camera::start(&connector, 1, 3, None).await;
        let mut other = Client::open(&connector).await;
        let pool = Pool::in_memory(9, 2, FRAME).expect("pool");
        let buffers = [BufferInfo {
            index: 0,
            size: 4096,
            descriptor: 0,
            offset: 0,
        }];
        let fds = [
            pool.page_fd().expect("page"),
            pool.descriptors()
                .next()
                .expect("memory")
                .try_clone_to_owned()
                .expect("clone"),
        ];
        let offer = |handle| BufferOffer {
            handle,
            generation: 9,
            buffers: List::new(&buffers),
            reader_slots: 0,
        };
        let seq = other.send(&offer(camera.handle), &fds).await;
        assert_eq!(other.next().await, kind::REFUSAL);
        let refusal: Refusal = other.body();
        assert_eq!(
            (refusal.request_seq, refusal.error()),
            (seq, Some(Error::Denied))
        );
        other.send(&offer(4242), &fds).await;
        assert_eq!(other.next().await, kind::REFUSAL);
        assert_eq!(other.body::<Refusal>().error(), Some(Error::NotFound));
        // A pool with nothing attached is not a pool.
        let mut camera = camera;
        camera.client.send(&offer(camera.handle), &[]).await;
        assert_eq!(camera.client.next().await, kind::REFUSAL);
        assert_eq!(
            camera.client.body::<Refusal>().error(),
            Some(Error::Malformed)
        );
    });
}

#[test]
fn a_comm_core_restart_does_not_interrupt_frames() {
    run(async {
        let (first, connector) = board();
        let mut camera = Camera::start(&connector, 1, 4, None).await;
        let mut viewer = Viewer::start(&connector, 2, true).await;
        camera.pump().await;
        viewer.pump().await;
        assert!(camera.frame(1));
        let (_, held) = viewer.newest().expect("a frame");

        // The comm core goes away. Both programs lose their connections and nothing else.
        drop(first);
        drop(connector);
        let mut fill = 1_u8;
        for _ in 0..50 {
            fill += 1;
            assert!(camera.frame(fill), "no frame dropped");
            let (seen, frame) = viewer.newest().expect("frames keep coming");
            assert_eq!(seen, fill);
            assert!(whole(&frame, fill));
            assert!(whole(&held, 1), "and what was held stays held");
        }

        // A new comm core. The camera declares again with the ring it has and offers its pool
        // again; the viewer subscribes again with the doorbell and marks it has.
        let (_second, connector) = board();
        let ring = camera.ring.try_clone().expect("clone");
        let pool_generation = camera.writer.pool().generation();
        let mut rejoined = Camera::rejoin(&connector, camera, ring).await;
        assert_eq!(rejoined.writer.pool().generation(), pool_generation);
        viewer.client = Client::open(&connector).await;
        viewer.subscribe(2, true).await;
        rejoined.pump().await;
        viewer.pump().await;
        // Through the reconnecting, and after it, frames never stopped and the held one held.
        for _ in 0..50 {
            fill += 1;
            assert!(rejoined.frame(fill));
            let (seen, frame) = viewer.newest().expect("frames keep coming");
            assert_eq!(seen, fill);
            assert!(whole(&frame, fill) && whole(&held, 1));
        }
        // And a viewer that is new to the restarted comm core gets the same pool.
        let mut late = Viewer::start(&connector, 2, true).await;
        rejoined.pump().await;
        late.pump().await;
        assert!(rejoined.frame(200));
        assert_eq!(late.newest().expect("a frame").0, 200);
    });
}

impl Camera {
    /// Reconnects a camera that kept running: the same ring, the same pool, the same table.
    async fn rejoin(connector: &MemConnector, mut camera: Self, ring: OwnedFd) -> Self {
        let mut client = Client::open(connector).await;
        let fields = [Field {
            name: "frame",
            ty: FieldType::Buffers,
            unit: "",
            type_name: "",
        }];
        let declare = Declare {
            topic: TOPIC.parse().expect("topic"),
            fields: List::new(&fields),
            history: 8,
            expiry_ms: 0,
            max_payload: 64,
        };
        client.send(&declare, &[ring]).await;
        assert_eq!(client.next().await, kind::TOPIC_OPENED);
        camera.handle = client.body::<Opened>().handle;
        camera.client = client;
        camera.offer().await;
        camera
    }
}
