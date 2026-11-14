//! The server over the in-memory carrier: sessions, the check, deadlines, and sessions that
//! misbehave.

use std::cell::RefCell;
use std::os::fd::OwnedFd;
use std::rc::Rc;
use std::time::Duration;

use orion_measure::CountingAlloc;
use orion_measure::thread_counts;
use orion_msg_session::{Hello, Ping, Pong, Refusal, Reject, RejectReason, Welcome};
use orion_server::mem::{self, MemConnection, MemConnector};
use orion_server::{
    Admits, Checked, Comm, Cx, Delivery, Error, FdSet, Incoming, Limits, Policy, Principal, Roles,
    Security, Service, SessionId, Sessions, What,
};
use orion_wire::frame::{self, FrameHeader};
use orion_wire::{Body, Decode, FrameView, encode_body, kind};

#[global_allocator]
static GLOBAL: CountingAlloc = CountingAlloc::system();

const ROOT: Principal = Principal::Local { uid: 0, gid: 0 };
const USER: Principal = Principal::Local {
    uid: 1000,
    gid: 1000,
};

/// Runs `test` on one thread with a paused clock, as the comm core runs.
fn run(test: impl Future<Output = ()>) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .expect("runtime");
    tokio::task::LocalSet::new().block_on(&runtime, test);
}

/// What the test service saw.
#[derive(Default)]
struct Seen {
    closed: Vec<SessionId>,
    callers: Vec<Principal>,
    fds: usize,
    watcher: Option<SessionId>,
}

/// A service on the record kinds, one behaviour each.
struct Probe(Rc<RefCell<Seen>>);

impl Service for Probe {
    fn kinds(&self) -> &'static [u8] {
        &[
            kind::GET,
            kind::PUT,
            kind::WATCH,
            kind::CHANGED,
            kind::FILE_BEGIN,
            kind::FILE_CHUNK,
        ]
    }

    fn asks<'a>(&self, frame: &FrameView<'a>) -> Result<(What<'a>, Policy), Error> {
        match frame.kind() {
            // Anyone may read; only root may write.
            kind::PUT => Ok((What::Write("probe"), Policy::new(Admits::ROOT))),
            kind::FILE_BEGIN if frame.payload().is_empty() => Err(Error::Malformed),
            _ => Ok((What::Read("probe"), Policy::new(Admits::ANYONE))),
        }
    }

    fn handle(&mut self, request: Checked<Incoming<'_>>, cx: &mut Cx<'_>) -> Result<(), Error> {
        let mut seen = self.0.borrow_mut();
        match request.get().kind() {
            // Answers with the number it was sent.
            kind::GET | kind::PUT => {
                seen.callers.push(request.who().clone());
                let ping: PingLike = PingLike::decode(request.get().frame().payload())?;
                cx.reply(&Pong { now_ms: ping.0 })
            }
            // Remembers the caller, and takes a descriptor if one came.
            kind::WATCH => {
                seen.watcher = Some(cx.from());
                let mut incoming = request.into_inner();
                match incoming.take_fd(0) {
                    Some(fd) => {
                        seen.fds += incoming.fds();
                        cx.reply_with_fds(&Pong { now_ms: 7 }, [fd])
                    }
                    None => Ok(()),
                }
            }
            kind::CHANGED => Err(Error::Busy),
            kind::FILE_BEGIN => panic!("a request this service cannot survive"),
            // Sends to the watcher, a frame that may be dropped.
            _ => {
                let watcher = seen.watcher.ok_or(Error::NotFound)?;
                let now_ms = cx.now_ms();
                let _ = cx
                    .sessions()
                    .send(watcher, &Pong { now_ms }, Delivery::MayDrop);
                Ok(())
            }
        }
    }

    fn closed(&mut self, session: SessionId, sessions: &mut Sessions) {
        assert!(sessions.get(session).is_none(), "already out of the table");
        self.0.borrow_mut().closed.push(session);
    }
}

/// A body of one number, for the probe's requests.
struct PingLike(u64);

impl<'de> Decode<'de> for PingLike {
    fn decode(bytes: &'de [u8]) -> Result<Self, Error> {
        Ping::decode(bytes).map(|ping| Self(ping.now_ms))
    }
}

struct Board {
    comm: Comm,
    connector: MemConnector,
    seen: Rc<RefCell<Seen>>,
}

fn board(security: Security, limits: Limits) -> Board {
    let comm = Comm::new("board".parse().expect("name"), security, None, limits);
    let seen = Rc::new(RefCell::new(Seen::default()));
    comm.add_service(Probe(seen.clone()));
    let roles = Roles::CLIENT.and(Roles::PROVIDER);
    let (carrier, connector) = mem::carrier(roles, 16 * 1024);
    let serving = comm.clone();
    tokio::task::spawn_local(async move { serving.serve(carrier).await });
    Board {
        comm,
        connector,
        seen,
    }
}

/// A client: one end of a connection, one buffer each way.
struct Client {
    connection: MemConnection,
    out: [u8; 1024],
    buf: [u8; 4096],
    len: usize,
    fds: FdSet,
    seq: u16,
}

impl Client {
    fn new(board: &Board, principal: Principal) -> Self {
        Self {
            connection: board.connector.connect(principal),
            out: [0; 1024],
            buf: [0; 4096],
            len: 0,
            fds: FdSet::default(),
            seq: 0,
        }
    }

    async fn open(board: &Board, principal: Principal) -> Self {
        let mut client = Self::new(board, principal);
        let welcome: Welcome = client.hello(Roles::CLIENT).await.expect("welcome");
        assert_eq!(welcome.node.as_str(), "board");
        client
    }

    async fn hello<'a, B: Body + Decode<'a>>(&'a mut self, roles: Roles) -> Option<B> {
        self.hello_at(roles, orion_wire::INTERFACE_REVISION).await
    }

    async fn hello_at<'a, B: Body + Decode<'a>>(
        &'a mut self,
        roles: Roles,
        revision: u16,
    ) -> Option<B> {
        let hello = Hello {
            name: "test",
            roles,
            max_frame: 4096,
            revision,
            level: 0,
        };
        self.send(&hello).await;
        self.recv().await
    }

    async fn send<B: Body>(&mut self, body: &B) -> u16 {
        self.seq += 1;
        let len = encode_body(body, self.seq, &mut self.out).expect("encode");
        self.connection
            .send(&self.out[..len], &[])
            .await
            .expect("send");
        self.seq
    }

    /// A frame of `kind` holding one number.
    async fn ask(&mut self, kind: u8, number: u64, fds: &[OwnedFd]) -> u16 {
        self.seq += 1;
        let mut payload = [0_u8; 16];
        let used = orion_wire::body::encode_fields(&Ping { now_ms: number }, &mut payload)
            .expect("fields");
        let header = FrameHeader::new(kind, self.seq);
        let len = frame::encode(header, &payload[..used], &mut self.out).expect("frame");
        self.connection
            .send(&self.out[..len], fds)
            .await
            .expect("send");
        self.seq
    }

    /// The next frame, if the connection is still open.
    async fn next(&mut self) -> Option<FrameView<'_>> {
        self.fds = FdSet::default();
        self.len = self
            .connection
            .recv(&mut self.buf, &mut self.fds)
            .await
            .expect("recv")?;
        Some(frame::decode(&self.buf[..self.len]).expect("a whole frame"))
    }

    async fn recv<'a, B: Body + Decode<'a>>(&'a mut self) -> Option<B> {
        let frame = self.next().await?;
        Some(orion_wire::decode_body(&frame).unwrap_or_else(|_| panic!("kind {:#x}", frame.kind())))
    }

    /// Asks and waits for the answer.
    async fn round_trip(&mut self, number: u64) -> u64 {
        self.ask(kind::GET, number, &[]).await;
        self.recv::<Pong>().await.expect("answer").now_ms
    }

    async fn is_closed(&mut self) -> bool {
        self.next().await.is_none()
    }
}

#[test]
fn a_session_opens_pings_and_reaches_a_service() {
    run(async {
        let board = board(Security::Off, Limits::default());
        let mut client = Client::new(&board, USER);
        let welcome: Welcome = client.hello(Roles::CLIENT).await.expect("welcome");
        assert_eq!(
            (
                welcome.heartbeat_ms,
                welcome.max_frame,
                welcome.revision,
                welcome.idle_ms,
                welcome.level,
            ),
            (
                2000,
                4096,
                orion_wire::INTERFACE_REVISION,
                6000,
                orion_wire::INTERFACE_LEVEL
            )
        );
        let session = SessionId(welcome.session);

        client.send(&Ping { now_ms: 42 }).await;
        assert_eq!(client.recv::<Pong>().await.expect("pong").now_ms, 42);
        assert_eq!(client.round_trip(9).await, 9);
        assert_eq!(board.seen.borrow().callers, [USER]);

        // A kind nobody answers is refused at once, and the session carries on.
        for unknown in [kind::EXECUTOR_OFFER, 0xEE] {
            let seq = client.ask(unknown, 0, &[]).await;
            let refusal: Refusal = client.recv().await.expect("refusal");
            assert_eq!(
                (refusal.request_seq, refusal.error()),
                (seq, Some(Error::Unsupported))
            );
        }
        assert_eq!(client.round_trip(10).await, 10);

        let info = board
            .comm
            .with(|sessions, _| sessions.get(session).cloned())
            .expect("session");
        assert_eq!(
            (info.name.as_str(), info.roles, info.open),
            ("test", Roles::CLIENT, true)
        );
        let stats = board.comm.stats();
        assert_eq!(
            (stats.opened, stats.requests, stats.unknown, stats.refused),
            (1, 2, 2, 2)
        );

        // Closing the connection closes the session and tells the service.
        drop(client);
        tokio::time::sleep(Duration::from_millis(1)).await;
        assert_eq!(board.seen.borrow().closed, [session]);
        assert!(board.comm.with(|sessions, _| sessions.is_empty()));
    });
}

#[test]
fn a_service_is_only_reached_through_the_check() {
    run(async {
        let board = board(Security::On, Limits::default());
        let mut anonymous = Client::open(&board, Principal::Anonymous).await;
        let mut root = Client::open(&board, ROOT).await;

        // Anyone may read. Only root may write, and the service never sees the refused one.
        assert_eq!(anonymous.round_trip(1).await, 1);
        let seq = anonymous.ask(kind::PUT, 2, &[]).await;
        let refusal: Refusal = anonymous.recv().await.expect("refusal");
        assert_eq!(
            (refusal.request_seq, refusal.error()),
            (seq, Some(Error::Denied))
        );
        root.ask(kind::PUT, 3, &[]).await;
        assert_eq!(root.recv::<Pong>().await.expect("answer").now_ms, 3);
        assert_eq!(board.seen.borrow().callers, [Principal::Anonymous, ROOT]);

        // A service's own error, and a frame it cannot read, come back as refusals too.
        let seq = root.ask(kind::CHANGED, 0, &[]).await;
        let refusal: Refusal = root.recv().await.expect("refusal");
        assert_eq!(
            (refusal.request_seq, refusal.error(), refusal.detail),
            (seq, Some(Error::Busy), "busy")
        );
        let len = frame::encode(FrameHeader::new(kind::FILE_BEGIN, 99), &[], &mut root.out)
            .expect("frame");
        root.connection
            .send(&root.out[..len], &[])
            .await
            .expect("send");
        let refusal: Refusal = root.recv().await.expect("refusal");
        assert_eq!(
            (refusal.request_seq, refusal.error()),
            (99, Some(Error::Malformed))
        );
        assert_eq!(board.comm.stats().refused, 3);
    });
    // With security off the same write goes through for anyone.
    run(async {
        let board = board(Security::Off, Limits::default());
        let mut anonymous = Client::open(&board, Principal::Anonymous).await;
        anonymous.ask(kind::PUT, 5, &[]).await;
        assert_eq!(anonymous.recv::<Pong>().await.expect("answer").now_ms, 5);
    });
}

#[test]
fn connections_that_break_the_protocol_are_turned_away() {
    run(async {
        let limits = Limits {
            max_sessions: 2,
            ..Limits::default()
        };
        let board = board(Security::Off, limits);

        // A frame of another protocol version: told so, then closed.
        let mut old = Client::new(&board, USER);
        let len = encode_body(&Ping { now_ms: 0 }, 0, &mut old.out).expect("frame");
        old.out[0] = 9;
        let crc = orion_wire::crc32c(&old.out[..len - 4]);
        old.out[len - 4..len].copy_from_slice(&crc.to_le_bytes());
        old.connection
            .send(&old.out[..len], &[])
            .await
            .expect("send");
        assert_eq!(
            old.recv::<Reject>().await.expect("reject").reason,
            RejectReason::VersionMismatch
        );
        assert!(old.is_closed().await);

        // Another wire revision, newer or older: told so with both numbers, then closed.
        for other in [
            orion_wire::INTERFACE_REVISION + 1,
            orion_wire::INTERFACE_REVISION - 1,
        ] {
            let mut stranger = Client::new(&board, USER);
            let reject: Reject = stranger
                .hello_at(Roles::CLIENT, other)
                .await
                .expect("reject");
            assert_eq!(
                (reject.reason, reject.node_revision, reject.peer_revision),
                (
                    RejectReason::RevisionMismatch,
                    orion_wire::INTERFACE_REVISION,
                    other
                )
            );
            assert!(stranger.is_closed().await);
        }

        // A role this carrier does not offer.
        let mut node = Client::new(&board, USER);
        let reject: Reject = node.hello(Roles::NODE).await.expect("reject");
        assert_eq!(reject.reason, RejectReason::RoleNotOffered);
        assert!(node.is_closed().await);

        // Anything but a hello first; a second hello; a frame with a bad CRC.
        let mut early = Client::new(&board, USER);
        early.ask(kind::GET, 1, &[]).await;
        assert!(early.is_closed().await);
        let mut twice = Client::open(&board, USER).await;
        assert!(twice.hello::<Welcome>(Roles::CLIENT).await.is_none());
        let mut noisy = Client::open(&board, USER).await;
        let len = encode_body(&Ping { now_ms: 0 }, 0, &mut noisy.out).expect("frame");
        noisy.out[len - 1] ^= 1;
        noisy
            .connection
            .send(&noisy.out[..len], &[])
            .await
            .expect("send");
        assert!(noisy.is_closed().await);

        // No room: the third connection is told to come back.
        let _first = Client::open(&board, USER).await;
        let _second = Client::open(&board, USER).await;
        let mut third = Client::new(&board, USER);
        assert_eq!(
            third.recv::<Reject>().await.expect("reject").reason,
            RejectReason::Busy
        );
        assert!(third.is_closed().await);
        // And one of them leaving makes room.
        drop(_first);
        tokio::time::sleep(Duration::from_millis(1)).await;
        Client::open(&board, USER).await;
    });
}

#[test]
fn deadlines_close_sessions_that_go_quiet() {
    run(async {
        let board = board(Security::Off, Limits::default());
        let started = tokio::time::Instant::now();

        // Never says hello: gone at the hello deadline, not before.
        let mut silent = Client::new(&board, USER);
        assert!(silent.is_closed().await);
        assert_eq!(started.elapsed(), Duration::from_millis(5000));

        // Says hello, then nothing: gone after three heartbeats.
        let mut quiet = Client::open(&board, USER).await;
        let opened = tokio::time::Instant::now();
        assert!(quiet.is_closed().await);
        assert_eq!(opened.elapsed(), Duration::from_millis(6000));

        // Pings keep a session for as long as they come.
        let mut alive = Client::open(&board, USER).await;
        for round in 0..10 {
            tokio::time::sleep(Duration::from_millis(2000)).await;
            alive.send(&Ping { now_ms: round }).await;
            assert_eq!(alive.recv::<Pong>().await.expect("pong").now_ms, round);
        }
        assert_eq!(board.comm.stats().dropped, 2);
    });
}

#[test]
fn comm_idle_ms_sets_how_long_a_session_may_go_quiet() {
    run(async {
        let limits = Limits {
            idle_ms: 20_000,
            ..Limits::default()
        };
        let board = board(Security::Off, limits);

        // The client is told, so it waits as long for the comm core.
        let mut client = Client::new(&board, USER);
        let welcome: Welcome = client.hello(Roles::CLIENT).await.expect("welcome");
        assert_eq!((welcome.heartbeat_ms, welcome.idle_ms), (2000, 20_000));

        // Quiet past three heartbeats, and still there; gone at `comm.idle_ms`.
        let opened = tokio::time::Instant::now();
        tokio::time::sleep(Duration::from_millis(19_000)).await;
        client.send(&Ping { now_ms: 1 }).await;
        assert_eq!(client.recv::<Pong>().await.expect("pong").now_ms, 1);
        let heard = tokio::time::Instant::now();
        assert!(client.is_closed().await);
        assert_eq!(heard.elapsed(), Duration::from_millis(20_000));
        assert!(opened.elapsed() > Duration::from_millis(6000));
    });
}

#[test]
fn a_session_that_misbehaves_cannot_stall_another() {
    run(async {
        let limits = Limits {
            outbox_bytes: 8 * 1024,
            ..Limits::default()
        };
        let board = board(Security::Off, limits);
        let mut good = Client::open(&board, USER).await;

        // One that floods requests and never reads an answer. Its connection holds 16 KiB and
        // its outbox 8 KiB; past that the server stops answering it and closes it.
        let mut flood = Client::open(&board, USER).await;
        let flooding = tokio::task::spawn_local(async move {
            let mut sent = 0_u64;
            while flood
                .connection
                .send(&ask_frame(kind::GET, sent), &[])
                .await
                .is_ok()
            {
                sent += 1;
                if sent.is_multiple_of(64) {
                    tokio::task::yield_now().await;
                }
            }
            sent
        });

        // One that subscribes and then never reads what it is sent.
        let mut deaf = Client::open(&board, USER).await;
        deaf.ask(kind::WATCH, 0, &[]).await;

        // One whose request makes the service panic.
        let mut poison = Client::open(&board, USER).await;
        poison.ask(kind::FILE_BEGIN, 1, &[]).await;
        assert!(poison.is_closed().await);

        // Through all of it the well-behaved session is answered, every time, and each answer
        // takes no time on the clock: nothing it does waits on the others.
        let started = tokio::time::Instant::now();
        for round in 0..6000 {
            assert_eq!(good.round_trip(round).await, round);
            // Each round also has the service send to the deaf session.
            good.ask(kind::FILE_CHUNK, 0, &[]).await;
        }
        assert_eq!(started.elapsed(), Duration::ZERO);

        // The deaf one has only lost frames so far: it is still there.
        let deaf_id = board.seen.borrow().watcher.expect("watcher");
        let dropped = board
            .comm
            .with(|sessions, _| sessions.get(deaf_id).map(|info| info.dropped));
        assert!(dropped.expect("still there") > 500, "{dropped:?}");

        // The flooder is closed for not reading: its answers had nowhere to go. That takes
        // until its write deadline, which also closes the deaf one. The good session keeps
        // pinging meanwhile, as a live one does.
        let mut round = 0;
        while !flooding.is_finished() || round < 5 {
            tokio::time::sleep(Duration::from_millis(1500)).await;
            round += 1;
            assert_eq!(good.round_trip(round).await, round);
        }
        let sent = flooding.await.expect("flooder");
        assert!(sent > 100, "{sent}");
        assert_eq!(board.comm.stats().panics, 1);
        assert_eq!(
            board.seen.borrow().closed.len(),
            3,
            "the flooder, the poisoned one, the deaf one"
        );
        assert_eq!(board.comm.with(|sessions, _| sessions.len()), 1);
        assert_eq!(good.round_trip(77).await, 77);
    });
}

fn ask_frame(kind: u8, number: u64) -> Vec<u8> {
    let mut payload = [0_u8; 16];
    let used =
        orion_wire::body::encode_fields(&Ping { now_ms: number }, &mut payload).expect("fields");
    let mut out = vec![0; 64];
    let len = frame::encode(
        FrameHeader::new(kind, number as u16),
        &payload[..used],
        &mut out,
    )
    .expect("frame");
    out.truncate(len);
    out
}

#[test]
fn a_session_that_stops_taking_frames_is_closed_at_its_write_deadline() {
    run(async {
        let board = board(Security::Off, Limits::default());
        // It asks for more than its connection holds, and reads none of it.
        let mut stuck = Client::open(&board, USER).await;
        for round in 0..2000 {
            if stuck
                .connection
                .send(&ask_frame(kind::GET, round), &[])
                .await
                .is_err()
            {
                break;
            }
            tokio::task::yield_now().await;
        }
        tokio::time::sleep(Duration::from_millis(5001)).await;
        assert_eq!(board.seen.borrow().closed.len(), 1);
        assert!(board.comm.with(|sessions, _| sessions.is_empty()));
    });
}

#[test]
fn descriptors_travel_with_a_request_and_with_its_answer() {
    run(async {
        let board = board(Security::Off, Limits::default());
        let mut client = Client::open(&board, USER).await;
        let file = OwnedFd::from(std::fs::File::open("/proc/self/exe").expect("file"));
        client.ask(kind::WATCH, 0, &[file]).await;
        assert_eq!(client.recv::<Pong>().await.expect("answer").now_ms, 7);
        assert_eq!(client.fds.len(), 1);
        let back = std::fs::File::from(client.fds.take(0).expect("descriptor"));
        assert!(back.metadata().expect("metadata").len() > 0);
    });
}

#[test]
fn a_request_allocates_nothing_once_the_session_is_open() {
    run(async {
        let board = board(Security::On, Limits::default());
        let mut client = Client::open(&board, ROOT).await;
        let mut other = Client::open(&board, USER).await;
        // Warm up with the same mix, so every buffer has grown to what it needs.
        for round in 0..512 {
            client.round_trip(round).await;
            client.send(&Ping { now_ms: round }).await;
            client.recv::<Pong>().await.expect("pong");
            other.ask(kind::PUT, round, &[]).await;
            other.recv::<Refusal>().await.expect("refusal");
            client.ask(kind::PUT, round, &[]).await;
            client.recv::<Pong>().await.expect("answer");
            board.seen.borrow_mut().callers.clear();
        }
        let before = thread_counts();
        for round in 0..10_000 {
            // A request and its answer, a ping, a refusal, and a write that passes the check.
            assert_eq!(client.round_trip(round).await, round);
            client.send(&Ping { now_ms: round }).await;
            assert_eq!(client.recv::<Pong>().await.expect("pong").now_ms, round);
            other.ask(kind::PUT, round, &[]).await;
            assert_eq!(
                other.recv::<Refusal>().await.expect("refusal").error(),
                Some(Error::Denied)
            );
            client.ask(kind::PUT, round, &[]).await;
            client.recv::<Pong>().await.expect("answer");
            // The probe records its callers; keep that from growing.
            board.seen.borrow_mut().callers.clear();
        }
        let allocated = thread_counts().since(before);
        assert_eq!(allocated.allocs, 0, "{allocated:?} in 40,000 requests");
        assert_eq!(board.comm.stats().requests, 2 * 512 + 20_000);
    });
}
