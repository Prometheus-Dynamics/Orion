//! The harness for the server's tests: a board on the in-memory carrier, a probe service and a
//! client that speaks frames by hand.

#![allow(dead_code)]

pub use std::cell::RefCell;
pub use std::os::fd::OwnedFd;
pub use std::rc::Rc;
pub use std::time::Duration;

pub use orion_msg_session::{Hello, Ping, Pong, Reject, RejectReason, Welcome};
pub use orion_server::Dropped;
pub use orion_server::mem::{self, MemConnection, MemConnector};
pub use orion_server::{
    Admits, Checked, Comm, Cx, Delivery, Error, FdSet, Incoming, Limits, Policy, Principal, Roles,
    Security, Service, SessionId, Sessions, What,
};
pub use orion_wire::frame::{self, FrameHeader};
pub use orion_wire::{Body, Decode, FrameView, encode_body, kind};

pub const ROOT: Principal = Principal::Local { uid: 0, gid: 0 };
pub const USER: Principal = Principal::Local {
    uid: 1000,
    gid: 1000,
};

/// Runs `test` on one thread with a paused clock, as the comm core runs.
pub fn run(test: impl Future<Output = ()>) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .expect("runtime");
    tokio::task::LocalSet::new().block_on(&runtime, test);
}

/// What the test service saw.
#[derive(Default)]
pub struct Seen {
    pub closed: Vec<SessionId>,
    pub callers: Vec<Principal>,
    pub fds: usize,
    pub watcher: Option<SessionId>,
}

/// A service on the record kinds, one behaviour each.
pub struct Probe(Rc<RefCell<Seen>>);

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
pub struct PingLike(u64);

impl<'de> Decode<'de> for PingLike {
    fn decode(bytes: &'de [u8]) -> Result<Self, Error> {
        Ping::decode(bytes).map(|ping| Self(ping.now_ms))
    }
}

pub struct Board {
    pub comm: Comm,
    pub connector: MemConnector,
    pub seen: Rc<RefCell<Seen>>,
}

pub fn board(security: Security, limits: Limits) -> Board {
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
pub struct Client {
    pub connection: MemConnection,
    pub out: [u8; 1024],
    pub buf: [u8; 4096],
    pub len: usize,
    pub fds: FdSet,
    pub seq: u16,
}

impl Client {
    pub fn new(board: &Board, principal: Principal) -> Self {
        Self::over(board.connector.connect(principal))
    }

    /// A program on the board, over its socket, before it says hello.
    pub fn new_local(board: &Board, principal: Principal) -> Self {
        Self::over(board.connector.connect_local(principal))
    }

    pub fn over(connection: MemConnection) -> Self {
        Self {
            connection,
            out: [0; 1024],
            buf: [0; 4096],
            len: 0,
            fds: FdSet::default(),
            seq: 0,
        }
    }

    /// A program on the board, over its socket.
    pub async fn open_local(board: &Board, principal: Principal) -> Self {
        let mut client = Self::new_local(board, principal);
        let welcome: Welcome = client.hello(Roles::CLIENT).await.expect("welcome");
        assert_eq!(welcome.node.as_str(), "board");
        client
    }

    pub async fn open(board: &Board, principal: Principal) -> Self {
        let mut client = Self::new(board, principal);
        let welcome: Welcome = client.hello(Roles::CLIENT).await.expect("welcome");
        assert_eq!(welcome.node.as_str(), "board");
        client
    }

    pub async fn hello<'a, B: Body + Decode<'a>>(&'a mut self, roles: Roles) -> Option<B> {
        self.hello_at(roles, orion_wire::INTERFACE_REVISION).await
    }

    pub async fn hello_at<'a, B: Body + Decode<'a>>(
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

    pub async fn send<B: Body>(&mut self, body: &B) -> u16 {
        self.seq += 1;
        let len = encode_body(body, self.seq, &mut self.out).expect("encode");
        self.connection
            .send(&self.out[..len], &[])
            .await
            .expect("send");
        self.seq
    }

    /// A frame of `kind` holding one number.
    pub async fn ask(&mut self, kind: u8, number: u64, fds: &[OwnedFd]) -> u16 {
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
    pub async fn next(&mut self) -> Option<FrameView<'_>> {
        self.fds = FdSet::default();
        self.len = self
            .connection
            .recv(&mut self.buf, &mut self.fds)
            .await
            .expect("recv")?;
        Some(frame::decode(&self.buf[..self.len]).expect("a whole frame"))
    }

    pub async fn recv<'a, B: Body + Decode<'a>>(&'a mut self) -> Option<B> {
        let frame = self.next().await?;
        Some(orion_wire::decode_body(&frame).unwrap_or_else(|_| panic!("kind {:#x}", frame.kind())))
    }

    /// Asks and waits for the answer.
    pub async fn round_trip(&mut self, number: u64) -> u64 {
        self.ask(kind::GET, number, &[]).await;
        self.recv::<Pong>().await.expect("answer").now_ms
    }

    pub async fn is_closed(&mut self) -> bool {
        self.next().await.is_none()
    }
}

pub fn whys(board: &Board) -> Vec<Dropped> {
    board
        .comm
        .drops()
        .into_iter()
        .map(|drop| drop.why)
        .collect()
}

/// A frame of `kind` holding one number, sequenced by it.
pub fn ask_frame(kind: u8, number: u64) -> Vec<u8> {
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
