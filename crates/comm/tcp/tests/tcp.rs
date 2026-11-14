//! Two comm cores over loopback, security on and off.

use std::cell::RefCell;
use std::net::SocketAddr;
use std::rc::Rc;
use std::time::{Duration, Instant};

use orion_keys::{Keys, KeysConfig, SharedKeys};
use orion_msg_session::{Hello, Ping, Pong, Refusal, Reject, RejectReason, Welcome};
use orion_server::{
    Admits, Checked, Comm, Cx, Error, Incoming, Limits, Policy, Principal, Roles, Security,
    Service, What,
};
use orion_tcp::{TcpCarrier, TcpConnection, TcpOptions, connect, dial};
use orion_types::{KeyId, NodeName};
use orion_wire::frame::{self, FrameHeader};
use orion_wire::{Body, Decode, FrameView, encode_body, kind};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn run(test: impl Future<Output = ()>) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    tokio::task::LocalSet::new().block_on(&runtime, async {
        tokio::time::timeout(Duration::from_secs(60), test)
            .await
            .expect("the test finishes");
    });
}

/// Answers `GET` for anyone and `PUT` for peers only, and remembers who asked.
struct Probe(Rc<RefCell<Vec<Principal>>>);

impl Service for Probe {
    fn kinds(&self) -> &'static [u8] {
        &[kind::GET, kind::PUT]
    }

    fn asks<'a>(&self, frame: &FrameView<'a>) -> Result<(What<'a>, Policy), Error> {
        Ok(match frame.kind() {
            kind::PUT => (What::Write("probe"), Policy::new(Admits::PEER)),
            _ => (What::Read("probe"), Policy::new(Admits::ANYONE)),
        })
    }

    fn handle(&mut self, request: Checked<Incoming<'_>>, cx: &mut Cx<'_>) -> Result<(), Error> {
        self.0.borrow_mut().push(request.who().clone());
        cx.reply(&Pong {
            now_ms: request.get().frame().payload().len() as u64,
        })
    }
}

/// One comm core: its keys, its server on a loopback port, and who its service has seen.
struct Node {
    name: NodeName,
    keys: SharedKeys,
    options: TcpOptions,
    addr: SocketAddr,
    seen: Rc<RefCell<Vec<Principal>>>,
}

impl Node {
    async fn start(name: &str, security: Security) -> Self {
        let keys =
            SharedKeys::new(Keys::open(KeysConfig::in_memory("lab"), security).expect("keys"));
        let name: NodeName = name.parse().expect("name");
        let options = TcpOptions {
            security,
            keys: keys.clone(),
            max_frame: 64 * 1024,
            handshake: Duration::from_secs(5),
        };
        let comm = Comm::new(
            name.clone(),
            security,
            Some(Rc::new(keys.clone())),
            Limits::default(),
        );
        let seen = Rc::new(RefCell::new(Vec::new()));
        comm.add_service(Probe(seen.clone()));
        let carrier = TcpCarrier::bind("127.0.0.1:0".parse().expect("addr"), options.clone())
            .await
            .expect("bind");
        let addr = carrier.local_addr().expect("addr");
        tokio::task::spawn_local(async move { comm.serve(carrier).await });
        Self {
            name,
            keys,
            options,
            addr,
            seen,
        }
    }

    fn public(&self) -> [u8; 32] {
        self.keys.lock().identity().public()
    }

    /// Pins each node's key at the other, as enrollment does.
    fn pair(&self, other: &Self) {
        assert!(
            self.keys
                .lock()
                .trust_peer(&other.name, other.public())
                .expect("trust")
        );
        assert!(
            other
                .keys
                .lock()
                .trust_peer(&self.name, self.public())
                .expect("trust")
        );
    }
}

/// A session opened over a connection.
struct Session {
    connection: TcpConnection,
    buf: Vec<u8>,
    len: usize,
}

impl Session {
    async fn open(connection: TcpConnection, roles: Roles) -> (Self, Welcome) {
        let mut session = Self {
            connection,
            buf: vec![0; 64 * 1024],
            len: 0,
        };
        let hello = Hello {
            name: "test",
            roles,
            max_frame: 64 * 1024,
            revision: 1,
            level: 0,
        };
        session.send(&hello).await;
        let welcome: Welcome = session.recv().await.expect("welcome");
        (session, welcome)
    }

    async fn send<B: Body>(&mut self, body: &B) {
        let mut out = [0_u8; 256];
        let len = encode_body(body, 1, &mut out).expect("encode");
        self.connection.send(&out[..len]).await.expect("send");
    }

    async fn ask(&mut self, kind: u8, payload: &[u8]) {
        let mut out = vec![0; payload.len() + 16];
        let len = frame::encode(FrameHeader::new(kind, 7), payload, &mut out).expect("frame");
        self.connection.send(&out[..len]).await.expect("send");
    }

    async fn recv<'a, B: Body + Decode<'a>>(&'a mut self) -> Result<B, Option<Error>> {
        self.len = self
            .connection
            .recv(&mut self.buf)
            .await
            .expect("recv")
            .expect("open");
        let frame = frame::decode(&self.buf[..self.len]).expect("frame");
        if frame.kind() == kind::REFUSAL {
            return Err(Refusal::decode(frame.payload()).expect("refusal").error());
        }
        Ok(orion_wire::decode_body(&frame).unwrap_or_else(|_| panic!("kind {:#x}", frame.kind())))
    }

    /// A request of `size` bytes and the size the service says it got.
    async fn round_trip(&mut self, kind: u8, size: usize) -> Result<u64, Option<Error>> {
        self.ask(kind, &vec![3; size]).await;
        self.recv::<Pong>().await.map(|pong| pong.now_ms)
    }
}

#[test]
fn two_comm_cores_talk_each_way_and_know_each_other_by_their_keys() {
    run(async {
        let left = Node::start("left", Security::On).await;
        let right = Node::start("right", Security::On).await;
        left.pair(&right);

        // One connection each way: each node dials the other and expects its pinned key.
        let to_right = connect(right.addr, &left.options, Some(right.public()))
            .await
            .expect("left dials right");
        let to_left = connect(left.addr, &right.options, Some(left.public()))
            .await
            .expect("right dials left");
        assert_eq!(to_right.peer_key(), Some(&right.public()));
        let (mut on_right, welcome) = Session::open(to_right, Roles::PEER).await;
        assert_eq!(welcome.node, right.name);
        let (mut on_left, welcome) = Session::open(to_left, Roles::PEER.and(Roles::CLIENT)).await;
        assert_eq!(welcome.node, left.name);

        // Requests of every size, pings, and a request only a peer may make.
        for size in [0, 1, 1000, 60_000] {
            assert_eq!(on_right.round_trip(kind::GET, size).await, Ok(size as u64));
            assert_eq!(on_left.round_trip(kind::PUT, size).await, Ok(size as u64));
        }
        on_right.send(&Ping { now_ms: 5 }).await;
        assert_eq!(on_right.recv::<Pong>().await.expect("pong").now_ms, 5);

        // Each server knows the other end as the peer pinned to the key it proved.
        assert_eq!(right.seen.borrow()[0], Principal::Peer(left.name.clone()));
        assert_eq!(left.seen.borrow()[0], Principal::Peer(right.name.clone()));
    });
}

#[test]
fn a_peer_of_another_wire_revision_is_told_both_numbers() {
    run(async {
        let left = Node::start("left", Security::On).await;
        let right = Node::start("right", Security::On).await;
        left.pair(&right);
        let connection = connect(right.addr, &left.options, Some(right.public()))
            .await
            .expect("left dials right");
        let mut session = Session {
            connection,
            buf: vec![0; 64 * 1024],
            len: 0,
        };
        let other = orion_wire::INTERFACE_REVISION + 1;
        session
            .send(&Hello {
                name: "test",
                roles: Roles::PEER,
                max_frame: 64 * 1024,
                revision: other,
                level: 0,
            })
            .await;
        let reject: Reject = session.recv().await.expect("reject");
        assert_eq!(
            (reject.reason, reject.node_revision, reject.peer_revision),
            (
                RejectReason::RevisionMismatch,
                orion_wire::INTERFACE_REVISION,
                other
            )
        );
    });
}

#[test]
fn a_key_nobody_pinned_is_an_operator_and_a_revoked_one_is_turned_away() {
    run(async {
        let board = Node::start("board", Security::On).await;
        let tool = Node::start("tool", Security::On).await;

        // Not a peer: known by its key, as an operator. Granted nothing, so it can read what
        // anyone may and nothing else.
        let connection = connect(board.addr, &tool.options, None)
            .await
            .expect("connect");
        let (mut session, _) = Session::open(connection, Roles::CLIENT).await;
        assert_eq!(
            session.round_trip(kind::PUT, 1).await,
            Err(Some(Error::Denied))
        );
        assert!(
            board.seen.borrow().is_empty(),
            "the service never saw the refused request"
        );
        // Enrolled with read access by an administrator, it may read; it is still not a peer.
        board
            .keys
            .lock()
            .approve_operator(tool.public(), "tool")
            .expect("approve");
        assert_eq!(session.round_trip(kind::GET, 4).await, Ok(4));
        assert_eq!(
            board.seen.borrow()[0],
            Principal::Operator(KeyId(tool.public()))
        );
        assert_eq!(
            session.round_trip(kind::PUT, 1).await,
            Err(Some(Error::Denied))
        );

        // Revoked: the next connection does not get past the handshake.
        board.keys.lock().revoke(tool.public()).expect("revoke");
        let refused = connect(board.addr, &tool.options, None)
            .await
            .expect_err("revoked");
        assert_eq!(refused.kind(), std::io::ErrorKind::PermissionDenied);

        // A client that expects one node and reaches another refuses to talk to it.
        let wrong = connect(board.addr, &tool.options, Some([9; 32]))
            .await
            .expect_err("another node");
        assert_eq!(wrong.kind(), std::io::ErrorKind::PermissionDenied);
        // And one that has revoked the node's key refuses it too.
        let other = Node::start("other", Security::On).await;
        other.keys.lock().revoke(board.public()).expect("revoke");
        assert!(connect(board.addr, &other.options, None).await.is_err());
    });
}

#[test]
fn with_security_off_a_plain_session_works_and_with_it_on_one_is_refused() {
    run(async {
        let open = Node::start("open", Security::Off).await;
        let client = Node::start("client", Security::Off).await;
        let connection = connect(open.addr, &client.options, None)
            .await
            .expect("plain");
        assert_eq!(connection.peer_key(), None);
        let (mut session, welcome) = Session::open(connection, Roles::CLIENT).await;
        assert_eq!(welcome.node, open.name);
        // Nobody in particular, and with the check off that is enough for everything.
        assert_eq!(session.round_trip(kind::PUT, 9).await, Ok(9));
        assert_eq!(open.seen.borrow()[0], Principal::Anonymous);

        // A node with security on turns a plain session away, and says why.
        let secure = Node::start("secure", Security::On).await;
        let refused = connect(secure.addr, &client.options, None)
            .await
            .expect_err("plain refused");
        assert_eq!(refused.kind(), std::io::ErrorKind::PermissionDenied);
        assert!(refused.to_string().contains("signed"), "{refused}");

        // A signed session to a node with security off is fine: it just proves more than asked.
        let signer = Node::start("signer", Security::On).await;
        let connection = connect(open.addr, &signer.options, None)
            .await
            .expect("signed to open");
        let (mut session, _) = Session::open(connection, Roles::CLIENT).await;
        assert_eq!(session.round_trip(kind::PUT, 2).await, Ok(2));
    });
}

/// Passes bytes both ways between a client and `target`, and after `after` bytes from the
/// client flips one bit.
async fn tampering_proxy(target: SocketAddr, after: usize) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::task::spawn_local(async move {
        let (client, _) = listener.accept().await.expect("accept");
        let server = tokio::net::TcpStream::connect(target)
            .await
            .expect("connect");
        let (mut from_client, mut to_client) = client.into_split();
        let (mut from_server, mut to_server) = server.into_split();
        tokio::task::spawn_local(async move {
            let _ = tokio::io::copy(&mut from_server, &mut to_client).await;
        });
        let mut passed = 0;
        let mut byte = [0_u8; 1];
        while from_client.read_exact(&mut byte).await.is_ok() {
            if passed == after {
                byte[0] ^= 0x10;
            }
            passed += 1;
            if to_server.write_all(&byte).await.is_err() {
                break;
            }
        }
    });
    addr
}

#[test]
fn a_frame_changed_in_flight_ends_the_session() {
    run(async {
        let board = Node::start("board", Security::On).await;
        let peer = Node::start("peer", Security::On).await;
        board.pair(&peer);
        // The handshake is 6 + 96 + 64 bytes from the client. The hello frame follows; the
        // bit flipped is inside the request after it.
        let hello_len =
            4 + {
                let mut out = [0_u8; 64];
                let hello = Hello {
                    name: "test",
                    roles: Roles::PEER,
                    max_frame: 64 * 1024,
                    revision: 1,
                    level: 0,
                };
                encode_body(&hello, 1, &mut out).expect("encode")
            } + 16;
        let proxy = tampering_proxy(board.addr, 166 + hello_len + 4 + 6).await;
        let connection = connect(proxy, &peer.options, Some(board.public()))
            .await
            .expect("connect");
        let (mut session, _) = Session::open(connection, Roles::PEER).await;
        session.ask(kind::PUT, &[1; 32]).await;
        // The server never handled the changed frame; it closed the session.
        assert!(
            session
                .connection
                .recv(&mut session.buf)
                .await
                .expect("recv")
                .is_none()
        );
        assert!(board.seen.borrow().is_empty());
    });
}

#[test]
fn what_is_not_a_handshake_or_takes_too_long_holds_nobody_up() {
    run(async {
        let mut board = Node::start("board", Security::On).await;
        board.options.handshake = Duration::from_millis(300);
        let peer = Node::start("peer", Security::On).await;
        // Connections that say nothing, say something else, or stop halfway.
        let _silent = tokio::net::TcpStream::connect(board.addr)
            .await
            .expect("connect");
        let mut wrong = tokio::net::TcpStream::connect(board.addr)
            .await
            .expect("connect");
        wrong
            .write_all(b"GET / HTTP/1.1\r\n\r\n")
            .await
            .expect("write");
        let mut half = tokio::net::TcpStream::connect(board.addr)
            .await
            .expect("connect");
        half.write_all(b"ORTC\x01\x01").await.expect("write");
        // A real one is served at once all the same.
        let started = Instant::now();
        let connection = connect(board.addr, &peer.options, None)
            .await
            .expect("connect");
        let (mut session, _) = Session::open(connection, Roles::CLIENT).await;
        assert!(
            session.round_trip(kind::GET, 3).await.is_err(),
            "an operator granted nothing"
        );
        assert!(started.elapsed() < Duration::from_secs(3));
        // The one that said something else is closed without an answer it could mistake.
        let mut answer = Vec::new();
        let _ = tokio::time::timeout(Duration::from_secs(5), wrong.read_to_end(&mut answer)).await;
        assert!(answer.is_empty(), "{answer:?}");

        // A client whose handshake is not answered gives up at its deadline.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let mute = listener.local_addr().expect("addr");
        let mut options = peer.options.clone();
        options.handshake = Duration::from_millis(200);
        let error = connect(mute, &options, None).await.expect_err("no answer");
        assert_eq!(error.kind(), std::io::ErrorKind::TimedOut);
    });
}

#[test]
fn a_dialer_never_gives_up_and_never_slows_down() {
    run(async {
        let peer = Node::start("peer", Security::On).await;
        // Nothing listens here yet.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("addr");
        drop(listener);

        let tries = Rc::new(RefCell::new(Vec::new()));
        let connections = Rc::new(RefCell::new(0_u32));
        let (noted, counted) = (tries.clone(), connections.clone());
        let options = peer.options.clone();
        let dialing = tokio::task::spawn_local(dial(
            addr,
            options,
            None,
            move |status| noted.borrow_mut().push((Instant::now(), status.failures)),
            move |connection| {
                *counted.borrow_mut() += 1;
                // Hold it briefly, then let it go, as a connection that drops would.
                async move {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    drop(connection);
                }
            },
        ));

        // Three seconds with nobody there: it keeps trying at the same short interval.
        tokio::time::sleep(Duration::from_secs(3)).await;
        let failures: Vec<(Instant, u32)> = tries.borrow().clone();
        assert!(
            (8..=13).contains(&failures.len()),
            "{} tries in 3 s",
            failures.len()
        );
        let gaps: Vec<Duration> = failures
            .windows(2)
            .map(|pair| pair[1].0 - pair[0].0)
            .collect();
        assert!(
            gaps.iter().all(|gap| *gap < Duration::from_millis(600)),
            "a wait grew: {gaps:?}"
        );
        assert_eq!(
            failures.last().expect("tries").1 as usize,
            failures.len(),
            "all failures so far"
        );
        assert_eq!(*connections.borrow(), 0);

        // The node appears: connected within a moment, and again each time the connection ends.
        let mut options = peer.options.clone();
        options.security = Security::On;
        let carrier = TcpCarrier::bind(addr, options).await.expect("bind");
        let comm = Comm::new(
            "late".parse().expect("name"),
            Security::On,
            None,
            Limits::default(),
        );
        tokio::task::spawn_local(async move { comm.serve(carrier).await });
        tokio::time::sleep(Duration::from_secs(2)).await;
        let connected = *connections.borrow();
        assert!(connected >= 3, "{connected} connections in 2 s");
        // A success clears the count of failures.
        assert!(tries.borrow().iter().any(|(_, failures)| *failures == 0));
        dialing.abort();
    });
}
