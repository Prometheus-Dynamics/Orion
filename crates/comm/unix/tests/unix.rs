//! The Unix socket carrier against a real server: connect, request, descriptors both ways.

use std::cell::RefCell;
use std::io::{Read, Seek, Write};
use std::os::fd::OwnedFd;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::time::Duration;

use orion_msg_session::{Hello, Ping, Pong, Reject, RejectReason, Welcome};
use orion_server::{
    Admits, Checked, Comm, Cx, Error, FdSet, Incoming, Limits, Policy, Principal, Roles, Security,
    Service, What,
};
use orion_unix::{UnixCarrier, UnixConnection};
use orion_wire::frame::{self, FrameHeader};
use orion_wire::{Body, Decode, FrameView, encode_body, kind};

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

/// A fresh directory for one test's socket.
fn directory(name: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!("orion-unix-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&path);
    std::fs::create_dir_all(&path).expect("directory");
    path
}

/// Answers `GET` with who asked, and `PUT` by writing into the descriptor it was sent and
/// sending back a descriptor of its own.
struct Probe(Rc<RefCell<Vec<Principal>>>);

impl Service for Probe {
    fn kinds(&self) -> &'static [u8] {
        &[kind::GET, kind::PUT]
    }

    fn asks<'a>(&self, _frame: &FrameView<'a>) -> Result<(What<'a>, Policy), Error> {
        Ok((What::Read("probe"), Policy::new(Admits::LOCAL)))
    }

    fn handle(&mut self, request: Checked<Incoming<'_>>, cx: &mut Cx<'_>) -> Result<(), Error> {
        self.0.borrow_mut().push(request.who().clone());
        let mut incoming = request.into_inner();
        if incoming.kind() == kind::GET {
            return cx.reply(&Pong {
                now_ms: incoming.frame().payload().len() as u64,
            });
        }
        let mut theirs = std::fs::File::from(incoming.take_fd(0).ok_or(Error::Malformed)?);
        theirs
            .write_all(b"written by the server")
            .map_err(|_| Error::Internal)?;
        let mut ours = tempfile();
        ours.write_all(b"made by the server")
            .map_err(|_| Error::Internal)?;
        cx.reply_with_fds(&Pong { now_ms: 2 }, [OwnedFd::from(ours)])
    }
}

/// An unnamed file to pass around.
fn tempfile() -> std::fs::File {
    let path =
        std::env::temp_dir().join(format!("orion-unix-file-{}-{:p}", std::process::id(), &()));
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(&path)
        .expect("file");
    std::fs::remove_file(path).expect("unlink");
    file
}

fn serve(path: &Path, limits: Limits) -> (Comm, Rc<RefCell<Vec<Principal>>>) {
    let comm = Comm::new("board".parse().expect("name"), Security::On, None, limits);
    let seen = Rc::new(RefCell::new(Vec::new()));
    comm.add_service(Probe(seen.clone()));
    let carrier = UnixCarrier::bind(path, Some(0o600), &[]).expect("bind");
    assert_eq!(carrier.path(), path);
    let serving = comm.clone();
    tokio::task::spawn_local(async move { serving.serve(carrier).await });
    (comm, seen)
}

struct Client {
    connection: UnixConnection,
    buf: Vec<u8>,
    fds: FdSet,
}

impl Client {
    async fn connect(path: &Path) -> Self {
        Self {
            connection: UnixConnection::connect(path).await.expect("connect"),
            buf: vec![0; 128 * 1024],
            fds: FdSet::default(),
        }
    }

    async fn open(path: &Path, roles: Roles) -> Self {
        let mut client = Self::connect(path).await;
        let welcome: Welcome = client.hello(roles).await;
        assert_eq!(welcome.node.as_str(), "board");
        client
    }

    async fn hello<'a, B: Body + Decode<'a>>(&'a mut self, roles: Roles) -> B {
        self.hello_at(roles, orion_wire::INTERFACE_REVISION).await
    }

    async fn hello_at<'a, B: Body + Decode<'a>>(&'a mut self, roles: Roles, revision: u16) -> B {
        let hello = Hello {
            name: "test",
            roles,
            max_frame: 64 * 1024,
            revision,
            level: 0,
        };
        self.send(&hello).await;
        self.recv().await.expect("an answer to hello")
    }

    async fn send<B: Body>(&mut self, body: &B) {
        let mut out = [0_u8; 256];
        let len = encode_body(body, 1, &mut out).expect("encode");
        self.connection.send(&out[..len], &[]).await.expect("send");
    }

    async fn ask(&mut self, kind: u8, payload: &[u8], fds: &[OwnedFd]) {
        let mut out = vec![0; payload.len() + 16];
        let len = frame::encode(FrameHeader::new(kind, 1), payload, &mut out).expect("frame");
        self.connection.send(&out[..len], fds).await.expect("send");
    }

    async fn recv<'a, B: Body + Decode<'a>>(&'a mut self) -> Option<B> {
        let len = self
            .connection
            .recv(&mut self.buf, &mut self.fds)
            .await
            .expect("recv")?;
        let frame = frame::decode(&self.buf[..len]).expect("a whole frame");
        Some(orion_wire::decode_body(&frame).unwrap_or_else(|_| panic!("kind {:#x}", frame.kind())))
    }
}

#[test]
fn a_client_connects_asks_and_is_known_by_its_credentials() {
    run(async {
        let path = directory("request").join("orion.sock");
        let (comm, seen) = serve(&path, Limits::default());
        let mut client = Client::open(&path, Roles::CLIENT.and(Roles::PROVIDER)).await;

        client.ask(kind::GET, b"12345", &[]).await;
        assert_eq!(client.recv::<Pong>().await.expect("answer").now_ms, 5);
        // The server knows the client by what the kernel says, which the policy admitted.
        let me = Principal::Local {
            uid: std::os::unix::fs::MetadataExt::uid(&std::fs::metadata(&path).expect("socket")),
            gid: std::os::unix::fs::MetadataExt::gid(
                &std::fs::metadata("/proc/self").expect("self"),
            ),
        };
        assert_eq!(*seen.borrow(), [me]);

        // The connection is kept open: many requests, frames up to the limit, and pings.
        for size in [0, 1, 4000, 60_000] {
            client.ask(kind::GET, &vec![7; size], &[]).await;
            assert_eq!(
                client.recv::<Pong>().await.expect("answer").now_ms,
                size as u64
            );
        }
        client.send(&Ping { now_ms: 9 }).await;
        assert_eq!(client.recv::<Pong>().await.expect("pong").now_ms, 9);
        assert_eq!(comm.stats().opened, 1);

        // Closing it is seen at once.
        drop(client);
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(comm.with(|sessions, _| sessions.is_empty()));
    });
}

#[test]
fn descriptors_travel_both_ways() {
    run(async {
        let path = directory("descriptors").join("orion.sock");
        let _server = serve(&path, Limits::default());
        let mut client = Client::open(&path, Roles::CLIENT).await;

        let mut mine = tempfile();
        let sent = OwnedFd::from(mine.try_clone().expect("clone"));
        client.ask(kind::PUT, b"", &[sent]).await;
        assert_eq!(client.recv::<Pong>().await.expect("answer").now_ms, 2);

        // The server wrote through the descriptor it was sent: it is the same file.
        let mut text = String::new();
        mine.rewind().expect("rewind");
        mine.read_to_string(&mut text).expect("read");
        assert_eq!(text, "written by the server");
        // And the one that came back is the server's file.
        assert_eq!(client.fds.len(), 1);
        let mut theirs = std::fs::File::from(client.fds.take(0).expect("descriptor"));
        text.clear();
        theirs.rewind().expect("rewind");
        theirs.read_to_string(&mut text).expect("read");
        assert_eq!(text, "made by the server");
    });
}

#[test]
fn a_client_of_another_wire_revision_is_told_both_numbers() {
    run(async {
        let path = directory("revision").join("orion.sock");
        let _server = serve(&path, Limits::default());
        let other = orion_wire::INTERFACE_REVISION + 1;
        let mut client = Client::connect(&path).await;
        let reject: Reject = client.hello_at(Roles::CLIENT, other).await;
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
fn only_the_node_users_may_be_the_node_logic() {
    run(async {
        let path = directory("roles").join("orion.sock");
        let _server = serve(&path, Limits::default());
        // This test does not run as root, and its user is not a node user.
        // (As root every role is offered, so there is nothing to see.)
        let uid = std::os::unix::fs::MetadataExt::uid(&std::fs::metadata(&path).expect("socket"));
        let mut client = Client::connect(&path).await;
        if uid == 0 {
            let _: Welcome = client.hello(Roles::NODE).await;
        } else {
            let reject: Reject = client.hello(Roles::NODE).await;
            assert_eq!(reject.reason, RejectReason::RoleNotOffered);
        }

        // With this user named as a node user, it may.
        let path = directory("roles-node").join("orion.sock");
        let comm = Comm::new(
            "board".parse().expect("name"),
            Security::On,
            None,
            Limits::default(),
        );
        let carrier = UnixCarrier::bind(&path, None, &[uid]).expect("bind");
        tokio::task::spawn_local(async move { comm.serve(carrier).await });
        let mut node = Client::connect(&path).await;
        let _: Welcome = node.hello(Roles::NODE.and(Roles::CLIENT)).await;
    });
}

#[test]
fn a_stale_socket_is_replaced_a_live_one_is_not_and_a_file_is_never_touched() {
    run(async {
        let dir = directory("stale");
        let path = dir.join("orion.sock");

        // A socket left behind by a program that is gone.
        drop(std::os::unix::net::UnixListener::bind(&path).expect("leftover"));
        assert!(path.exists());
        let carrier = UnixCarrier::bind(&path, None, &[]).expect("a stale socket is replaced");

        // While that one listens, a second bind is refused and the first keeps working.
        let second = UnixCarrier::bind(&path, None, &[]).expect_err("the socket is live");
        assert_eq!(second.kind(), std::io::ErrorKind::AddrInUse);
        let comm = Comm::new(
            "board".parse().expect("name"),
            Security::Off,
            None,
            Limits::default(),
        );
        tokio::task::spawn_local(async move { comm.serve(carrier).await });
        Client::open(&path, Roles::CLIENT).await;

        // Something that is not a socket is left exactly as it was.
        let file = dir.join("notes.txt");
        std::fs::write(&file, "keep me").expect("file");
        let refused = UnixCarrier::bind(&file, None, &[]).expect_err("not a socket");
        assert_eq!(refused.kind(), std::io::ErrorKind::AlreadyExists);
        assert_eq!(
            std::fs::read_to_string(&file).expect("still there"),
            "keep me"
        );

        // The mode asked for is the mode the socket has.
        let private = dir.join("private.sock");
        let _carrier = UnixCarrier::bind(&private, Some(0o600), &[]).expect("bind");
        let mode = std::os::unix::fs::PermissionsExt::mode(
            &std::fs::metadata(&private).expect("socket").permissions(),
        );
        assert_eq!(mode & 0o777, 0o600);
    });
}

#[test]
fn a_reader_that_falls_behind_gets_every_frame_whole() {
    run(async {
        // The server sends far more than the socket holds while the client is not reading,
        // so sends are split across the socket's buffer. Every frame still arrives whole.
        let path = directory("behind").join("orion.sock");
        let limits = Limits {
            outbox_bytes: 8 * 1024 * 1024,
            ..Limits::default()
        };
        let _server = serve(&path, limits);
        let mut client = Client::open(&path, Roles::CLIENT).await;
        let sizes: Vec<usize> = (0..120).map(|round| 100 + round * 500).collect();
        for size in &sizes {
            client.ask(kind::GET, &vec![1; *size], &[]).await;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        for size in &sizes {
            assert_eq!(
                client.recv::<Pong>().await.expect("answer").now_ms,
                *size as u64
            );
        }
    });
}

#[test]
fn a_program_on_the_socket_is_kept_while_it_is_quiet_and_gone_when_it_hangs_up() {
    run(async {
        let path = directory("quiet").join("orion.sock");
        // Real time: the shortest deadlines a test may build.
        let limits = Limits {
            heartbeat_ms: 20,
            idle_ms: 60,
            ..Limits::default()
        };
        let (comm, _seen) = serve(&path, limits);
        let mut client = Client::open(&path, Roles::CLIENT).await;

        // Five idle times with nothing said: still there, seen as quiet, and not a drop.
        tokio::time::sleep(Duration::from_millis(300)).await;
        let quiet = comm.with(|sessions, _| {
            sessions
                .iter()
                .map(|info| (info.local, info.quiet_since.is_some()))
                .collect::<Vec<_>>()
        });
        assert_eq!(quiet, [(true, true)]);
        assert_eq!((comm.drops(), comm.stats().dropped), (vec![], 0));
        // It still answers.
        client.send(&Ping { now_ms: 3 }).await;
        assert_eq!(client.recv::<Pong>().await.expect("pong").now_ms, 3);

        // Every holder of the socket has closed it: gone at once.
        drop(client);
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(comm.with(|sessions, _| sessions.is_empty()));
        assert_eq!(comm.drops(), vec![]);
    });
}
