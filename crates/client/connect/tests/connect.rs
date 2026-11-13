//! One session, the same calls, wherever the comm core is: the same test on the board's
//! socket, on the network plain and signed, and over a link.

#![cfg(target_os = "linux")]

use std::os::fd::OwnedFd;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::mpsc;
use std::time::{Duration, Instant};

use orion_connect::{Address, ClientError, Identity, Options, Roles, Session};
use orion_keys::{Keys, KeysConfig, SharedKeys};
use orion_link_serial::{Serial, StreamCarrier};
use orion_measure::{CountingAlloc, thread_counts};
use orion_msg_session::{Identify, Identity as IdentityAnswer, RejectReason};
use orion_server::{Comm, Limits, Security};
use orion_tcp::{TcpCarrier, TcpOptions};
use orion_unix::UnixCarrier;
use orion_wire::{Bytes, kind};
use rustix::pty::{OpenptFlags, grantpt, openpt, ptsname, unlockpt};

#[global_allocator]
static GLOBAL: CountingAlloc = CountingAlloc::system();

const HEARTBEAT: Duration = Duration::from_millis(100);

/// A comm core in its own thread: the board's socket, the network, and a serial link whose
/// other end is the returned descriptor.
struct Board {
    socket: PathBuf,
    network: std::net::SocketAddr,
    key: [u8; 32],
    link: Option<OwnedFd>,
    keys: SharedKeys,
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    thread: Option<std::thread::JoinHandle<()>>,
    dir: PathBuf,
}

impl Board {
    fn start(name: &str, security: Security) -> Self {
        let dir = std::env::temp_dir().join(format!("orion-connect-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("dir");
        Self::start_in(dir, security, None)
    }

    /// Starts a board in `dir`, on `port` if it is to come back where it was.
    fn start_in(dir: PathBuf, security: Security, port: Option<u16>) -> Self {
        let socket = dir.join("orion.sock");
        let master = openpt(OpenptFlags::RDWR | OpenptFlags::NOCTTY).expect("openpt");
        grantpt(&master).expect("grantpt");
        unlockpt(&master).expect("unlockpt");
        let slave = PathBuf::from(
            ptsname(&master, Vec::new())
                .expect("ptsname")
                .into_string()
                .expect("path"),
        );
        let keys =
            SharedKeys::new(Keys::open(KeysConfig::in_memory("lab"), security).expect("keys"));
        let key = keys.lock().identity().public();
        let (ready, started) = mpsc::channel();
        let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
        let (for_board, path) = (keys.clone(), socket.clone());
        let for_link = keys.clone();
        let thread = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("runtime");
            tokio::task::LocalSet::new().block_on(&runtime, async move {
                let limits = Limits {
                    heartbeat_ms: HEARTBEAT.as_millis() as u32,
                    ..Limits::default()
                };
                let comm = Comm::new(
                    "board".parse().expect("name"),
                    security,
                    Some(Rc::new(for_board.clone())),
                    limits,
                );
                comm.add_service(orion_hello::Hello::new(
                    "board".parse().expect("name"),
                    for_board.clone(),
                    None,
                ));
                let unix = UnixCarrier::bind(&path, Some(0o600), &[]).expect("socket");
                let options = TcpOptions {
                    security,
                    keys: for_board,
                    max_frame: 64 * 1024,
                    handshake: Duration::from_secs(5),
                };
                let listen = std::net::SocketAddr::from(([127, 0, 0, 1], port.unwrap_or(0)));
                let tcp = TcpCarrier::bind(listen, options).await.expect("listen");
                let network = tcp.local_addr().expect("addr");
                let link = StreamCarrier::new(
                    "uart0".parse().expect("name"),
                    Serial {
                        path: slave,
                        baud: 115_200,
                    },
                )
                .signed(for_link);
                let opened = link.stats();
                for carrier in [
                    tokio::task::spawn_local({
                        let comm = comm.clone();
                        async move { comm.serve(unix).await }
                    }),
                    tokio::task::spawn_local({
                        let comm = comm.clone();
                        async move { comm.serve(tcp).await }
                    }),
                    tokio::task::spawn_local({
                        let comm = comm.clone();
                        async move { comm.serve(link).await }
                    }),
                ] {
                    drop(carrier);
                }
                // The link is raw only once the board has opened it.
                while opened().opens == 0 {
                    tokio::time::sleep(Duration::from_millis(2)).await;
                }
                ready.send(network).expect("ready");
                let _ = stopped.await;
            });
        });
        let network = started.recv().expect("the board starts");
        Self {
            socket,
            network,
            key,
            link: Some(master),
            keys,
            stop: Some(stop),
            thread: Some(thread),
            dir,
        }
    }

    fn stop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(thread) = self.thread.take() {
            thread.join().expect("the board stops");
        }
    }
}

impl Drop for Board {
    fn drop(&mut self) {
        // The directory is left for the test to remove: a board started again in it must not
        // lose its socket when the one before it is dropped.
        self.stop();
    }
}

fn options(identity: Option<Identity>) -> Options {
    Options {
        identity,
        ..Options::default()
    }
}

/// Asks the board who it is and returns the name it gives.
fn who(session: &mut Session, nonce: &[u8]) -> String {
    let seq = session
        .send(&Identify {
            nonce: Bytes(nonce),
        })
        .expect("send");
    let answer = session.answer(seq, kind::IDENTITY).expect("an answer");
    let identity: IdentityAnswer<'_> = answer.body().expect("identity");
    assert_eq!((identity.request_seq, identity.nonce.0), (seq, nonce));
    identity.node.to_string()
}

/// The test every kind of address runs.
fn exercise(mut session: Session, descriptors: bool) {
    let welcomed = session.welcomed().clone();
    assert_eq!(
        (welcomed.node.as_str(), welcomed.heartbeat),
        ("board", HEARTBEAT)
    );
    assert!(welcomed.max_frame >= 512 && welcomed.session > 0);
    assert_eq!(session.passes_descriptors(), descriptors);

    // A request and its answer.
    assert_eq!(who(&mut session, &[1, 2, 3]), "board");

    // Nothing to read is nothing, after the wait and not before.
    let before = Instant::now();
    assert!(
        session
            .next(Duration::from_millis(30))
            .expect("next")
            .is_none()
    );
    assert!(before.elapsed() >= Duration::from_millis(30));

    // The session outlives many heartbeats with nothing said by the program: `next` keeps it.
    let quiet = HEARTBEAT * 8;
    let before = Instant::now();
    while before.elapsed() < quiet {
        assert!(
            session
                .next(Duration::from_millis(50))
                .expect("the session is kept")
                .is_none()
        );
    }
    assert_eq!(who(&mut session, &[4]), "board");

    // Requests and answers allocate nothing once the session is up.
    for round in 0..50_u8 {
        who(&mut session, &[round]);
    }
    let before = thread_counts();
    for round in 0..500_u32 {
        let seq = session
            .send(&Identify {
                nonce: Bytes(&round.to_le_bytes()),
            })
            .expect("send");
        let answer = session.answer(seq, kind::IDENTITY).expect("an answer");
        assert_eq!(answer.kind(), kind::IDENTITY);
    }
    let allocated = thread_counts().since(before);
    assert_eq!(allocated.allocs, 0, "{allocated:?} in 500 requests");

    // A frame larger than both sides agreed to is refused here, not sent.
    let big = vec![0_u8; welcomed.max_frame as usize];
    assert!(matches!(
        session.send(&Identify { nonce: Bytes(&big) }),
        Err(ClientError::TooLarge)
    ));
    assert_eq!(who(&mut session, &[5]), "board");
}

#[test]
fn the_same_calls_work_on_the_boards_socket_the_network_and_a_link() {
    let mut board = Board::start("same", Security::Off);
    let local = Address::Local(board.socket.clone());
    exercise(
        Session::connect(&local, "tool", options(None)).expect("local"),
        true,
    );

    let network: Address = format!("orion+tcp://{}", board.network)
        .parse()
        .expect("address");
    exercise(
        Session::connect(&network, "tool", options(None)).expect("network"),
        false,
    );

    let link = board.link.take().expect("link");
    exercise(
        Session::over_link(link, "tool", options(None)).expect("link"),
        false,
    );
    let _ = std::fs::remove_dir_all(&board.dir);
}

#[test]
fn a_signed_network_session_is_the_same_again_and_checks_who_is_there() {
    let board = Board::start("signed", Security::On);
    let me = Identity::generate();
    board
        .keys
        .lock()
        .approve_operator(me.public(), "me")
        .expect("enroll");
    let copy = || Some(Identity::from_secret(&me.secret()));
    let pinned = Address::Network {
        host: board.network.to_string(),
        key: Some(board.key),
    };
    exercise(
        Session::connect(&pinned, "tool", options(copy())).expect("signed"),
        false,
    );

    // A node that is not the one that was meant.
    let wrong = Address::Network {
        host: board.network.to_string(),
        key: Some([9; 32]),
    };
    let error = Session::connect(&wrong, "tool", options(copy())).expect_err("another node");
    // Both keys, for a tool to show: the one asked for and the one the node proved.
    match &error {
        ClientError::OtherKey { asked, answered } => {
            assert_eq!(*asked, [9; 32]);
            assert_eq!(*answered, board.key);
        }
        other => panic!("{other}"),
    }
    assert!(error.to_string().contains(&"09".repeat(32)), "{error}");
    // No key, to a node with security on.
    let open = Address::Network {
        host: board.network.to_string(),
        key: None,
    };
    let error = Session::connect(&open, "tool", options(None)).expect_err("needs a key");
    assert!(
        matches!(error, ClientError::Denied(why) if why.contains("signed session")),
        "{error}"
    );
    assert!(!error.is_lost(), "asking again does not change the answer");
    // A key the node has revoked.
    let revoked = Identity::generate();
    board.keys.lock().revoke(revoked.public()).expect("revoke");
    let error = Session::connect(&open, "tool", options(Some(revoked))).expect_err("revoked");
    assert!(matches!(error, ClientError::Denied(_)), "{error}");
    // A role this way in does not offer.
    let node_role = Options {
        roles: Roles::NODE,
        identity: copy(),
        ..Options::default()
    };
    let error = Session::connect(&open, "tool", node_role).expect_err("role");
    assert!(
        matches!(error, ClientError::Rejected(RejectReason::RoleNotOffered)),
        "{error}"
    );
    let _ = std::fs::remove_dir_all(&board.dir);
}

#[test]
fn a_tool_with_a_key_signs_its_session_on_a_link() {
    let mut board = Board::start("signed-link", Security::On);
    let me = Identity::generate();
    board
        .keys
        .lock()
        .approve_operator(me.public(), "me")
        .expect("enroll");
    let copy = || Some(Identity::from_secret(&me.secret()));
    let link = board.link.take().expect("link");
    let end = || link.try_clone().expect("the link again");
    // The board's last session on the link has to be over before the next one is heard.
    let gone = || std::thread::sleep(HEARTBEAT * 8);

    // Signed: the same calls, sealed, in a frame a little shorter than a plain link's.
    let session = Session::over_link(end(), "tool", options(copy())).expect("signed");
    assert!(session.welcomed().max_frame <= 8192 - 32);
    exercise(session, false);
    gone();

    // A key the board revoked is told so, and the link stays the board's for the next.
    let revoked = Identity::generate();
    board.keys.lock().revoke(revoked.public()).expect("revoke");
    let error = Session::over_link(end(), "tool", options(Some(revoked))).expect_err("revoked");
    assert!(
        matches!(error, ClientError::Denied(why) if why.contains("refused this client's key")),
        "{error}"
    );

    // No key, with security on: the link's device, as before.
    exercise(
        Session::over_link(end(), "device", options(None)).expect("plain"),
        false,
    );
    gone();
    // And signed again after it.
    exercise(
        Session::over_link(end(), "tool", options(copy())).expect("signed again"),
        false,
    );
    let _ = std::fs::remove_dir_all(&board.dir);
}

#[test]
fn a_serial_address_names_the_key_its_board_must_prove() {
    let key = "ab".repeat(32);
    let address: Address = format!("serial:/dev/ttyACM0@921600#{key}")
        .parse()
        .expect("address");
    assert_eq!(
        address,
        Address::Serial {
            path: "/dev/ttyACM0".into(),
            baud: 921_600,
            key: Some([0xab; 32]),
        }
    );
    assert_eq!(
        address.to_string(),
        format!("serial:/dev/ttyACM0@921600#{key}")
    );
    // A key needs a signed session, and this client has none: said before any port is opened.
    let nowhere: Address = format!("serial:/nonexistent/tty#{key}")
        .parse()
        .expect("address");
    let error = Session::connect(&nowhere, "tool", options(None)).expect_err("no key");
    assert!(
        matches!(error, ClientError::Denied(why) if why.contains("signed session")),
        "{error}"
    );
}

#[test]
fn there_is_one_way_back_and_it_never_gives_up() {
    let mut board = Board::start("back", Security::Off);
    let (dir, port) = (board.dir.clone(), board.network.port());
    for address in [
        Address::Local(board.socket.clone()),
        format!("orion+tcp://{}", board.network)
            .parse()
            .expect("address"),
    ] {
        let mut session = Session::connect(&address, "tool", options(None)).expect("connect");
        assert_eq!(who(&mut session, &[1]), "board");

        // The comm core goes away: the session finds out, as an error that says "lost".
        board.stop();
        let lost = loop {
            match session.next(Duration::from_millis(50)) {
                Ok(_) => {}
                Err(error) => break error,
            }
        };
        assert!(lost.is_lost(), "{lost}");

        // Nothing is there: every try fails, a short fixed time apart, for as long as asked.
        let mut tries = Vec::new();
        let gave_up = session.reconnect(|status| {
            tries.push((Instant::now(), status.failures));
            tries.len() < 5
        });
        assert!(gave_up.expect_err("nothing is there").is_lost());
        assert_eq!(
            tries
                .iter()
                .map(|(_, failures)| *failures)
                .collect::<Vec<_>>(),
            [1, 2, 3, 4, 5]
        );
        for pair in tries.windows(2) {
            let apart = pair[1].0 - pair[0].0;
            assert!(
                apart >= Duration::from_millis(240) && apart < Duration::from_millis(600),
                "{apart:?}"
            );
        }

        // It comes back at the same address: the same session is whole again.
        board = Board::start_in(dir.clone(), Security::Off, Some(port));
        session.reconnect(|_| true).expect("back");
        assert_eq!(session.reconnects(), 1);
        assert_eq!(who(&mut session, &[2]), "board");
    }

    // Nothing was ever there.
    let nowhere = Address::Local(dir.join("no-such.sock"));
    let error = Session::connect(&nowhere, "tool", options(None)).expect_err("nothing");
    assert!(error.is_lost(), "{error}");
    drop(board);
    let _ = std::fs::remove_dir_all(dir);
}

#[cfg(feature = "tokio")]
#[test]
fn a_session_waits_inside_a_runtime_without_blocking_it() {
    let board = Board::start("async", Security::Off);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    runtime.block_on(async {
        for address in [
            Address::Local(board.socket.clone()),
            format!("orion+tcp://{}", board.network)
                .parse()
                .expect("address"),
        ] {
            let mut session = Session::connect(&address, "tool", options(None)).expect("connect");
            // Another task keeps turning while the session waits.
            let turning = tokio::spawn(async {
                let mut turns = 0_u32;
                let until = Instant::now() + Duration::from_millis(400);
                while Instant::now() < until {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    turns += 1;
                }
                turns
            });
            let before = Instant::now();
            assert!(
                session
                    .next_async(HEARTBEAT * 4)
                    .await
                    .expect("kept")
                    .is_none()
            );
            assert!(before.elapsed() >= HEARTBEAT * 4);
            assert!(
                turning.await.expect("task") >= 20,
                "the runtime was held up"
            );
            let seq = session
                .send(&Identify { nonce: Bytes(&[1]) })
                .expect("send");
            let answer = loop {
                if let Some(incoming) = session
                    .next_async(Duration::from_secs(2))
                    .await
                    .expect("next")
                {
                    break incoming.kind();
                }
            };
            assert_eq!(answer, kind::IDENTITY);
            let _ = seq;
        }
    });
}

/// A half of a link the program opened itself, as a blocking file.
struct Half(std::fs::File);

impl orion_connect::LinkRead for Half {
    fn read(&mut self, into: &mut [u8]) -> std::io::Result<usize> {
        std::io::Read::read(&mut self.0, into)
    }
}

impl orion_connect::LinkWrite for Half {
    fn write_all(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        std::io::Write::write_all(&mut self.0, bytes)
    }
}

/// The board's link, in two halves. Its reading thread is never ended here, so each board's
/// link is opened this way once.
fn halves(board: &mut Board) -> (Half, Half) {
    let link = board.link.take().expect("link");
    let write = Half(std::fs::File::from(
        link.try_clone().expect("the link again"),
    ));
    (Half(std::fs::File::from(link)), write)
}

#[test]
fn a_link_the_program_opened_in_two_halves_carries_the_same_calls() {
    // Plain.
    let mut board = Board::start("halves", Security::Off);
    let (read, write) = halves(&mut board);
    exercise(
        Session::over(read, write, None, "tool", options(None)).expect("halves"),
        false,
    );
    let _ = std::fs::remove_dir_all(&board.dir);

    // Signed, and the board proves the key it was expected to have.
    let mut board = Board::start("halves-signed", Security::On);
    let me = Identity::generate();
    board
        .keys
        .lock()
        .approve_operator(me.public(), "me")
        .expect("enroll");
    let copy = || Some(Identity::from_secret(&me.secret()));
    let (read, write) = halves(&mut board);
    let session =
        Session::over(read, write, Some(board.key), "tool", options(copy())).expect("signed");
    assert!(session.welcomed().max_frame <= 8192 - 32);
    exercise(session, false);
    let _ = std::fs::remove_dir_all(&board.dir);

    // Another key than the one expected: refused, with both keys.
    let mut board = Board::start("halves-other", Security::On);
    board
        .keys
        .lock()
        .approve_operator(me.public(), "me")
        .expect("enroll");
    let (read, write) = halves(&mut board);
    match Session::over(read, write, Some([9; 32]), "tool", options(copy())) {
        Err(ClientError::OtherKey { asked, answered }) => {
            assert_eq!((asked, answered), ([9; 32], board.key));
        }
        other => panic!("expected another key, got {:?}", other.err()),
    }
    // A key expected with none to sign with.
    let mut board2 = Board::start("halves-nokey", Security::On);
    let (read, write) = halves(&mut board2);
    assert!(matches!(
        Session::over(read, write, Some(board2.key), "tool", options(None)),
        Err(ClientError::Denied(_))
    ));
    let _ = std::fs::remove_dir_all(&board.dir);
    let _ = std::fs::remove_dir_all(&board2.dir);
}
