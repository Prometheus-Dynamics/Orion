//! Files against a receiver that stands in for node logic's: a push that is interrupted
//! carries on where it stopped, on the board's socket and across the network.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::io::Cursor;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::time::Duration;

use orion_connect::{Address, Client, ClientError, Options, Session};
use orion_files::{FileState, Files, Pull, digest};
use orion_keys::{Keys, KeysConfig, SharedKeys};
use orion_msg_files::{FileBegin, FileChunk, FileCommit, FilePull, FileStatus};
use orion_server::{
    Admits, Checked, Comm, Cx, Delivery, Error, Incoming, Limits, Policy, Security, Service, What,
};
use orion_tcp::{TcpCarrier, TcpOptions};
use orion_unix::UnixCarrier;
use orion_wire::{FrameView, kind};
use sha2::{Digest, Sha256};

/// What the test can see and do from outside the board's thread.
#[derive(Default)]
struct Outside {
    /// Bytes of chunks the receiver was sent, whether it wanted them or not.
    offered: AtomicU64,
    /// Close the sender's session once this many bytes have been offered. Zero for never.
    cut_after: AtomicU64,
    /// What is in place: destination to contents.
    committed: Mutex<BTreeMap<String, Vec<u8>>>,
    /// What a pull can fetch: URL to contents.
    urls: Mutex<BTreeMap<String, Vec<u8>>>,
}

struct Receiving {
    destination: String,
    size: u64,
    digest: [u8; 32],
    data: Vec<u8>,
}

struct Receiver {
    outside: Arc<Outside>,
    transfers: BTreeMap<u64, Receiving>,
}

fn status(transfer: u64, received: u64, state: FileState, detail: &str) -> FileStatus<'_> {
    FileStatus {
        transfer,
        received,
        state,
        detail,
    }
}

impl Receiver {
    /// Checks a whole file against its digest and puts it in place.
    fn commit(&mut self, transfer: u64) -> (u64, FileState, &'static str) {
        let Some(receiving) = self.transfers.remove(&transfer) else {
            return (0, FileState::Rejected, "no such transfer");
        };
        let received = receiving.data.len() as u64;
        if received != receiving.size {
            self.transfers.insert(transfer, receiving);
            return (received, FileState::Rejected, "not all of it is here");
        }
        let found: [u8; 32] = Sha256::digest(&receiving.data).into();
        if found != receiving.digest {
            return (received, FileState::Rejected, "the digest does not match");
        }
        self.outside
            .committed
            .lock()
            .expect("lock")
            .insert(receiving.destination, receiving.data);
        (received, FileState::Committed, "")
    }
}

impl Service for Receiver {
    fn kinds(&self) -> &'static [u8] {
        &[
            kind::FILE_BEGIN,
            kind::FILE_CHUNK,
            kind::FILE_COMMIT,
            kind::FILE_PULL,
        ]
    }

    fn asks<'a>(&self, _frame: &FrameView<'a>) -> Result<(What<'a>, Policy), Error> {
        Ok((
            What::Write("files"),
            Policy::new(Admits::ANYONE).without_grant(),
        ))
    }

    fn handle(&mut self, request: Checked<Incoming<'_>>, cx: &mut Cx<'_>) -> Result<(), Error> {
        let incoming = request.get();
        match incoming.kind() {
            kind::FILE_BEGIN => {
                let begin: FileBegin = incoming.body()?;
                if begin.destination.as_str().starts_with("readonly.") {
                    return Err(Error::Denied);
                }
                // The same transfer again carries on. Anything else under its number starts over.
                let same = self.transfers.get(&begin.transfer).is_some_and(|known| {
                    known.size == begin.size
                        && known.digest == begin.digest
                        && known.destination == begin.destination.as_str()
                });
                if !same {
                    self.transfers.insert(
                        begin.transfer,
                        Receiving {
                            destination: begin.destination.to_string(),
                            size: begin.size,
                            digest: begin.digest,
                            data: Vec::new(),
                        },
                    );
                }
                let received = self.transfers[&begin.transfer].data.len() as u64;
                cx.reply(&status(begin.transfer, received, FileState::Receiving, ""))
            }
            kind::FILE_CHUNK => {
                let chunk: FileChunk<'_> = incoming.body()?;
                let offered = self
                    .outside
                    .offered
                    .fetch_add(chunk.data.0.len() as u64, Ordering::Relaxed)
                    + chunk.data.0.len() as u64;
                let cut = self.outside.cut_after.load(Ordering::Relaxed);
                let receiving = self
                    .transfers
                    .get_mut(&chunk.transfer)
                    .ok_or(Error::NotFound)?;
                // Only the next bytes in order are taken.
                if chunk.offset == receiving.data.len() as u64 {
                    receiving.data.extend_from_slice(chunk.data.0);
                }
                if cut != 0 && offered >= cut {
                    // The cable is pulled: the sender hears nothing more on this session.
                    self.outside.cut_after.store(0, Ordering::Relaxed);
                    let from = cx.from();
                    cx.sessions().close(from);
                    return Ok(());
                }
                let received = receiving.data.len() as u64;
                cx.reply(&status(chunk.transfer, received, FileState::Receiving, ""))
            }
            kind::FILE_COMMIT => {
                let commit: FileCommit = incoming.body()?;
                let (received, state, detail) = self.commit(commit.transfer);
                cx.reply(&status(commit.transfer, received, state, detail))
            }
            _ => {
                let pull: FilePull<'_> = incoming.body()?;
                let fetched = self
                    .outside
                    .urls
                    .lock()
                    .expect("lock")
                    .get(pull.url)
                    .cloned();
                let Some(data) = fetched else {
                    return cx.reply(&status(
                        pull.transfer,
                        0,
                        FileState::Rejected,
                        "nothing at that URL",
                    ));
                };
                // Fetched in two halves, each reported, as a real fetch reports as it goes.
                let from = cx.from();
                let half = data.len() as u64 / 2;
                let sessions = cx.sessions();
                sessions.send(
                    from,
                    &status(pull.transfer, half, FileState::Receiving, ""),
                    Delivery::Must,
                )?;
                sessions.send(
                    from,
                    &status(pull.transfer, data.len() as u64, FileState::Verifying, ""),
                    Delivery::Must,
                )?;
                self.transfers.insert(
                    pull.transfer,
                    Receiving {
                        destination: pull.destination.to_string(),
                        size: pull.size,
                        digest: pull.digest,
                        data,
                    },
                );
                let (received, state, detail) = self.commit(pull.transfer);
                sessions.send(
                    from,
                    &status(pull.transfer, received, state, detail),
                    Delivery::Must,
                )
            }
        }
    }
}

struct Board {
    socket: PathBuf,
    network: std::net::SocketAddr,
    outside: Arc<Outside>,
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    thread: Option<std::thread::JoinHandle<()>>,
    dir: PathBuf,
}

impl Board {
    fn start(name: &str) -> Self {
        let dir = std::env::temp_dir().join(format!("orion-files-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("dir");
        let socket = dir.join("orion.sock");
        let outside = Arc::new(Outside::default());
        let (ready, started) = mpsc::channel();
        let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
        let (path, shared) = (socket.clone(), outside.clone());
        let thread = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("runtime");
            tokio::task::LocalSet::new().block_on(&runtime, async move {
                let comm = Comm::new(
                    "board".parse().expect("name"),
                    Security::Off,
                    None,
                    Limits::default(),
                );
                comm.add_service(Receiver {
                    outside: shared,
                    transfers: BTreeMap::new(),
                });
                let unix = UnixCarrier::bind(&path, Some(0o600), &[]).expect("socket");
                let options = TcpOptions {
                    security: Security::Off,
                    keys: SharedKeys::new(
                        Keys::open(KeysConfig::in_memory("lab"), Security::Off).expect("keys"),
                    ),
                    max_frame: 64 * 1024,
                    handshake: Duration::from_secs(5),
                };
                let tcp = TcpCarrier::bind("127.0.0.1:0".parse().expect("addr"), options)
                    .await
                    .expect("listen");
                ready.send(tcp.local_addr().expect("addr")).expect("ready");
                let (on_socket, on_network) = (comm.clone(), comm);
                tokio::task::spawn_local(async move { on_socket.serve(unix).await });
                tokio::task::spawn_local(async move { on_network.serve(tcp).await });
                let _ = stopped.await;
            });
        });
        Self {
            socket,
            network: started.recv().expect("the board starts"),
            outside,
            stop: Some(stop),
            thread: Some(thread),
            dir,
        }
    }
}

impl Drop for Board {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// A megabyte and a bit that is not the same anywhere twice.
fn contents(seed: u64) -> Vec<u8> {
    let mut state = seed | 1;
    (0..1_100_003)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state as u8
        })
        .collect()
}

/// The test every kind of address runs.
fn push_interrupted_and_resumed(board: &Board, address: &Address, destination: &str, seed: u64) {
    let file = contents(seed);
    let size = file.len() as u64;
    let client =
        Client::new(Session::connect(address, "installer", Options::default()).expect("connect"));
    let files = Files::new(&client);
    let offered_before = board.outside.offered.load(Ordering::Relaxed);

    // The connection is lost with the file part way there.
    board
        .outside
        .cut_after
        .store(offered_before + size / 3, Ordering::Relaxed);
    let told = Rc::new(RefCell::new(Vec::new()));
    let seen = told.clone();
    let interrupted = files.push(destination, &mut Cursor::new(&file), move |received, of| {
        assert_eq!(of, size);
        seen.borrow_mut().push(received);
    });
    let error = interrupted.expect_err("the connection was cut");
    assert!(error.is_lost(), "{error}");
    assert!(
        !board
            .outside
            .committed
            .lock()
            .expect("lock")
            .contains_key(destination)
    );
    let first_try = board.outside.offered.load(Ordering::Relaxed) - offered_before;
    assert!(
        first_try >= size / 3 && first_try < size,
        "{first_try} of {size} offered before the cut"
    );

    // Connect again and push the same file: it carries on from what the board has.
    client
        .session(|session| session.reconnect(|_| true))
        .expect("back");
    let resumed_from = Rc::new(RefCell::new(None));
    let first = resumed_from.clone();
    let done = files
        .push(destination, &mut Cursor::new(&file), move |received, _| {
            first.borrow_mut().get_or_insert(received);
        })
        .expect("the push ends");
    assert_eq!((done.state, done.received), (FileState::Committed, size));
    let resumed_from = resumed_from.borrow().expect("told where it resumed");
    assert!(
        resumed_from >= size / 3 - 9 * 32 * 1024 && resumed_from > 0,
        "resumed from {resumed_from}"
    );
    // What the board had was not sent again.
    let second_try = board.outside.offered.load(Ordering::Relaxed) - offered_before - first_try;
    assert_eq!(
        second_try,
        size - resumed_from,
        "the second try sent exactly what was missing"
    );
    // And what is in place is the file.
    assert_eq!(
        board
            .outside
            .committed
            .lock()
            .expect("lock")
            .get(destination),
        Some(&file)
    );
    // Progress only ever went forward.
    assert!(told.borrow().windows(2).all(|pair| pair[0] <= pair[1]));
}

#[test]
fn an_interrupted_push_resumes_on_the_socket_and_across_the_network() {
    let board = Board::start("resume");
    push_interrupted_and_resumed(
        &board,
        &Address::Local(board.socket.clone()),
        "firmware.local",
        1,
    );
    let network: Address = format!("orion+tcp://{}", board.network)
        .parse()
        .expect("address");
    push_interrupted_and_resumed(&board, &network, "firmware.network", 2);
}

#[test]
fn a_file_is_checked_before_it_is_in_place_and_refusals_are_heard() {
    let board = Board::start("checked");
    let client = Client::new(
        Session::connect(
            &Address::Local(board.socket.clone()),
            "tool",
            Options::default(),
        )
        .expect("connect"),
    );
    let files = Files::new(&client);

    // An empty file and a small one are files too.
    let done = files
        .push("empty", &mut Cursor::new(Vec::new()), |_, _| {})
        .expect("push");
    assert_eq!((done.state, done.received), (FileState::Committed, 0));
    let done = files
        .push("small", &mut Cursor::new(b"hello".to_vec()), |_, _| {})
        .expect("push");
    assert_eq!(done.state, FileState::Committed);
    assert_eq!(
        board
            .outside
            .committed
            .lock()
            .expect("lock")
            .get("small")
            .map(Vec::as_slice),
        Some(b"hello".as_slice())
    );
    // The same file again is the same transfer: it ends committed, and nothing is sent twice.
    let before = board.outside.offered.load(Ordering::Relaxed);
    let again = files
        .push("small", &mut Cursor::new(b"hello".to_vec()), |_, _| {})
        .expect("push");
    assert_eq!(again.state, FileState::Committed);
    assert_eq!(
        board.outside.offered.load(Ordering::Relaxed) - before,
        5,
        "a finished transfer is a new one"
    );

    // A destination the board will not write.
    let refused = files.push(
        "readonly.config",
        &mut Cursor::new(b"x".to_vec()),
        |_, _| {},
    );
    assert!(
        matches!(
            refused,
            Err(ClientError::Refused(orion_types::Error::Denied))
        ),
        "{refused:?}"
    );
    assert!(matches!(
        files.push("not a name", &mut Cursor::new(b"x".to_vec()), |_, _| {}),
        Err(ClientError::Protocol(_))
    ));

    // A pull: the board fetches the file itself, checks it, and says how it went.
    let file = contents(7);
    let (sum, size) = digest(&mut Cursor::new(&file)).expect("digest");
    board
        .outside
        .urls
        .lock()
        .expect("lock")
        .insert("https://updates.example/image".into(), file.clone());
    let mut heard = Vec::new();
    let offered = board.outside.offered.load(Ordering::Relaxed);
    let pulled = files
        .pull(
            &Pull {
                transfer: 77,
                url: "https://updates.example/image",
                destination: "image",
                size,
                digest: sum,
            },
            Duration::from_secs(5),
            |received, _| heard.push(received),
        )
        .expect("pull");
    assert_eq!(
        (pulled.state, pulled.received),
        (FileState::Committed, size)
    );
    assert_eq!(heard.last(), Some(&size));
    assert_eq!(
        board.outside.committed.lock().expect("lock").get("image"),
        Some(&file)
    );
    assert_eq!(
        offered,
        board.outside.offered.load(Ordering::Relaxed),
        "a pull sends no bytes of the file"
    );

    // What was fetched is not what was asked for: rejected, and not put in place.
    let wrong = files
        .pull(
            &Pull {
                transfer: 78,
                url: "https://updates.example/image",
                destination: "other",
                size,
                digest: [0; 32],
            },
            Duration::from_secs(5),
            |_, _| {},
        )
        .expect("pull");
    assert_eq!(
        (wrong.state, wrong.detail.as_str()),
        (FileState::Rejected, "the digest does not match")
    );
    assert!(
        !board
            .outside
            .committed
            .lock()
            .expect("lock")
            .contains_key("other")
    );
    let missing = files
        .pull(
            &Pull {
                transfer: 79,
                url: "https://updates.example/nothing",
                destination: "other",
                size,
                digest: sum,
            },
            Duration::from_secs(5),
            |_, _| {},
        )
        .expect("pull");
    assert_eq!(missing.state, FileState::Rejected);
}
