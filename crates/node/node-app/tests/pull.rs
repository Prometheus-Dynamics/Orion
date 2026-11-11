//! A node fetches a file itself from a local server: whole, gone on with after the server
//! hung up, and refused when it is not the file its digest says.

#![cfg(feature = "pull")]

use std::io::{BufRead, BufReader, Cursor, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use orion::files::{FileState, Pull, Transfer, digest};
use orion::types::Error;
use orion::{Address, ClientError, Orion};
use orion_node_app::{Node, NodeSettings};
use orion_stubs::Board;
use tokio::sync::oneshot;

fn dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("orion-node-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("directory");
    dir
}

fn settings(dir: &Path) -> NodeSettings {
    let mut source = orion_config::Source::parse("", []).expect("no settings");
    let mut settings = NodeSettings::from_source(&mut source).expect("defaults");
    settings.config.socket = dir.join("orion.sock");
    settings.config.state_dir = dir.join("state");
    settings.files_dir = dir.join("files");
    settings.audit = Some(orion_audit::AuditConfig::new(&dir.join("state/audit.log")));
    settings.config.node_name = "board".parse().expect("name");
    settings
}

/// Node logic in a thread of its own, until dropped.
struct Logic {
    kill: Option<oneshot::Sender<()>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Logic {
    fn start(settings: NodeSettings) -> Self {
        let (kill, killed) = oneshot::channel::<()>();
        let thread = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("runtime");
            runtime.block_on(async move {
                let mut node = Node::load(settings).expect("node logic starts");
                tokio::select! {
                    () = node.run(std::future::pending()) => {}
                    _ = killed => {}
                }
            });
        });
        Self {
            kill: Some(kill),
            thread: Some(thread),
        }
    }
}

impl Drop for Logic {
    fn drop(&mut self) {
        if let (Some(kill), Some(thread)) = (self.kill.take(), self.thread.take()) {
            let _ = kill.send(());
            let _ = thread.join();
        }
    }
}

/// A small HTTP server for one file that honours `Range`, hangs up part way through its
/// first answer if told to, and remembers the `Range` of each request.
struct Server {
    address: SocketAddr,
    asked: Arc<Mutex<Vec<Option<String>>>>,
}

impl Server {
    fn start(file: Vec<u8>, drop_after: Option<usize>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("listen");
        let address = listener.local_addr().expect("address");
        let asked = Arc::new(Mutex::new(Vec::new()));
        let heard = asked.clone();
        std::thread::spawn(move || {
            for (number, stream) in listener.incoming().flatten().enumerate() {
                let range = serve(stream, &file, drop_after.filter(|_| number == 0));
                heard.lock().expect("lock").push(range);
            }
        });
        Self { address, asked }
    }

    fn url(&self) -> String {
        format!("http://{}/detector.bin", self.address)
    }
}

fn serve(mut stream: TcpStream, file: &[u8], drop_after: Option<usize>) -> Option<String> {
    let mut reader = BufReader::new(stream.try_clone().expect("clone"));
    let mut range = None;
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line).unwrap_or(0) == 0 || line == "\r\n" {
            break;
        }
        if let Some(value) = line.to_ascii_lowercase().strip_prefix("range:") {
            range = Some(value.trim().to_owned());
        }
    }
    let from = range
        .as_deref()
        .and_then(|range| range.strip_prefix("bytes="))
        .and_then(|range| range.strip_suffix('-'))
        .and_then(|from| from.parse::<usize>().ok())
        .filter(|from| *from <= file.len());
    let body = &file[from.unwrap_or(0)..];
    let head = match from {
        Some(from) => format!(
            "HTTP/1.1 206 Partial Content\r\nContent-Range: bytes {from}-{}/{}\r\n",
            file.len() - 1,
            file.len()
        ),
        None => "HTTP/1.1 200 OK\r\n".to_owned(),
    };
    let _ = write!(
        stream,
        "{head}Content-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    let _ = stream.write_all(&body[..drop_after.unwrap_or(body.len()).min(body.len())]);
    range
}

fn contents() -> Vec<u8> {
    (0..900_000_u32)
        .map(|index| (index.wrapping_mul(2_654_435_761) >> 24) as u8)
        .collect()
}

/// Asks the node to fetch, until node logic answers.
fn pull(orion: &Orion, pull: &Pull<'_>) -> Result<Transfer, ClientError> {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match orion.files().pull(pull, Duration::from_secs(20), |_, _| {}) {
            Err(ClientError::Refused(Error::Unavailable)) if Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(10));
            }
            other => return other,
        }
    }
}

#[test]
fn a_node_fetches_a_file_and_goes_on_after_the_server_hung_up() {
    let dir = dir("pull");
    let board = Board::start_for_node(&dir);
    let _logic = Logic::start(settings(&dir));
    let orion =
        Orion::connect(&Address::Local(board.socket().to_owned()), "updater").expect("connect");
    let data = contents();
    let (sum, size) = digest(&mut Cursor::new(data.clone())).expect("digest");
    let place = dir.join("files/models/detector.bin");

    // The server hangs up after 300 KiB of its first answer.
    let server = Server::start(data.clone(), Some(300_000));
    let url = server.url();
    let asking = Pull {
        transfer: 41,
        url: &url,
        destination: "models/detector.bin",
        size,
        digest: sum,
    };
    let first = pull(&orion, &asking).expect("an answer");
    assert_eq!(first.state, FileState::Rejected, "{first:?}");
    assert_eq!(first.received, 300_000);
    assert!(
        !place.exists(),
        "nothing is in place until it is whole and checked"
    );

    // Asked again with the same number: only the rest is fetched, and it is put in place.
    let second = pull(&orion, &asking).expect("an answer");
    assert_eq!(second.state, FileState::Committed, "{}", second.detail);
    assert_eq!(second.received, size);
    assert_eq!(std::fs::read(&place).expect("in place"), data);
    assert_eq!(
        *server.asked.lock().expect("lock"),
        [None, Some("bytes=300000-".to_owned())]
    );
    let left = std::fs::read_dir(dir.join("files/.incoming"))
        .expect("incoming")
        .count();
    assert_eq!(left, 0);
}

#[test]
fn what_was_fetched_is_checked_like_any_file() {
    let dir = dir("pull-digest");
    let board = Board::start_for_node(&dir);
    let _logic = Logic::start(settings(&dir));
    let orion =
        Orion::connect(&Address::Local(board.socket().to_owned()), "updater").expect("connect");
    let data = contents();
    let (sum, size) = digest(&mut Cursor::new(data.clone())).expect("digest");

    // The server has another file of the same size than the one that was asked for.
    let mut other = data.clone();
    other[1234] ^= 0x40;
    let server = Server::start(other, None);
    let url = server.url();
    let refused = pull(
        &orion,
        &Pull {
            transfer: 42,
            url: &url,
            destination: "firmware.bin",
            size,
            digest: sum,
        },
    )
    .expect("an answer");
    assert_eq!(refused.state, FileState::Rejected);
    assert_eq!(refused.detail, "the digest does not match");
    assert!(!dir.join("files/firmware.bin").exists());

    // Not a URL a node fetches.
    let local = pull(
        &orion,
        &Pull {
            transfer: 43,
            url: "file:///etc/passwd",
            destination: "passwd",
            size: 10,
            digest: sum,
        },
    )
    .expect("an answer");
    assert_eq!(local.state, FileState::Rejected);
    assert_eq!(local.detail, "not a URL this node fetches");

    // A server that is not there.
    let nowhere = TcpListener::bind("127.0.0.1:0").expect("listen");
    let gone = format!("http://{}/a", nowhere.local_addr().expect("address"));
    drop(nowhere);
    let unreachable = pull(
        &orion,
        &Pull {
            transfer: 44,
            url: &gone,
            destination: "a.bin",
            size,
            digest: sum,
        },
    )
    .expect("an answer");
    assert_eq!(unreachable.state, FileState::Rejected);
    assert!(
        unreachable.detail.starts_with("the server was not reached"),
        "{}",
        unreachable.detail
    );
}
