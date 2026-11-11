//! A file for a registered receiver goes straight to it: the node keeps none of it, and a
//! large file passes with memory that does not grow with the file.

use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use orion::files::{FileState, Handed, digest};
use orion::types::Error;
use orion::{Address, ClientError, Orion};
use orion_node_app::{Node, NodeSettings};
use orion_stubs::Board;
use sha2::{Digest, Sha256};
use tokio::sync::oneshot;

/// The file: large beside a chunk (32 KiB) and beside what may be in flight (256 KiB).
const SIZE: u64 = 96 << 20;

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

/// A file that is made as it is read and never held: byte `n` is a function of `n`.
struct Made {
    at: u64,
    size: u64,
}

fn byte(at: u64) -> u8 {
    (at.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 56) as u8
}

impl Read for Made {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let want = (self.size - self.at).min(buf.len() as u64) as usize;
        for (offset, slot) in buf[..want].iter_mut().enumerate() {
            *slot = byte(self.at + offset as u64);
        }
        self.at += want as u64;
        Ok(want)
    }
}

impl Seek for Made {
    fn seek(&mut self, to: SeekFrom) -> std::io::Result<u64> {
        self.at = match to {
            SeekFrom::Start(at) => at,
            SeekFrom::End(back) => self.size.saturating_add_signed(back),
            SeekFrom::Current(by) => self.at.saturating_add_signed(by),
        };
        Ok(self.at)
    }
}

/// The program that manages the board: it receives everything under `slots/` and writes it
/// to the slot itself, checking the digest as the bytes pass.
struct Manager {
    stop: Arc<AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Manager {
    fn start(socket: &Path, slot: &Path) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let (socket, slot, stopping) = (socket.to_owned(), slot.to_owned(), stop.clone());
        let (ready, started) = std::sync::mpsc::channel();
        let thread = std::thread::spawn(move || {
            let orion = Orion::connect(&Address::Local(socket), "board-manager").expect("connect");
            let deadline = Instant::now() + Duration::from_secs(10);
            let mut sink = loop {
                match orion.files().sink("slots/") {
                    Ok(sink) => break sink,
                    Err(ClientError::Refused(Error::Unavailable)) if Instant::now() < deadline => {
                        std::thread::sleep(Duration::from_millis(10));
                    }
                    Err(error) => panic!("sink: {error}"),
                }
            };
            ready.send(()).expect("ready");
            let mut writing: Option<(u64, std::fs::File, Sha256, u64, [u8; 32], u64)> = None;
            while !stopping.load(Ordering::Relaxed) {
                let Ok(handed) = sink.next(Duration::from_millis(20)) else {
                    return;
                };
                match handed {
                    Some(Handed::Begin {
                        transfer,
                        size,
                        digest,
                        ..
                    }) => {
                        let file = std::fs::File::create(&slot).expect("the slot");
                        writing = Some((transfer, file, Sha256::new(), 0, digest, size));
                        let _ = sink.status(transfer, 0, FileState::Receiving, "");
                    }
                    Some(Handed::Chunk {
                        transfer,
                        offset,
                        data,
                    }) => {
                        let Some((number, file, hasher, received, ..)) = &mut writing else {
                            continue;
                        };
                        if *number == transfer && offset == *received {
                            file.write_all(&data).expect("write the slot");
                            hasher.update(&data);
                            *received += data.len() as u64;
                        }
                        let _ = sink.status(transfer, *received, FileState::Receiving, "");
                    }
                    Some(Handed::Commit { transfer }) => {
                        let Some((_, file, hasher, received, digest, size)) = writing.take() else {
                            continue;
                        };
                        file.sync_all().expect("sync the slot");
                        let found: [u8; 32] = hasher.finalize().into();
                        let (state, detail) = if received == size && found == digest {
                            (FileState::Committed, "")
                        } else {
                            (FileState::Rejected, "the digest does not match")
                        };
                        let _ = sink.status(transfer, received, state, detail);
                    }
                    None => {}
                }
            }
        });
        started
            .recv_timeout(Duration::from_secs(15))
            .expect("the manager is the receiver");
        Self {
            stop,
            thread: Some(thread),
        }
    }
}

impl Drop for Manager {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

/// The most memory this process has had at once, in bytes.
fn peak_memory() -> u64 {
    let status = std::fs::read_to_string("/proc/self/status").expect("status");
    let line = status
        .lines()
        .find(|line| line.starts_with("VmHWM:"))
        .expect("VmHWM");
    let kilobytes: u64 = line
        .split_whitespace()
        .nth(1)
        .expect("a number")
        .parse()
        .expect("a number");
    kilobytes * 1024
}

#[test]
fn a_large_file_passes_to_its_receiver_with_memory_bounded_by_the_chunk() {
    let dir = dir("sink");
    let board = Board::start_for_node(&dir);
    let address = Address::Local(board.socket().to_owned());
    let _logic = Logic::start(settings(&dir));
    let slot = dir.join("slot-b.img");
    let _manager = Manager::start(board.socket(), &slot);
    let sender = Orion::connect(&address, "updater").expect("connect");

    // A small file first, so that everything that is set up once has been.
    let mut small = Made {
        at: 0,
        size: 300_000,
    };
    let done = sender
        .files()
        .push("slots/warm-up.img", &mut small, |_, _| {})
        .expect("push");
    assert_eq!(done.state, FileState::Committed, "{}", done.detail);

    // The whole process is in this measurement: the sender, the comm core, node logic and
    // the receiver. None of them may grow with the file.
    let before = peak_memory();
    let mut source = Made { at: 0, size: SIZE };
    let mut steps = 0_u64;
    let done = sender
        .files()
        .push("slots/b.img", &mut source, |_, _| steps += 1)
        .expect("push");
    let grew = peak_memory().saturating_sub(before);
    assert_eq!(done.state, FileState::Committed, "{}", done.detail);
    assert_eq!(done.received, SIZE);
    assert!(
        steps > 1000,
        "the receiver's answers paced the sender: {steps}"
    );
    println!(
        "{} MiB passed; peak memory grew by {} KiB",
        SIZE >> 20,
        grew >> 10
    );
    // 96 MiB passed. A copy of it anywhere would show; a few chunks in flight do not.
    assert!(grew < 8 << 20, "memory grew by {grew} bytes");

    // It is where the receiver put it, whole.
    drop(sender);
    let (expected, _) = digest(&mut Made { at: 0, size: SIZE }).expect("digest");
    let mut written = std::fs::File::open(&slot).expect("the slot");
    let (found, len) = digest(&mut written).expect("digest");
    assert_eq!((len, found), (SIZE, expected));
    let sender = Orion::connect(&address, "updater").expect("connect");
    // And the node kept none of it: nothing in its store, nothing in what it was receiving.
    assert!(!dir.join("files/slots").exists());
    let parts = std::fs::read_dir(dir.join("files/.incoming"))
        .expect("incoming")
        .count();
    assert_eq!(parts, 0);

    // A file that is not for the receiver is the node's own.
    let mut other = Made {
        at: 0,
        size: 100_000,
    };
    let done = sender
        .files()
        .push("models/a.bin", &mut other, |_, _| {})
        .expect("push");
    assert_eq!(done.state, FileState::Committed);
    assert_eq!(
        std::fs::metadata(dir.join("files/models/a.bin"))
            .expect("in the store")
            .len(),
        100_000
    );
}

#[test]
fn a_receiver_that_goes_ends_the_transfer_and_the_prefix_is_the_stores_again() {
    let dir = dir("sink-gone");
    let board = Board::start_for_node(&dir);
    let address = Address::Local(board.socket().to_owned());
    let _logic = Logic::start(settings(&dir));
    let slot = dir.join("slot-b.img");
    let manager = Manager::start(board.socket(), &slot);
    let sender = Orion::connect(&address, "updater").expect("connect");
    let mut file = Made {
        at: 0,
        size: 200_000,
    };
    let done = sender
        .files()
        .push("slots/b.img", &mut file, |_, _| {})
        .expect("push");
    assert_eq!(done.state, FileState::Committed);
    assert!(!dir.join("files/slots").exists());

    drop(manager);
    // With no receiver, the same destination is a place in the node's store.
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let mut file = Made {
            at: 0,
            size: 200_000,
        };
        let done = sender
            .files()
            .push("slots/b.img", &mut file, |_, _| {})
            .expect("push");
        if done.state == FileState::Committed && dir.join("files/slots/b.img").exists() {
            break;
        }
        assert!(Instant::now() < deadline, "{done:?}");
        std::thread::sleep(Duration::from_millis(20));
    }
}
