//! Files through node logic: a push that is interrupted goes on from where it was, across a
//! restart of node logic too, and a file that does not match its digest is never put in
//! place.

use std::io::{Cursor, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use orion::files::FileState;
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

/// A megabyte and a bit that is not the same anywhere twice.
fn contents() -> Vec<u8> {
    (0..1_200_000_u32)
        .map(|index| (index.wrapping_mul(2_654_435_761) >> 24) as u8)
        .collect()
}

/// A source whose reads fail once `fail_at` bytes have been sent: the cable is pulled. The
/// first pass over it, which is the sender taking its digest, is let through.
struct Failing {
    inner: Cursor<Vec<u8>>,
    /// Whether this is the pass that sends.
    sending: bool,
    fail_at: u64,
}

impl Read for Failing {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.sending && self.inner.position() >= self.fail_at {
            return Err(std::io::Error::other("the cable was pulled"));
        }
        let read = self.inner.read(buf)?;
        if read == 0 {
            // The digest pass reached the end: what follows is sending.
            self.sending = true;
        }
        Ok(read)
    }
}

impl Seek for Failing {
    fn seek(&mut self, to: SeekFrom) -> std::io::Result<u64> {
        self.inner.seek(to)
    }
}

/// A source that reads differently the second time: what is sent is not what the digest was
/// taken of.
struct Lying {
    inner: Cursor<Vec<u8>>,
    passes: u32,
}

impl Read for Lying {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let at = self.inner.position();
        let read = self.inner.read(buf)?;
        if self.passes >= 1 && at == 0 && read > 0 {
            buf[0] ^= 0xFF;
        }
        if read == 0 {
            self.passes += 1;
        }
        Ok(read)
    }
}

impl Seek for Lying {
    fn seek(&mut self, to: SeekFrom) -> std::io::Result<u64> {
        self.inner.seek(to)
    }
}

/// Pushes until node logic answers.
fn push(
    orion: &Orion,
    destination: &str,
    source: &mut (impl Read + Seek),
    seen: &mut Vec<u64>,
) -> Result<orion::files::Transfer, ClientError> {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match orion
            .files()
            .push(destination, source, |received, _| seen.push(received))
        {
            Err(ClientError::Refused(Error::Unavailable)) if Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(10));
            }
            other => return other,
        }
    }
}

#[test]
fn a_push_that_is_interrupted_goes_on_from_where_it_was() {
    let dir = dir("files-resume");
    let board = Board::start_for_node(&dir);
    let address = Address::Local(board.socket().to_owned());
    let logic = Logic::start(settings(&dir));
    let data = contents();
    let place = dir.join("files/models/detector.bin");

    // The first try: the source fails after 400 KiB.
    let sender = Orion::connect(&address, "sender").expect("connect");
    let mut failing = Failing {
        inner: Cursor::new(data.clone()),
        sending: false,
        fail_at: 400_000,
    };
    let mut seen = Vec::new();
    let failed = push(&sender, "models/detector.bin", &mut failing, &mut seen);
    assert!(matches!(failed, Err(ClientError::Io(_))), "{failed:?}");
    assert!(!place.exists(), "nothing is in place until it is whole");
    drop(sender);

    // Node logic is killed and started again: what it had is on disk.
    drop(logic);
    let _logic = Logic::start(settings(&dir));

    // The second try, from another session: it goes on, and does not start over.
    let sender = Orion::connect(&address, "sender").expect("connect");
    let mut seen = Vec::new();
    let done = push(
        &sender,
        "models/detector.bin",
        &mut Cursor::new(data.clone()),
        &mut seen,
    )
    .expect("push");
    assert_eq!(done.state, FileState::Committed, "{}", done.detail);
    assert_eq!(done.received, data.len() as u64);
    let resumed_at = seen[0];
    assert!(
        (300_000..=400_000).contains(&resumed_at),
        "it went on from {resumed_at}"
    );
    assert_eq!(std::fs::read(&place).expect("in place"), data);

    // The same file again is sent again and is the same file.
    let mut seen = Vec::new();
    let again = push(
        &sender,
        "models/detector.bin",
        &mut Cursor::new(data.clone()),
        &mut seen,
    )
    .expect("push");
    assert_eq!(again.state, FileState::Committed);
    assert_eq!(seen[0], 0);
    // Nothing is left behind where parts are kept.
    let left: Vec<_> = std::fs::read_dir(dir.join("files/.incoming"))
        .expect("incoming")
        .flatten()
        .map(|entry| entry.file_name())
        .collect();
    assert!(left.is_empty(), "{left:?}");
}

#[test]
fn a_file_that_does_not_match_its_digest_is_refused() {
    let dir = dir("files-digest");
    let board = Board::start_for_node(&dir);
    let address = Address::Local(board.socket().to_owned());
    let _logic = Logic::start(settings(&dir));
    let sender = Orion::connect(&address, "sender").expect("connect");
    let place = dir.join("files/firmware.bin");

    // Something good is there first.
    let good = b"the firmware that works".to_vec();
    let mut seen = Vec::new();
    let done = push(
        &sender,
        "firmware.bin",
        &mut Cursor::new(good.clone()),
        &mut seen,
    )
    .expect("push");
    assert_eq!(done.state, FileState::Committed);

    // Then one whose bytes are not the ones its digest was taken of.
    let mut lying = Lying {
        inner: Cursor::new(contents()),
        passes: 0,
    };
    let refused = push(&sender, "firmware.bin", &mut lying, &mut seen).expect("an answer");
    assert_eq!(refused.state, FileState::Rejected);
    assert_eq!(refused.detail, "the digest does not match");
    // What was there is untouched, and the bad bytes are gone.
    assert_eq!(std::fs::read(&place).expect("in place"), good);
    let left = std::fs::read_dir(dir.join("files/.incoming"))
        .expect("incoming")
        .count();
    assert_eq!(left, 0);
}
