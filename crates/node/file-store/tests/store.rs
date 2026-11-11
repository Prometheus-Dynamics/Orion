//! The store by itself: what each message does to a transfer.

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use orion_file_store::{Files, FilesConfig, MAX_TRANSFERS};
use orion_msg_files::{FileBegin, FileChunk, FileCommit, FileState, FileStatus};
use orion_reply::{Asked, Out};
use orion_types::Error;
use orion_wire::body::encode_fields;
use orion_wire::{Body, Bytes, Decode};
use sha2::{Digest, Sha256};

#[derive(Default)]
struct Sent {
    frames: Vec<(u32, Vec<u8>)>,
    scratch: Vec<u8>,
}

impl Out for Sent {
    fn frame(&mut self, session: u32, _kind: u8, payload: &[u8], _may_drop: bool) {
        self.frames.push((session, payload.to_vec()));
    }

    fn scratch(&mut self) -> &mut Vec<u8> {
        if self.scratch.is_empty() {
            self.scratch = vec![0; 70_000];
        }
        &mut self.scratch
    }
}

/// What a status said: who was told, bytes received, state, detail.
type Said = (u32, u64, FileState, String);

struct Bench {
    files: Files,
    sent: Sent,
    root: PathBuf,
}

fn root(name: &str) -> PathBuf {
    let root = std::env::temp_dir().join(format!("orion-file-store-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    root
}

fn digest(data: &[u8]) -> [u8; 32] {
    Sha256::digest(data).into()
}

impl Bench {
    fn new(name: &str) -> Self {
        Self::at(&root(name))
    }

    fn at(root: &Path) -> Self {
        Self {
            files: Files::open(FilesConfig::new(root)).expect("store"),
            sent: Sent::default(),
            root: root.to_owned(),
        }
    }

    fn ask<B: Body>(&mut self, from: u32, body: &B) -> Result<Said, Error> {
        let mut payload = vec![0_u8; 70_000];
        let len = encode_fields(body, &mut payload).expect("encode");
        let asked = Asked {
            session: from,
            seq: 1,
            kind: B::KIND,
            class: 4,
            uid: 1000,
            caller: "",
            payload: &payload[..len],
        };
        self.files.request(&asked, &mut self.sent)?;
        Ok(self.said())
    }

    #[track_caller]
    fn said(&mut self) -> Said {
        let (session, payload) = self.sent.frames.remove(0);
        let status = FileStatus::decode(&payload).expect("status");
        (
            session,
            status.received,
            status.state,
            status.detail.to_owned(),
        )
    }

    fn begin(&mut self, transfer: u64, destination: &str, data: &[u8]) -> Result<Said, Error> {
        self.ask(
            1,
            &FileBegin {
                transfer,
                destination: destination.parse().expect("name"),
                size: data.len() as u64,
                digest: digest(data),
            },
        )
    }

    fn chunk(&mut self, transfer: u64, offset: u64, data: &[u8]) -> Result<Said, Error> {
        self.ask(
            1,
            &FileChunk {
                transfer,
                offset,
                data: Bytes(data),
            },
        )
    }

    /// Commits and waits for the helper.
    fn commit(&mut self, transfer: u64) -> Said {
        let first = self.ask(1, &FileCommit { transfer }).expect("commit");
        if first.2 != FileState::Verifying {
            return first;
        }
        let deadline = Instant::now() + Duration::from_secs(10);
        while self.sent.frames.is_empty() {
            assert!(Instant::now() < deadline, "the check never ended");
            assert!(self.files.verifying());
            std::thread::sleep(Duration::from_millis(1));
            self.files.poll(&mut self.sent);
        }
        assert!(!self.files.verifying());
        self.said()
    }
}

const DATA: &[u8] = b"0123456789abcdefghij";

#[test]
fn chunks_in_order_then_commit_puts_the_file_in_place() {
    let mut bench = Bench::new("order");
    let began = bench.begin(1, "a/b/file.bin", DATA).expect("begin");
    assert_eq!((began.1, began.2), (0, FileState::Receiving));
    assert_eq!(bench.chunk(1, 0, &DATA[..8]).expect("chunk").1, 8);
    assert_eq!(bench.chunk(1, 8, &DATA[8..]).expect("chunk").1, 20);
    assert!(!bench.root.join("a/b/file.bin").exists());
    let done = bench.commit(1);
    assert_eq!((done.1, done.2), (20, FileState::Committed));
    assert_eq!(
        std::fs::read(bench.root.join("a/b/file.bin")).expect("file"),
        DATA
    );
    assert_eq!(bench.files.transfers(), 0);
}

#[test]
fn a_chunk_that_is_not_the_next_is_not_taken() {
    let mut bench = Bench::new("gap");
    bench.begin(1, "file.bin", DATA).expect("begin");
    // Ahead of where the file is: not taken, and the answer says where it is.
    assert_eq!(bench.chunk(1, 8, &DATA[8..]).expect("chunk").1, 0);
    assert_eq!(bench.chunk(1, 0, &DATA[..8]).expect("chunk").1, 8);
    // Sent again: not taken twice.
    assert_eq!(bench.chunk(1, 0, &DATA[..8]).expect("chunk").1, 8);
    assert_eq!(bench.chunk(1, 8, &DATA[8..]).expect("chunk").1, 20);
    assert_eq!(bench.commit(1).2, FileState::Committed);
    assert_eq!(
        std::fs::read(bench.root.join("file.bin")).expect("file"),
        DATA
    );
}

#[test]
fn a_transfer_goes_on_in_a_store_that_was_closed_and_opened() {
    let root = root("reopen");
    let mut bench = Bench::at(&root);
    bench.begin(7, "file.bin", DATA).expect("begin");
    bench.chunk(7, 0, &DATA[..12]).expect("chunk");
    drop(bench);

    let mut bench = Bench::at(&root);
    // A chunk for a transfer this store has not been told of: it must begin again first.
    assert_eq!(bench.chunk(7, 12, &DATA[12..]), Err(Error::NotFound));
    let began = bench.begin(7, "file.bin", DATA).expect("begin");
    assert_eq!(began.1, 12, "it goes on from what was kept");
    bench.chunk(7, 12, &DATA[12..]).expect("chunk");
    assert_eq!(bench.commit(7).2, FileState::Committed);
    assert_eq!(std::fs::read(root.join("file.bin")).expect("file"), DATA);
}

#[test]
fn the_same_number_for_another_file_starts_over() {
    let mut bench = Bench::new("restart");
    bench.begin(1, "file.bin", DATA).expect("begin");
    bench.chunk(1, 0, &DATA[..12]).expect("chunk");
    for (destination, data) in [
        ("other.bin", DATA),
        ("file.bin", &b"another file altogether"[..]),
    ] {
        let began = bench.begin(1, destination, data).expect("begin");
        assert_eq!(began.1, 0, "{destination}");
        bench.chunk(1, 0, &data[..5]).expect("chunk");
    }
    // And closed and opened: the part of the other file is not taken for this one.
    let root = bench.root.clone();
    drop(bench);
    let mut bench = Bench::at(&root);
    assert_eq!(bench.begin(1, "file.bin", DATA).expect("begin").1, 0);
}

#[test]
fn a_commit_before_every_byte_is_here_is_rejected_and_the_transfer_stays() {
    let mut bench = Bench::new("early");
    bench.begin(1, "file.bin", DATA).expect("begin");
    bench.chunk(1, 0, &DATA[..8]).expect("chunk");
    let early = bench.commit(1);
    assert_eq!((early.1, early.2), (8, FileState::Rejected));
    assert_eq!(early.3, "not all of it is here");
    bench.chunk(1, 8, &DATA[8..]).expect("chunk");
    assert_eq!(bench.commit(1).2, FileState::Committed);
    // A commit of nothing.
    let none = bench.commit(99);
    assert_eq!(
        (none.2, none.3.as_str()),
        (FileState::Rejected, "no such transfer")
    );
}

#[test]
fn bytes_that_do_not_match_the_digest_are_never_put_in_place() {
    let mut bench = Bench::new("digest");
    std::fs::create_dir_all(&bench.root).expect("root");
    std::fs::write(bench.root.join("file.bin"), b"what was there").expect("before");
    bench.begin(1, "file.bin", DATA).expect("begin");
    let mut wrong = DATA.to_vec();
    wrong[3] ^= 1;
    bench.chunk(1, 0, &wrong).expect("chunk");
    let refused = bench.commit(1);
    assert_eq!(refused.2, FileState::Rejected);
    assert_eq!(refused.3, "the digest does not match");
    assert_eq!(
        std::fs::read(bench.root.join("file.bin")).expect("file"),
        b"what was there"
    );
    assert_eq!(bench.files.transfers(), 0);
    // The bad part is gone: the same transfer starts from nothing.
    assert_eq!(bench.begin(1, "file.bin", DATA).expect("begin").1, 0);
}

#[test]
fn more_bytes_than_the_size_are_rejected() {
    let mut bench = Bench::new("over");
    bench.begin(1, "file.bin", DATA).expect("begin");
    let over = bench
        .chunk(1, 0, b"0123456789abcdefghij-and-more")
        .expect("chunk");
    assert_eq!(
        (over.2, over.3.as_str()),
        (FileState::Rejected, "more bytes than its size")
    );
    assert_eq!(bench.files.transfers(), 0);
}

#[test]
fn a_file_larger_than_the_node_takes_is_rejected_at_begin() {
    let root = root("large");
    let mut bench = Bench {
        files: Files::open(FilesConfig {
            root: root.clone(),
            max_size: 10,
        })
        .expect("store"),
        sent: Sent::default(),
        root,
    };
    let refused = bench.begin(1, "file.bin", DATA).expect("begin");
    assert_eq!(refused.2, FileState::Rejected);
    assert_eq!(bench.files.transfers(), 0);
}

#[test]
fn a_destination_cannot_leave_the_store() {
    let mut bench = Bench::new("escape");
    let outside = root("escape-outside");
    std::fs::create_dir_all(&outside).expect("outside");
    std::fs::create_dir_all(&bench.root).expect("root");
    // Someone put a link in the store that leads out of it.
    std::os::unix::fs::symlink(&outside, bench.root.join("link")).expect("link");
    bench.begin(1, "link/file.bin", DATA).expect("begin");
    bench.chunk(1, 0, DATA).expect("chunk");
    let refused = bench.commit(1);
    assert_eq!(refused.2, FileState::Rejected);
    assert_eq!(refused.3, "the destination is outside the store");
    assert!(!outside.join("file.bin").exists());
    // Nor into where parts are kept.
    let own = bench.begin(2, ".incoming/x", DATA).expect("begin");
    assert_eq!(own.2, FileState::Rejected);
    // A destination that is a directory.
    std::fs::create_dir_all(bench.root.join("dir")).expect("dir");
    bench.begin(3, "dir", DATA).expect("begin");
    bench.chunk(3, 0, DATA).expect("chunk");
    assert_eq!(bench.commit(3).3, "the destination is a directory");
}

#[test]
fn only_so_many_transfers_at_once() {
    let mut bench = Bench::new("many");
    for transfer in 0..MAX_TRANSFERS as u64 {
        bench
            .begin(transfer, &format!("f{transfer}"), DATA)
            .expect("begin");
    }
    assert_eq!(bench.begin(1_000, "one-more", DATA), Err(Error::Busy));
    // One that is known goes on.
    bench.begin(3, "f3", DATA).expect("begin");
}

#[test]
fn whoever_committed_is_told_unless_it_is_gone() {
    let mut bench = Bench::new("gone");
    bench.begin(1, "file.bin", DATA).expect("begin");
    bench.chunk(1, 0, DATA).expect("chunk");
    let first = bench.ask(1, &FileCommit { transfer: 1 }).expect("commit");
    assert_eq!(first.2, FileState::Verifying);
    bench.files.gone(1);
    let deadline = Instant::now() + Duration::from_secs(10);
    while bench.files.verifying() {
        assert!(Instant::now() < deadline);
        std::thread::sleep(Duration::from_millis(1));
        bench.files.poll(&mut bench.sent);
    }
    assert!(bench.sent.frames.is_empty(), "there is nobody to tell");
    // The file is in place all the same.
    assert_eq!(
        std::fs::read(bench.root.join("file.bin")).expect("file"),
        DATA
    );
}
