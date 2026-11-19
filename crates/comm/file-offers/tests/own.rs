//! The comm core's own files: given by the comm core itself, listed in an index a client
//! reads, each bounded, its bytes held for a fetch that goes on, and its prefix no program's.

#![cfg(target_os = "linux")]

use std::cell::Cell;
use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::mpsc;

use orion_connect::{Address, Client, ClientError, Options, Session};
use orion_file_offers::{FileOffersService, OwnFiles, OwnName, Reading, Ready};
use orion_files::index::{Entry, Index};
use orion_files::{FileState, Files, OfferedBy};
use orion_scratch::Scratch;
use orion_server::{Comm, Error, Limits, Security, Sessions};
use orion_unix::UnixCarrier;

/// Bytes that differ from place to place, so a chunk in the wrong place is seen.
fn content(size: usize) -> Vec<u8> {
    (0..size).map(|at| (at * 31 % 251) as u8).collect()
}

/// `own/`: `count`, which says how often it was made; `big`, larger than its bound; `broken`,
/// which cannot be read; `sessions`, how many there are.
struct Fake {
    made: Rc<Cell<u32>>,
}

const BIG_BOUND: u64 = 300 * 1024;

impl OwnFiles for Fake {
    fn prefix(&self) -> &str {
        "own/"
    }

    fn names(&self) -> Vec<OwnName> {
        let name = |name: &str, max_bytes, user_data| OwnName {
            name: name.into(),
            max_bytes,
            user_data,
        };
        vec![
            name("own/count", 100, false),
            name("own/big", BIG_BOUND, true),
            name("own/broken", 10, false),
            name("own/sessions", 100, true),
        ]
    }

    fn read(&mut self, name: &str, sessions: &Sessions, _: Ready) -> Reading {
        match name {
            "own/count" => {
                self.made.set(self.made.get() + 1);
                Reading::Now(format!("made {} times", self.made.get()).into_bytes())
            }
            "own/big" => Reading::Now(content(600 * 1024)),
            "own/sessions" => Reading::Now(format!("{}", sessions.iter().count()).into_bytes()),
            _ => Reading::Not(format!("{name}: cannot be read now")),
        }
    }
}

/// A comm core whose own files are [`Fake`], in a thread of its own, on a socket of its own.
fn comm_core(name: &str) -> (PathBuf, tokio::sync::oneshot::Sender<()>, Scratch) {
    let dir = Scratch::new(&format!("own-{name}"));
    let socket = dir.join("orion.sock");
    let (ready, started) = mpsc::channel();
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let path = socket.clone();
    std::thread::spawn(move || {
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
            let own = Fake {
                made: Rc::default(),
            };
            comm.add_service(FileOffersService::standard().with_own(&comm, own));
            let unix = UnixCarrier::bind(&path, Some(0o600), &[]).expect("socket");
            tokio::task::spawn_local(async move { comm.serve(unix).await });
            ready.send(()).expect("ready");
            let _ = stopped.await;
        });
    });
    started.recv().expect("the comm core starts");
    (socket, stop, dir)
}

fn files(socket: &Path, name: &str) -> Files {
    let session = Session::connect(&Address::Local(socket.to_owned()), name, Options::default())
        .expect("connect");
    Files::new(&Client::new(session))
}

fn fetch(files: &Files, name: &str, have: &[u8]) -> (orion_files::Transfer, Vec<u8>) {
    let mut into = Cursor::new(have.to_vec());
    into.set_position(have.len() as u64);
    let transfer = files
        .fetch(name, &mut into, have.len() as u64, |_, _| {})
        .expect(name);
    (transfer, into.into_inner())
}

#[test]
fn the_comm_core_s_own_index_is_one_a_client_reads() {
    let (socket, _stop, _dir) = comm_core("index");
    let client = files(&socket, "collector");
    let (transfer, text) = fetch(&client, "own/index", &[]);
    assert_eq!(transfer.state, FileState::Committed, "{}", transfer.detail);
    let index = Index::parse(std::str::from_utf8(&text).expect("text")).expect("an index");
    let entry = |name: &str, max_bytes, user_data| Entry {
        name: name.into(),
        max_bytes,
        user_data,
    };
    assert_eq!(
        index,
        Index {
            total: 100 + BIG_BOUND + 10 + 100,
            entries: vec![
                entry("own/count", 100, false),
                entry("own/big", BIG_BOUND, true),
                entry("own/broken", 10, false),
                entry("own/sessions", 100, true),
            ],
        }
    );
    let (offers, _) = client.offers().expect("offers");
    assert_eq!(offers[0].prefix, "own/");
    assert_eq!(offers[0].by, OfferedBy::CommCore);
    assert_eq!(offers[0].name, "");
}

#[test]
fn an_own_file_is_given_whole_to_its_bound_and_paced_by_the_client() {
    let (socket, _stop, _dir) = comm_core("big");
    let client = files(&socket, "collector");
    let (transfer, bytes) = fetch(&client, "own/big", &[]);
    assert_eq!(transfer.state, FileState::Committed, "{}", transfer.detail);
    assert_eq!(bytes, content(BIG_BOUND as usize), "cut off at its bound");
    let (transfer, bytes) = fetch(&client, "own/sessions", &[]);
    assert_eq!(transfer.state, FileState::Committed, "{}", transfer.detail);
    assert_eq!(bytes, b"1", "the file is made with the sessions there are");
}

#[test]
fn an_own_file_holds_its_bytes_for_a_fetch_that_goes_on() {
    let (socket, _stop, _dir) = comm_core("held");
    let client = files(&socket, "collector");
    let (transfer, whole) = fetch(&client, "own/count", &[]);
    assert_eq!(transfer.state, FileState::Committed, "{}", transfer.detail);
    assert_eq!(whole, b"made 1 times");
    // Broken off after five bytes, it goes on from them, and is the same file.
    let (transfer, rest) = fetch(&client, "own/count", &whole[..5]);
    assert_eq!(transfer.state, FileState::Committed, "{}", transfer.detail);
    assert_eq!(rest, whole);
    assert_eq!(transfer.digest, {
        use sha2::Digest;
        <[u8; 32]>::from(sha2::Sha256::digest(&whole))
    });
}

#[test]
fn an_own_name_that_is_not_one_or_cannot_be_read_is_rejected_saying_so() {
    let (socket, _stop, _dir) = comm_core("rejected");
    let client = files(&socket, "collector");
    let (transfer, _) = fetch(&client, "own/nothing", &[]);
    assert_eq!(transfer.state, FileState::Rejected);
    assert_eq!(transfer.detail, "own/nothing: no such file");
    let (transfer, _) = fetch(&client, "own/broken", &[]);
    assert_eq!(transfer.state, FileState::Rejected);
    assert_eq!(transfer.detail, "own/broken: cannot be read now");
}

#[test]
fn no_program_may_offer_the_comm_core_s_own_prefix_or_one_under_it() {
    let (socket, _stop, _dir) = comm_core("refused");
    let program = files(&socket, "logger");
    for prefix in ["own/", "own/deeper/"] {
        let error = program.offer(prefix).expect_err("the comm core's");
        assert!(
            matches!(&error, ClientError::Refused { error: Error::Busy, detail } if detail == &format!("{prefix} is under own/, which the comm core offers")),
            "{error}"
        );
    }
    let (offers, clashes) = program.offers().expect("offers");
    assert_eq!(clashes, 2);
    assert_eq!(offers.len(), 1);
}
