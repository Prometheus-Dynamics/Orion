//! Files the comm core gives itself, paced by the client as a program's are: its own, under
//! `orion/`, which explain a board's trouble from Orion's side, and those its settings name,
//! which serve a prefix while no program offers it. In running and in recovery.
//!
//! **A name's bytes hold for a fetch.** Each is made when it is first fetched and kept for
//! [`SNAPSHOT_MS`]; a fetch that broke off and goes on within that time is given the same
//! bytes, so its digest holds. After it, the name is made again, and a fetch going on from
//! before is told the digest does not match, and fetches from the start.

use std::cell::RefCell;
use std::rc::{Rc, Weak};

use orion_msg_files::{FETCH_WINDOW, FileGive, FileGiveChunk, FileState, FileTaken};
use orion_server::{Comm, Delivery, SessionId, Sessions, WeakComm};
use orion_wire::Bytes;
use sha2::{Digest, Sha256};

/// How long the bytes of one of the comm core's own files hold for fetches of it.
pub const SNAPSHOT_MS: u64 = 60_000;

/// The most of the comm core's own files being given at once. One more is refused as busy.
const MAX_GIVING: usize = 64;

/// One of the comm core's own files, as its index lists it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OwnName {
    /// Its whole name, the prefix included.
    pub name: String,
    /// The most bytes it can be: more is cut off.
    pub max_bytes: u64,
    /// Whether it may hold a user's data.
    pub user_data: bool,
}

/// What the comm core offers under one prefix, giving the files itself.
pub trait OwnFiles {
    /// The prefix, ending in `/`.
    fn prefix(&self) -> &str;

    /// Each file but the index, which is made from these.
    fn names(&self) -> Vec<OwnName>;

    /// Begins making `name`, one of [`OwnFiles::names`]. `sessions` is every session there
    /// is. A file made later, as a command's output is, says so and hands its bytes to
    /// `ready` when they are made; until then, every fetch of it waits.
    fn read(&mut self, name: &str, sessions: &Sessions, ready: Ready) -> Reading;
}

/// How a file is being made.
#[derive(Debug)]
pub enum Reading {
    /// Its bytes, now.
    Now(Vec<u8>),
    /// They will be handed to the [`Ready`] given, from a task of the comm core's thread.
    Later,
    /// It cannot be made: the words the client is told.
    Not(String),
}

/// Where the bytes of a file made later go. A file said to be made later must be handed to it,
/// made or not: until then, every fetch of it waits.
#[must_use = "every fetch of the file waits until it is done"]
pub struct Ready {
    own: Weak<RefCell<Own>>,
    comm: WeakComm,
    name: String,
    /// Which making of the name it is: one that is no longer awaited is ignored.
    id: u64,
}

impl std::fmt::Debug for Ready {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Ready({}, {})", self.name, self.id)
    }
}

impl Ready {
    /// Hands over the file's bytes, or why it was not made. Call it from a task on the comm
    /// core's thread, never from within [`OwnFiles::read`].
    pub fn done(self, made: Result<Vec<u8>, String>) {
        let (Some(own), Some(comm)) = (self.own.upgrade(), self.comm.upgrade()) else {
            return;
        };
        comm.with(|sessions, now_ms| {
            own.borrow_mut()
                .made((&self.name, self.id), made, now_ms, sessions);
        });
    }
}

#[derive(Debug)]
struct Snapshot {
    name: String,
    made_ms: u64,
    bytes: Rc<[u8]>,
    digest: [u8; 32],
}

#[derive(Debug)]
struct Giving {
    client: SessionId,
    transfer: u64,
    bytes: Rc<[u8]>,
    sent: u64,
    acked: u64,
    chunk: usize,
}

/// A fetch waiting for its file to be made.
#[derive(Debug)]
struct Waiting {
    client: SessionId,
    transfer: u64,
    name: String,
    from: u64,
    chunk: usize,
}

/// Files the comm core gives itself, under one prefix, being given.
pub(crate) struct Own {
    files: Box<dyn OwnFiles>,
    snapshots: Vec<Snapshot>,
    giving: Vec<Giving>,
    waiting: Vec<Waiting>,
    /// Each name being made, and which making of it.
    making: Vec<(String, u64)>,
    next: u64,
    me: Weak<RefCell<Self>>,
    comm: WeakComm,
}

/// Files the comm core gives itself, shared with what makes them later.
pub(crate) type Shared = Rc<RefCell<Own>>;

impl std::fmt::Debug for Own {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Own({}, {} giving)",
            self.files.prefix(),
            self.giving.len()
        )
    }
}

/// Why one of the comm core's own files is not given.
pub(crate) enum NotGiven {
    /// Too many are being given.
    Busy,
    /// The words the client is told, rejected.
    Rejected(String),
}

impl Own {
    pub(crate) fn new(comm: &Comm, files: Box<dyn OwnFiles>) -> Shared {
        Rc::new_cyclic(|me| {
            RefCell::new(Self {
                files,
                snapshots: Vec::new(),
                giving: Vec::new(),
                waiting: Vec::new(),
                making: Vec::new(),
                next: 0,
                me: me.clone(),
                comm: comm.downgrade(),
            })
        })
    }

    pub(crate) fn prefix(&self) -> &str {
        self.files.prefix()
    }

    /// Whether `name` is one of these, or under their prefix.
    pub(crate) fn has(&self, name: &str) -> bool {
        name.starts_with(self.files.prefix())
    }

    /// The index, as `orion_files::index` reads it.
    fn index(&self) -> Vec<u8> {
        let names = self.files.names();
        let total: u64 = names.iter().map(|name| name.max_bytes).sum();
        let mut text = format!("orion-index 1\ntotal {total}\n");
        for name in &names {
            let data = if name.user_data {
                "user-data"
            } else {
                "no-user-data"
            };
            text.push_str(&format!("{} {} {data}\n", name.name, name.max_bytes));
        }
        text.into_bytes()
    }

    /// The bytes of `name` already made, if they still hold.
    fn held(&mut self, name: &str, now_ms: u64) -> Option<(Rc<[u8]>, [u8; 32])> {
        self.snapshots
            .retain(|snapshot| now_ms.saturating_sub(snapshot.made_ms) < SNAPSHOT_MS);
        self.snapshots
            .iter()
            .find(|snapshot| snapshot.name == name)
            .map(|made| (made.bytes.clone(), made.digest))
    }

    /// Keeps `bytes` as `name`'s, cut at its bound.
    fn keep(&mut self, name: &str, mut bytes: Vec<u8>, now_ms: u64) -> (Rc<[u8]>, [u8; 32]) {
        if let Some(bound) = self.bound(name) {
            bytes.truncate(usize::try_from(bound).unwrap_or(usize::MAX));
        }
        let digest: [u8; 32] = Sha256::digest(&bytes).into();
        let bytes: Rc<[u8]> = bytes.into();
        self.snapshots.retain(|snapshot| snapshot.name != name);
        self.snapshots.push(Snapshot {
            name: name.to_owned(),
            made_ms: now_ms,
            bytes: bytes.clone(),
            digest,
        });
        (bytes, digest)
    }

    fn bound(&self, name: &str) -> Option<u64> {
        self.files
            .names()
            .into_iter()
            .find(|own| own.name == name)
            .map(|own| own.max_bytes)
    }

    fn is_index(&self, name: &str) -> bool {
        name.strip_prefix(self.files.prefix()) == Some("index")
    }

    /// Begins giving `name` to `client` from byte `from`, in chunks of `chunk` bytes, now or
    /// once it is made.
    pub(crate) fn give(
        &mut self,
        fetch: (SessionId, u64, &str, u64),
        chunk: usize,
        now_ms: u64,
        sessions: &mut Sessions,
    ) -> Result<(), NotGiven> {
        let (client, transfer, name, from) = fetch;
        // A fetch asked for again, after the client heard nothing, begins again.
        self.giving
            .retain(|giving| !(giving.client == client && giving.transfer == transfer));
        self.waiting
            .retain(|waiting| !(waiting.client == client && waiting.transfer == transfer));
        if self.giving.len() + self.waiting.len() >= MAX_GIVING {
            return Err(NotGiven::Busy);
        }
        let chunk = chunk.max(1);
        let held = match self.held(name, now_ms) {
            Some(held) => held,
            None if self.is_index(name) => {
                let index = self.index();
                self.keep(name, index, now_ms)
            }
            None if self.bound(name).is_none() => {
                return Err(NotGiven::Rejected(format!("{name}: no such file")));
            }
            // Already being made: this one waits for it too.
            None if self.making.iter().any(|(making, _)| making == name) => {
                self.wait(client, transfer, name, from, chunk);
                return Ok(());
            }
            None => {
                self.next += 1;
                let ready = Ready {
                    own: self.me.clone(),
                    comm: self.comm.clone(),
                    name: name.to_owned(),
                    id: self.next,
                };
                match self.files.read(name, sessions, ready) {
                    Reading::Now(bytes) => self.keep(name, bytes, now_ms),
                    Reading::Later => {
                        self.making.push((name.to_owned(), self.next));
                        self.wait(client, transfer, name, from, chunk);
                        return Ok(());
                    }
                    Reading::Not(why) => return Err(NotGiven::Rejected(why)),
                }
            }
        };
        self.begin(client, transfer, held, from, chunk, sessions);
        Ok(())
    }

    fn wait(&mut self, client: SessionId, transfer: u64, name: &str, from: u64, chunk: usize) {
        self.waiting.push(Waiting {
            client,
            transfer,
            name: name.to_owned(),
            from,
            chunk,
        });
    }

    fn begin(
        &mut self,
        client: SessionId,
        transfer: u64,
        (bytes, digest): (Rc<[u8]>, [u8; 32]),
        from: u64,
        chunk: usize,
        sessions: &mut Sessions,
    ) {
        let size = bytes.len() as u64;
        let _ = sessions.send(
            client,
            &FileGive {
                transfer,
                size,
                digest,
            },
            Delivery::Must,
        );
        let from = from.min(size);
        let mut giving = Giving {
            client,
            transfer,
            bytes,
            sent: from,
            acked: from,
            chunk,
        };
        feed(&mut giving, sessions);
        self.giving.push(giving);
    }

    /// `name` is made, or is not: each fetch waiting for it begins, or is rejected. A making
    /// no longer awaited is ignored.
    fn made(
        &mut self,
        (name, id): (&str, u64),
        made: Result<Vec<u8>, String>,
        now_ms: u64,
        sessions: &mut Sessions,
    ) {
        let Some(at) = self
            .making
            .iter()
            .position(|(making, which)| making == name && *which == id)
        else {
            return;
        };
        self.making.swap_remove(at);
        let (waiting, others) = std::mem::take(&mut self.waiting)
            .into_iter()
            .partition(|waiting| waiting.name == name);
        self.waiting = others;
        let held = made.map(|bytes| self.keep(name, bytes, now_ms));
        for Waiting {
            client,
            transfer,
            from,
            chunk,
            ..
        } in waiting
        {
            match &held {
                Ok(held) => self.begin(client, transfer, held.clone(), from, chunk, sessions),
                Err(why) => {
                    let _ = sessions.send(
                        client,
                        &FileTaken {
                            transfer,
                            received: 0,
                            state: FileState::Rejected,
                            detail: why,
                        },
                        Delivery::Must,
                    );
                }
            }
        }
    }

    /// What the client said of a fetch of one of these, if it is one: `true` if it was.
    pub(crate) fn taken(
        &mut self,
        client: SessionId,
        taken: &FileTaken<'_>,
        sessions: &mut Sessions,
    ) -> bool {
        let Some(at) = self
            .giving
            .iter()
            .position(|giving| giving.client == client && giving.transfer == taken.transfer)
        else {
            return false;
        };
        if matches!(taken.state, FileState::Committed | FileState::Rejected) {
            self.giving.swap_remove(at);
            return true;
        }
        let giving = &mut self.giving[at];
        giving.acked = giving.acked.max(taken.received);
        feed(giving, sessions);
        true
    }

    /// `session` is gone: what was being given to it is dropped.
    pub(crate) fn closed(&mut self, session: SessionId) {
        self.giving.retain(|giving| giving.client != session);
        self.waiting.retain(|waiting| waiting.client != session);
    }
}

/// Sends what the window has room for.
fn feed(giving: &mut Giving, sessions: &mut Sessions) {
    let size = giving.bytes.len() as u64;
    let window = u64::from(FETCH_WINDOW) * giving.chunk as u64;
    while giving.sent < size && giving.sent < giving.acked + window {
        let start = usize::try_from(giving.sent).unwrap_or(usize::MAX);
        let end = start.saturating_add(giving.chunk).min(giving.bytes.len());
        if sessions
            .send(
                giving.client,
                &FileGiveChunk {
                    transfer: giving.transfer,
                    offset: giving.sent,
                    data: Bytes(&giving.bytes[start..end]),
                },
                Delivery::Must,
            )
            .is_err()
        {
            return;
        }
        giving.sent = end as u64;
    }
}
