//! The comm core's own files, under a prefix of its own (`orion/`): what explains a board's
//! trouble from Orion's side, handed over as any offered file is, in running and in recovery.
//! The comm core gives them itself, paced by the client as a program's are.
//!
//! **A name's bytes hold for a fetch.** Each is made when it is first fetched and kept for
//! [`SNAPSHOT_MS`]; a fetch that broke off and goes on within that time is given the same
//! bytes, so its digest holds. After it, the name is made again, and a fetch going on from
//! before is told the digest does not match, and fetches from the start.

use std::rc::Rc;

use orion_msg_files::{FETCH_WINDOW, FileGive, FileGiveChunk, FileState, FileTaken};
use orion_server::{Delivery, SessionId, Sessions};
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

/// What the comm core offers of its own.
pub trait OwnFiles {
    /// The prefix, ending in `/`. No program may offer it, or a prefix under it.
    fn prefix(&self) -> &str;

    /// Each file but the index, which is made from these.
    fn names(&self) -> Vec<OwnName>;

    /// The bytes of `name` now, if it is one of [`OwnFiles::names`] and can be read. `sessions`
    /// is every session there is.
    fn read(&mut self, name: &str, sessions: &Sessions) -> Option<Vec<u8>>;
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

/// The comm core's own files, being given.
pub(crate) struct Own {
    files: Box<dyn OwnFiles>,
    snapshots: Vec<Snapshot>,
    giving: Vec<Giving>,
}

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
    pub(crate) fn new(files: Box<dyn OwnFiles>) -> Self {
        Self {
            files,
            snapshots: Vec::new(),
            giving: Vec::new(),
        }
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

    /// The bytes of `name` for a fetch now: those already made, if they still hold.
    fn snapshot(
        &mut self,
        name: &str,
        now_ms: u64,
        sessions: &Sessions,
    ) -> Result<(Rc<[u8]>, [u8; 32]), String> {
        self.snapshots
            .retain(|snapshot| now_ms.saturating_sub(snapshot.made_ms) < SNAPSHOT_MS);
        if let Some(made) = self.snapshots.iter().find(|snapshot| snapshot.name == name) {
            return Ok((made.bytes.clone(), made.digest));
        }
        let bytes = if name.strip_prefix(self.files.prefix()) == Some("index") {
            self.index()
        } else {
            let Some(bound) = self
                .files
                .names()
                .into_iter()
                .find(|own| own.name == name)
                .map(|own| own.max_bytes)
            else {
                return Err(format!("{name}: no such file"));
            };
            let mut bytes = self
                .files
                .read(name, sessions)
                .ok_or_else(|| format!("{name}: cannot be read now"))?;
            bytes.truncate(usize::try_from(bound).unwrap_or(usize::MAX));
            bytes
        };
        let digest: [u8; 32] = Sha256::digest(&bytes).into();
        let bytes: Rc<[u8]> = bytes.into();
        self.snapshots.push(Snapshot {
            name: name.to_owned(),
            made_ms: now_ms,
            bytes: bytes.clone(),
            digest,
        });
        Ok((bytes, digest))
    }

    /// Begins giving `name` to `client` from byte `from`, in chunks of `chunk` bytes.
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
        if self.giving.len() >= MAX_GIVING {
            return Err(NotGiven::Busy);
        }
        let (bytes, digest) = self
            .snapshot(name, now_ms, sessions)
            .map_err(NotGiven::Rejected)?;
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
            chunk: chunk.max(1),
        };
        feed(&mut giving, sessions);
        self.giving.push(giving);
        Ok(())
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
