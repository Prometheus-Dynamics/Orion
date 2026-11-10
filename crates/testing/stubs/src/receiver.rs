//! A file receiver: takes chunks in order, checks the digest, keeps what is committed.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use orion_msg_files::{FileBegin, FileChunk, FileCommit, FilePull, FileState, FileStatus};
use orion_server::{Admits, Checked, Cx, Error, Incoming, Policy, Service, What};
use orion_wire::{FrameView, kind};
use sha2::{Digest, Sha256};

/// What is in place, and what a pull can fetch: both seen and set from outside the comm
/// core's thread.
#[derive(Debug, Default)]
pub struct Shelf {
    /// Destination to contents.
    pub committed: Mutex<BTreeMap<String, Vec<u8>>>,
    /// URL to contents.
    pub urls: Mutex<BTreeMap<String, Vec<u8>>>,
}

#[derive(Debug)]
struct Receiving {
    destination: String,
    size: u64,
    digest: [u8; 32],
    data: Vec<u8>,
}

/// The receiver.
#[derive(Debug)]
pub struct Receiver {
    shelf: Arc<Shelf>,
    transfers: BTreeMap<u64, Receiving>,
}

impl Receiver {
    /// A receiver that puts files on `shelf`.
    #[must_use]
    pub const fn new(shelf: Arc<Shelf>) -> Self {
        Self {
            shelf,
            transfers: BTreeMap::new(),
        }
    }

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
        if let Ok(mut committed) = self.shelf.committed.lock() {
            committed.insert(receiving.destination, receiving.data);
        }
        (received, FileState::Committed, "")
    }
}

const fn status(transfer: u64, received: u64, state: FileState, detail: &str) -> FileStatus<'_> {
    FileStatus {
        transfer,
        received,
        state,
        detail,
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
                let received = self
                    .transfers
                    .get(&begin.transfer)
                    .map_or(0, |known| known.data.len() as u64);
                cx.reply(&status(begin.transfer, received, FileState::Receiving, ""))
            }
            kind::FILE_CHUNK => {
                let chunk: FileChunk<'_> = incoming.body()?;
                let receiving = self
                    .transfers
                    .get_mut(&chunk.transfer)
                    .ok_or(Error::NotFound)?;
                if chunk.offset == receiving.data.len() as u64 {
                    receiving.data.extend_from_slice(chunk.data.0);
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
                    .shelf
                    .urls
                    .lock()
                    .ok()
                    .and_then(|urls| urls.get(pull.url).cloned());
                let Some(data) = fetched else {
                    return cx.reply(&status(
                        pull.transfer,
                        0,
                        FileState::Rejected,
                        "nothing at that URL",
                    ));
                };
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
                cx.reply(&status(pull.transfer, received, state, detail))
            }
        }
    }
}
