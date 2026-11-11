//! Files that go straight to where they belong.
//!
//! Some files are not the node's to keep: an image for a boot slot is written to the slot by
//! the program that manages the board, and a board with little storage cannot hold it once in
//! the node's store and once more in the slot. Such a program asks for every file under a
//! prefix with `FileSink`. From then on a file sent to a destination under that prefix is not
//! stored here. Its `FileBegin`, each `FileChunk` and its `FileCommit` are passed to the
//! program as they come, and the program's `FileStatus` answers are passed back to the
//! sender. The program is the receiver: it says how many bytes it has, it checks the digest,
//! it says committed or rejected.
//!
//! Nothing of the file stays here. A chunk is handed on in the call that brought it, so the
//! memory this takes is one chunk at a time, whatever the size of the file. The sender is
//! paced by the receiver's answers, as it would be by the node's.

use std::collections::BTreeMap;

use orion_msg_files::{
    FileBegin, FileChunk, FileCommit, FileSink, FileSinkReady, FileState, FileStatus,
};
use orion_reply::{Asked, Out};
use orion_types::Error;
use orion_wire::{body, kind};

/// The most prefixes that can be asked for. One more is refused as busy.
pub const MAX_SINKS: usize = 64;

/// The most transfers passing through at once. One more is refused as busy.
pub const MAX_TRANSFERS: usize = 256;

#[derive(Debug)]
struct Sink {
    prefix: String,
    session: u32,
}

#[derive(Clone, Copy, Debug)]
struct Passing {
    /// The program the file goes to.
    sink: u32,
    /// The session that last sent something of it, which the answers go to.
    sender: u32,
}

/// The receivers of one node, and the transfers passing through to them.
#[derive(Debug, Default)]
pub struct Sinks {
    sinks: Vec<Sink>,
    passing: BTreeMap<u64, Passing>,
}

impl Sinks {
    /// The kinds answered here, besides the three of a transfer, which are looked at with
    /// [`Sinks::pass`] before the node's own store takes them.
    pub const KINDS: &'static [u8] = &[kind::FILE_SINK, kind::FILE_STATUS];

    /// No receivers.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// How many prefixes are asked for.
    #[must_use]
    pub fn sinks(&self) -> usize {
        self.sinks.len()
    }

    /// How many transfers are passing through.
    #[must_use]
    pub fn passing(&self) -> usize {
        self.passing.len()
    }

    /// Forgets every receiver and transfer: the comm core they came through is gone.
    pub fn forget_sessions(&mut self) {
        self.sinks.clear();
        self.passing.clear();
    }

    /// The receiver for `destination`: the one with the longest prefix it starts with.
    fn sink_for(&self, destination: &str) -> Option<u32> {
        self.sinks
            .iter()
            .filter(|sink| destination.starts_with(&sink.prefix))
            .max_by_key(|sink| sink.prefix.len())
            .map(|sink| sink.session)
    }

    /// Answers `FileSink` and `FileStatus`.
    ///
    /// # Errors
    ///
    /// [`Error`] for the caller, as a refusal.
    pub fn request(&mut self, asked: &Asked<'_>, out: &mut dyn Out) -> Result<(), Error> {
        match asked.kind {
            kind::FILE_SINK => {
                let sink: FileSink<'_> = asked.body()?;
                if sink.prefix.is_empty() || sink.prefix.split('/').any(|part| part == "..") {
                    return Err(Error::InvalidName);
                }
                match self.sinks.iter().find(|known| known.prefix == sink.prefix) {
                    Some(known) if known.session == asked.session => {}
                    Some(_) => return Err(Error::AlreadyExists),
                    None => {
                        if self.sinks.len() >= MAX_SINKS {
                            return Err(Error::Busy);
                        }
                        self.sinks.push(Sink {
                            prefix: sink.prefix.to_owned(),
                            session: asked.session,
                        });
                    }
                }
                out.send(
                    asked.session,
                    &FileSinkReady {
                        request_seq: asked.seq,
                    },
                )
            }
            kind::FILE_STATUS => {
                // A receiver's answer about a transfer that passes through it.
                let status: FileStatus<'_> = asked.body()?;
                let passing = self
                    .passing
                    .get(&status.transfer)
                    .copied()
                    .filter(|passing| passing.sink == asked.session)
                    .ok_or(Error::NotFound)?;
                if matches!(status.state, FileState::Committed | FileState::Rejected) {
                    self.passing.remove(&status.transfer);
                }
                out.frame(passing.sender, kind::FILE_STATUS, asked.payload, false);
                Ok(())
            }
            _ => Err(Error::Unsupported),
        }
    }

    /// Passes a `FileBegin`, `FileChunk` or `FileCommit` on to the receiver it is for, as it
    /// came. `Ok(false)` if it is for no receiver: it is the node's own store's.
    ///
    /// # Errors
    ///
    /// [`Error`] for the sender, as a refusal.
    pub fn pass(&mut self, asked: &Asked<'_>, out: &mut dyn Out) -> Result<bool, Error> {
        let transfer = match asked.kind {
            kind::FILE_BEGIN => {
                let begin: FileBegin = asked.body()?;
                let Some(sink) = self.sink_for(begin.destination.as_str()) else {
                    // Not a receiver's. If it was passing through one that has since gone,
                    // it starts over in the store.
                    self.passing.remove(&begin.transfer);
                    return Ok(false);
                };
                if !self.passing.contains_key(&begin.transfer)
                    && self.passing.len() >= MAX_TRANSFERS
                {
                    return Err(Error::Busy);
                }
                self.passing.insert(
                    begin.transfer,
                    Passing {
                        sink,
                        sender: asked.session,
                    },
                );
                begin.transfer
            }
            kind::FILE_CHUNK => FileChunk::decode_transfer(asked.payload)?,
            kind::FILE_COMMIT => asked.body::<FileCommit>()?.transfer,
            _ => return Ok(false),
        };
        let Some(passing) = self.passing.get_mut(&transfer) else {
            return Ok(false);
        };
        // Whoever sent this is who is told next: a sender that came back in a new session.
        passing.sender = asked.session;
        out.frame(passing.sink, asked.kind, asked.payload, false);
        Ok(true)
    }

    /// A session is gone. A receiver that goes takes its prefixes with it, and whoever was
    /// sending to it is told the transfer is over. A sender that goes changes nothing: it
    /// may come back and go on.
    pub fn gone(&mut self, session: u32, out: &mut dyn Out) {
        self.sinks.retain(|sink| sink.session != session);
        let orphaned: Vec<(u64, u32)> = self
            .passing
            .iter()
            .filter(|(_, passing)| passing.sink == session)
            .map(|(transfer, passing)| (*transfer, passing.sender))
            .collect();
        for (transfer, sender) in orphaned {
            self.passing.remove(&transfer);
            if sender != session {
                let _ = out.send(
                    sender,
                    &FileStatus {
                        transfer,
                        received: 0,
                        state: FileState::Rejected,
                        detail: "the program that receives it went away",
                    },
                );
            }
        }
    }
}

/// Reads the transfer of a chunk without looking at its bytes.
trait ChunkTransfer {
    fn decode_transfer(payload: &[u8]) -> Result<u64, Error>;
}

impl ChunkTransfer for FileChunk<'_> {
    fn decode_transfer(payload: &[u8]) -> Result<u64, Error> {
        body::take::<u64>(payload).map(|(transfer, _)| transfer)
    }
}
