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
//!
//! A receiver with little room, a microcontroller on a serial line, says how large a chunk
//! it takes (`max_chunk`). It is then sent one frame at a time: a frame waits here until the
//! one before is answered, and a chunk larger than it takes is cut into pieces that each wait
//! their turn. What waits is what the sender has in flight, a few chunks, and no more than
//! [`MAX_WAITING`] bytes of a transfer. A sender that begins again, because it heard nothing,
//! is passed on at once: whatever waited is dropped, and the receiver says where to go on.
//!
//! A receiver that takes any chunk is sent each as it comes, and what is on its way to it,
//! sent and not yet answered, is kept within its session's outbox in the comm core: a frame
//! that does not fit there would end the receiver's session. A chunk that would take it past
//! that, across every transfer to the receiver, is refused as busy, and the sender goes back
//! to where the receiver last said it was. What is sent stays in order: a chunk after one that
//! was turned away is turned away too, and a chunk already on its way, sent again by a sender
//! that went back, is not sent twice.
//!
//! The image can say which prefixes are receivers' (`files.receivers`). A file for one of them
//! is never the node's own store's: begun while no receiver has asked for it (the receiver is
//! still starting, or went away), it is refused as unavailable, in words, and the sender asks
//! again. Without that, a file that arrives before its receiver would be stored here and
//! answered as committed, while the receiver never saw it.

#![cfg_attr(not(test), deny(clippy::unwrap_used))]

use std::collections::{BTreeMap, VecDeque};

use orion_msg_files::{
    FileBegin, FileChunk, FileCommit, FileSink, FileSinkReady, FileState, FileStatus,
};
use orion_reply::{Asked, Out};
use orion_types::Error;
use orion_wire::body::encode_fields;
use orion_wire::{Bytes, Decode, body, kind};

/// The most prefixes that can be asked for. One more is refused as busy.
pub const MAX_SINKS: usize = 64;

/// The most transfers passing through at once. One more is refused as busy.
pub const MAX_TRANSFERS: usize = 256;

/// The most bytes of one transfer that wait for a receiver that takes a frame at a time.
/// More is refused as busy, and the sender tries again.
pub const MAX_WAITING: usize = 1 << 20;

/// Room for what a chunk carries besides its bytes.
const CHUNK_FIELDS: usize = 32;

/// What a receiver's outbox keeps free of chunks: one more chunk's frame as a push sends it
/// (32 KiB and its envelope), for the answers and everything else on their way.
const SPARE: usize = 32 * 1024 + 64;

#[derive(Debug)]
struct Sink {
    prefix: String,
    session: u32,
    /// The most bytes of a chunk it takes; 0 for any, and then frames are not held back.
    max_chunk: u32,
}

#[derive(Debug)]
struct Passing {
    /// The program the file goes to.
    sink: u32,
    /// The session that last sent something of it, which the answers go to.
    sender: u32,
    /// The receiver's limit, as it was when the transfer began.
    max_chunk: u32,
    /// Under a prefix the image says is a receiver's.
    reserved: bool,
    /// Whether a frame was sent to a receiver that takes one at a time and is not answered.
    answer_owed: bool,
    /// The frames that wait for it, as kind and body.
    waiting: VecDeque<(u8, Vec<u8>)>,
    waiting_bytes: usize,
    /// For a receiver that takes any chunk: from where the receiver last said it was (or the
    /// first chunk sent since the transfer began) to the end of the last chunk sent.
    on_way: Option<(u64, u64)>,
}

impl Passing {
    /// Bytes sent to the receiver and not yet answered.
    fn unanswered(&self) -> u64 {
        self.on_way.map_or(0, |(from, to)| to.saturating_sub(from))
    }
}

impl Passing {
    /// Sends the next frame that waits, if the receiver is not still on one.
    fn feed(&mut self, out: &mut dyn Out) {
        if self.answer_owed {
            return;
        }
        if let Some((kind, payload)) = self.waiting.pop_front() {
            self.waiting_bytes -= payload.len();
            out.frame(self.sink, kind, &payload, false);
            self.answer_owed = true;
        }
    }

    /// Holds a frame for a receiver that takes one at a time, cutting a chunk that is too
    /// large for it.
    fn hold(&mut self, kind: u8, payload: &[u8]) -> Result<(), Error> {
        let limit = self.max_chunk as usize;
        if kind != kind::FILE_CHUNK {
            self.waiting.push_back((kind, payload.to_vec()));
            self.waiting_bytes += payload.len();
            return Ok(());
        }
        let chunk = FileChunk::decode(payload)?;
        if self.waiting_bytes + chunk.data.0.len() > MAX_WAITING {
            return Err(Error::Busy);
        }
        let mut offset = chunk.offset;
        for piece in chunk.data.0.chunks(limit.max(1)) {
            let mut body = vec![0_u8; piece.len() + CHUNK_FIELDS];
            let used = encode_fields(
                &FileChunk {
                    transfer: chunk.transfer,
                    offset,
                    data: Bytes(piece),
                },
                &mut body,
            )?;
            body.truncate(used);
            self.waiting_bytes += body.len();
            self.waiting.push_back((kind, body));
            offset += piece.len() as u64;
        }
        Ok(())
    }
}

/// The receivers of one node, and the transfers passing through to them.
#[derive(Debug)]
pub struct Sinks {
    sinks: Vec<Sink>,
    passing: BTreeMap<u64, Passing>,
    /// The most bytes of chunks on their way to one receiver.
    room: u64,
    /// The prefixes that are receivers', whether one has asked or not (`files.receivers`).
    receivers: Vec<String>,
    /// Transfers under those prefixes whose receiver went away, newest last.
    lost: VecDeque<u64>,
    /// Why the last request was refused, in words.
    said: Option<String>,
}

impl Default for Sinks {
    fn default() -> Self {
        Self::new(orion_wire::OUTBOX_BYTES)
    }
}

impl Sinks {
    /// The kinds answered here, besides the three of a transfer, which are looked at with
    /// [`Sinks::pass`] before the node's own store takes them.
    pub const KINDS: &'static [u8] = &[kind::FILE_SINK, kind::FILE_STATUS];

    /// No receivers, on a comm core whose sessions each have `outbox_bytes` of outbox
    /// (`comm.outbox_bytes`).
    #[must_use]
    pub fn new(outbox_bytes: u32) -> Self {
        Self {
            sinks: Vec::new(),
            passing: BTreeMap::new(),
            room: (outbox_bytes as usize).saturating_sub(SPARE) as u64,
            receivers: Vec::new(),
            lost: VecDeque::new(),
            said: None,
        }
    }

    /// The same, with `prefixes` as receivers' whether one has asked for them or not: a file
    /// under one is never the node's own store's.
    #[must_use]
    pub fn with_receivers(mut self, prefixes: Vec<String>) -> Self {
        self.receivers = prefixes;
        self
    }

    /// Why the last request was refused, in words, if it said.
    pub const fn take_said(&mut self) -> Option<String> {
        self.said.take()
    }

    /// The receivers' prefix `destination` is under, if the image named one.
    fn receivers_prefix(&self, destination: &str) -> Option<&str> {
        self.receivers
            .iter()
            .map(String::as_str)
            .filter(|prefix| destination.starts_with(prefix))
            .max_by_key(|prefix| prefix.len())
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
        self.lost.clear();
    }

    /// The receiver for `destination`: the one with the longest prefix it starts with.
    fn sink_for(&self, destination: &str) -> Option<&Sink> {
        self.sinks
            .iter()
            .filter(|sink| destination.starts_with(&sink.prefix))
            .max_by_key(|sink| sink.prefix.len())
    }

    /// The largest chunk the receiver of `destination` takes, 0 for any. `None` if no
    /// receiver asked for it: it would be the node's own store's.
    #[must_use]
    pub fn max_chunk(&self, destination: &str) -> Option<u32> {
        self.sink_for(destination).map(|sink| sink.max_chunk)
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
                match self
                    .sinks
                    .iter_mut()
                    .find(|known| known.prefix == sink.prefix)
                {
                    Some(known) if known.session == asked.session => {
                        known.max_chunk = sink.max_chunk;
                    }
                    Some(_) => return Err(Error::AlreadyExists),
                    None => {
                        if self.sinks.len() >= MAX_SINKS {
                            return Err(Error::Busy);
                        }
                        self.sinks.push(Sink {
                            prefix: sink.prefix.to_owned(),
                            session: asked.session,
                            max_chunk: sink.max_chunk,
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
                    .get_mut(&status.transfer)
                    .filter(|passing| passing.sink == asked.session)
                    .ok_or(Error::NotFound)?;
                let sender = passing.sender;
                if matches!(status.state, FileState::Committed | FileState::Rejected) {
                    self.passing.remove(&status.transfer);
                } else {
                    if let Some((from, _)) = &mut passing.on_way {
                        *from = (*from).max(status.received);
                    }
                    passing.answer_owed = false;
                    passing.feed(out);
                }
                out.frame(sender, kind::FILE_STATUS, asked.payload, false);
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
                let Some(&Sink {
                    session: sink,
                    max_chunk,
                    ..
                }) = self.sink_for(begin.destination.as_str())
                else {
                    self.passing.remove(&begin.transfer);
                    // A receiver's, by the image's word, with none here yet or any more:
                    // not the store's. The sender asks again.
                    if let Some(prefix) = self.receivers_prefix(begin.destination.as_str()) {
                        self.said = Some(format!(
                            "nothing receives {} yet: {prefix} is a receiver's",
                            begin.destination.as_str()
                        ));
                        return Err(Error::Unavailable);
                    }
                    // Not a receiver's. If it was passing through one that has since gone,
                    // it starts over in the store.
                    return Ok(false);
                };
                let reserved = self.receivers_prefix(begin.destination.as_str()).is_some();
                self.lost.retain(|lost| *lost != begin.transfer);
                if !self.passing.contains_key(&begin.transfer)
                    && self.passing.len() >= MAX_TRANSFERS
                {
                    return Err(Error::Busy);
                }
                // Begun again: what waited is dropped, and the receiver says where to go on.
                self.passing.insert(
                    begin.transfer,
                    Passing {
                        sink,
                        sender: asked.session,
                        max_chunk,
                        reserved,
                        answer_owed: false,
                        waiting: VecDeque::new(),
                        waiting_bytes: 0,
                        on_way: None,
                    },
                );
                begin.transfer
            }
            kind::FILE_CHUNK => FileChunk::decode_transfer(asked.payload)?,
            kind::FILE_COMMIT => asked.body::<FileCommit>()?.transfer,
            _ => return Ok(false),
        };
        let Some(passing) = self.passing.get(&transfer) else {
            if self.lost.contains(&transfer) {
                self.said = Some("the program that received it went away: begin it again".into());
                return Err(Error::Unavailable);
            }
            return Ok(false);
        };
        let sink = passing.sink;
        let chunk = if passing.max_chunk == 0 && asked.kind == kind::FILE_CHUNK {
            let chunk = FileChunk::decode(asked.payload)?;
            Some((chunk.offset, chunk.offset + chunk.data.0.len() as u64))
        } else {
            None
        };
        if let Some((offset, end)) = chunk {
            if let Some((from, to)) = passing.on_way {
                if offset >= from && end <= to {
                    // On its way already: the sender went back after a refusal.
                    return Ok(true);
                }
                if offset > to {
                    // One before it was turned away: what is on its way stays in order, and
                    // the sender goes back.
                    return Err(Error::Busy);
                }
            }
            let unanswered: u64 = self
                .passing
                .values()
                .filter(|other| other.sink == sink)
                .map(Passing::unanswered)
                .sum();
            if unanswered + (end - offset) > self.room {
                return Err(Error::Busy);
            }
        }
        let passing = self.passing.get_mut(&transfer).expect("looked up above");
        // Whoever sent this is who is told next: a sender that came back in a new session.
        passing.sender = asked.session;
        if passing.max_chunk == 0 {
            if let Some((offset, end)) = chunk {
                let (from, to) = passing.on_way.get_or_insert((offset, offset));
                *from = (*from).min(offset);
                *to = (*to).max(end);
            }
            out.frame(passing.sink, asked.kind, asked.payload, false);
        } else {
            passing.hold(asked.kind, asked.payload)?;
            passing.feed(out);
        }
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
            if self
                .passing
                .remove(&transfer)
                .is_some_and(|passing| passing.reserved)
            {
                // What is still on its way for it is not the store's either.
                if self.lost.len() >= MAX_TRANSFERS {
                    self.lost.pop_front();
                }
                self.lost.push_back(transfer);
            }
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
