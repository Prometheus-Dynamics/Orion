//! Files from a board: a client fetches what a program there offers.
//!
//! The client says which file and how much of it it has already; the program gives the rest,
//! with the file's size and digest, and the client checks the digest over all of it. A fetch
//! that broke goes on from what is here. [`Files::start_fetch`] begins one to be stepped, so a
//! program does its other work on the same session meanwhile; [`Files::fetch`] is the loop.

use std::collections::VecDeque;
use std::io::{Read, Seek, SeekFrom, Write};
use std::time::{Duration, Instant};

use orion_connect::{ClientError, Cx, Handler, Incoming};
use orion_msg_files::{
    FileFetch, FileGive, FileGiveChunk, FileOffered, FileOffersList, FileState, FileTaken,
    OfferedBy,
};
use orion_wire::kind;
use sha2::{Digest, Sha256};

use crate::{Files, Step, Transfer};

/// The numbers a client gives its fetches have this bit set; the comm core numbers what it
/// hands a program from one up. So the answers to both, on one session, are told apart.
const CLIENT_NUMBER: u64 = 1 << 63;

/// A few entries by number, kept in a list: there are never many, and a list costs a board
/// far less code than a tree.
#[derive(Debug)]
pub(crate) struct Table<V>(Vec<(u64, V)>);

impl<V> Default for Table<V> {
    fn default() -> Self {
        Self(Vec::new())
    }
}

impl<V> Table<V> {
    pub(crate) fn get(&self, key: &u64) -> Option<&V> {
        self.0
            .iter()
            .find(|(at, _)| at == key)
            .map(|(_, value)| value)
    }

    pub(crate) fn get_mut(&mut self, key: &u64) -> Option<&mut V> {
        self.0
            .iter_mut()
            .find(|(at, _)| at == key)
            .map(|(_, value)| value)
    }

    pub(crate) fn contains_key(&self, key: &u64) -> bool {
        self.get(key).is_some()
    }

    pub(crate) fn insert(&mut self, key: u64, value: V) {
        match self.get_mut(&key) {
            Some(known) => *known = value,
            None => self.0.push((key, value)),
        }
    }

    pub(crate) fn remove(&mut self, key: &u64) -> Option<V> {
        let at = self.0.iter().position(|(known, _)| known == key)?;
        Some(self.0.swap_remove(at).1)
    }
}

/// A fetch as heard so far.
#[derive(Debug, Default)]
pub(crate) struct Heard {
    /// The file's size and digest, once given.
    pub(crate) given: Option<(u64, [u8; 32])>,
    /// Chunks heard and not yet written.
    pub(crate) chunks: VecDeque<(u64, Vec<u8>)>,
    /// Why it is not given, if it is not.
    pub(crate) rejected: Option<String>,
}

/// A fetch handed to this program, as the comm core said it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Wanted {
    pub(crate) transfer: u64,
    pub(crate) name: String,
    pub(crate) from: u64,
    pub(crate) caller: String,
    pub(crate) max_chunk: u32,
}

/// Where a file this program gives is, as its client last said.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct Taken {
    pub(crate) received: u64,
    pub(crate) state: Option<FileState>,
    pub(crate) detail: String,
}

/// One prefix offered on the board.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Offered {
    /// The prefix.
    pub prefix: String,
    /// What serves it.
    pub by: OfferedBy,
    /// The name the program gave in its hello; empty for the comm core.
    pub name: String,
    /// The program's principal, as the audit log names it; empty for the comm core.
    pub principal: String,
    /// Since when, in milliseconds on the comm core's clock.
    pub since_ms: u64,
}

/// What this session hears of fetches, both ways.
#[derive(Debug, Default)]
pub(crate) struct Fetches {
    /// Fetches this program makes, by number.
    pub(crate) fetching: Table<Heard>,
    /// Offers answered, by the `seq` of the request.
    pub(crate) offered: Vec<u16>,
    /// Lists of offers, by the `seq` of the request.
    pub(crate) lists: Vec<(u16, u64, Vec<Offered>)>,
    /// Fetches handed to this program and not yet taken up, under each prefix it offers.
    pub(crate) wanted: VecDeque<Wanted>,
    /// Files this program gives, by the comm core's number.
    pub(crate) taken: Table<Taken>,
}

impl Handler for Fetches {
    fn kinds(&self) -> &'static [u8] {
        &[
            kind::FILE_OFFERED,
            kind::FILE_OFFERS_LIST,
            kind::FILE_FETCH,
            kind::FILE_GIVE,
            kind::FILE_GIVE_CHUNK,
            kind::FILE_TAKEN,
        ]
    }

    fn handle(&mut self, frame: &Incoming<'_>, _cx: &mut Cx<'_>) -> Result<(), ClientError> {
        match frame.kind() {
            kind::FILE_OFFERED => {
                let offered: FileOffered = frame.body()?;
                self.offered.push(offered.request_seq);
            }
            kind::FILE_OFFERS_LIST => {
                let list: FileOffersList<'_> = frame.body()?;
                let offers = list
                    .offers
                    .iter()
                    .filter_map(Result::ok)
                    .map(|entry| Offered {
                        prefix: entry.prefix.to_owned(),
                        by: entry.by,
                        name: entry.name.to_owned(),
                        principal: entry.principal.to_owned(),
                        since_ms: entry.since_ms,
                    })
                    .collect();
                self.lists.push((list.request_seq, list.clashes, offers));
            }
            kind::FILE_FETCH => {
                let fetch: FileFetch<'_> = frame.body()?;
                self.taken.insert(fetch.transfer, Taken::default());
                self.wanted.push_back(Wanted {
                    transfer: fetch.transfer,
                    name: fetch.name.as_str().to_owned(),
                    from: fetch.from,
                    caller: fetch.caller.to_owned(),
                    max_chunk: fetch.max_chunk,
                });
            }
            kind::FILE_GIVE => {
                let give: FileGive = frame.body()?;
                if let Some(heard) = self.fetching.get_mut(&give.transfer) {
                    heard.given = Some((give.size, give.digest));
                }
            }
            kind::FILE_GIVE_CHUNK => {
                let chunk: FileGiveChunk<'_> = frame.body()?;
                if let Some(heard) = self.fetching.get_mut(&chunk.transfer) {
                    heard
                        .chunks
                        .push_back((chunk.offset, chunk.data.0.to_vec()));
                }
            }
            _ => {
                let taken: FileTaken<'_> = frame.body()?;
                if let Some(heard) = self.fetching.get_mut(&taken.transfer) {
                    // The program, or the comm core, says the file is not given.
                    heard.rejected = Some(taken.detail.to_owned());
                } else if let Some(known) = self.taken.get_mut(&taken.transfer) {
                    known.received = taken.received;
                    known.state = Some(taken.state);
                    known.detail.clear();
                    known.detail.push_str(taken.detail);
                }
            }
        }
        Ok(())
    }
}

/// A fetch under way. [`Fetching::step`] moves it on; dropping it stops it, and what was
/// written stays, for a fetch of the same file to go on from.
pub struct Fetching<W> {
    files: Files,
    into: W,
    transfer: u64,
    seq: u16,
    /// Bytes written, in order from the start.
    written: u64,
    hasher: Sha256,
    last_heard: Instant,
    /// How much the program was last told is here.
    told: u64,
    over: bool,
}

impl<W> std::fmt::Debug for Fetching<W> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Fetching({:016x}, {} bytes)",
            self.transfer, self.written
        )
    }
}

impl Files {
    /// Begins fetching the file `name` into `into`, which holds its first `have` bytes
    /// already (zero for none), and returns the fetch to be [stepped](Fetching::step). The
    /// bytes here are read through once, to check the digest over all of it at the end.
    ///
    /// # Errors
    ///
    /// If the name is not one, the node is too old to offer files, `into` can't be read, or
    /// the connection broke.
    pub fn start_fetch<W: Read + Write + Seek>(
        &self,
        name: &str,
        mut into: W,
        have: u64,
    ) -> Result<Fetching<W>, ClientError> {
        self.hear_fetches()?;
        let name = name
            .parse()
            .map_err(|_| ClientError::Protocol("a file that is not a name"))?;
        // What is here already counts towards the digest.
        into.seek(SeekFrom::Start(0))?;
        let mut hasher = Sha256::new();
        let mut buffer = vec![0_u8; crate::CHUNK];
        let mut left = have;
        let mut looked = Instant::now();
        while left > 0 {
            let want = buffer
                .len()
                .min(usize::try_from(left).unwrap_or(usize::MAX));
            let read = into.read(&mut buffer[..want])?;
            if read == 0 {
                return Err(ClientError::Io(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    format!("{have} bytes said to be here, {} found", have - left),
                )));
            }
            hasher.update(&buffer[..read]);
            left -= read as u64;
            if looked.elapsed() >= Duration::from_millis(250) {
                looked = Instant::now();
                self.client.drain()?;
            }
        }
        let transfer = {
            let mut fetches = self.fetches.borrow_mut();
            let mut transfer = CLIENT_NUMBER | (self.next_fetch.get() & !CLIENT_NUMBER);
            while fetches.fetching.contains_key(&transfer) {
                transfer = CLIENT_NUMBER | (transfer.wrapping_add(1) & !CLIENT_NUMBER);
            }
            self.next_fetch.set(transfer.wrapping_add(1));
            fetches.fetching.insert(transfer, Heard::default());
            transfer
        };
        let seq = self.client.send(&FileFetch {
            transfer,
            name,
            from: have,
            caller: "",
            max_chunk: 0,
        });
        let seq = match seq {
            Ok(seq) => seq,
            Err(error) => {
                self.fetches.borrow_mut().fetching.remove(&transfer);
                return Err(error);
            }
        };
        Ok(Fetching {
            files: self.clone(),
            into,
            transfer,
            seq,
            written: have,
            hasher,
            last_heard: Instant::now(),
            told: have,
            over: false,
        })
    }

    /// Fetches the file `name` into `into`, which holds its first `have` bytes already, and
    /// returns how it ended: committed once the digest held, or rejected with why. `progress`
    /// is told how many bytes are here, of how many, as it goes.
    ///
    /// # Errors
    ///
    /// As [`Files::start_fetch`] and [`Fetching::step`].
    pub fn fetch(
        &self,
        name: &str,
        into: &mut (impl Read + Write + Seek),
        have: u64,
        mut progress: impl FnMut(u64, u64),
    ) -> Result<Transfer, ClientError> {
        let mut fetching = self.start_fetch(name, into, have)?;
        let mut told = None;
        loop {
            match fetching.step(Duration::from_millis(200))? {
                Step::Done(transfer) => return Ok(transfer),
                Step::Waiting => {}
                Step::Progress { received, size } => {
                    if told != Some(received) {
                        told = Some(received);
                        progress(received, size);
                    }
                }
            }
        }
    }

    /// Every prefix offered on the board, and how many offers were refused because another
    /// program held the prefix.
    ///
    /// # Errors
    ///
    /// If the node is too old to offer files, refuses, or the connection broke.
    pub fn offers(&self) -> Result<(Vec<Offered>, u64), ClientError> {
        self.hear_fetches()?;
        let seq = self
            .client
            .send(&orion_msg_files::FileOffers { reserved: 0 })?;
        self.client.answered(seq, || {
            self.fetches
                .borrow()
                .lists
                .iter()
                .any(|(answered, ..)| *answered == seq)
        })?;
        let mut fetches = self.fetches.borrow_mut();
        let at = fetches
            .lists
            .iter()
            .position(|(answered, ..)| *answered == seq)
            .unwrap_or_else(|| unreachable!("answered"));
        let (_, clashes, offers) = fetches.lists.remove(at);
        Ok((offers, clashes))
    }
}

impl<W: Read + Write + Seek> Fetching<W> {
    /// The fetch's number.
    #[must_use]
    pub const fn transfer(&self) -> u64 {
        self.transfer
    }

    /// Moves the fetch on, waiting at most about `wait`: writes what has come, and says how
    /// much is here.
    ///
    /// # Errors
    ///
    /// If `into` can't be written, the comm core refuses the fetch, the program goes quiet for
    /// the session's timeout, or the connection broke. The fetch is then over.
    pub fn step(&mut self, wait: Duration) -> Result<Step, ClientError> {
        if self.over {
            return Err(ClientError::Protocol("a fetch that is over"));
        }
        let result = self.step_once(wait);
        if matches!(result, Err(_) | Ok(Step::Done(_))) {
            self.over = true;
            self.files
                .fetches
                .borrow_mut()
                .fetching
                .remove(&self.transfer);
        }
        result
    }

    fn done(&self, state: FileState, detail: String, digest: [u8; 32]) -> Step {
        Step::Done(Transfer {
            transfer: self.transfer,
            received: self.written,
            state,
            detail,
            digest,
        })
    }

    /// Tells the program how much is here, and where the fetch is.
    fn tell(&self, state: FileState, detail: &str) -> Result<(), ClientError> {
        self.files.client.send(&FileTaken {
            transfer: self.transfer,
            received: self.written,
            state,
            detail,
        })?;
        Ok(())
    }

    fn step_once(&mut self, wait: Duration) -> Result<Step, ClientError> {
        let (given, chunks, rejected) = {
            let mut fetches = self.files.fetches.borrow_mut();
            let heard = fetches
                .fetching
                .get_mut(&self.transfer)
                .unwrap_or_else(|| unreachable!("a fetch is heard until it is over"));
            (
                heard.given,
                std::mem::take(&mut heard.chunks),
                heard.rejected.take(),
            )
        };
        if let Some(why) = rejected {
            return Ok(self.done(FileState::Rejected, why, [0; 32]));
        }
        if let Some(error) = self.files.client.refusal(self.seq) {
            return Err(error);
        }
        let Some((size, digest)) = given else {
            self.files.client.reply_left(self.last_heard)?;
            self.files.client.pump(wait)?;
            return Ok(Step::Waiting);
        };
        if size < self.written {
            let why = format!(
                "the file is {size} bytes, shorter than the {} here",
                self.written
            );
            self.tell(FileState::Rejected, &why)?;
            return Ok(self.done(FileState::Rejected, why, digest));
        }
        if !chunks.is_empty() {
            self.last_heard = Instant::now();
            self.into.seek(SeekFrom::Start(self.written))?;
            for (offset, data) in chunks {
                // A chunk given again, after the program heard nothing, is here already.
                if offset != self.written {
                    continue;
                }
                let room = usize::try_from(size - self.written).unwrap_or(usize::MAX);
                let data = &data[..data.len().min(room)];
                self.into.write_all(data)?;
                self.hasher.update(data);
                self.written += data.len() as u64;
            }
        }
        if self.written == size {
            self.into.flush()?;
            let got: [u8; 32] = self.hasher.clone().finalize().into();
            if got != digest {
                let why = "the digest does not match: what is here is not the file".to_owned();
                self.tell(FileState::Rejected, &why)?;
                return Ok(self.done(FileState::Rejected, why, digest));
            }
            self.tell(FileState::Committed, "")?;
            return Ok(self.done(FileState::Committed, String::new(), digest));
        }
        // How much is here paces the program: it keeps a few chunks on their way, no more.
        if self.told != self.written {
            self.told = self.written;
            self.tell(FileState::Receiving, "")?;
        }
        self.files.client.reply_left(self.last_heard)?;
        self.files.client.pump(wait)?;
        Ok(Step::Progress {
            received: self.written,
            size,
        })
    }
}
