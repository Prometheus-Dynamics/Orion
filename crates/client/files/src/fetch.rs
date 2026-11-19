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

/// How a fetch whose program stopped giving it ends: `Aborted`, with these words and the
/// program's reason after them.
const STOPPED: &str = "the program that offers it stopped giving it";

/// How long a fetch hears nothing from the program before it says so ([`Step::Stalled`]), and
/// again each time as long passes: 5 s. It keeps waiting: the program may be busy, and the
/// comm core says if the program goes. Only a limit the caller sets ([`Fetching::give_up_after`])
/// ends it.
pub const NO_PROGRESS: Duration = Duration::from_secs(5);

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
    /// Why it is not given, if it is not: rejected, or aborted by the program giving it.
    pub(crate) rejected: Option<(FileState, String)>,
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
                    // The program, or the comm core, says the file is not given: the program
                    // stopped giving it, or any other answer, which ends it as not given.
                    heard.rejected = Some(if taken.state == FileState::Aborted {
                        (FileState::Aborted, format!("{STOPPED}: {}", taken.detail))
                    } else {
                        (FileState::Rejected, taken.detail.to_owned())
                    });
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
    /// The most bytes the file may be.
    bound: Option<u64>,
    /// How long without a chunk before [`Step::Stalled`] is said.
    no_progress: Duration,
    /// How long without a chunk ends the fetch, if the caller set it.
    give_up: Option<Duration>,
    /// When [`Step::Stalled`] was last said.
    reported: Instant,
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
            bound: None,
            no_progress: NO_PROGRESS,
            give_up: None,
            reported: Instant::now(),
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
        progress: impl FnMut(u64, u64),
    ) -> Result<Transfer, ClientError> {
        self.start_fetch(name, into, have)?.finish(progress)
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

    /// Holds the fetch to `bound` bytes, from the prefix's index ([`crate::index`]): a file
    /// said to be larger is not taken, and nothing past the bound is written.
    #[must_use]
    pub const fn within(mut self, bound: u64) -> Self {
        self.bound = Some(bound);
        self
    }

    /// Says [`Step::Stalled`] after `quiet` without a chunk, not [`NO_PROGRESS`].
    #[must_use]
    pub const fn stalled_after(mut self, quiet: Duration) -> Self {
        self.no_progress = quiet;
        self
    }

    /// Ends the fetch once the program has given nothing for `quiet` at a stretch: not taken,
    /// with "gave up: no progress for <quiet>, the limit set for this fetch", and the program
    /// is told the same. Without it, which is the default, a fetch never gives up on a program
    /// that is there: it says it is stalled, and waits.
    #[must_use]
    pub const fn give_up_after(mut self, quiet: Duration) -> Self {
        self.give_up = Some(quiet);
        self
    }

    /// Steps until the fetch is over, telling `progress` how much is here, of how much.
    /// A stall is waited out, unless [`Fetching::give_up_after`] says otherwise.
    ///
    /// # Errors
    ///
    /// As [`Fetching::step`].
    pub fn finish(mut self, mut progress: impl FnMut(u64, u64)) -> Result<Transfer, ClientError> {
        let mut told = None;
        loop {
            match self.step(Duration::from_millis(200))? {
                Step::Done(transfer) => return Ok(transfer),
                Step::Waiting | Step::Stalled { .. } => {}
                Step::Progress { received, size } => {
                    if told != Some(received) {
                        told = Some(received);
                        progress(received, size);
                    }
                }
            }
        }
    }

    /// Moves the fetch on, waiting at most about `wait`: writes what has come, and says how
    /// much is here.
    ///
    /// # Errors
    ///
    /// If `into` can't be written, the comm core refuses the fetch, or the connection broke.
    /// The fetch is then over. A program that goes quiet does not end it, unless
    /// [`Fetching::give_up_after`] was set: see [`Step::Stalled`].
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
        if let Some((state, why)) = rejected {
            return Ok(self.done(state, why, [0; 32]));
        }
        if let Some(error) = self.files.client.refusal(self.seq) {
            return Err(error);
        }
        let Some((size, digest)) = given else {
            if let Some(step) = self.quiet(None, [0; 32])? {
                return Ok(step);
            }
            self.files.client.pump(wait)?;
            return Ok(Step::Waiting);
        };
        if let Some(bound) = self.bound
            && size > bound
        {
            let why = format!("the file is {size} bytes, more than its bound of {bound}");
            self.tell(FileState::Rejected, &why)?;
            return Ok(self.done(FileState::Rejected, why, digest));
        }
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
        if let Some(step) = self.quiet(Some(size), digest)? {
            return Ok(step);
        }
        self.files.client.pump(wait)?;
        Ok(Step::Progress {
            received: self.written,
            size,
        })
    }

    /// Whether the program has been quiet long enough to say so, or to give up as the caller
    /// set: none of this while chunks come.
    fn quiet(&mut self, size: Option<u64>, digest: [u8; 32]) -> Result<Option<Step>, ClientError> {
        let quiet = self.last_heard.elapsed();
        if let Some(limit) = self.give_up
            && quiet >= limit
        {
            let why = format!("gave up: no progress for {limit:?}, the limit set for this fetch");
            self.tell(FileState::Rejected, &why)?;
            return Ok(Some(self.done(FileState::Rejected, why, digest)));
        }
        if quiet >= self.no_progress && self.reported.elapsed() >= self.no_progress {
            self.reported = Instant::now();
            return Ok(Some(Step::Stalled {
                received: self.written,
                size,
                quiet,
            }));
        }
        Ok(None)
    }
}
