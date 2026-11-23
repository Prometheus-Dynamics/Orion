//! A push, a step at a time: the caller makes its other calls on the same session between
//! steps, and [`Files::push`] is the loop that only steps.

use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime};

use orion_connect::{ClientError, Reply};
use orion_msg_files::{FileBegin, FileChunk, FileCommit};
use orion_wire::Bytes;
use sha2::{Digest, Sha256};

use crate::{CHUNK, CHUNK_OVERHEAD, FileState, Files, Transfer, WINDOW, digest_while};

/// What is known of a file already: its SHA-256 and size, from an earlier push
/// ([`Transfer::digest`]) or from wherever the program keeps them. A push given it does not
/// read the file through to hash it first.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Known {
    /// Its SHA-256.
    pub digest: [u8; 32],
    /// Its size.
    pub size: u64,
}

/// How a push is going, after a step.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Step {
    /// Begun, and the board has not yet said how much of it it has.
    Waiting,
    /// Under way: the board has `received` of `size` bytes.
    Progress {
        /// Bytes the board has, in order from the start.
        received: u64,
        /// The file's size.
        size: u64,
    },
    /// A fetch the program has given nothing more of for `quiet`, [`crate::NO_PROGRESS`] or
    /// what the fetch was set to: it keeps waiting. Said once each time that much passes.
    Stalled {
        /// Bytes here, in order from the start.
        received: u64,
        /// The file's size, once the program said it.
        size: Option<u64>,
        /// How long since the program last gave anything.
        quiet: Duration,
    },
    /// Over: committed, or rejected with why, or aborted by the program giving it, with why.
    Done(Transfer),
}

enum Phase {
    /// Begun, and waiting to hear how much the board has.
    Begin { seq: u16, nothing: Transfer },
    /// Sending chunks, a few at a time.
    Sending {
        status: Transfer,
        sent: u64,
        busy: Busy,
    },
    /// Every byte is there: waiting for the board's check.
    Commit { seq: u16, since: Instant },
    /// Said how it ended.
    Over,
}

/// A push under way. [`Pushing::step`] moves it on; dropping it stops following it, and the
/// board keeps what it has for a push of the same file to the same place to carry on from.
pub struct Pushing<S> {
    files: Files,
    source: S,
    transfer: u64,
    known: Known,
    destination: orion_types::FileName,
    phase: Phase,
    buffer: Vec<u8>,
    /// When the board was last heard about this transfer.
    last_heard: Instant,
    timeout: Duration,
    /// The file, and its size and change time when the push began, to be the same at the end.
    unchanged: Option<(PathBuf, (u64, SystemTime))>,
}

impl<S> std::fmt::Debug for Pushing<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Pushing({:016x})", self.transfer)
    }
}

/// The transfer's number: the same bytes to the same place are the same transfer, whoever
/// sends them.
fn number(digest: &[u8; 32], destination: &str) -> u64 {
    let mut hasher = Sha256::new();
    hasher.update(digest);
    hasher.update(destination.as_bytes());
    let number: [u8; 32] = hasher.finalize().into();
    u64::from_le_bytes([
        number[0], number[1], number[2], number[3], number[4], number[5], number[6], number[7],
    ])
}

/// A file's size and when it last changed.
fn looked_at(path: &Path) -> Option<(u64, SystemTime)> {
    let meta = std::fs::metadata(path).ok()?;
    Some((meta.len(), meta.modified().ok()?))
}

impl Files {
    /// Begins pushing `source` to `destination`, and returns the push to be [stepped]
    /// (Pushing::step). With `known`, the file is not read through to hash it first; without,
    /// it is, and the session is looked after meanwhile.
    ///
    /// # Errors
    ///
    /// If the source can't be read, the destination is not a name, or the connection broke.
    pub fn start_push<S: Read + Seek>(
        &self,
        destination: &str,
        mut source: S,
        known: Option<Known>,
    ) -> Result<Pushing<S>, ClientError> {
        let known = match known {
            Some(known) => known,
            None => {
                let (digest, size) = digest_while(&mut source, || self.client.drain())?;
                Known { digest, size }
            }
        };
        let name: orion_types::FileName = destination
            .parse()
            .map_err(|_| ClientError::Protocol("a destination that is not a name"))?;
        let transfer = number(&known.digest, destination);
        self.follow(transfer, known.digest);
        let nothing = self.status(transfer);
        let seq = self.client.send(&FileBegin {
            transfer,
            destination: name.clone(),
            size: known.size,
            digest: known.digest,
        });
        let seq = match seq {
            Ok(seq) => seq,
            Err(error) => {
                self.forget(transfer);
                return Err(error);
            }
        };
        let limit = self
            .client
            .session(|session| session.welcomed().max_frame as usize);
        let chunk = CHUNK.min(limit.saturating_sub(CHUNK_OVERHEAD)).max(1);
        Ok(Pushing {
            files: self.clone(),
            source,
            transfer,
            known,
            destination: name,
            phase: Phase::Begin { seq, nothing },
            buffer: vec![0; chunk],
            last_heard: Instant::now(),
            timeout: self.client.session(|session| session.timeout()),
            unchanged: None,
        })
    }

    /// Pushes everything `source` holds to `destination`, and returns how it ended:
    /// committed, or rejected with why. `progress` is told how many bytes the board has, of
    /// how many, as it goes.
    ///
    /// The transfer's number comes from the file's digest and its destination. If the same
    /// file was part way there, from this program or another, this carries on from where it
    /// stopped: call it again after reconnecting and nothing already sent is sent again.
    ///
    /// # Errors
    ///
    /// If the source can't be read, the board refuses the transfer, or the connection broke.
    /// After a broken connection, reconnect and push again.
    pub fn push(
        &self,
        destination: &str,
        source: &mut (impl Read + Seek),
        progress: impl FnMut(u64, u64),
    ) -> Result<Transfer, ClientError> {
        self.start_push(destination, source, None)?.finish(progress)
    }

    /// As [`Files::push`], for a file whose digest and size are known already: it is not read
    /// through to hash it first. A `known` that is wrong is found by the board's check, and
    /// the push ends rejected.
    ///
    /// # Errors
    ///
    /// As [`Files::push`].
    pub fn push_with(
        &self,
        destination: &str,
        source: &mut (impl Read + Seek),
        known: Known,
        progress: impl FnMut(u64, u64),
    ) -> Result<Transfer, ClientError> {
        self.start_push(destination, source, Some(known))?
            .finish(progress)
    }

    /// Pushes the file at `path`, as [`Files::push_with`] if `known` is given and
    /// [`Files::push`] if not. Its size and change time are looked at when the push begins
    /// and again before it is committed: a file that changed meanwhile is not committed, and
    /// the push fails saying so.
    ///
    /// # Errors
    ///
    /// As [`Files::push`], and an `Io` error of kind `Other` for a file that changed.
    pub fn push_file(
        &self,
        destination: &str,
        path: &Path,
        known: Option<Known>,
        progress: impl FnMut(u64, u64),
    ) -> Result<Transfer, ClientError> {
        let before = looked_at(path).ok_or_else(|| {
            ClientError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("{} cannot be looked at", path.display()),
            ))
        })?;
        let file = std::fs::File::open(path)?;
        let mut pushing = self.start_push(destination, file, known)?;
        pushing.unchanged = Some((path.to_owned(), before));
        pushing.finish(progress)
    }
}

impl<S: Read + Seek> Pushing<S> {
    /// The transfer's number.
    #[must_use]
    pub const fn transfer(&self) -> u64 {
        self.transfer
    }

    /// Moves the push on, waiting at most about `wait` for the board: sends what the window
    /// has room for, and takes what the board said.
    ///
    /// # Errors
    ///
    /// If the source can't be read, the board refuses the transfer or goes quiet for the
    /// session's timeout, or the connection broke. The push is then over.
    pub fn step(&mut self, wait: Duration) -> Result<Step, ClientError> {
        let result = self.step_once(wait);
        if matches!(result, Err(_) | Ok(Step::Done(_))) {
            self.phase = Phase::Over;
            self.files.forget(self.transfer);
        }
        result
    }

    /// Steps until the push is over, telling `progress` as the board's count moves.
    ///
    /// # Errors
    ///
    /// As [`Pushing::step`].
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

    fn progress(&self, received: u64) -> Step {
        Step::Progress {
            received,
            size: self.known.size,
        }
    }

    fn step_once(&mut self, wait: Duration) -> Result<Step, ClientError> {
        let size = self.known.size;
        match &mut self.phase {
            Phase::Over => Err(ClientError::Protocol("a push that is over")),
            Phase::Begin { seq, nothing } => {
                let now = self.files.status(self.transfer);
                if now != *nothing {
                    // Where the board says to go on from: zero for a new transfer.
                    let sent = now.received.min(size);
                    self.source.seek(SeekFrom::Start(sent))?;
                    self.last_heard = Instant::now();
                    let received = now.received;
                    self.phase = Phase::Sending {
                        status: now,
                        sent,
                        busy: Busy::default(),
                    };
                    return Ok(self.progress(received));
                }
                if let Some(error) = self.files.client.refusal(*seq) {
                    if !busy(&error) {
                        return Err(error);
                    }
                    // Not taken yet: asked again after a moment, within the same bound.
                    self.files.client.reply_left(self.last_heard)?;
                    self.files.client.pump(BUSY_FIRST.min(wait))?;
                    *seq = self.files.client.send(&FileBegin {
                        transfer: self.transfer,
                        destination: self.destination.clone(),
                        size: self.known.size,
                        digest: self.known.digest,
                    })?;
                    return Ok(Step::Waiting);
                }
                // Heard about the transfer last then: a wait for an answer since.
                self.files.client.reply_left(self.last_heard)?;
                self.files.client.pump(wait)?;
                Ok(Step::Waiting)
            }
            Phase::Sending { status, sent, busy } => {
                if status.state != FileState::Receiving {
                    // Rejected part way, or already complete from an earlier try.
                    return Ok(Step::Done(status.clone()));
                }
                if status.received >= size {
                    self.commit()?;
                    return Ok(self.progress(size));
                }
                // A few chunks on their way, and no more: the board's answers pace the sender.
                let chunk = self.buffer.len() as u64;
                while busy.until.is_none_or(|until| Instant::now() >= until)
                    && *sent < size
                    && *sent - status.received < WINDOW * chunk
                {
                    busy.until = None;
                    let want = ((size - *sent) as usize).min(self.buffer.len());
                    self.source.read_exact(&mut self.buffer[..want])?;
                    let seq = self.files.client.send(&FileChunk {
                        transfer: self.transfer,
                        offset: *sent,
                        data: Bytes(&self.buffer[..want]),
                    })?;
                    busy.on_way.push((seq, *sent));
                    *sent += want as u64;
                    busy.most = busy.most.max(*sent);
                }
                let paused = busy.until.map_or(wait, |until| {
                    until.saturating_duration_since(Instant::now())
                });
                self.files.client.pump(wait.min(paused))?;
                if let Some(error) = busy.refused(&self.files.client) {
                    return Err(error);
                }
                if busy.until.is_some() {
                    // Turned away as busy: go back to where the board last said it was.
                    *sent = status.received;
                    self.source.seek(SeekFrom::Start(*sent))?;
                }
                let now = self.files.status(self.transfer);
                if now != *status {
                    if now.received < status.received || now.received > busy.most.max(*sent) {
                        return Err(ClientError::Protocol("a transfer that went backwards"));
                    }
                    if now.received > *sent {
                        // Chunks sent before a busy one, answered after going back.
                        *sent = now.received;
                        self.source.seek(SeekFrom::Start(*sent))?;
                    }
                    busy.answered(now.received, chunk);
                    *status = now;
                    self.last_heard = Instant::now();
                } else {
                    self.files.client.reply_left(self.last_heard)?;
                }
                let received = status.received;
                Ok(self.progress(received))
            }
            Phase::Commit { seq, since } => {
                let now = self.files.status(self.transfer);
                if matches!(now.state, FileState::Committed | FileState::Rejected) {
                    return Ok(Step::Done(now));
                }
                let refused = self.files.client.refusal(*seq);
                if let Some(error) = &refused
                    && !busy(error)
                {
                    return Err(refused.expect("just matched"));
                }
                // Checking the digest of a large file takes the board a while: twelve
                // timeouts, or whatever bound the caller set.
                let left = match self.files.client.reply() {
                    Reply::Timeout => {
                        let left =
                            (*since + self.timeout * 12).saturating_duration_since(Instant::now());
                        if left.is_zero() {
                            return Err(ClientError::Io(std::io::ErrorKind::TimedOut.into()));
                        }
                        left
                    }
                    Reply::Alive { .. } | Reply::Within(_) => {
                        self.files.client.reply_left(*since)?
                    }
                };
                if refused.is_some() {
                    // Not taken yet: asked again after a moment, within the same bound.
                    self.files.client.pump(left.min(wait).min(BUSY_FIRST))?;
                    *seq = self.files.client.send(&FileCommit {
                        transfer: self.transfer,
                    })?;
                    return Ok(self.progress(size));
                }
                self.files.client.pump(left.min(wait))?;
                Ok(self.progress(size))
            }
        }
    }

    /// Every byte is there: asks the board to check and keep it, unless the file changed.
    fn commit(&mut self) -> Result<(), ClientError> {
        if let Some((path, before)) = &self.unchanged
            && looked_at(path).as_ref() != Some(before)
        {
            return Err(ClientError::Io(std::io::Error::other(
                "the file changed while it was being pushed",
            )));
        }
        let seq = self.files.client.send(&FileCommit {
            transfer: self.transfer,
        })?;
        self.phase = Phase::Commit {
            seq,
            since: Instant::now(),
        };
        Ok(())
    }
}

impl<S> Drop for Pushing<S> {
    fn drop(&mut self) {
        if !matches!(self.phase, Phase::Over) {
            self.files.forget(self.transfer);
        }
    }
}

/// The chunks of a push on their way, and whether the board turned one away as busy: its
/// comm core, or the program that receives the file, had no room for it yet.
#[derive(Debug, Default)]
struct Busy {
    /// Each chunk's request and where it starts, until the board has it.
    on_way: Vec<(u16, u64)>,
    /// When to send again, after a chunk was turned away.
    until: Option<Instant>,
    /// The furthest any chunk sent reaches: the board may yet answer up to there.
    most: u64,
    /// How long to wait after the next one: doubles each time, and starts again once the
    /// board takes something.
    backoff: Duration,
}

/// Whether the board turned a request away only for now: its comm core, or the program that
/// receives the file, had no room for it yet.
fn busy(error: &ClientError) -> bool {
    error.is_busy()
}

/// The first wait after a chunk is turned away as busy, and the longest.
const BUSY_FIRST: Duration = Duration::from_millis(20);
const BUSY_MOST: Duration = Duration::from_millis(320);

impl Busy {
    /// The refusal of a chunk on its way that is not busy, which ends the push. A busy one
    /// sets the wait, and forgets every chunk on its way: they are sent again.
    fn refused(&mut self, client: &orion_connect::Client) -> Option<ClientError> {
        let mut busy = false;
        for (seq, _) in &self.on_way {
            match client.refusal(*seq) {
                Some(error) if self::busy(&error) => busy = true,
                Some(error) => return Some(error),
                None => {}
            }
        }
        if busy {
            self.on_way.clear();
            self.backoff = (self.backoff * 2).clamp(BUSY_FIRST, BUSY_MOST);
            self.until = Some(Instant::now() + self.backoff);
        }
        None
    }

    /// The board has `received` bytes: the chunks before that are not waited on any more.
    fn answered(&mut self, received: u64, chunk: u64) {
        self.on_way.retain(|(_, offset)| offset + chunk > received);
        self.backoff = Duration::ZERO;
    }
}
