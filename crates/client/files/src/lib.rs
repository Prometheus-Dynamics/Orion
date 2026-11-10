//! The client's files.
//!
//! A file goes to a board in chunks, is checked against its digest when all of it is there,
//! and only then is put where it belongs. The wire names a destination, not a path: the
//! board decides where a destination is.
//!
//! **A push resumes.** A transfer has a number. Beginning the same transfer again, after the
//! connection broke or the program was restarted, is answered with how many bytes the board
//! already has, and the push carries on from there. [`Files::push`] takes the number from the
//! file's digest, so the same file to the same place is the same transfer without the caller
//! keeping anything.
//!
//! **A pull** asks the board to fetch the file itself, from a URL, and reports how it goes.
//! Nothing but the request crosses this connection.
//!
//! Both work on any address. A push keeps only a few chunks in flight, so it neither fills
//! the connection nor waits for an answer per chunk.

use std::cell::RefCell;
use std::fmt;
use std::io::{Read, Seek, SeekFrom};
use std::rc::Rc;
use std::time::{Duration, Instant};

use orion_connect::{Client, ClientError, Cx, Handler, Incoming};
use orion_msg_files::{FileBegin, FileChunk, FileCommit, FilePull, FileStatus};
use orion_wire::{Bytes, kind};
use sha2::{Digest, Sha256};

pub use orion_msg_files::FileState;

/// The most bytes of a file in one message.
pub const CHUNK: usize = 32 * 1024;
/// How many chunks may be on their way without an answer.
const WINDOW: u64 = 8;
/// The bytes of a chunk's message that are not the file's.
const CHUNK_OVERHEAD: usize = 64;

/// Where a transfer is, as the board last said.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Transfer {
    /// The transfer's number.
    pub transfer: u64,
    /// Bytes the board has, in order from the start.
    pub received: u64,
    /// Where it is.
    pub state: FileState,
    /// Why, if it was rejected.
    pub detail: String,
}

#[derive(Default)]
struct State {
    /// The newest status of each transfer this program is following.
    transfers: Vec<Transfer>,
}

impl Handler for State {
    fn kinds(&self) -> &'static [u8] {
        &[kind::FILE_STATUS]
    }

    fn handle(&mut self, frame: &Incoming<'_>, _cx: &mut Cx<'_>) -> Result<(), ClientError> {
        let status: FileStatus<'_> = frame.body()?;
        if let Some(transfer) = self
            .transfers
            .iter_mut()
            .find(|transfer| transfer.transfer == status.transfer)
        {
            transfer.received = status.received;
            transfer.state = status.state;
            transfer.detail.clear();
            transfer.detail.push_str(status.detail);
        }
        Ok(())
    }
}

/// The SHA-256 of everything `source` holds, and how much that is. The source is left at its
/// start.
///
/// # Errors
///
/// If the source can't be read.
pub fn digest(source: &mut (impl Read + Seek)) -> std::io::Result<([u8; 32], u64)> {
    source.seek(SeekFrom::Start(0))?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0_u8; CHUNK];
    let mut size = 0_u64;
    loop {
        let read = source.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
        size += read as u64;
    }
    source.seek(SeekFrom::Start(0))?;
    Ok((hasher.finalize().into(), size))
}

/// A file for the board to fetch itself.
#[derive(Clone, Copy, Debug)]
pub struct Pull<'a> {
    /// The transfer's number, chosen by the caller.
    pub transfer: u64,
    /// Where the board fetches it from.
    pub url: &'a str,
    /// Where it should go.
    pub destination: &'a str,
    /// Its size.
    pub size: u64,
    /// Its SHA-256.
    pub digest: [u8; 32],
}

/// The files of one session.
#[derive(Clone)]
pub struct Files {
    client: Client,
    state: Rc<RefCell<State>>,
}

impl fmt::Debug for Files {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Files")
    }
}

impl Files {
    /// The files of `client`'s session.
    #[must_use]
    pub fn new(client: &Client) -> Self {
        let state = Rc::new(RefCell::new(State::default()));
        client.add(state.clone());
        Self {
            client: client.clone(),
            state,
        }
    }

    fn follow(&self, transfer: u64) {
        let mut state = self.state.borrow_mut();
        state.transfers.retain(|known| known.transfer != transfer);
        state.transfers.push(Transfer {
            transfer,
            received: 0,
            // Not a state the board sends: nothing has been heard yet.
            state: FileState::Unknown(0),
            detail: String::new(),
        });
    }

    fn status(&self, transfer: u64) -> Transfer {
        let state = self.state.borrow();
        state
            .transfers
            .iter()
            .find(|known| known.transfer == transfer)
            .cloned()
            .unwrap_or_else(|| unreachable!("a transfer is followed until it is forgotten"))
    }

    fn forget(&self, transfer: u64) {
        self.state
            .borrow_mut()
            .transfers
            .retain(|known| known.transfer != transfer);
    }

    /// Waits until the board says something new about `transfer`, for the session's timeout.
    fn heard(&self, seq: u16, transfer: u64, before: &Transfer) -> Result<Transfer, ClientError> {
        self.client
            .answered(seq, || self.status(transfer) != *before)?;
        Ok(self.status(transfer))
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
        mut progress: impl FnMut(u64, u64),
    ) -> Result<Transfer, ClientError> {
        let (digest, size) = digest(source)?;
        let name = destination
            .parse()
            .map_err(|_| ClientError::Protocol("a destination that is not a name"))?;
        // The same bytes to the same place are the same transfer, whoever sends them.
        let mut hasher = Sha256::new();
        hasher.update(digest);
        hasher.update(destination.as_bytes());
        let number: [u8; 32] = hasher.finalize().into();
        let transfer = u64::from_le_bytes([
            number[0], number[1], number[2], number[3], number[4], number[5], number[6], number[7],
        ]);

        self.follow(transfer);
        let result = self.push_followed(transfer, name, size, digest, source, &mut progress);
        self.forget(transfer);
        result
    }

    fn push_followed(
        &self,
        transfer: u64,
        destination: orion_types::FileName,
        size: u64,
        digest: [u8; 32],
        source: &mut (impl Read + Seek),
        progress: &mut impl FnMut(u64, u64),
    ) -> Result<Transfer, ClientError> {
        let nothing = self.status(transfer);
        let seq = self.client.send(&FileBegin {
            transfer,
            destination,
            size,
            digest,
        })?;
        let mut status = self.heard(seq, transfer, &nothing)?;
        // Where the board says to go on from: zero for a new transfer.
        let mut sent = status.received.min(size);
        source.seek(SeekFrom::Start(sent))?;
        let limit = self
            .client
            .session(|session| session.welcomed().max_frame as usize);
        let chunk = CHUNK.min(limit.saturating_sub(CHUNK_OVERHEAD)).max(1);
        let mut buffer = vec![0_u8; chunk];
        let timeout = self.client.session(|session| session.timeout());
        let mut last_heard = Instant::now();
        progress(status.received, size);

        while status.state == FileState::Receiving && status.received < size {
            // A few chunks on their way, and no more: the board's answers pace the sender.
            while sent < size && sent - status.received < WINDOW * chunk as u64 {
                let want = ((size - sent) as usize).min(chunk);
                source.read_exact(&mut buffer[..want])?;
                self.client.send(&FileChunk {
                    transfer,
                    offset: sent,
                    data: Bytes(&buffer[..want]),
                })?;
                sent += want as u64;
            }
            self.client.pump(Duration::from_millis(200))?;
            let now = self.status(transfer);
            if now != status {
                if now.received < status.received || now.received > sent {
                    return Err(ClientError::Protocol("a transfer that went backwards"));
                }
                if now.received != status.received {
                    progress(now.received, size);
                }
                status = now;
                last_heard = Instant::now();
            } else if last_heard.elapsed() > timeout {
                return Err(ClientError::Io(std::io::ErrorKind::TimedOut.into()));
            }
        }
        if status.state != FileState::Receiving {
            // Rejected part way, or already complete from an earlier try.
            return Ok(status);
        }
        let seq = self.client.send(&FileCommit { transfer })?;
        // Checking the digest of a large file takes the board a while.
        let deadline = Instant::now() + timeout * 12;
        loop {
            let before = self.status(transfer);
            if matches!(before.state, FileState::Committed | FileState::Rejected) {
                return Ok(before);
            }
            if let Some(error) = self.client.refusal(seq) {
                return Err(ClientError::Refused(error));
            }
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return Err(ClientError::Io(std::io::ErrorKind::TimedOut.into()));
            }
            self.client.pump(left.min(Duration::from_millis(200)))?;
        }
    }

    /// Asks the board to fetch a file itself and put it where it belongs, and waits up to
    /// `wait` for how it ends. The board checks it against the digest and size asked for.
    /// `progress` is told how many bytes the board has.
    ///
    /// # Errors
    ///
    /// If the board refuses to pull, the connection broke, or it has not ended in time. It
    /// goes on at the board then; ask again with the same number to follow it.
    pub fn pull(
        &self,
        pull: &Pull<'_>,
        wait: Duration,
        mut progress: impl FnMut(u64, u64),
    ) -> Result<Transfer, ClientError> {
        let Pull {
            transfer,
            url,
            destination,
            size,
            digest,
        } = *pull;
        let destination = destination
            .parse()
            .map_err(|_| ClientError::Protocol("a destination that is not a name"))?;
        self.follow(transfer);
        let seq = self.client.send(&FilePull {
            transfer,
            url,
            destination,
            size,
            digest,
        });
        let deadline = Instant::now() + wait;
        let mut told = u64::MAX;
        let result = seq.and_then(|seq| {
            loop {
                let status = self.status(transfer);
                if status.received != told && !matches!(status.state, FileState::Unknown(_)) {
                    told = status.received;
                    progress(told, size);
                }
                if matches!(status.state, FileState::Committed | FileState::Rejected) {
                    return Ok(status);
                }
                if let Some(error) = self.client.refusal(seq) {
                    return Err(ClientError::Refused(error));
                }
                let left = deadline.saturating_duration_since(Instant::now());
                if left.is_zero() {
                    return Err(ClientError::Io(std::io::ErrorKind::TimedOut.into()));
                }
                self.client.pump(left.min(Duration::from_millis(200)))?;
            }
        });
        self.forget(transfer);
        result
    }
}
