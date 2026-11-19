//! Offering files: a program on the board gives the files under a prefix to whoever fetches
//! them. Orion hands the program each request, by name; the program gives the file or says why
//! not, and Orion knows nothing of what it holds.

use std::cell::RefCell;
use std::io::{Read, Seek, SeekFrom};
use std::rc::Rc;
use std::time::{Duration, Instant};

use orion_connect::ClientError;
use orion_msg_files::{FETCH_WINDOW, FileGive, FileGiveChunk, FileOffer, FileState, FileTaken};
use orion_wire::Bytes;

use crate::fetch::{Fetches, Wanted};
use crate::{CHUNK, CHUNK_OVERHEAD, Files, Known, digest_while};

/// Files under a prefix, offered by this program for as long as its session lives.
pub struct Offer {
    files: Files,
    fetches: Rc<RefCell<Fetches>>,
    prefix: String,
}

impl std::fmt::Debug for Offer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Offer({})", self.prefix)
    }
}

/// A file asked of this program: give it, or say why not. Dropped without either, it is
/// refused, so the client is not left waiting.
#[derive(Debug)]
pub struct Asked {
    files: Files,
    wanted: Wanted,
    /// Given or refused: nothing is left to say when it is dropped.
    answered: bool,
}

/// How a file this program gives is going, after a step.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Giving {
    /// Under way: the client has `received` of `size` bytes.
    Progress {
        /// Bytes the client has, in order from the start.
        received: u64,
        /// The file's size.
        size: u64,
    },
    /// Over: the client checked the digest and has it, or it is not taken, and why.
    Done {
        /// Whether the client has it.
        taken: bool,
        /// Why not, if it is not.
        detail: String,
    },
}

/// A file being given. [`Gift::step`] moves it on; [`Gift::abort`] stops it, saying why, and
/// so does dropping it before it is over.
pub struct Gift<R> {
    files: Files,
    source: R,
    transfer: u64,
    size: u64,
    sent: u64,
    /// What the client last said it has, in order from the start.
    acked: u64,
    chunk: usize,
    buffer: Vec<u8>,
    last_heard: Instant,
    over: bool,
}

impl<R> std::fmt::Debug for Gift<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Gift({}, {} of {})", self.transfer, self.sent, self.size)
    }
}

impl Files {
    /// Offers every file whose name starts with `prefix`, one or more parts of a name ending
    /// in `/`: whoever fetches one is handed to this program, at [`Offer::next`].
    ///
    /// # Errors
    ///
    /// If the node is too old to offer files, or refuses: another program offers the prefix
    /// (named in the refusal), the prefix is not one, or this program may not offer.
    pub fn offer(&self, prefix: &str) -> Result<Offer, ClientError> {
        self.hear_fetches()?;
        let seq = self.client.send(&FileOffer { prefix })?;
        self.client
            .answered(seq, || self.fetches.borrow().offered.contains(&seq))?;
        self.fetches
            .borrow_mut()
            .offered
            .retain(|answered| *answered != seq);
        Ok(Offer {
            files: self.clone(),
            fetches: self.fetches.clone(),
            prefix: prefix.to_owned(),
        })
    }
}

impl Offer {
    /// The prefix offered.
    #[must_use]
    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    /// The next file asked of this program, waiting up to `wait` for one.
    ///
    /// # Errors
    ///
    /// If the connection broke.
    pub fn next(&mut self, wait: Duration) -> Result<Option<Asked>, ClientError> {
        let deadline = Instant::now() + wait;
        loop {
            let wanted = {
                let mut fetches = self.fetches.borrow_mut();
                let at = fetches
                    .wanted
                    .iter()
                    .position(|wanted| wanted.name.starts_with(&self.prefix));
                at.and_then(|at| fetches.wanted.remove(at))
            };
            if let Some(wanted) = wanted {
                return Ok(Some(Asked {
                    files: self.files.clone(),
                    wanted,
                    answered: false,
                }));
            }
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return Ok(None);
            }
            self.files.client.pump(left)?;
        }
    }
}

impl Asked {
    /// The file asked for.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.wanted.name
    }

    /// How much of it the client has already.
    #[must_use]
    pub const fn from(&self) -> u64 {
        self.wanted.from
    }

    /// Who asked, as the audit log names them.
    #[must_use]
    pub fn caller(&self) -> &str {
        &self.wanted.caller
    }

    /// Does not give the file, saying why: the name is not one this program has, say.
    ///
    /// # Errors
    ///
    /// If the connection broke.
    pub fn refuse(mut self, why: &str) -> Result<(), ClientError> {
        self.answered = true;
        self.refused(why)
    }

    fn refused(&self, why: &str) -> Result<(), ClientError> {
        self.files
            .fetches
            .borrow_mut()
            .taken
            .remove(&self.wanted.transfer);
        self.files.client.send(&FileTaken {
            transfer: self.wanted.transfer,
            received: 0,
            state: FileState::Rejected,
            detail: why,
        })?;
        Ok(())
    }

    /// Gives `source`, read through once first for its digest, and returns the gift to be
    /// [stepped](Gift::step). Its bytes must stay the same until the client has them all: a
    /// file that changes is found by the client's check, and the gift ends not taken.
    ///
    /// # Errors
    ///
    /// If the source can't be read, or the connection broke.
    pub fn give<R: Read + Seek>(self, mut source: R) -> Result<Gift<R>, ClientError> {
        let (digest, size) = digest_while(&mut source, || self.files.client.drain())?;
        self.give_known(source, Known { digest, size })
    }

    /// As [`Asked::give`], for a source whose digest and size are known already.
    ///
    /// # Errors
    ///
    /// If the source can't be read, or the connection broke.
    pub fn give_known<R: Read + Seek>(
        mut self,
        mut source: R,
        known: Known,
    ) -> Result<Gift<R>, ClientError> {
        let transfer = self.wanted.transfer;
        self.files.client.send(&FileGive {
            transfer,
            size: known.size,
            digest: known.digest,
        })?;
        let sent = self.wanted.from.min(known.size);
        source.seek(SeekFrom::Start(sent))?;
        let own = self
            .files
            .client
            .session(|session| session.welcomed().max_frame as usize)
            .saturating_sub(CHUNK_OVERHEAD);
        let theirs = match self.wanted.max_chunk {
            0 => CHUNK,
            limit => limit as usize,
        };
        let chunk = CHUNK.min(own).min(theirs).max(1);
        self.answered = true;
        Ok(Gift {
            files: self.files.clone(),
            source,
            transfer,
            size: known.size,
            sent,
            acked: sent,
            chunk,
            buffer: vec![0; chunk],
            last_heard: Instant::now(),
            over: false,
        })
    }
}

impl Drop for Asked {
    fn drop(&mut self) {
        if !self.answered {
            let _ = self.refused("the program that offers it did not give it");
        }
    }
}

impl<R> Gift<R> {
    /// Stops giving the file, saying why: the client's fetch ends at once, as aborted, with
    /// `why` after words that say the program stopped giving it. A gift that is over already
    /// is left as it ended.
    ///
    /// # Errors
    ///
    /// If the connection broke: the client then hears that the program went.
    pub fn abort(mut self, why: &str) -> Result<(), ClientError> {
        self.stop(why)
    }

    fn stop(&mut self, why: &str) -> Result<(), ClientError> {
        if self.over {
            return Ok(());
        }
        self.over = true;
        self.files.fetches.borrow_mut().taken.remove(&self.transfer);
        self.files.client.send(&FileTaken {
            transfer: self.transfer,
            received: self.acked,
            state: FileState::Aborted,
            detail: why,
        })?;
        Ok(())
    }
}

impl<R> Drop for Gift<R> {
    fn drop(&mut self) {
        let _ = self.stop("it was dropped before it was given");
    }
}

impl<R: Read + Seek> Gift<R> {
    /// Moves the gift on, waiting at most about `wait` for the client: sends what the window
    /// has room for, and takes what the client said.
    ///
    /// # Errors
    ///
    /// If the source can't be read, the client goes quiet for the session's timeout, or the
    /// connection broke. The gift is then over.
    pub fn step(&mut self, wait: Duration) -> Result<Giving, ClientError> {
        if self.over {
            return Err(ClientError::Protocol("a gift that is over"));
        }
        let result = self.step_once(wait);
        if matches!(result, Err(_) | Ok(Giving::Done { .. })) {
            self.over = true;
            self.files.fetches.borrow_mut().taken.remove(&self.transfer);
        }
        result
    }

    /// Steps until the gift is over.
    ///
    /// # Errors
    ///
    /// As [`Gift::step`].
    pub fn finish(mut self) -> Result<Giving, ClientError> {
        loop {
            if let done @ Giving::Done { .. } = self.step(Duration::from_millis(200))? {
                return Ok(done);
            }
        }
    }

    fn step_once(&mut self, wait: Duration) -> Result<Giving, ClientError> {
        let taken = self
            .files
            .fetches
            .borrow()
            .taken
            .get(&self.transfer)
            .cloned()
            .unwrap_or_default();
        match taken.state {
            Some(FileState::Committed) => {
                return Ok(Giving::Done {
                    taken: true,
                    detail: String::new(),
                });
            }
            Some(FileState::Rejected) => {
                return Ok(Giving::Done {
                    taken: false,
                    detail: taken.detail,
                });
            }
            Some(_) if taken.received > self.acked => {
                self.acked = taken.received;
                self.last_heard = Instant::now();
            }
            _ => {}
        }
        // A few chunks on their way, and no more: the client's answers pace the giver.
        let window = u64::from(FETCH_WINDOW) * self.chunk as u64;
        while self.sent < self.size && self.sent < self.acked + window {
            let want = self
                .chunk
                .min(usize::try_from(self.size - self.sent).unwrap_or(usize::MAX));
            let read = self.source.read(&mut self.buffer[..want])?;
            if read == 0 {
                return Err(ClientError::Io(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "the source ended before its size",
                )));
            }
            self.files.client.send(&FileGiveChunk {
                transfer: self.transfer,
                offset: self.sent,
                data: Bytes(&self.buffer[..read]),
            })?;
            self.sent += read as u64;
        }
        self.files.client.reply_left(self.last_heard)?;
        self.files.client.pump(wait)?;
        Ok(Giving::Progress {
            received: self.acked,
            size: self.size,
        })
    }
}
