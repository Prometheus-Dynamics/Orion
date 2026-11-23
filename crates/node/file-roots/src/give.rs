//! The giving side of a fetch from a root, as a program gives the files it offers.
//!
//! The comm core hands node logic a `FileFetch` for each name under `roots/`. The file is
//! opened beneath its root, read through once by a helper thread for its digest, and then
//! given a chunk at a time with a few chunks on their way: the client's `FileTaken` paces it,
//! so a slow client, or a node logic that is slow to read, holds the file back and loses
//! none of it. A file that grows while it is given is given as it was when it was read for
//! its digest; one that shrinks is aborted, saying so.

use std::collections::BTreeMap;
use std::fs::File;
use std::io::Read;
use std::os::unix::fs::FileExt;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use orion_connect::ClientError;
use orion_msg_files::{FETCH_WINDOW, FileFetch, FileGive, FileGiveChunk, FileState, FileTaken};
use orion_wire::Bytes;
use rustix::fs::{FileType, OFlags, fstat};
use sha2::{Digest, Sha256};

use crate::path::{self, At};
use crate::{Link, Root, walk};

/// The largest chunk given.
pub const CHUNK: usize = 32 * 1024;

/// The most files given at once. One more is refused, saying so.
pub const MAX_GIVES: usize = 64;

/// How long a client that owes an answer may say nothing before its file is no longer given.
pub const QUIET: Duration = Duration::from_secs(60);

/// What a file is given from.
#[derive(Debug)]
enum Source {
    File(File),
    /// Made here: a root's index.
    Bytes(Vec<u8>),
}

#[derive(Debug)]
struct Give {
    /// Which reading for its digest is this one's: one asked for again is read again.
    ticket: u64,
    /// `None` while a helper reads it for its digest.
    source: Option<Source>,
    from: u64,
    size: u64,
    sent: u64,
    acked: u64,
    chunk: usize,
    heard: Instant,
}

/// What a helper finds: the source, its digest and its size, or why not.
type Found = Result<(Source, [u8; 32], u64), String>;

/// A helper reading a file for a give: its ticket, the transfer, and its thread.
type Helper = (u64, u64, JoinHandle<Found>);

/// The files being given.
#[derive(Default)]
pub struct Gives {
    gives: BTreeMap<u64, Give>,
    next: u64,
    helpers: Vec<Helper>,
    buffer: Vec<u8>,
}

impl std::fmt::Debug for Gives {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Gives({})", self.gives.len())
    }
}

fn refuse(
    link: &mut impl Link,
    transfer: u64,
    state: FileState,
    why: &str,
) -> Result<(), ClientError> {
    link.send(&FileTaken {
        transfer,
        received: 0,
        state,
        detail: why,
    })
}

/// Reads `source` through for its digest, and how long it is.
fn hash(mut source: impl Read) -> std::io::Result<([u8; 32], u64)> {
    let mut hasher = Sha256::new();
    let mut block = vec![0_u8; 256 * 1024];
    let mut size = 0_u64;
    loop {
        match source.read(&mut block) {
            Ok(0) => return Ok((hasher.finalize().into(), size)),
            Ok(read) => {
                hasher.update(&block[..read]);
                size += read as u64;
            }
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
}

/// Opens the file `at` names, beneath its root: a file, never a directory or a pipe.
fn open(at: &At<'_>) -> Result<File, String> {
    let root =
        path::open_root(at.root).map_err(|error| format!("the root: {}", path::words(error)))?;
    let file =
        path::beneath(&root, at.path, OFlags::RDONLY | OFlags::NONBLOCK).map_err(path::words)?;
    let stat = fstat(&file).map_err(path::words)?;
    if FileType::from_raw_mode(stat.st_mode) != FileType::RegularFile {
        return Err("not a file".to_owned());
    }
    Ok(File::from(file))
}

/// What the client is asked to take next, if anything: `Err` with why the file cannot be.
fn fill(
    give: &mut Give,
    transfer: u64,
    buffer: &mut Vec<u8>,
    link: &mut impl Link,
) -> Result<Result<(), String>, ClientError> {
    let Some(source) = &give.source else {
        return Ok(Ok(()));
    };
    let owed = give.sent > give.acked;
    let window = u64::from(FETCH_WINDOW) * give.chunk as u64;
    while give.sent < give.size && give.sent < give.acked + window {
        let want = give
            .chunk
            .min(usize::try_from(give.size - give.sent).unwrap_or(usize::MAX));
        let data: &[u8] = match source {
            Source::Bytes(bytes) => {
                let at = usize::try_from(give.sent).unwrap_or(usize::MAX);
                &bytes[at..at + want]
            }
            Source::File(file) => {
                buffer.resize(want, 0);
                match file.read_at(&mut buffer[..want], give.sent) {
                    Ok(0) => {
                        return Ok(Err(
                            "it changed while it was given: it is shorter".to_owned()
                        ));
                    }
                    Ok(read) => &buffer[..read],
                    Err(error) => return Ok(Err(format!("it could not be read: {error}"))),
                }
            }
        };
        let read = data.len() as u64;
        link.send(&FileGiveChunk {
            transfer,
            offset: give.sent,
            data: Bytes(data),
        })?;
        give.sent += read;
    }
    // The client's silence counts from when it was first owed something.
    if !owed && give.sent > give.acked {
        give.heard = Instant::now();
    }
    Ok(Ok(()))
}

impl Gives {
    /// Whether a helper is reading a file for its digest.
    #[must_use]
    pub const fn hashing(&self) -> bool {
        !self.helpers.is_empty()
    }

    /// Whether a file is being given: [`Gives::poll`] wants calling now and then.
    #[must_use]
    pub fn active(&self) -> bool {
        !self.gives.is_empty()
    }

    /// Forgets every file being given: the comm core they went through is gone.
    pub fn forget(&mut self) {
        self.gives.clear();
    }

    /// Begins giving what `fetch` asks for, or says why not.
    ///
    /// # Errors
    ///
    /// If the connection broke.
    pub(crate) fn fetch(
        &mut self,
        roots: &[Root],
        fetch: &FileFetch<'_>,
        link: &mut impl Link,
    ) -> Result<(), ClientError> {
        let transfer = fetch.transfer;
        // Asked again: read again, from where the client says it is.
        self.gives.remove(&transfer);
        if self.gives.len() >= MAX_GIVES {
            let why = format!("busy: {MAX_GIVES} files are being given");
            return refuse(link, transfer, FileState::Rejected, &why);
        }
        let at = match path::find(roots, fetch.name.as_str()) {
            Ok(at) => at,
            Err(why) => return refuse(link, transfer, FileState::Rejected, &why),
        };
        let job = if at.path == "index" {
            Job::Index(at.root.clone())
        } else if at.path.is_empty() {
            let why = format!(
                "a root is not a file: its index is {}{}/index",
                crate::PREFIX,
                at.root.name
            );
            return refuse(link, transfer, FileState::Rejected, &why);
        } else {
            match open(&at) {
                Ok(file) => Job::File(file),
                Err(why) => return refuse(link, transfer, FileState::Rejected, &why),
            }
        };
        let ticket = self.next;
        self.next += 1;
        let chunk = match fetch.max_chunk {
            0 => CHUNK,
            most => CHUNK.min(most as usize),
        }
        .max(1);
        self.gives.insert(
            transfer,
            Give {
                ticket,
                source: None,
                from: fetch.from,
                size: 0,
                sent: 0,
                acked: 0,
                chunk,
                heard: Instant::now(),
            },
        );
        match std::thread::Builder::new()
            .name("orion-file-give".into())
            .spawn(move || job.run())
        {
            Ok(helper) => {
                self.helpers.push((ticket, transfer, helper));
                Ok(())
            }
            Err(error) => {
                self.gives.remove(&transfer);
                let why = format!("no helper could be started: {error}");
                refuse(link, transfer, FileState::Rejected, &why)
            }
        }
    }

    /// What the client said of a file it is given.
    ///
    /// # Errors
    ///
    /// If the connection broke.
    pub fn taken(
        &mut self,
        taken: &FileTaken<'_>,
        link: &mut impl Link,
    ) -> Result<(), ClientError> {
        let transfer = taken.transfer;
        let Some(give) = self.gives.get_mut(&transfer) else {
            return Ok(());
        };
        if !matches!(taken.state, FileState::Receiving | FileState::Verifying) {
            // Taken whole, or not taken: over either way.
            self.gives.remove(&transfer);
            return Ok(());
        }
        give.heard = Instant::now();
        give.acked = give.acked.max(taken.received.min(give.sent));
        self.step(transfer, link)
    }

    fn step(&mut self, transfer: u64, link: &mut impl Link) -> Result<(), ClientError> {
        let Some(give) = self.gives.get_mut(&transfer) else {
            return Ok(());
        };
        if let Err(why) = fill(give, transfer, &mut self.buffer, link)? {
            let acked = give.acked;
            self.gives.remove(&transfer);
            return link.send(&FileTaken {
                transfer,
                received: acked,
                state: FileState::Aborted,
                detail: &why,
            });
        }
        Ok(())
    }

    /// Takes what the helpers found, and lets go of a file whose client went quiet.
    ///
    /// # Errors
    ///
    /// If the connection broke.
    pub fn poll(&mut self, link: &mut impl Link) -> Result<(), ClientError> {
        let mut at = 0;
        while at < self.helpers.len() {
            if !self.helpers[at].2.is_finished() {
                at += 1;
                continue;
            }
            let (ticket, transfer, helper) = self.helpers.swap_remove(at);
            let found = helper
                .join()
                .unwrap_or_else(|_| Err("its helper failed".to_owned()));
            let Some(give) = self
                .gives
                .get_mut(&transfer)
                .filter(|give| give.ticket == ticket)
            else {
                continue;
            };
            match found {
                Ok((source, digest, size)) => {
                    give.source = Some(source);
                    give.size = size;
                    give.sent = give.from.min(size);
                    give.acked = give.sent;
                    link.send(&FileGive {
                        transfer,
                        size,
                        digest,
                    })?;
                    self.step(transfer, link)?;
                }
                Err(why) => {
                    self.gives.remove(&transfer);
                    refuse(link, transfer, FileState::Rejected, &why)?;
                }
            }
        }
        let quiet: Vec<u64> = self
            .gives
            .iter()
            .filter(|(_, give)| give.sent > give.acked && give.heard.elapsed() >= QUIET)
            .map(|(transfer, _)| *transfer)
            .collect();
        for transfer in quiet {
            if let Some(give) = self.gives.remove(&transfer) {
                link.send(&FileTaken {
                    transfer,
                    received: give.acked,
                    state: FileState::Aborted,
                    detail: "the client said nothing for a minute",
                })?;
            }
        }
        Ok(())
    }
}

/// What a helper reads.
enum Job {
    File(File),
    Index(Root),
}

impl Job {
    fn run(self) -> Found {
        match self {
            Self::File(file) => {
                let (digest, size) =
                    hash(&file).map_err(|error| format!("it could not be read: {error}"))?;
                Ok((Source::File(file), digest, size))
            }
            Self::Index(root) => {
                let bytes = walk::index(&root)?;
                let digest: [u8; 32] = Sha256::digest(&bytes).into();
                let size = bytes.len() as u64;
                Ok((Source::Bytes(bytes), digest, size))
            }
        }
    }
}
