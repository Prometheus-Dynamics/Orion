//! Files a node receives.
//!
//! A file comes in chunks from any connection. Its bytes are appended to a part file in the
//! store's own directory, so a transfer that is interrupted, by the sender, the connection or
//! node logic itself, goes on from the byte it reached: the sender begins again with the
//! same number, size, digest and destination and is told where to continue. When every byte
//! is here the sender commits; the file is then read back, its SHA-256 compared with the
//! digest it came with, and only if they match is it moved to its destination, in one
//! rename. Nothing is ever at a destination that did not match its digest.
//!
//! Appending a chunk is a write to the page cache and is done at once. Checking the digest
//! reads the whole file and may take seconds, so it is done by a helper thread that is
//! started for that commit and ends with it. [`Files::poll`] collects what the helpers found.

mod verify;

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::mpsc::{self, Receiver, Sender};

use orion_msg_files::{FileBegin, FileChunk, FileCommit, FileState, FileStatus};
use orion_reply::{Asked, Out};
use orion_types::{Error, FileName};
use orion_wire::kind;
use tracing::warn;

/// The most transfers under way at once. One more is refused as busy.
pub const MAX_TRANSFERS: usize = 64;

/// What the store is told.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FilesConfig {
    /// Where files go: a destination is a path under this.
    pub root: PathBuf,
    /// The largest file taken.
    pub max_size: u64,
}

impl FilesConfig {
    /// Files under `root`, of up to 16 GiB.
    #[must_use]
    pub fn new(root: &Path) -> Self {
        Self {
            root: root.to_owned(),
            max_size: 16 << 30,
        }
    }
}

/// What is kept beside a part file, to find the transfer again after a restart.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Meta {
    destination: FileName,
    size: u64,
    digest: [u8; 32],
}

impl Meta {
    fn text(&self) -> String {
        let digest: String = self
            .digest
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        format!("{}\n{digest}\n{}\n", self.size, self.destination)
    }

    fn read(path: &Path) -> Option<Self> {
        let text = fs::read_to_string(path).ok()?;
        let mut lines = text.lines();
        let size = lines.next()?.parse().ok()?;
        let hex = lines.next()?;
        let destination = lines.next()?.parse().ok()?;
        if hex.len() != 64 {
            return None;
        }
        let mut digest = [0_u8; 32];
        for (index, byte) in digest.iter_mut().enumerate() {
            *byte = u8::from_str_radix(hex.get(index * 2..index * 2 + 2)?, 16).ok()?;
        }
        Some(Self {
            destination,
            size,
            digest,
        })
    }
}

#[derive(Debug)]
struct Receiving {
    meta: Meta,
    part: File,
    received: u64,
    /// Whether a helper is checking it.
    verifying: bool,
    /// The session that committed it, to tell how that ended.
    committer: Option<u32>,
}

/// The files of one node.
#[derive(Debug)]
pub struct Files {
    config: FilesConfig,
    incoming: PathBuf,
    transfers: BTreeMap<u64, Receiving>,
    found: Receiver<verify::Found>,
    finder: Sender<verify::Found>,
    verifying: usize,
}

const fn status(transfer: u64, received: u64, state: FileState, detail: &str) -> FileStatus<'_> {
    FileStatus {
        transfer,
        received,
        state,
        detail,
    }
}

impl Files {
    /// The kinds answered here.
    pub const KINDS: &'static [u8] = &[kind::FILE_BEGIN, kind::FILE_CHUNK, kind::FILE_COMMIT];

    /// Opens the store, making its directories.
    ///
    /// # Errors
    ///
    /// If the directories cannot be made.
    pub fn open(config: FilesConfig) -> io::Result<Self> {
        let incoming = config.root.join(".incoming");
        fs::create_dir_all(&incoming)?;
        let (finder, found) = mpsc::channel();
        Ok(Self {
            config,
            incoming,
            transfers: BTreeMap::new(),
            found,
            finder,
            verifying: 0,
        })
    }

    /// Where files go.
    #[must_use]
    pub fn root(&self) -> &Path {
        &self.config.root
    }

    /// Where the file for `destination` is, or will be.
    #[must_use]
    pub fn path(&self, destination: &FileName) -> PathBuf {
        self.config.root.join(destination.as_str())
    }

    /// How many transfers are under way.
    #[must_use]
    pub fn transfers(&self) -> usize {
        self.transfers.len()
    }

    /// Whether a helper is checking a digest: [`Files::poll`] will have something soon.
    #[must_use]
    pub const fn verifying(&self) -> bool {
        self.verifying != 0
    }

    fn part_path(&self, transfer: u64) -> PathBuf {
        self.incoming.join(format!("{transfer:016x}.part"))
    }

    fn meta_path(&self, transfer: u64) -> PathBuf {
        self.incoming.join(format!("{transfer:016x}.meta"))
    }

    fn discard(&mut self, transfer: u64) {
        self.transfers.remove(&transfer);
        let _ = fs::remove_file(self.part_path(transfer));
        let _ = fs::remove_file(self.meta_path(transfer));
    }

    /// Opens the part file of `transfer` for `meta`: the one from before if it is the same
    /// transfer, a new one otherwise.
    fn open_part(&self, transfer: u64, meta: &Meta) -> io::Result<(File, u64)> {
        let (part_path, meta_path) = (self.part_path(transfer), self.meta_path(transfer));
        let same = Meta::read(&meta_path).as_ref() == Some(meta);
        if !same {
            let _ = fs::remove_file(&part_path);
            fs::write(&meta_path, meta.text())?;
        }
        let part = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&part_path)?;
        let mut received = part.metadata()?.len();
        if received > meta.size {
            // More than the file: it is not that file.
            part.set_len(0)?;
            received = 0;
        }
        Ok((part, received))
    }

    fn begin(&mut self, asked: &Asked<'_>, out: &mut dyn Out) -> Result<(), Error> {
        let begin: FileBegin = asked.body()?;
        let transfer = begin.transfer;
        let meta = Meta {
            destination: begin.destination,
            size: begin.size,
            digest: begin.digest,
        };
        if meta.destination.as_str().split('/').next() == Some(".incoming") {
            // The store's own directory, where parts are kept.
            return out.send(
                asked.session,
                &status(transfer, 0, FileState::Rejected, "not a place for files"),
            );
        }
        if meta.size > self.config.max_size {
            return out.send(
                asked.session,
                &status(
                    transfer,
                    0,
                    FileState::Rejected,
                    "larger than this node takes",
                ),
            );
        }
        match self.transfers.get(&transfer) {
            Some(known) if known.meta == meta => {}
            Some(known) if known.verifying => return Err(Error::Busy),
            Some(_) => self.discard(transfer),
            None => {}
        }
        if !self.transfers.contains_key(&transfer) {
            if self.transfers.len() >= MAX_TRANSFERS {
                return Err(Error::Busy);
            }
            match self.open_part(transfer, &meta) {
                Ok((part, received)) => {
                    self.transfers.insert(
                        transfer,
                        Receiving {
                            meta,
                            part,
                            received,
                            verifying: false,
                            committer: None,
                        },
                    );
                }
                Err(error) => {
                    let detail = format!("the file cannot be kept: {error}");
                    return out.send(
                        asked.session,
                        &status(transfer, 0, FileState::Rejected, &detail),
                    );
                }
            }
        }
        let known = &self.transfers[&transfer];
        let state = if known.verifying {
            FileState::Verifying
        } else {
            FileState::Receiving
        };
        out.send(asked.session, &status(transfer, known.received, state, ""))
    }

    fn chunk(&mut self, asked: &Asked<'_>, out: &mut dyn Out) -> Result<(), Error> {
        let chunk: FileChunk<'_> = asked.body()?;
        let transfer = chunk.transfer;
        let receiving = self.transfers.get_mut(&transfer).ok_or(Error::NotFound)?;
        if receiving.verifying {
            let received = receiving.received;
            return out.send(
                asked.session,
                &status(transfer, received, FileState::Verifying, ""),
            );
        }
        // Only the next bytes are taken. Anything else is a chunk sent before the sender
        // heard where to go on from: the answer says where that is.
        if chunk.offset == receiving.received {
            let end = receiving.received + chunk.data.len() as u64;
            if end > receiving.meta.size {
                self.discard(transfer);
                return out.send(
                    asked.session,
                    &status(transfer, 0, FileState::Rejected, "more bytes than its size"),
                );
            }
            if let Err(error) = receiving.part.write_all(chunk.data.0) {
                let detail = format!("the file cannot be written: {error}");
                self.discard(transfer);
                return out.send(
                    asked.session,
                    &status(transfer, 0, FileState::Rejected, &detail),
                );
            }
            receiving.received = end;
        }
        let received = receiving.received;
        out.send(
            asked.session,
            &status(transfer, received, FileState::Receiving, ""),
        )
    }

    fn commit(&mut self, asked: &Asked<'_>, out: &mut dyn Out) -> Result<(), Error> {
        let commit: FileCommit = asked.body()?;
        let transfer = commit.transfer;
        let (part_path, destination) = (
            self.part_path(transfer),
            self.transfers
                .get(&transfer)
                .map(|receiving| self.path(&receiving.meta.destination)),
        );
        let Some(receiving) = self.transfers.get_mut(&transfer) else {
            return out.send(
                asked.session,
                &status(transfer, 0, FileState::Rejected, "no such transfer"),
            );
        };
        let received = receiving.received;
        if receiving.verifying {
            receiving.committer = Some(asked.session);
            return out.send(
                asked.session,
                &status(transfer, received, FileState::Verifying, ""),
            );
        }
        if received != receiving.meta.size {
            return out.send(
                asked.session,
                &status(
                    transfer,
                    received,
                    FileState::Rejected,
                    "not all of it is here",
                ),
            );
        }
        receiving.verifying = true;
        receiving.committer = Some(asked.session);
        self.verifying += 1;
        verify::start(verify::Job {
            transfer,
            part: part_path,
            destination: destination.unwrap_or_default(),
            root: self.config.root.clone(),
            digest: receiving.meta.digest,
            found: self.finder.clone(),
        });
        out.send(
            asked.session,
            &status(transfer, received, FileState::Verifying, ""),
        )
    }

    /// Answers one request.
    ///
    /// # Errors
    ///
    /// [`Error`] for the caller, as a refusal.
    pub fn request(&mut self, asked: &Asked<'_>, out: &mut dyn Out) -> Result<(), Error> {
        match asked.kind {
            kind::FILE_BEGIN => self.begin(asked, out),
            kind::FILE_CHUNK => self.chunk(asked, out),
            kind::FILE_COMMIT => self.commit(asked, out),
            _ => Err(Error::Unsupported),
        }
    }

    /// Tells whoever committed a file how the check of its digest ended. Call it between
    /// requests while [`Files::verifying`].
    pub fn poll(&mut self, out: &mut dyn Out) {
        while let Ok(found) = self.found.try_recv() {
            self.verifying = self.verifying.saturating_sub(1);
            let Some(receiving) = self.transfers.remove(&found.transfer) else {
                continue;
            };
            let _ = fs::remove_file(self.meta_path(found.transfer));
            let (state, detail) = match &found.result {
                Ok(()) => (FileState::Committed, ""),
                Err(why) => {
                    warn!(
                        "the file for {} was not put in place: {why}",
                        receiving.meta.destination
                    );
                    let _ = fs::remove_file(self.part_path(found.transfer));
                    (FileState::Rejected, why.as_str())
                }
            };
            if let Some(session) = receiving.committer {
                let _ = out.send(
                    session,
                    &status(found.transfer, receiving.received, state, detail),
                );
            }
        }
    }

    /// A session is gone. Its transfers stay on disk to be gone on with; one whose check is
    /// under way has nobody to tell.
    pub fn gone(&mut self, session: u32) {
        for receiving in self.transfers.values_mut() {
            if receiving.committer == Some(session) {
                receiving.committer = None;
            }
        }
    }
}
