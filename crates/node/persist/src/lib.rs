//! Keeping the record store across restarts.
//!
//! The desired records are kept in one snapshot file in the state directory. Two rules
//! shape everything here:
//!
//! **A save replaces the snapshot in one step.** The new snapshot is written beside the old
//! one and made safe on disk, and only then given the snapshot's name. Power can go at any
//! moment: what is found afterwards is the old snapshot whole or the new one whole, never
//! part of either. The snapshot before the last is kept as well.
//!
//! **A file that cannot be read never stops start-up.** A snapshot that is cut short, fails
//! its checksum or is not a snapshot at all is moved aside, kept for whoever wants to look,
//! and the one before it is used; if that is no good either, the node starts with nothing
//! and its peers send it what it is missing. A node that will not start is worse than a
//! node that has forgotten.
//!
//! The file follows the wire's rules: each record is one wire record with its length in
//! front, so a newer build's extra fields are passed over by an older one. A snapshot of
//! another format version has another name, and is neither read nor touched.

mod disk;

use std::io;
use std::path::{Path, PathBuf};

use orion_msg_records::RecordEntry;
use orion_store::{Record, Version};
use orion_wire::body::encode_fields;
use orion_wire::{Decode, crc32c};
use tracing::warn;

pub use disk::{Disk, RealDisk};

/// The format of the snapshot file. A change that an older build could not read raises it,
/// and with it the file's name.
pub const FORMAT: u32 = 1;
const MAGIC: &[u8; 8] = b"ORIONST1";
const HEADER: usize = MAGIC.len() + 8;
const CRC: usize = 4;
/// The most a single record may take in the file.
const MAX_RECORD: usize = 16 << 20;

/// Which file what was loaded came from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Source {
    /// The snapshot.
    Current,
    /// The snapshot before it: the newest could not be read.
    Previous,
    /// Neither: there was nothing, or nothing that could be read.
    Nothing,
}

/// What start-up found.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Loaded {
    /// The records, to give to `Store::restored`.
    pub records: Vec<Record>,
    /// Which file they came from.
    pub source: Source,
    /// What was wrong with each file that could not be used, for the log.
    pub problems: Vec<String>,
}

/// The snapshot of one state directory.
#[derive(Debug)]
pub struct Persist<D: Disk = RealDisk> {
    dir: PathBuf,
    disk: D,
    /// How many unreadable files this process has moved aside.
    moved_aside: u32,
}

/// Puts `records` into the snapshot's bytes.
fn encode(records: impl Iterator<Item = Record>) -> io::Result<Vec<u8>> {
    let mut out = Vec::with_capacity(4096);
    out.extend_from_slice(MAGIC);
    out.extend_from_slice(&FORMAT.to_le_bytes());
    out.extend_from_slice(&0_u32.to_le_bytes());
    let mut count = 0_u32;
    let mut scratch = Vec::new();
    for record in records {
        let entry = record.entry();
        scratch.resize(record.key.len() + record.version.value.len() + 64, 0);
        let len = encode_fields(&entry, &mut scratch).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("the record {} cannot be encoded", record.key),
            )
        })?;
        out.extend_from_slice(&(len as u32).to_le_bytes());
        out.extend_from_slice(&scratch[..len]);
        count += 1;
    }
    out[MAGIC.len() + 4..HEADER].copy_from_slice(&count.to_le_bytes());
    let crc = crc32c(&out);
    out.extend_from_slice(&crc.to_le_bytes());
    Ok(out)
}

/// Reads a snapshot's bytes, or says what is wrong with them.
fn decode(bytes: &[u8]) -> Result<Vec<Record>, &'static str> {
    if bytes.len() < HEADER + CRC {
        return Err("it is cut short");
    }
    let (body, crc) = bytes.split_at(bytes.len() - CRC);
    if &body[..MAGIC.len()] != MAGIC {
        return Err("it is not a snapshot");
    }
    if crc != crc32c(body).to_le_bytes() {
        return Err("its checksum does not match");
    }
    let word = |at: usize| u32::from_le_bytes([body[at], body[at + 1], body[at + 2], body[at + 3]]);
    if word(MAGIC.len()) != FORMAT {
        return Err("it is another format");
    }
    let count = word(MAGIC.len() + 4) as usize;
    let mut records = Vec::with_capacity(count.min(1 << 16));
    let mut rest = &body[HEADER..];
    for _ in 0..count {
        if rest.len() < 4 {
            return Err("a record is cut short");
        }
        let len = u32::from_le_bytes([rest[0], rest[1], rest[2], rest[3]]) as usize;
        if len > MAX_RECORD || rest.len() < 4 + len {
            return Err("a record is cut short");
        }
        let entry =
            RecordEntry::decode(&rest[4..4 + len]).map_err(|_| "a record does not decode")?;
        records.push(Record {
            class: entry.class,
            key: entry.key.to_owned(),
            version: Version {
                stamp: entry.stamp,
                deleted: entry.deleted,
                value: entry.value.0.to_vec(),
            },
        });
        rest = &rest[4 + len..];
    }
    Ok(records)
}

impl Persist<RealDisk> {
    /// The snapshot in the directory `dir`, on the real disk. The directory is made if it is
    /// not there.
    ///
    /// # Errors
    ///
    /// If the directory cannot be made.
    pub fn open(dir: &Path) -> io::Result<Self> {
        std::fs::create_dir_all(dir)?;
        Ok(Self::on(dir, RealDisk))
    }
}

impl<D: Disk> Persist<D> {
    /// The snapshot in the directory `dir`, on `disk`.
    pub fn on(dir: &Path, disk: D) -> Self {
        Self {
            dir: dir.to_owned(),
            disk,
            moved_aside: 0,
        }
    }

    /// The disk, for a test to look at.
    pub const fn disk(&self) -> &D {
        &self.disk
    }

    /// Gives the disk back.
    pub fn into_disk(self) -> D {
        self.disk
    }

    fn file(&self, suffix: &str) -> PathBuf {
        self.dir.join(format!("records.v{FORMAT}.snap{suffix}"))
    }

    /// Where the snapshot is.
    #[must_use]
    pub fn path(&self) -> PathBuf {
        self.file("")
    }

    /// Saves `records` as the snapshot, replacing the last one in one step.
    ///
    /// # Errors
    ///
    /// If the disk fails. The last snapshot is then still whole: a save that fails loses
    /// only itself.
    pub fn save(&mut self, records: impl Iterator<Item = Record>) -> io::Result<()> {
        let bytes = encode(records)?;
        let (current, previous, next) = (self.file(""), self.file(".prev"), self.file(".next"));
        // Beside the snapshot, and safe on disk before it takes the snapshot's name.
        self.disk.write(&next, &bytes)?;
        self.disk.sync(&next)?;
        // The snapshot being replaced is kept as the one before. If there is none yet, there
        // is nothing to keep.
        match self.disk.rename(&current, &previous) {
            Err(error) if error.kind() != io::ErrorKind::NotFound => return Err(error),
            _ => {}
        }
        self.disk.rename(&next, &current)?;
        self.disk.sync_dir(&self.dir)
    }

    /// Reads one file. `None` if it is not there.
    fn read(&mut self, path: &Path, problems: &mut Vec<String>) -> Option<Vec<Record>> {
        let name = path
            .file_name()
            .map(|name| name.to_string_lossy().into_owned())
            .unwrap_or_default();
        let bytes = match self.disk.read(path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return None,
            Err(error) => {
                problems.push(format!("{name} cannot be read: {error}"));
                return None;
            }
        };
        match decode(&bytes) {
            Ok(records) => Some(records),
            Err(why) => {
                // Kept, under another name, so the next save does not write over the
                // evidence and the next start does not trip on it again.
                self.moved_aside += 1;
                let aside = self
                    .dir
                    .join(format!("{name}.unreadable-{}", self.moved_aside));
                let moved = if self.disk.rename(path, &aside).is_ok() {
                    "moved aside"
                } else {
                    "left where it is"
                };
                problems.push(format!("{name} was not used, {why}; {moved}"));
                None
            }
        }
    }

    /// Reads what was kept. This never fails: a snapshot that cannot be read is moved aside
    /// and the one before it is used, and if that is no good either the answer is nothing.
    pub fn load(&mut self) -> Loaded {
        let mut problems = Vec::new();
        // A snapshot that was being written when the power went is not one.
        let _ = self.disk.remove(&self.file(".next"));
        let (current, previous) = (self.file(""), self.file(".prev"));
        let (records, source) = match self.read(&current, &mut problems) {
            Some(records) => (records, Source::Current),
            None => match self.read(&previous, &mut problems) {
                Some(records) => (records, Source::Previous),
                None => (Vec::new(), Source::Nothing),
            },
        };
        for problem in &problems {
            warn!(dir = %self.dir.display(), "{problem}");
        }
        Loaded {
            records,
            source,
            problems,
        }
    }
}
