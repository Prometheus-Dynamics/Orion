//! The node's blob store: large files pushed by local clients in resumable, digest-checked chunks
//! (`docs/blobs.md`).
//!
//! Memory is bounded: a chunk is at most `chunk_max_bytes` and goes straight to the file, the
//! entry count is capped at [`MAX_BLOBS`], and the bytes of all blobs together never exceed
//! `max_bytes` (partial and committed alike). Committing hashes the file on one background thread,
//! in fixed-size reads, so a large commit never blocks the node.

use crate::config::BlobTuning;
use orion::control_plane::{BlobState, BlobStatus};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::fs::{self, File, OpenOptions};
use std::io::Read;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, mpsc};

/// Most blobs held at once, partial and committed together.
pub(crate) const MAX_BLOBS: usize = 64;
/// Read size of the digest check (allocated once per verification).
const VERIFY_READ_BYTES: usize = 1024 * 1024;
const PART_SUFFIX: &str = ".part";
const DIGEST_SUFFIX: &str = ".sha256";
const MAX_ID_BYTES: usize = 128;

#[derive(Debug)]
enum EntryState {
    Receiving,
    Verifying,
    Committed,
    Rejected(String),
}

#[derive(Debug)]
struct Entry {
    size: u64,
    sha256: String,
    received: u64,
    state: EntryState,
    /// Bytes charged to the store budget for this entry.
    reserved: u64,
    /// The partial file, while receiving.
    file: Option<Arc<File>>,
    /// Found on disk at startup: its declared size and digest arrive with `BeginBlob`.
    adopted: bool,
}

#[derive(Debug, Default)]
struct Inner {
    entries: BTreeMap<String, Entry>,
    reserved_total: u64,
}

/// The store. Cheap to clone; every clone shares the same entries and verifier.
#[derive(Clone)]
pub(crate) struct BlobStore {
    inner: Arc<Mutex<Inner>>,
    dir: Arc<PathBuf>,
    max_bytes: u64,
    chunk_max_bytes: usize,
    verify_tx: mpsc::SyncSender<String>,
}

impl BlobStore {
    /// Opens (creating) `dir`, recovers the partial and committed blobs it holds, and starts the
    /// verifier thread.
    pub(crate) fn open(dir: &Path, tuning: &BlobTuning) -> Result<Self, String> {
        fs::create_dir_all(dir)
            .map_err(|err| format!("cannot create blob dir {}: {err}", dir.display()))?;
        let inner = Arc::new(Mutex::new(scan(dir)?));
        let (verify_tx, verify_rx) = mpsc::sync_channel::<String>(MAX_BLOBS);
        let worker_inner = Arc::clone(&inner);
        let worker_dir = dir.to_path_buf();
        std::thread::Builder::new()
            .name("orion-blob-verify".into())
            .spawn(move || {
                // Ends when every store clone is dropped and the channel closes.
                for id in verify_rx {
                    verify(&worker_dir, &worker_inner, &id);
                }
            })
            .map_err(|err| format!("cannot start the blob verifier: {err}"))?;
        Ok(Self {
            inner,
            dir: Arc::new(dir.to_path_buf()),
            max_bytes: tuning.max_bytes,
            chunk_max_bytes: tuning.chunk_max_bytes,
            verify_tx,
        })
    }

    /// The store for a node's configuration: `ORION_NODE_BLOB_DIR`, else `<state dir>/blobs`;
    /// `None` when neither is set.
    pub(crate) fn open_configured(
        tuning: &BlobTuning,
        state_dir: Option<&Path>,
    ) -> Result<Option<Self>, String> {
        let dir = match (&tuning.dir, state_dir) {
            (Some(dir), _) => dir.clone(),
            (None, Some(state)) => state.join("blobs"),
            (None, None) => return Ok(None),
        };
        Self::open(&dir, tuning).map(Some)
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// `BeginBlob`: starts a blob, or resumes one receiving with the same size and digest.
    pub(crate) fn begin(&self, id: &str, size: u64, sha256: &str) -> Result<BlobStatus, String> {
        validate_id(id)?;
        let sha256 = normalize_sha256(sha256)?;
        if size == 0 {
            return Err("a blob has at least one byte".into());
        }
        if size > self.max_bytes {
            return Err(format!(
                "blob of {size} bytes is over ORION_NODE_BLOB_MAX_BYTES ({})",
                self.max_bytes
            ));
        }
        let mut inner = self.lock();
        let adopted_reserved = match inner.entries.get(id) {
            Some(entry) => match (&entry.state, entry.adopted) {
                // An adopted partial takes the declared size; its budget moves from what is on disk.
                (EntryState::Receiving, true) => {
                    if entry.received > size {
                        return Err(format!(
                            "blob `{id}` already holds {} bytes, more than {size}",
                            entry.received
                        ));
                    }
                    Some(entry.reserved)
                }
                (EntryState::Receiving, false) if entry.size == size && entry.sha256 == sha256 => {
                    return Ok(self.status(id, entry));
                }
                (EntryState::Committed, _) if entry.size == size && entry.sha256 == sha256 => {
                    return Ok(self.status(id, entry));
                }
                _ => {
                    return Err(format!(
                        "blob `{id}` exists with another size or digest, or is {}; delete it first",
                        state_name(&entry.state)
                    ));
                }
            },
            None => None,
        };
        if let Some(old) = adopted_reserved {
            let total = inner.reserved_total - old + size;
            if total > self.max_bytes {
                return Err(self.full_message(total));
            }
            inner.reserved_total = total;
            let entry = inner.entries.get_mut(id).expect("entry checked above");
            entry.size = size;
            entry.sha256 = sha256;
            entry.reserved = size;
            entry.adopted = false;
            return Ok(self.status(id, &inner.entries[id]));
        }
        if inner.entries.len() >= MAX_BLOBS {
            return Err(format!(
                "blob store holds {MAX_BLOBS} blobs; delete one first"
            ));
        }
        let total = inner.reserved_total + size;
        if total > self.max_bytes {
            return Err(self.full_message(total));
        }
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(self.part_path(id))
            .map_err(|err| format!("cannot create blob `{id}`: {err}"))?;
        inner.reserved_total = total;
        let entry = Entry {
            size,
            sha256,
            received: 0,
            state: EntryState::Receiving,
            reserved: size,
            file: Some(Arc::new(file)),
            adopted: false,
        };
        inner.entries.insert(id.to_owned(), entry);
        Ok(self.status(id, &inner.entries[id]))
    }

    /// `WriteBlobChunk`: writes `data` at `offset` when it is the blob's `received` offset. Any
    /// other offset writes nothing and answers with the status, which carries the resume point.
    pub(crate) fn write(&self, id: &str, offset: u64, data: &[u8]) -> Result<BlobStatus, String> {
        if data.is_empty() || data.len() > self.chunk_max_bytes {
            return Err(format!(
                "a chunk carries 1..={} bytes",
                self.chunk_max_bytes
            ));
        }
        let mut inner = self.lock();
        let entry = inner
            .entries
            .get_mut(id)
            .ok_or_else(|| format!("unknown blob `{id}`"))?;
        match (&entry.state, entry.adopted) {
            (EntryState::Receiving, false) => {}
            (EntryState::Receiving, true) => {
                return Err(format!("blob `{id}` needs BeginBlob before its chunks"));
            }
            (state, _) => return Err(format!("blob `{id}` is {}", state_name(state))),
        }
        if offset == entry.received {
            let end = offset
                .checked_add(data.len() as u64)
                .filter(|end| *end <= entry.size)
                .ok_or_else(|| format!("chunk runs past the {} bytes of `{id}`", entry.size))?;
            let file = entry
                .file
                .as_ref()
                .ok_or_else(|| format!("blob `{id}` has no open file"))?;
            file.write_all_at(data, offset)
                .map_err(|err| format!("cannot write blob `{id}`: {err}"))?;
            entry.received = end;
        }
        Ok(self.status(id, &inner.entries[id]))
    }

    /// `CommitBlob`: starts the digest check of a fully received blob. Answers `Verifying`, or
    /// the final state if the check already ran.
    pub(crate) fn commit(&self, id: &str) -> Result<BlobStatus, String> {
        let mut inner = self.lock();
        let entry = inner
            .entries
            .get_mut(id)
            .ok_or_else(|| format!("unknown blob `{id}`"))?;
        match (&entry.state, entry.adopted) {
            (EntryState::Receiving, true) => {
                return Err(format!("blob `{id}` needs BeginBlob before commit"));
            }
            (EntryState::Receiving, false) => {}
            (EntryState::Verifying | EntryState::Committed | EntryState::Rejected(_), _) => {
                return Ok(self.status(id, &inner.entries[id]));
            }
        }
        if entry.received != entry.size {
            return Err(format!(
                "blob `{id}` has {} of {} bytes",
                entry.received, entry.size
            ));
        }
        entry.state = EntryState::Verifying;
        entry.file = None;
        if self.verify_tx.try_send(id.to_owned()).is_err() {
            let entry = inner.entries.get_mut(id).expect("entry checked above");
            entry.state = EntryState::Receiving;
            return Err("the blob verifier is busy; commit again".into());
        }
        Ok(self.status(id, &inner.entries[id]))
    }

    /// `QueryBlobs`: every blob, or the one named.
    pub(crate) fn query(&self, id: Option<&str>) -> Result<Vec<BlobStatus>, String> {
        let inner = self.lock();
        match id {
            Some(id) => {
                let entry = inner
                    .entries
                    .get(id)
                    .ok_or_else(|| format!("unknown blob `{id}`"))?;
                Ok(vec![self.status(id, entry)])
            }
            None => Ok(inner
                .entries
                .iter()
                .map(|(id, entry)| self.status(id, entry))
                .collect()),
        }
    }

    /// `DeleteBlob`: drops the bytes and the record. A blob being verified cannot be deleted.
    pub(crate) fn delete(&self, id: &str) -> Result<(), String> {
        let mut inner = self.lock();
        let entry = inner
            .entries
            .get(id)
            .ok_or_else(|| format!("unknown blob `{id}`"))?;
        if matches!(entry.state, EntryState::Verifying) {
            return Err(format!("blob `{id}` is being verified"));
        }
        let entry = inner.entries.remove(id).expect("entry checked above");
        inner.reserved_total -= entry.reserved;
        remove_files(&self.dir, id);
        Ok(())
    }

    fn status(&self, id: &str, entry: &Entry) -> BlobStatus {
        let (state, path) = match &entry.state {
            EntryState::Receiving => (BlobState::Receiving, None),
            EntryState::Verifying => (BlobState::Verifying, None),
            EntryState::Committed => (
                BlobState::Committed,
                Some(self.final_path(id).display().to_string()),
            ),
            EntryState::Rejected(reason) => (
                BlobState::Rejected {
                    reason: reason.clone(),
                },
                None,
            ),
        };
        BlobStatus {
            blob_id: id.to_owned(),
            size: entry.size,
            received: entry.received,
            sha256: entry.sha256.clone(),
            state,
            path,
        }
    }

    fn full_message(&self, needed: u64) -> String {
        format!(
            "blob store is full: {needed} bytes would exceed ORION_NODE_BLOB_MAX_BYTES ({})",
            self.max_bytes
        )
    }

    fn part_path(&self, id: &str) -> PathBuf {
        self.dir.join(format!("{id}{PART_SUFFIX}"))
    }

    fn final_path(&self, id: &str) -> PathBuf {
        self.dir.join(id)
    }
}

fn state_name(state: &EntryState) -> &'static str {
    match state {
        EntryState::Receiving => "receiving",
        EntryState::Verifying => "verifying",
        EntryState::Committed => "committed",
        EntryState::Rejected(_) => "rejected",
    }
}

/// Runs on the verifier thread: hashes the partial file, then moves it into place or drops it.
fn verify(dir: &Path, inner: &Mutex<Inner>, id: &str) {
    let (size, expected) = {
        let guard = inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match guard.entries.get(id) {
            Some(entry) => (entry.size, entry.sha256.clone()),
            None => return,
        }
    };
    let part = dir.join(format!("{id}{PART_SUFFIX}"));
    let failure = match digest_file(&part, size) {
        // Durable before it is named: sync the bytes, then rename and record the digest.
        Ok(actual) if actual == expected => File::open(&part)
            .and_then(|file| file.sync_all())
            .map_err(|err| format!("cannot sync blob: {err}"))
            .and_then(|()| place(dir, id, &expected))
            .err(),
        Ok(actual) => Some(format!("sha256 mismatch: file is {actual}")),
        Err(err) => Some(err),
    };
    let mut guard = inner
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let Inner {
        entries,
        reserved_total,
    } = &mut *guard;
    let Some(entry) = entries.get_mut(id) else {
        return;
    };
    match failure {
        None => entry.state = EntryState::Committed,
        Some(reason) => {
            let _ = fs::remove_file(&part);
            *reserved_total -= entry.reserved;
            entry.reserved = 0;
            entry.received = 0;
            entry.state = EntryState::Rejected(reason);
        }
    }
}

/// Moves the verified partial file to its final name and records its digest beside it.
fn place(dir: &Path, id: &str, sha256: &str) -> Result<(), String> {
    fs::rename(dir.join(format!("{id}{PART_SUFFIX}")), dir.join(id))
        .map_err(|err| format!("cannot place blob: {err}"))?;
    let digest_path = dir.join(format!("{id}{DIGEST_SUFFIX}"));
    let tmp = dir.join(format!("{id}{DIGEST_SUFFIX}.tmp"));
    fs::write(&tmp, sha256)
        .and_then(|()| fs::rename(&tmp, &digest_path))
        .map_err(|err| format!("cannot record blob digest: {err}"))
}

/// SHA-256 of the first `size` bytes of `path`, read in `VERIFY_READ_BYTES` pieces.
fn digest_file(path: &Path, size: u64) -> Result<String, String> {
    let mut file = File::open(path).map_err(|err| format!("cannot read blob: {err}"))?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0u8; VERIFY_READ_BYTES];
    let mut total = 0u64;
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|err| format!("cannot read blob: {err}"))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
        total += read as u64;
    }
    if total != size {
        return Err(format!("blob has {total} bytes, expected {size}"));
    }
    Ok(to_hex(&hasher.finalize()))
}

fn remove_files(dir: &Path, id: &str) {
    for name in [
        format!("{id}{PART_SUFFIX}"),
        id.to_owned(),
        format!("{id}{DIGEST_SUFFIX}"),
    ] {
        let _ = fs::remove_file(dir.join(name));
    }
}

/// Recovers the store from `dir`: partial files become adoptable entries (their declared size
/// comes with `BeginBlob`), and committed files with a digest beside them become committed.
fn scan(dir: &Path) -> Result<Inner, String> {
    let mut inner = Inner::default();
    let listing = fs::read_dir(dir)
        .map_err(|err| format!("cannot read blob dir {}: {err}", dir.display()))?;
    for item in listing {
        let item = item.map_err(|err| format!("cannot read blob dir: {err}"))?;
        let Ok(name) = item.file_name().into_string() else {
            continue;
        };
        if inner.entries.len() >= MAX_BLOBS {
            break;
        }
        if let Some(id) = name.strip_suffix(PART_SUFFIX) {
            if validate_id(id).is_err() || inner.entries.contains_key(id) {
                continue;
            }
            let Ok(file) = OpenOptions::new().read(true).write(true).open(item.path()) else {
                continue;
            };
            let len = file.metadata().map(|meta| meta.len()).unwrap_or(0);
            inner.reserved_total += len;
            inner.entries.insert(
                id.to_owned(),
                Entry {
                    size: 0,
                    sha256: String::new(),
                    received: len,
                    state: EntryState::Receiving,
                    reserved: len,
                    file: Some(Arc::new(file)),
                    adopted: true,
                },
            );
        } else if validate_id(&name).is_ok() {
            let Ok(sha256) = fs::read_to_string(dir.join(format!("{name}{DIGEST_SUFFIX}"))) else {
                continue;
            };
            let Ok(sha256) = normalize_sha256(sha256.trim()) else {
                continue;
            };
            let size = item.metadata().map(|meta| meta.len()).unwrap_or(0);
            inner.reserved_total += size;
            inner.entries.insert(
                name,
                Entry {
                    size,
                    sha256,
                    received: size,
                    state: EntryState::Committed,
                    reserved: size,
                    file: None,
                    adopted: false,
                },
            );
        }
    }
    Ok(inner)
}

/// A blob id is a file-name-safe token: 1 to 128 bytes of `[A-Za-z0-9._-]`, starting with a letter
/// or digit, and not ending in the suffixes the store uses for its own files.
pub(crate) fn validate_id(id: &str) -> Result<(), String> {
    let valid = !id.is_empty()
        && id.len() <= MAX_ID_BYTES
        && id.as_bytes()[0].is_ascii_alphanumeric()
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
        && !id.ends_with(PART_SUFFIX)
        && !id.ends_with(DIGEST_SUFFIX);
    if valid {
        Ok(())
    } else {
        Err(format!(
            "invalid blob id `{id}`: 1 to {MAX_ID_BYTES} characters of letters, digits, `.`, `_` \
             or `-`, starting with a letter or digit"
        ))
    }
}

/// A SHA-256 digest as 64 lowercase hex digits.
pub(crate) fn normalize_sha256(sha256: &str) -> Result<String, String> {
    if sha256.len() == 64 && sha256.bytes().all(|b| b.is_ascii_hexdigit()) {
        Ok(sha256.to_ascii_lowercase())
    } else {
        Err("sha256 must be 64 hex digits".into())
    }
}

fn to_hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(out, "{byte:02x}");
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tuning(max_bytes: u64, chunk_max_bytes: usize) -> BlobTuning {
        BlobTuning {
            dir: None,
            max_bytes,
            chunk_max_bytes,
        }
    }

    fn temp_dir(name: &str) -> PathBuf {
        let dir =
            std::env::temp_dir().join(format!("orion-blob-test-{name}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        dir
    }

    fn digest_of(data: &[u8]) -> String {
        let mut hasher = Sha256::new();
        hasher.update(data);
        to_hex(&hasher.finalize())
    }

    /// Waits for the verifier to leave `Verifying`.
    fn wait_settled(store: &BlobStore, id: &str) -> BlobStatus {
        for _ in 0..500 {
            let status = store.query(Some(id)).expect("blob is known").pop().unwrap();
            if status.state != BlobState::Verifying {
                return status;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        panic!("blob {id} stayed verifying");
    }

    #[test]
    fn chunks_resume_by_offset_and_commit_after_verification() {
        let dir = temp_dir("resume");
        let store = BlobStore::open(&dir, &tuning(1 << 20, 4096)).expect("store opens");
        let data: Vec<u8> = (0..10_000u32).map(|i| (i % 251) as u8).collect();
        let sha = digest_of(&data);
        let begun = store
            .begin("image-1", data.len() as u64, &sha)
            .expect("begin");
        assert_eq!(begun.state, BlobState::Receiving);
        store
            .write("image-1", 0, &data[..4096])
            .expect("first chunk");

        // A chunk at the wrong offset writes nothing and says where to resume.
        let status = store
            .write("image-1", 9000, &data[9000..])
            .expect("answered");
        assert_eq!(status.received, 4096);

        // The client reconnects and asks again: same size and digest resume the blob.
        let resumed = store
            .begin("image-1", data.len() as u64, &sha.to_uppercase())
            .expect("resume");
        assert_eq!(resumed.received, 4096);
        for (i, piece) in data[4096..].chunks(4096).enumerate() {
            store
                .write("image-1", 4096 + (i * 4096) as u64, piece)
                .expect("rest");
        }
        assert_eq!(
            store.commit("image-1").expect("commit").state,
            BlobState::Verifying
        );

        let committed = wait_settled(&store, "image-1");
        assert_eq!(committed.state, BlobState::Committed);
        assert_eq!(committed.sha256, sha);
        let path = committed.path.expect("committed blobs have a path");
        assert_eq!(fs::read(&path).expect("file in place"), data);
        assert!(!dir.join("image-1.part").exists());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_digest_mismatch_is_rejected_and_its_bytes_are_released() {
        let dir = temp_dir("mismatch");
        let store = BlobStore::open(&dir, &tuning(100, 64)).expect("store opens");
        let data = vec![7u8; 100];
        store
            .begin("bad", 100, &digest_of(&[1u8; 100]))
            .expect("the whole budget fits one blob");
        store.write("bad", 0, &data[..64]).expect("chunk");
        store.write("bad", 64, &data[64..]).expect("chunk");
        store.commit("bad").expect("commit");
        let rejected = wait_settled(&store, "bad");
        assert!(
            matches!(&rejected.state, BlobState::Rejected { reason } if reason.contains("mismatch"))
        );
        assert!(!dir.join("bad.part").exists());
        // The bytes went back to the budget.
        store
            .begin("next", 100, &digest_of(&data))
            .expect("the rejected bytes no longer count");
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn budget_ids_sizes_and_chunks_are_bounded() {
        let dir = temp_dir("bounds");
        let store = BlobStore::open(&dir, &tuning(1000, 512)).expect("store opens");
        let sha = digest_of(b"x");
        assert!(
            store.begin("../escape", 1, &sha).is_err(),
            "path-like ids are refused"
        );
        assert!(
            store.begin("name.part", 1, &sha).is_err(),
            "store suffixes are refused"
        );
        assert!(store.begin("empty", 0, &sha).is_err());
        assert!(store.begin("big", 1001, &sha).is_err(), "over the byte cap");
        assert!(store.begin("bad-digest", 1, "xyz").is_err());
        store.begin("a", 600, &sha).expect("600 fits");
        let refused = store
            .begin("b", 500, &sha)
            .expect_err("600 + 500 is over 1000");
        assert!(refused.contains("full"));
        store.begin("a", 600, &sha).expect("the same begin resumes");
        store
            .write("a", 0, &[0u8; 513])
            .expect_err("chunks are capped");
        store.delete("a").expect("delete frees the budget");
        store.begin("b", 500, &sha).expect("now it fits");
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn partials_survive_a_restart_and_resume_with_their_declared_size() {
        let dir = temp_dir("restart");
        let data = vec![9u8; 3000];
        let sha = digest_of(&data);
        {
            let store = BlobStore::open(&dir, &tuning(1 << 20, 1024)).expect("opens");
            store.begin("resumable", 3000, &sha).expect("begin");
            store
                .write("resumable", 0, &data[..1000])
                .expect("first part");
        }
        let store = BlobStore::open(&dir, &tuning(1 << 20, 1024)).expect("reopens");
        let adopted = store
            .query(Some("resumable"))
            .expect("found on disk")
            .pop()
            .unwrap();
        assert_eq!(adopted.received, 1000);
        assert!(
            store.write("resumable", 1000, &data[1000..]).is_err(),
            "begin again first"
        );
        let resumed = store
            .begin("resumable", 3000, &sha)
            .expect("resume after restart");
        assert_eq!(resumed.received, 1000);
        for (i, piece) in data[1000..].chunks(1024).enumerate() {
            store
                .write("resumable", 1000 + (i * 1024) as u64, piece)
                .expect("rest");
        }
        store.commit("resumable").expect("commit");
        assert_eq!(
            wait_settled(&store, "resumable").state,
            BlobState::Committed
        );
        drop(store);

        // A committed blob is still there after a restart, with its digest.
        let store = BlobStore::open(&dir, &tuning(1 << 20, 1024)).expect("reopens again");
        let kept = store.query(Some("resumable")).expect("kept").pop().unwrap();
        assert_eq!(kept.state, BlobState::Committed);
        assert_eq!(kept.sha256, sha);
        let _ = fs::remove_dir_all(&dir);
    }
}
