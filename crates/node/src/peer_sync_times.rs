//! `peer-sync-times.json`: when each registered peer last completed a sync round with this node.
//! The stale-peer guard reads it so a restart cannot reset a peer's clock (`docs/peer-sync.md`,
//! "Stale peers").
//!
//! Records are merged, never replaced: a write only adds or updates entries for the peers that are
//! registered at that moment, and only `remove_peer` drops one. A peer that is not registered yet
//! at some write therefore keeps its record.

use crate::NodeError;
use crate::lock::lock_mutex;
use crate::storage_io::{atomic_write_file, blocking_read_file};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

const FILE_NAME: &str = "peer-sync-times.json";
/// Format version of `peer-sync-times.json`. A state directory without the file has no records:
/// every peer then starts its clock when it is registered (see `NodeApp::sync_clock_start_for`).
const STORE_VERSION: u32 = 1;
/// Serializes the read-merge-write cycles of concurrent sync rounds.
static UPDATE_LOCK: Mutex<()> = Mutex::new(());

#[derive(Debug, Serialize, Deserialize)]
struct StoreFile {
    version: u32,
    /// Node id to the time (ms since the Unix epoch) of its last completed sync round.
    last_successful_sync_ms: BTreeMap<String, u64>,
}

fn store_path(state_dir: &Path) -> PathBuf {
    state_dir.join(FILE_NAME)
}

/// Loads the records. A missing file has none.
pub(crate) fn load(state_dir: &Path) -> Result<BTreeMap<String, u64>, NodeError> {
    let path = store_path(state_dir);
    if !path.exists() {
        return Ok(BTreeMap::new());
    }
    let bytes = blocking_read_file(&path, "read peer sync time store")?;
    let file: StoreFile = serde_json::from_slice(&bytes)
        .map_err(|err| NodeError::Storage(format!("failed to decode {}: {err}", path.display())))?;
    if file.version != STORE_VERSION {
        return Err(NodeError::Storage(format!(
            "{} has unsupported version {}",
            path.display(),
            file.version
        )));
    }
    Ok(file.last_successful_sync_ms)
}

/// Applies `edit` to the stored records and writes the file only when they changed.
pub(crate) fn update(
    state_dir: &Path,
    edit: impl FnOnce(&mut BTreeMap<String, u64>),
) -> Result<(), NodeError> {
    let _guard = lock_mutex(UPDATE_LOCK.lock(), "peer sync time store");
    let before = load(state_dir)?;
    let mut records = before.clone();
    edit(&mut records);
    if records == before {
        return Ok(());
    }
    let file = StoreFile {
        version: STORE_VERSION,
        last_successful_sync_ms: records,
    };
    let bytes = serde_json::to_vec_pretty(&file)
        .map_err(|err| NodeError::Storage(format!("failed to encode peer sync times: {err}")))?;
    atomic_write_file(
        &store_path(state_dir),
        &bytes,
        "create peer sync time store directory",
        "write peer sync time store",
        "install peer sync time store",
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir(label: &str) -> PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or_default();
        std::env::temp_dir().join(format!(
            "orion-peer-sync-times-{label}-{}-{nanos}",
            std::process::id()
        ))
    }

    #[test]
    fn missing_file_has_no_records_and_updates_round_trip() {
        let dir = temp_dir("roundtrip");
        assert!(load(&dir).expect("absent file loads").is_empty());

        update(&dir, |records| {
            records.insert("node-b".into(), 42);
        })
        .expect("update writes");
        assert_eq!(load(&dir).expect("file loads").get("node-b"), Some(&42));

        update(&dir, |records| {
            records.remove("node-b");
        })
        .expect("remove writes");
        assert!(load(&dir).expect("file loads").is_empty());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn unknown_format_version_is_rejected() {
        let dir = temp_dir("version");
        std::fs::create_dir_all(&dir).expect("temp dir");
        std::fs::write(
            store_path(&dir),
            br#"{"version":99,"last_successful_sync_ms":{}}"#,
        )
        .expect("write fixture");
        assert!(load(&dir).is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
