//! The optional persistence file: JSON with every topic that has the `persistent` property.

use std::io::Write;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::error::Result;
use crate::message::Properties;
use crate::value::Value;

/// One persisted topic.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct Entry {
    pub name: String,
    #[serde(rename = "type")]
    pub type_name: String,
    #[serde(default)]
    pub properties: Properties,
    #[serde(default)]
    pub value: Option<Value>,
}

#[derive(Deserialize)]
struct FileOwned {
    #[serde(default)]
    topics: Vec<Entry>,
}

#[derive(Serialize)]
struct FileRef<'a> {
    topics: &'a [Entry],
}

/// Reads the file. A missing file is an empty table.
pub(super) fn load(path: &Path) -> Result<Vec<Entry>> {
    match std::fs::read(path) {
        Ok(bytes) => Ok(serde_json::from_slice::<FileOwned>(&bytes)?.topics),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(e) => Err(e.into()),
    }
}

/// The file contents: compact JSON.
pub(super) fn encode(entries: &[Entry]) -> Result<Vec<u8>> {
    Ok(serde_json::to_vec(&FileRef { topics: entries })?)
}

/// Writes the file atomically: a temporary file is synced to disk, renamed over the target, and
/// the directory is synced so the rename itself survives a crash.
pub(super) fn write_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut tmp = path.as_os_str().to_owned();
    tmp.push(".tmp");
    let tmp = std::path::PathBuf::from(tmp);
    {
        let mut file = std::fs::File::create(&tmp)?;
        file.write_all(bytes)?;
        file.sync_all()?;
    }
    std::fs::rename(&tmp, path)?;
    if let Some(dir) = path.parent().filter(|d| !d.as_os_str().is_empty())
        && let Ok(dir) = std::fs::File::open(dir)
    {
        // Not every platform can sync a directory; the file itself is already synced.
        let _ = dir.sync_all();
    }
    Ok(())
}
