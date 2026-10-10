//! The optional persistence file: JSON with every topic that has the `persistent` property.

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

/// Writes the file atomically (temporary file, then rename).
pub(super) fn save(path: &Path, entries: &[Entry]) -> Result<()> {
    let mut tmp = path.as_os_str().to_owned();
    tmp.push(".tmp");
    let bytes = serde_json::to_vec_pretty(&FileRef { topics: entries })?;
    std::fs::write(&tmp, bytes)?;
    std::fs::rename(&tmp, path)?;
    Ok(())
}
