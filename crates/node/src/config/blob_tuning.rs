//! Blob push limits (`docs/blobs.md`, `docs/node-env.md`).

use super::runtime_tuning::parse_env_or;
use crate::NodeError;
use std::{env, path::PathBuf};

const DEFAULT_BLOB_MAX_BYTES: u64 = 2 * 1024 * 1024 * 1024;
const DEFAULT_BLOB_CHUNK_MAX_BYTES: usize = 1024 * 1024;
/// Largest chunk a client may send: well under the 8 MiB control frame.
pub(crate) const BLOB_CHUNK_CEILING_BYTES: usize = 4 * 1024 * 1024;

/// Where blobs live, how many bytes they may take together, and the largest chunk.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlobTuning {
    /// `ORION_NODE_BLOB_DIR`; `None` uses `<state dir>/blobs`, and blobs are off without a state
    /// dir.
    pub dir: Option<PathBuf>,
    /// Bytes all blobs may hold together, partial and committed (`ORION_NODE_BLOB_MAX_BYTES`).
    pub max_bytes: u64,
    /// Largest chunk one `WriteBlobChunk` may carry (`ORION_NODE_BLOB_CHUNK_MAX_BYTES`, at most
    /// 4 MiB).
    pub chunk_max_bytes: usize,
}

impl Default for BlobTuning {
    fn default() -> Self {
        Self {
            dir: None,
            max_bytes: DEFAULT_BLOB_MAX_BYTES,
            chunk_max_bytes: DEFAULT_BLOB_CHUNK_MAX_BYTES,
        }
    }
}

fn blob_dir_from_env() -> Result<Option<PathBuf>, NodeError> {
    match env::var("ORION_NODE_BLOB_DIR") {
        Ok(value) if value.trim().is_empty() => Ok(None),
        Ok(value) => Ok(Some(PathBuf::from(value))),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(env::VarError::NotUnicode(_)) => Err(NodeError::Config(
            "ORION_NODE_BLOB_DIR must be valid unicode".into(),
        )),
    }
}

impl BlobTuning {
    pub(crate) fn from_env() -> Result<Self, NodeError> {
        let mut tuning = Self {
            dir: blob_dir_from_env()?,
            max_bytes: parse_env_or("ORION_NODE_BLOB_MAX_BYTES", DEFAULT_BLOB_MAX_BYTES)?,
            chunk_max_bytes: parse_env_or(
                "ORION_NODE_BLOB_CHUNK_MAX_BYTES",
                DEFAULT_BLOB_CHUNK_MAX_BYTES,
            )?,
        };
        tuning.normalize();
        Ok(tuning)
    }

    pub(crate) fn normalize(&mut self) {
        self.max_bytes = self.max_bytes.max(1);
        self.chunk_max_bytes = self.chunk_max_bytes.clamp(1, BLOB_CHUNK_CEILING_BYTES);
    }
}
