//! `discovered-peers.json`: peers enrolled through discovery, restored at startup.

use crate::NodeError;
use crate::storage_io::{atomic_write_file, blocking_read_file};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

const FILE_NAME: &str = "discovered-peers.json";
const STORE_VERSION: u32 = 1;

/// How a peer was enrolled.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum EnrollmentMethod {
    /// `orionctl peers enroll <node-id>`.
    Operator,
    /// Shared enrollment key handshake.
    EnrollmentKey,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct EnrolledPeer {
    pub(crate) node_id: String,
    pub(crate) base_url: String,
    pub(crate) public_key_hex: String,
    pub(crate) method: EnrollmentMethod,
    pub(crate) enrolled_at_ms: u64,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct StoreFile {
    version: u32,
    peers: Vec<EnrolledPeer>,
}

pub(crate) fn store_path(state_dir: &Path) -> PathBuf {
    state_dir.join(FILE_NAME)
}

pub(crate) fn load(state_dir: &Path) -> Result<Vec<EnrolledPeer>, NodeError> {
    let path = store_path(state_dir);
    if !path.exists() {
        return Ok(Vec::new());
    }
    let bytes = blocking_read_file(&path, "read discovered peer store")?;
    let file: StoreFile = serde_json::from_slice(&bytes)
        .map_err(|err| NodeError::Storage(format!("failed to decode {}: {err}", path.display())))?;
    if file.version != STORE_VERSION {
        return Err(NodeError::Storage(format!(
            "{} has unsupported version {}",
            path.display(),
            file.version
        )));
    }
    Ok(file.peers)
}

pub(crate) fn save(state_dir: &Path, peers: &[EnrolledPeer]) -> Result<(), NodeError> {
    let file = StoreFile {
        version: STORE_VERSION,
        peers: peers.to_vec(),
    };
    let bytes = serde_json::to_vec_pretty(&file)
        .map_err(|err| NodeError::Storage(format!("failed to encode discovered peers: {err}")))?;
    atomic_write_file(
        &store_path(state_dir),
        &bytes,
        "create discovered peer store directory",
        "write discovered peer store",
        "install discovered peer store",
    )
}

/// Inserts or replaces the record for `peer.node_id`.
pub(crate) fn upsert(state_dir: &Path, peer: EnrolledPeer) -> Result<(), NodeError> {
    let mut peers = load(state_dir)?;
    peers.retain(|existing| existing.node_id != peer.node_id);
    peers.push(peer);
    peers.sort_by(|a, b| a.node_id.cmp(&b.node_id));
    save(state_dir, &peers)
}

/// Removes the record for `node_id`; returns whether there was one.
pub(crate) fn remove(state_dir: &Path, node_id: &str) -> Result<bool, NodeError> {
    let mut peers = load(state_dir)?;
    let before = peers.len();
    peers.retain(|existing| existing.node_id != node_id);
    if peers.len() == before {
        return Ok(false);
    }
    save(state_dir, &peers)?;
    Ok(true)
}
