//! Blob push: a local client streams a large file (an OS image) to the node in resumable chunks.
//!
//! A blob is named by the client (`blob_id`, a file-name-safe token), declares its `size` and
//! `sha256` (64 hex digits of the whole file) when it begins, and then sends chunks at explicit
//! offsets. The node answers every chunk with the blob's status, so a client that lost its
//! connection asks the node where to resume (`received`) and continues from there. `CommitBlob`
//! verifies the digest on the node in the background; the client follows the state with
//! `QueryBlobs`. See `docs/blobs.md`.

use alloc::{string::String, vec::Vec};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};

/// Starts a blob, or resumes one that is already receiving with the same `size` and `sha256`.
#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct BlobBegin {
    pub blob_id: String,
    /// Total bytes of the file.
    pub size: u64,
    /// Lowercase or uppercase hex SHA-256 of the whole file.
    pub sha256: String,
}

/// One chunk of a blob, written at `offset`. The offset must equal the blob's `received` count;
/// any other offset is not written and the status says where to resume.
#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct BlobChunk {
    pub blob_id: String,
    pub offset: u64,
    pub data: Vec<u8>,
}

/// Where a blob is in its life.
#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum BlobState {
    /// Chunks are still arriving (`received` < `size`).
    Receiving,
    /// Every byte arrived; the node is checking the SHA-256 in the background.
    Verifying,
    /// Verified and in place at `path`; it stays until `DeleteBlob`.
    Committed,
    /// The digest did not match, or the blob could not be stored. Its bytes were dropped.
    Rejected { reason: String },
}

impl BlobState {
    /// `receiving`, `verifying`, `committed`, or `rejected`.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Receiving => "receiving",
            Self::Verifying => "verifying",
            Self::Committed => "committed",
            Self::Rejected { .. } => "rejected",
        }
    }
}

/// A blob's status, as the node holds it.
#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct BlobStatus {
    pub blob_id: String,
    pub size: u64,
    /// Bytes stored so far: the offset to resume from.
    pub received: u64,
    pub sha256: String,
    pub state: BlobState,
    /// Absolute path of the committed file (set once `Committed`).
    pub path: Option<String>,
}
