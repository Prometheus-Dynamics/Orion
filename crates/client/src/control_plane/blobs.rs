//! Blob push: streams a local file to the node in resumable chunks (`docs/blobs.md`).

use std::io::{Read, Seek, SeekFrom};
use std::path::Path;
use std::time::Duration;

use super::LocalControlPlaneClient;
use crate::error::ClientError;
use orion_control_plane::{BlobBegin, BlobChunk, BlobState, BlobStatus, ControlMessage};

/// How often the digest check is polled after `CommitBlob`.
const VERIFY_POLL: Duration = Duration::from_millis(50);

fn io_error(context: &str, err: std::io::Error) -> ClientError {
    ClientError::Rejected(format!("{context}: {err}"))
}

fn blob_status(message: ControlMessage) -> Result<BlobStatus, ClientError> {
    match message {
        ControlMessage::Blob(status) => Ok(status),
        ControlMessage::Rejected(reason) => Err(ClientError::Rejected(reason)),
        _ => Err(ClientError::NoMessageAvailable),
    }
}

impl LocalControlPlaneClient {
    /// Pushes the file at `path` as blob `blob_id`, whose SHA-256 (hex) the caller knows.
    ///
    /// Begins the blob (or resumes one the node already holds with this size and digest), sends
    /// `chunk_bytes` pieces from the offset the node reports, then commits and waits for the
    /// node's digest check. The returned status is `Committed`, or `Rejected` on a digest
    /// mismatch. Only one chunk is in memory at a time: the file is read, not loaded.
    pub async fn push_blob(
        &self,
        blob_id: &str,
        path: &Path,
        sha256: &str,
        chunk_bytes: usize,
    ) -> Result<BlobStatus, ClientError> {
        let size = std::fs::metadata(path)
            .map_err(|err| io_error("cannot stat the file", err))?
            .len();
        let mut status = blob_status(
            self.send_request(ControlMessage::BeginBlob(BlobBegin {
                blob_id: blob_id.to_owned(),
                size,
                sha256: sha256.to_owned(),
            }))
            .await?,
        )?;
        let mut file =
            std::fs::File::open(path).map_err(|err| io_error("cannot open the file", err))?;
        let mut buffer = vec![0u8; chunk_bytes.max(1)];
        while status.received < size {
            let offset = status.received;
            let want =
                usize::try_from((size - offset).min(buffer.len() as u64)).unwrap_or(buffer.len());
            file.seek(SeekFrom::Start(offset))
                .map_err(|err| io_error("cannot seek the file", err))?;
            file.read_exact(&mut buffer[..want])
                .map_err(|err| io_error("cannot read the file", err))?;
            status = blob_status(
                self.send_request(ControlMessage::WriteBlobChunk(BlobChunk {
                    blob_id: blob_id.to_owned(),
                    offset,
                    data: buffer[..want].to_vec(),
                }))
                .await?,
            )?;
        }
        status = blob_status(
            self.send_request(ControlMessage::CommitBlob(blob_id.to_owned()))
                .await?,
        )?;
        while status.state == BlobState::Verifying {
            tokio::time::sleep(VERIFY_POLL).await;
            let found = self
                .send_request(ControlMessage::QueryBlobs(Some(blob_id.to_owned())))
                .await?;
            status = match found {
                ControlMessage::Blobs(mut list) if !list.is_empty() => list.remove(0),
                ControlMessage::Rejected(reason) => return Err(ClientError::Rejected(reason)),
                _ => return Err(ClientError::NoMessageAvailable),
            };
        }
        Ok(status)
    }
}
