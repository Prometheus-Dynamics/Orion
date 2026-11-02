//! Blob push over local IPC: the handlers for `BeginBlob`, `WriteBlobChunk`, `CommitBlob`,
//! `QueryBlobs` and `DeleteBlob` (`docs/blobs.md`). The store is in `blob_store`.

use super::{NodeApp, NodeError};
use orion::control_plane::ControlMessage;

impl NodeApp {
    /// Serves one blob message. Roles and admission are checked by the authorizer first.
    pub(crate) fn apply_local_blob_message(
        &self,
        message: ControlMessage,
    ) -> Result<ControlMessage, NodeError> {
        let store = self.state.blobs.as_ref().ok_or_else(|| {
            NodeError::Blob(
                "blob push is off: set ORION_NODE_BLOB_DIR or ORION_NODE_STATE_DIR".into(),
            )
        })?;
        let result = match message {
            ControlMessage::BeginBlob(begin) => store
                .begin(&begin.blob_id, begin.size, &begin.sha256)
                .map(ControlMessage::Blob),
            ControlMessage::WriteBlobChunk(chunk) => store
                .write(&chunk.blob_id, chunk.offset, &chunk.data)
                .map(ControlMessage::Blob),
            ControlMessage::CommitBlob(id) => store.commit(&id).map(ControlMessage::Blob),
            ControlMessage::QueryBlobs(id) => store.query(id.as_deref()).map(ControlMessage::Blobs),
            ControlMessage::DeleteBlob(id) => store.delete(&id).map(|()| ControlMessage::Accepted),
            other => Ok(ControlMessage::Rejected(format!(
                "not a blob message: {other:?}"
            ))),
        };
        result.map_err(NodeError::Blob)
    }
}
