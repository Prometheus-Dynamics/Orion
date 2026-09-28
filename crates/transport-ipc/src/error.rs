use orion_core::ControlProtocolMismatch;
use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum IpcTransportError {
    #[error("failed to bind unix socket: {0}")]
    BindFailed(String),
    #[error("failed to connect to unix socket: {0}")]
    ConnectFailed(String),
    #[error("failed to connect to unix socket: {0}")]
    ConnectionRefused(String),
    #[error("failed to accept unix socket connection: {0}")]
    AcceptFailed(String),
    #[error("failed to read unix socket message: {0}")]
    ReadFailed(String),
    #[error("failed to write unix socket message: {0}")]
    WriteFailed(String),
    #[error("failed to encode unix socket message: {0}")]
    EncodeFailed(String),
    #[error("failed to decode unix socket message: {0}")]
    DecodeFailed(String),
    /// The peer speaks a different control-protocol wire version (see
    /// [`orion_core::CONTROL_PROTOCOL_VERSION`]). Detected from the fixed preamble before any
    /// archived payload is decoded.
    #[error("{}", ControlProtocolMismatch::new(*local, *remote))]
    ProtocolMismatch { local: u16, remote: u16 },
}

impl From<ControlProtocolMismatch> for IpcTransportError {
    fn from(mismatch: ControlProtocolMismatch) -> Self {
        Self::ProtocolMismatch {
            local: mismatch.local,
            remote: mismatch.remote,
        }
    }
}
