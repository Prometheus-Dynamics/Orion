use thiserror::Error;

use orion_control_plane::ClientRole;
#[cfg(feature = "ipc")]
use orion_transport_ipc::IpcTransportError;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ClientError {
    #[error("local client address is already registered")]
    AddressAlreadyRegistered,
    #[error("failed to send control message to Orion")]
    SendFailed,
    #[error("no control message available for this client session")]
    NoMessageAvailable,
    #[error("orion rejected the client request: {0}")]
    Rejected(String),
    #[error("client role mismatch: expected {expected:?}, found {found:?}")]
    RoleMismatch {
        expected: ClientRole,
        found: ClientRole,
    },
    #[cfg(feature = "ipc")]
    #[error(transparent)]
    Ipc(#[from] IpcTransportError),
}
