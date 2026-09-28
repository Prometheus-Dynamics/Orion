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
    #[error("invalid client identity name: {0}")]
    InvalidIdentityName(String),
    #[error("client role mismatch: expected {expected:?}, found {found:?}")]
    RoleMismatch {
        expected: ClientRole,
        found: ClientRole,
    },
    /// orion-node speaks a different control-protocol wire version than this client build.
    #[error(
        "incompatible Orion control protocol: this client speaks v{local} but orion-node speaks \
         v{remote}; upgrade orionctl/client libraries and orion-node together (they must come \
         from the same Orion release)"
    )]
    ProtocolMismatch { local: u16, remote: u16 },
    #[cfg(feature = "ipc")]
    #[error(transparent)]
    Ipc(IpcTransportError),
}

#[cfg(feature = "ipc")]
impl From<IpcTransportError> for ClientError {
    fn from(error: IpcTransportError) -> Self {
        match error {
            IpcTransportError::ProtocolMismatch { local, remote } => {
                Self::ProtocolMismatch { local, remote }
            }
            other => Self::Ipc(other),
        }
    }
}

#[cfg(all(test, feature = "ipc"))]
mod tests {
    use super::*;

    #[test]
    fn ipc_protocol_mismatch_maps_to_typed_client_error() {
        let error = ClientError::from(IpcTransportError::ProtocolMismatch {
            local: 2,
            remote: 3,
        });
        assert_eq!(
            error,
            ClientError::ProtocolMismatch {
                local: 2,
                remote: 3
            }
        );
        let message = error.to_string();
        assert!(message.contains("this client speaks v2 but orion-node speaks v3"));
        assert!(message.contains("upgrade orionctl"));
    }
}
