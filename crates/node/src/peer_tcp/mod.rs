//! `orion+tcp` peer transport (feature `peer-tcp`).
//!
//! Peer control requests travel over plain TCP inside the fixed-layout control frames used by
//! local IPC streams (`[b"OC"][CONTROL_PROTOCOL_VERSION u16 LE][len u32 LE][payload]`). Request
//! payloads are the HTTP codec's request bodies (so peer requests are signed exactly as over
//! HTTP); responses are signed by the responder and bound to the request they answer. There is no
//! TLS: the transport provides authenticity, integrity and replay protection, not confidentiality.
//! See `docs/peer-sync.md`.
//!
//! The frame payload layout lives in `orion_auth::peer_tcp` and the connection handling in
//! `orion_transport_ipc::ControlTcpClient`, shared with the remote operator client
//! (`orion-client`, feature `remote`).

mod server;

pub(crate) use orion_auth::peer_tcp::{
    PEER_TCP_RESPONSE_HEADER_MAX_BYTES as RESPONSE_HEADER_MAX_BYTES,
    PEER_TCP_STATUS_ERROR as STATUS_ERROR, PEER_TCP_STATUS_OK as STATUS_OK, PeerTcpFrameError,
    PeerTcpResponseFrame as ResponseFrame,
};
pub(crate) use orion_transport_ipc::ControlTcpClient as PeerTcpClient;

use orion::{control_plane::CommunicationFailureKind, transport::ipc::IpcTransportError};
use orion_transport_ipc::ControlTcpError;
use thiserror::Error;

/// Idle server connections are closed after this long without a request.
pub(crate) const PEER_TCP_IDLE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);

/// Errors of the `orion+tcp` peer transport.
#[derive(Debug, Error, PartialEq)]
pub enum PeerTcpError {
    #[error("invalid orion+tcp peer address: {0}")]
    InvalidAddress(String),
    #[error("failed to bind peer TCP listener on {addr}: {message}")]
    Bind { addr: String, message: String },
    #[error("failed to connect to peer at {addr}: {message}")]
    Connect { addr: String, message: String },
    #[error("peer TCP {operation} with {addr} timed out after {timeout_ms}ms")]
    Timeout {
        addr: String,
        operation: &'static str,
        timeout_ms: u64,
    },
    #[error("peer TCP connection to {addr} failed: {message}")]
    Connection { addr: String, message: String },
    #[error("peer TCP connection to {addr} was closed before a response arrived")]
    Closed { addr: String },
    #[error(transparent)]
    Frame(#[from] IpcTransportError),
    #[error("malformed peer TCP frame: {0}")]
    Decode(String),
    #[error("peer TCP request failed on the remote node: {0}")]
    Remote(String),
}

impl From<ControlTcpError> for PeerTcpError {
    fn from(error: ControlTcpError) -> Self {
        match error {
            ControlTcpError::Connect { addr, message } => Self::Connect { addr, message },
            ControlTcpError::Timeout {
                addr,
                operation,
                timeout_ms,
            } => Self::Timeout {
                addr,
                operation,
                timeout_ms,
            },
            ControlTcpError::Connection { addr, message } => Self::Connection { addr, message },
            ControlTcpError::Closed { addr } => Self::Closed { addr },
            ControlTcpError::Frame(frame) => Self::Frame(frame),
        }
    }
}

impl From<PeerTcpFrameError> for PeerTcpError {
    fn from(error: PeerTcpFrameError) -> Self {
        Self::Decode(error.0)
    }
}

impl PeerTcpError {
    pub(crate) fn communication_failure_kind(&self) -> CommunicationFailureKind {
        match self {
            Self::Timeout { .. } => CommunicationFailureKind::Timeout,
            Self::Connect { .. } => CommunicationFailureKind::Refused,
            Self::Connection { .. } | Self::Closed { .. } | Self::Bind { .. } => {
                CommunicationFailureKind::Transport
            }
            Self::Frame(IpcTransportError::ProtocolMismatch { .. }) => {
                CommunicationFailureKind::Protocol
            }
            Self::Frame(_) | Self::Decode(_) => CommunicationFailureKind::Decode,
            Self::InvalidAddress(_) | Self::Remote(_) => CommunicationFailureKind::Unknown,
        }
    }

    /// `true` for failures to reach the peer at all (as opposed to protocol or remote errors).
    pub(crate) fn is_connectivity_error(&self) -> bool {
        matches!(
            self,
            Self::Connect { .. }
                | Self::Timeout { .. }
                | Self::Connection { .. }
                | Self::Closed { .. }
        )
    }
}
