use crate::{ControlOperation, NodeError};
use orion::control_plane::{CommunicationFailureKind, OperationFailureCategory, PeerSyncErrorKind};
use orion_transport_http::{HttpRequestFailureKind, HttpTransportError};
#[cfg(feature = "transport-quic")]
use orion_transport_quic::QuicTransportError;
#[cfg(feature = "transport-tcp")]
use orion_transport_tcp::TcpTransportError;

pub(crate) fn classify_node_error(error: &NodeError) -> OperationFailureCategory {
    match error {
        NodeError::Config(_) => OperationFailureCategory::Validation,
        NodeError::LockPoisoned(_) => OperationFailureCategory::Concurrency,
        NodeError::DuplicateProvider(_)
        | NodeError::DuplicateExecutor(_)
        | NodeError::DuplicatePeer(_)
        | NodeError::ProviderNodeMismatch { .. }
        | NodeError::ExecutorNodeMismatch { .. }
        | NodeError::ClientRoleMismatch { .. } => OperationFailureCategory::Validation,
        NodeError::UnknownPeer(_) => OperationFailureCategory::UnknownPeer,
        NodeError::UnknownClient(_) => OperationFailureCategory::Validation,
        NodeError::RateLimitedClient { .. } => OperationFailureCategory::RateLimit,
        NodeError::InvalidPublicKeyLength
        | NodeError::InvalidSignatureLength
        | NodeError::Authentication(_) => OperationFailureCategory::Authentication,
        NodeError::AuthenticatedPeerRequired { .. }
        | NodeError::ConfiguredPeerRequired { .. }
        | NodeError::Authorization(_) => OperationFailureCategory::Authorization,
        NodeError::Runtime(_) => OperationFailureCategory::Runtime,
        NodeError::MutationApply(_) => OperationFailureCategory::Validation,
        NodeError::HttpTransport(_) => OperationFailureCategory::Transport,
        NodeError::IpcTransport(_) => OperationFailureCategory::Transport,
        #[cfg(feature = "transport-tcp")]
        NodeError::TcpTransport(_) => OperationFailureCategory::Transport,
        #[cfg(feature = "transport-quic")]
        NodeError::QuicTransport(_) => OperationFailureCategory::Transport,
        NodeError::PersistenceWorkerUnavailable
        | NodeError::PersistenceWorkerTerminated
        | NodeError::AuthStateWorkerUnavailable
        | NodeError::AuthStateWorkerTerminated
        | NodeError::InvalidHexPublicKey
        | NodeError::StoragePath { .. }
        | NodeError::Storage(_)
        | NodeError::StartupSpawn { .. }
        | NodeError::StartupSignalListener { .. }
        | NodeError::Startup(_) => OperationFailureCategory::Storage,
    }
}

fn is_tls_trust_error(message: &str) -> bool {
    let message = message.to_ascii_lowercase();
    message.contains("requires prior tls enrollment")
        || message.contains("did not advertise a transport binding")
        || message.contains("transport binding")
        || message.contains("trusted tls certificate")
}

fn is_tls_handshake_error(message: &str) -> bool {
    let message = message.to_ascii_lowercase();
    message.contains("tls handshake failed")
        || message.contains("certificate required")
        || message.contains("client certificate")
        || message.contains("unknown ca")
        || message.contains("bad certificate")
}

fn is_auth_policy_error(message: &str) -> bool {
    let message = message.to_ascii_lowercase();
    message.contains("configured peer identity is required")
        || message.contains("authentication failed")
        || message.contains("authorization failed")
        || message.contains("is revoked")
        || message.contains("cannot send payload")
        || message.contains("cannot publish")
}

fn is_transport_connectivity_error(message: &str) -> bool {
    let message = message.to_ascii_lowercase();
    message.contains("connection refused")
        || message.contains("dns")
        || message.contains("timed out")
        || message.contains("error sending request")
        || message.contains("unreachable")
}

fn classify_peer_sync_message_fallback(message: &str) -> PeerSyncErrorKind {
    if is_tls_trust_error(message) {
        PeerSyncErrorKind::TlsTrust
    } else if is_tls_handshake_error(message) {
        PeerSyncErrorKind::TlsHandshake
    } else if is_auth_policy_error(message) {
        PeerSyncErrorKind::AuthPolicy
    } else if is_transport_connectivity_error(message) {
        PeerSyncErrorKind::TransportConnectivity
    } else {
        PeerSyncErrorKind::PeerSync
    }
}

fn classify_http_transport_error_for_peer_sync(error: &HttpTransportError) -> PeerSyncErrorKind {
    match error {
        // TLS transport errors from the HTTP layer currently arrive as free-text messages,
        // so this is the narrow boundary where message fallback remains necessary.
        HttpTransportError::Tls(message) => classify_peer_sync_message_fallback(message),
        HttpTransportError::RequestFailed {
            kind: HttpRequestFailureKind::Connectivity | HttpRequestFailureKind::Timeout,
            ..
        } => PeerSyncErrorKind::TransportConnectivity,
        HttpTransportError::RequestFailed {
            kind: HttpRequestFailureKind::Other,
            message,
            // Untyped request failures are another boundary case where upstream transport
            // errors do not preserve enough structure for category mapping.
        } => classify_peer_sync_message_fallback(message),
        HttpTransportError::InvalidBaseUrl(_)
        | HttpTransportError::UnsupportedMethod(_)
        | HttpTransportError::UnsupportedPath(_)
        | HttpTransportError::UnsupportedControlMessage
        | HttpTransportError::DecodeRequest(_)
        | HttpTransportError::DecodeResponse(_)
        | HttpTransportError::EncodeRequest(_)
        | HttpTransportError::EncodeResponse(_)
        | HttpTransportError::UnexpectedStatus(_)
        | HttpTransportError::BindFailed(_)
        | HttpTransportError::ServeFailed(_) => PeerSyncErrorKind::PeerSync,
    }
}

pub(crate) fn classify_peer_sync_error_kind(message: &str) -> PeerSyncErrorKind {
    classify_peer_sync_message_fallback(message)
}

pub(crate) fn classify_peer_sync_error(error: &NodeError) -> PeerSyncErrorKind {
    match error {
        NodeError::Config(_)
        | NodeError::LockPoisoned(_)
        | NodeError::DuplicateProvider(_)
        | NodeError::DuplicateExecutor(_)
        | NodeError::DuplicatePeer(_)
        | NodeError::UnknownPeer(_)
        | NodeError::ProviderNodeMismatch { .. }
        | NodeError::ExecutorNodeMismatch { .. }
        | NodeError::Runtime(_)
        | NodeError::MutationApply(_)
        | NodeError::IpcTransport(_)
        | NodeError::PersistenceWorkerUnavailable
        | NodeError::PersistenceWorkerTerminated
        | NodeError::AuthStateWorkerUnavailable
        | NodeError::AuthStateWorkerTerminated
        | NodeError::StoragePath { .. }
        | NodeError::Storage(_)
        | NodeError::StartupSpawn { .. }
        | NodeError::StartupSignalListener { .. }
        | NodeError::Startup(_)
        | NodeError::UnknownClient(_)
        | NodeError::ClientRoleMismatch { .. }
        | NodeError::RateLimitedClient { .. } => PeerSyncErrorKind::PeerSync,
        NodeError::AuthenticatedPeerRequired {
            operation: ControlOperation::Mutations,
        }
        | NodeError::AuthenticatedPeerRequired {
            operation: ControlOperation::ObservedUpdate,
        }
        | NodeError::ConfiguredPeerRequired {
            operation: ControlOperation::Mutations,
        }
        | NodeError::ConfiguredPeerRequired {
            operation: ControlOperation::ObservedUpdate,
        }
        | NodeError::InvalidPublicKeyLength
        | NodeError::InvalidSignatureLength
        | NodeError::InvalidHexPublicKey
        | NodeError::Authentication(_)
        | NodeError::Authorization(_) => PeerSyncErrorKind::AuthPolicy,
        NodeError::AuthenticatedPeerRequired { .. } | NodeError::ConfiguredPeerRequired { .. } => {
            PeerSyncErrorKind::PeerSync
        }
        NodeError::HttpTransport(error) => classify_http_transport_error_for_peer_sync(error),
        #[cfg(feature = "transport-tcp")]
        NodeError::TcpTransport(_) => PeerSyncErrorKind::PeerSync,
        #[cfg(feature = "transport-quic")]
        NodeError::QuicTransport(_) => PeerSyncErrorKind::PeerSync,
    }
}

pub(crate) fn classify_http_communication_failure(
    error: &HttpTransportError,
) -> CommunicationFailureKind {
    match error {
        HttpTransportError::RequestFailed {
            kind: HttpRequestFailureKind::Timeout,
            ..
        } => CommunicationFailureKind::Timeout,
        HttpTransportError::RequestFailed {
            kind: HttpRequestFailureKind::Connectivity,
            ..
        } => CommunicationFailureKind::Transport,
        HttpTransportError::Tls(_) => CommunicationFailureKind::Tls,
        HttpTransportError::DecodeRequest(_) | HttpTransportError::DecodeResponse(_) => {
            CommunicationFailureKind::Decode
        }
        HttpTransportError::EncodeRequest(_)
        | HttpTransportError::EncodeResponse(_)
        | HttpTransportError::UnsupportedControlMessage
        | HttpTransportError::UnsupportedMethod(_)
        | HttpTransportError::UnsupportedPath(_)
        | HttpTransportError::UnexpectedStatus(_) => CommunicationFailureKind::Protocol,
        HttpTransportError::InvalidBaseUrl(_)
        | HttpTransportError::BindFailed(_)
        | HttpTransportError::ServeFailed(_)
        | HttpTransportError::RequestFailed {
            kind: HttpRequestFailureKind::Other,
            ..
        } => CommunicationFailureKind::Transport,
    }
}

#[cfg(feature = "transport-tcp")]
pub(crate) fn classify_tcp_communication_failure(
    error: &TcpTransportError,
) -> CommunicationFailureKind {
    match error {
        TcpTransportError::Tls(_) => CommunicationFailureKind::Tls,
        TcpTransportError::Codec(_) => CommunicationFailureKind::Decode,
        TcpTransportError::BindFailed(_)
        | TcpTransportError::AcceptFailed(_)
        | TcpTransportError::ConnectFailed(_)
        | TcpTransportError::ReadFailed(_)
        | TcpTransportError::WriteFailed(_) => CommunicationFailureKind::Transport,
    }
}

#[cfg(feature = "transport-quic")]
pub(crate) fn classify_quic_communication_failure(
    error: &QuicTransportError,
) -> CommunicationFailureKind {
    match error {
        QuicTransportError::Codec(_) => CommunicationFailureKind::Decode,
        QuicTransportError::Certificate(_) => CommunicationFailureKind::Tls,
        QuicTransportError::BindFailed(_)
        | QuicTransportError::AcceptFailed(_)
        | QuicTransportError::ConnectFailed(_)
        | QuicTransportError::OpenStreamFailed(_)
        | QuicTransportError::ReadFailed(_)
        | QuicTransportError::WriteFailed(_) => CommunicationFailureKind::Transport,
    }
}

pub(crate) fn is_client_auth_tls_error(message: &str) -> bool {
    let message = message.to_ascii_lowercase();
    is_tls_handshake_error(&message)
        || [
            "client cert",
            "certificate unknown",
            "peer is not authenticated",
            "peer sent no certificates",
        ]
        .iter()
        .any(|pattern| message.contains(pattern))
}

pub(crate) fn peer_sync_troubleshooting_hint(kind: &PeerSyncErrorKind) -> String {
    match kind {
        PeerSyncErrorKind::TlsTrust => {
            "verify the peer identity key and TLS trust enrollment; for required mTLS pretrust the peer with `orionctl trust enroll --tls-root-cert ...`".into()
        }
        PeerSyncErrorKind::TlsHandshake => {
            "check HTTP TLS/mTLS mode alignment, peer certificates, and whether the remote side trusts this node's client certificate".into()
        }
        PeerSyncErrorKind::AuthPolicy => {
            "check peer auth mode, revocation state, and whether this peer is allowed to perform the requested sync operation".into()
        }
        PeerSyncErrorKind::TransportConnectivity => {
            "check the peer base URL, listener bind state, network reachability, and whether the remote daemon is up".into()
        }
        _ => "inspect the peer's last error, observability recent events, and trust state for more detail".into(),
    }
}
