use thiserror::Error;

/// Errors of the remote operator client.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum RemoteError {
    #[error("invalid operator identity: {0}")]
    InvalidIdentity(String),
    #[error("invalid node URL: {0}")]
    InvalidUrl(String),
    /// Connecting, reading or writing failed, or timed out.
    #[error("orion+tcp transport error: {0}")]
    Transport(String),
    /// The node speaks a different control-protocol wire version than this client build.
    #[error(
        "incompatible Orion control protocol: this client speaks v{local} but the node speaks \
         v{remote}; use orion-client and orion-node from the same Orion release"
    )]
    ProtocolMismatch { local: u16, remote: u16 },
    /// A response could not be decoded.
    #[error("malformed response: {0}")]
    Decode(String),
    /// The node's identity or a response signature did not verify. Never retry blindly: this is
    /// what an impostor or a re-keyed node looks like.
    #[error("node authentication failed: {0}")]
    NodeAuthentication(String),
    /// The node refused the request (not enrolled, not allowed by the operator's policy, invalid
    /// request, ...). The message comes from the node and is covered by its signature.
    #[error("the node refused the request: {0}")]
    Rejected(String),
    /// Shared-key enrollment failed.
    #[error("enrollment failed: {0}")]
    Enrollment(String),
    /// The node answered with a response of an unexpected kind.
    #[error("unexpected response: {0}")]
    UnexpectedResponse(String),
    /// [`super::RemoteOperator::wait_for_action`] gave up before the action was final.
    #[error("action {action_id} is not final after {timeout_ms} ms")]
    ActionTimeout { action_id: String, timeout_ms: u64 },
    /// The action is not known to the node (never submitted there, or its result expired).
    #[error("action {0} is not known to the node")]
    UnknownAction(String),
    #[cfg(feature = "discovery")]
    #[error("mDNS discovery failed: {0}")]
    Discovery(String),
}

impl RemoteError {
    /// Whether the node refused the request because the operator is not enrolled there.
    pub fn is_not_enrolled(&self) -> bool {
        matches!(self, Self::Rejected(message) if message.contains("is not enrolled"))
    }
}
