use orion_core::ControlProtocolMismatch;
use thiserror::Error;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HttpRequestFailureKind {
    Connectivity,
    Timeout,
    Other,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum HttpTransportError {
    #[error("invalid HTTP base URL: {0}")]
    InvalidBaseUrl(String),
    #[error("unsupported HTTP method {0}")]
    UnsupportedMethod(String),
    #[error("unsupported HTTP path {0}")]
    UnsupportedPath(String),
    #[error("unsupported control message for HTTP transport")]
    UnsupportedControlMessage,
    #[error("failed to decode request body: {0}")]
    DecodeRequest(String),
    #[error("failed to decode response body: {0}")]
    DecodeResponse(String),
    #[error("failed to encode request body: {0}")]
    EncodeRequest(String),
    #[error("failed to encode response body: {0}")]
    EncodeResponse(String),
    #[error("unexpected HTTP status {0}")]
    UnexpectedStatus(u16),
    #[error("failed to send HTTP request: {message}")]
    RequestFailed {
        kind: HttpRequestFailureKind,
        message: String,
    },
    #[error("failed to configure HTTP TLS: {0}")]
    Tls(String),
    #[error("failed to bind HTTP listener: {0}")]
    BindFailed(String),
    #[error("failed to serve HTTP listener: {0}")]
    ServeFailed(String),
    /// The peer speaks a different control-protocol wire version (see
    /// [`orion_core::CONTROL_PROTOCOL_VERSION`]); detected from the
    /// `x-orion-control-protocol` header before any body is decoded.
    #[error("{}", ControlProtocolMismatch::new(*local, *remote))]
    ProtocolMismatch { local: u16, remote: u16 },
}

impl From<ControlProtocolMismatch> for HttpTransportError {
    fn from(mismatch: ControlProtocolMismatch) -> Self {
        Self::ProtocolMismatch {
            local: mismatch.local,
            remote: mismatch.remote,
        }
    }
}

impl HttpTransportError {
    pub fn request_failed(message: impl Into<String>) -> Self {
        Self::RequestFailed {
            kind: HttpRequestFailureKind::Other,
            message: message.into(),
        }
    }

    #[cfg(feature = "transport")]
    pub fn request_failed_from_reqwest(err: reqwest::Error) -> Self {
        let kind = if err.is_timeout() {
            HttpRequestFailureKind::Timeout
        } else if err.is_connect() {
            HttpRequestFailureKind::Connectivity
        } else {
            HttpRequestFailureKind::Other
        };
        Self::RequestFailed {
            kind,
            message: err.to_string(),
        }
    }
}
