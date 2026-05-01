use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum TcpTransportError {
    #[error("failed to bind TCP listener: {0}")]
    BindFailed(String),
    #[error("failed to accept TCP connection: {0}")]
    AcceptFailed(String),
    #[error("failed to connect TCP stream: {0}")]
    ConnectFailed(String),
    #[error("failed to read TCP frame: {0}")]
    ReadFailed(String),
    #[error("failed to write TCP frame: {0}")]
    WriteFailed(String),
    #[error("failed to configure TCP TLS: {0}")]
    Tls(String),
    #[error(transparent)]
    Codec(#[from] crate::TcpCodecError),
}
