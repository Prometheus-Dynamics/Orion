use thiserror::Error;

#[derive(Debug, Error)]
pub enum QuicTransportError {
    #[error("failed to generate QUIC certificate: {0}")]
    Certificate(String),
    #[error("failed to bind QUIC endpoint: {0}")]
    BindFailed(String),
    #[error("failed to accept QUIC connection: {0}")]
    AcceptFailed(String),
    #[error("failed to connect QUIC endpoint: {0}")]
    ConnectFailed(String),
    #[error("failed to open QUIC stream: {0}")]
    OpenStreamFailed(String),
    #[error("failed to read QUIC frame: {0}")]
    ReadFailed(String),
    #[error("failed to write QUIC frame: {0}")]
    WriteFailed(String),
    #[error(transparent)]
    Codec(#[from] crate::QuicCodecError),
}
