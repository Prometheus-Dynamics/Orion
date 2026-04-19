use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum IpcTransportError {
    #[error("failed to bind unix socket: {0}")]
    BindFailed(String),
    #[error("failed to connect to unix socket: {0}")]
    ConnectFailed(String),
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
}
