//! Errors returned by the NT4 client and server.

/// Errors from encoding or decoding NT4 messages, from the transport, and from the local API.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A binary frame or JSON control message does not follow the NT4 wire format.
    #[error("NT4 decode error: {0}")]
    Decode(String),
    /// A value could not be encoded as MessagePack.
    #[error("NT4 encode error: {0}")]
    Encode(String),
    /// A value does not match the type string of its topic.
    #[error("topic {topic} has type {expected}, got a {got} value")]
    TypeMismatch {
        /// Topic name.
        topic: String,
        /// The topic's NT4 type string.
        expected: String,
        /// The value's NT4 default type string.
        got: &'static str,
    },
    /// A type string this crate does not model (see `value::type_id_for_name`).
    #[error("unsupported NT4 type string {0:?}")]
    UnsupportedType(String),
    /// The topic does not exist on the server.
    #[error("no topic named {0}")]
    UnknownTopic(String),
    /// The WebSocket handshake or connection failed.
    #[error("WebSocket error: {0}")]
    WebSocket(#[from] tokio_tungstenite::tungstenite::Error),
    /// Socket I/O failed.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    /// A JSON control message failed to (de)serialize.
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    /// The peer did not select an NT4 WebSocket subprotocol.
    #[error("server did not select an NT4 subprotocol (offered {offered})")]
    Subprotocol {
        /// The subprotocols the client offered.
        offered: String,
    },
    /// The client task has stopped (its handles were dropped or it panicked).
    #[error("NT4 client task has stopped")]
    Closed,
}

/// Result alias used throughout the crate.
pub type Result<T> = std::result::Result<T, Error>;
