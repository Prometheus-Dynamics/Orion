//! What can go wrong between a client and a comm core.

use std::fmt;
use std::io;

use orion_msg_session::RejectReason;

/// Why a call on a session failed.
#[derive(Debug)]
pub enum ClientError {
    /// The connection could not be made, or broke.
    Io(io::Error),
    /// The comm core turned the session away at hello.
    Rejected(RejectReason),
    /// The comm core, or the node at a network address, is not who or what was expected: it
    /// needs a signed session and this client has no key, it does not hold the key that was
    /// asked for, or it refused this client's key.
    Denied(&'static str),
    /// The node at the address proved it holds a key, and not the one the address is pinned
    /// to (`#<key>`): another node, or this one with a new key. Both are here so a tool can
    /// show the two fingerprints.
    OtherKey {
        /// The key the address asked for.
        asked: [u8; 32],
        /// The key the node proved it holds.
        answered: [u8; 32],
    },
    /// The comm core answered a request with a refusal.
    Refused(orion_types::Error),
    /// The comm core said nothing for three heartbeats, or closed the session.
    Lost,
    /// Something arrived that is not the wire.
    Protocol(&'static str),
    /// The frame does not fit what both sides agreed to.
    TooLarge,
    /// Descriptors cannot travel on this kind of address.
    NoDescriptors,
}

impl fmt::Display for ClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(f, "{error}"),
            Self::Rejected(reason) => write!(f, "the session was rejected: {reason:?}"),
            Self::Denied(why) => f.write_str(why),
            Self::OtherKey { asked, answered } => write!(
                f,
                "the node is not the one that was expected: asked for key {}, it holds {}",
                hex(asked),
                hex(answered)
            ),
            Self::Refused(error) => write!(f, "refused: {error}"),
            Self::Lost => f.write_str("the connection was lost"),
            Self::Protocol(what) => write!(f, "not the wire: {what}"),
            Self::TooLarge => f.write_str("the frame is larger than both sides agreed to"),
            Self::NoDescriptors => f.write_str("descriptors do not travel on this address"),
        }
    }
}

fn hex(key: &[u8; 32]) -> String {
    key.iter().map(|byte| format!("{byte:02x}")).collect()
}

impl std::error::Error for ClientError {}

impl From<io::Error> for ClientError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

impl ClientError {
    /// Whether connecting again may help: the connection broke or was lost, as opposed to the
    /// comm core saying no.
    #[must_use]
    pub const fn is_lost(&self) -> bool {
        matches!(self, Self::Io(_) | Self::Lost)
    }
}
