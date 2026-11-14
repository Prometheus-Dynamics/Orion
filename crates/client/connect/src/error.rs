//! What can go wrong between a client and a comm core.

use std::fmt;
use std::io;

use orion_msg_session::RejectReason;

/// Why a call on a session failed.
///
/// More cases may come in a later release, so a `match` on it needs a `_` arm.
#[derive(Debug)]
#[non_exhaustive]
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
    /// The comm core, or node logic, answered a request with a refusal.
    Refused {
        /// What kind of refusal.
        error: orion_types::Error,
        /// The node's words, as it sent them. Never empty: a refusal sent with no words has
        /// the error's own name here.
        detail: String,
    },
    /// The comm core said nothing for three heartbeats, or closed the session.
    Lost,
    /// Something arrived that is not the wire.
    Protocol(&'static str),
    /// The frame does not fit what both sides agreed to.
    TooLarge,
    /// Descriptors cannot travel on this kind of address.
    NoDescriptors,
    /// The node speaks another wire revision: neither side could read the other right. Both
    /// are here, so a tool can say which side is behind.
    Revision {
        /// The node's.
        node: u16,
        /// This client's.
        client: u16,
    },
    /// The node is at an additive level below what was asked for. Nothing was sent.
    Level {
        /// What was asked for.
        feature: crate::Feature,
        /// The level that has it.
        needs: u16,
        /// The node's level.
        node: u16,
    },
    /// Shared memory from the comm core is of another layout than this client reads, so
    /// neither can read the other's rings. Both layouts are here, so a tool can say which side
    /// to update.
    Layout {
        /// What the memory was for: "a publisher's ring", "a buffer pool".
        what: &'static str,
        /// The layout the comm core's memory is in.
        found: u64,
        /// The layout this client reads.
        reads: u64,
        /// The node's additive level, from hello. A node at level 0 is from before levels,
        /// and every ring it shares is layout 1.
        level: u16,
    },
    /// Shared memory from the comm core, or a ring of this client's own, could not be used.
    Memory {
        /// What the memory was for.
        what: &'static str,
        /// Why, as the shared-memory crate said it.
        why: String,
    },
}

impl ClientError {
    /// A refusal of kind `error` with the node's words, or the error's own name when it sent
    /// none.
    #[must_use]
    pub fn refused(error: orion_types::Error, detail: &str) -> Self {
        let detail = if detail.is_empty() {
            error.as_str()
        } else {
            detail
        };
        Self::Refused {
            error,
            detail: detail.to_owned(),
        }
    }

    /// The kind of refusal, if this is one.
    #[must_use]
    pub fn refusal(&self) -> Option<orion_types::Error> {
        match self {
            Self::Refused { error, .. } => Some(*error),
            _ => None,
        }
    }
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
            Self::Refused { error, detail } if detail == error.as_str() => {
                write!(f, "refused: {error}")
            }
            Self::Refused { error, detail } => write!(f, "refused: {error}: {detail}"),
            Self::Lost => f.write_str("the connection was lost"),
            Self::Protocol(what) => write!(f, "not the wire: {what}"),
            Self::TooLarge => f.write_str("the frame is larger than both sides agreed to"),
            Self::NoDescriptors => f.write_str("descriptors do not travel on this address"),
            Self::Revision { node, client } => write!(
                f,
                "the node speaks wire revision {node} and this client {client}: update the older"
            ),
            Self::Level {
                feature,
                needs,
                node,
            } => write!(
                f,
                "{feature} needs a node at level {needs} or above, and this node is at level {node}"
            ),
            Self::Layout {
                what,
                found,
                reads,
                level,
            } => layout(f, what, *found, *reads, *level),
            Self::Memory { what, why } => write!(f, "{what} cannot be used: {why}"),
        }
    }
}

/// The words for [`ClientError::Layout`]: which side is older, and what to update.
fn layout(
    f: &mut fmt::Formatter<'_>,
    what: &str,
    found: u64,
    reads: u64,
    level: u16,
) -> fmt::Result {
    write!(
        f,
        "{what} from the comm core is shared-memory layout {found}, and this client reads layout {reads}: "
    )?;
    if found > reads {
        return f.write_str("this client is older; rebuild it against the comm core's version");
    }
    f.write_str("the comm core is older")?;
    if found == 1 {
        // Layout 2 came with the device library's ad5bf930.
        f.write_str(", built against the device library from before ad5bf930")?;
    }
    f.write_str("; update it")?;
    if level == 0 {
        f.write_str(
            " (it said level 0 at hello: it is from before levels, and every ring it shares is layout 1)",
        )?;
    }
    Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;
    use orion_types::Error;

    #[test]
    fn a_refusal_keeps_the_nodes_words_and_is_never_without_any() {
        let worded = ClientError::refused(Error::Busy, "cam.front is held by workload:a");
        assert!(matches!(
            &worded,
            ClientError::Refused { error: Error::Busy, detail } if detail == "cam.front is held by workload:a"
        ));
        assert_eq!(
            worded.to_string(),
            "refused: busy: cam.front is held by workload:a"
        );
        let bare = ClientError::refused(Error::Busy, "");
        assert!(matches!(
            &bare,
            ClientError::Refused { error: Error::Busy, detail } if detail == "busy"
        ));
        assert_eq!(bare.to_string(), "refused: busy");
        assert_eq!(bare.refusal(), Some(Error::Busy));
        assert_eq!(ClientError::Lost.refusal(), None);
    }

    fn layout(found: u64, reads: u64, level: u16) -> String {
        ClientError::Layout {
            what: "a publisher's ring",
            found,
            reads,
            level,
        }
        .to_string()
    }

    #[test]
    fn a_layout_says_which_side_is_older_and_what_to_update() {
        // The comm core of the last lock revision, at level 0.
        assert_eq!(
            layout(1, 2, 0),
            "a publisher's ring from the comm core is shared-memory layout 1, and this client \
             reads layout 2: the comm core is older, built against the device library from \
             before ad5bf930; update it (it said level 0 at hello: it is from before levels, \
             and every ring it shares is layout 1)"
        );
        // A comm core with levels that still shares layout 1: no word about level 0.
        assert_eq!(
            layout(1, 2, 2),
            "a publisher's ring from the comm core is shared-memory layout 1, and this client \
             reads layout 2: the comm core is older, built against the device library from \
             before ad5bf930; update it"
        );
        // A later layout than this client's: the client is the one behind.
        assert_eq!(
            layout(3, 2, 2),
            "a publisher's ring from the comm core is shared-memory layout 3, and this client \
             reads layout 2: this client is older; rebuild it against the comm core's version"
        );
        // Another older layout: no commit is named that is not its own.
        assert!(layout(2, 3, 3).ends_with("the comm core is older; update it"));
        assert_eq!(
            ClientError::Memory {
                what: "a buffer pool",
                why: "not a ring: no magic".into()
            }
            .to_string(),
            "a buffer pool cannot be used: not a ring: no magic"
        );
    }
}
