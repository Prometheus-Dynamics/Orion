//! A shared-memory error, as a [`ClientError`] that keeps its words.

use orion_connect::ClientError;
use shm_ring::RingError;

/// `error`, from memory that was for `what`, on a session whose node said `level` at hello.
/// A layout that is not this client's stays a [`ClientError::Layout`] with both layouts;
/// anything else is a [`ClientError::Memory`] with the shared-memory crate's words.
#[must_use]
pub fn from_ring(what: &'static str, level: u16, error: RingError) -> ClientError {
    match error {
        RingError::Layout { found, reads, .. } => ClientError::Layout {
            what,
            found,
            reads,
            level,
        },
        other => ClientError::Memory {
            what,
            why: other.to_string(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_layout_keeps_both_layouts_and_anything_else_its_words() {
        let layout = RingError::Layout {
            what: "this ring",
            found: 1,
            reads: 2,
        };
        assert!(matches!(
            from_ring("a publisher's ring", 0, layout),
            ClientError::Layout {
                what: "a publisher's ring",
                found: 1,
                reads: 2,
                level: 0
            }
        ));
        let other = from_ring("a subscriber's ring", 2, RingError::Malformed("bad magic"));
        assert!(
            matches!(&other, ClientError::Memory { what: "a subscriber's ring", why } if why == "not a ring: bad magic"),
            "{other:?}"
        );
    }
}
