//! Session bodies: the first and last things said on a connection.

#![no_std]

use orion_types::{Error, NodeName};
use orion_wire::{body, kind, open_enum};
use serde::{Deserialize, Serialize};

/// What a connection will act as. A program can be several at once.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Roles(pub u16);

impl Roles {
    /// Reads, watches, runs actions, subscribes.
    pub const CLIENT: Self = Self(1 << 0);
    /// Publishes resources and answers actions.
    pub const PROVIDER: Self = Self(1 << 1);
    /// Runs workloads.
    pub const EXECUTOR: Self = Self(1 << 2);
    /// The node logic on this board.
    pub const NODE: Self = Self(1 << 3);
    /// Another node's comm core.
    pub const PEER: Self = Self(1 << 4);
    /// A device on a link.
    pub const DEVICE: Self = Self(1 << 5);

    /// Both sets of roles.
    #[must_use]
    pub const fn and(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }

    /// Whether every role in `other` is here.
    #[must_use]
    pub const fn has(self, other: Self) -> bool {
        self.0 & other.0 == other.0
    }
}

open_enum! {
    /// Why a connection was turned away.
    pub enum RejectReason: u8 {
        /// The frame version isn't one this side speaks.
        VersionMismatch = 1,
        /// The check refused the connection.
        Denied = 2,
        /// This side has no room for another connection.
        Busy = 3,
        /// A role was asked for that this carrier doesn't offer.
        RoleNotOffered = 4,
    }
}

body! {
    /// Opens a session.
    pub struct Hello<'a> = kind::HELLO {
        /// Who is connecting, for logs and status.
        pub name: &'a str,
        /// What it will act as.
        pub roles: Roles,
        /// The largest frame it can receive.
        pub max_frame: u32,
        /// The highest interface revision it speaks.
        pub revision: u16,
    }
}

body! {
    /// Accepts a session.
    pub struct Welcome = kind::WELCOME {
        /// The node answering.
        pub node: NodeName,
        /// This session's number.
        pub session: u32,
        /// How often to ping.
        pub heartbeat_ms: u32,
        /// The largest frame to send: the smaller of both sides' limits.
        pub max_frame: u32,
        /// The highest interface revision this side speaks.
        pub revision: u16,
    }
}

body! {
    /// Turns a session away.
    pub struct Reject = kind::REJECT {
        /// Why.
        pub reason: RejectReason,
    }
}

body! {
    /// Liveness.
    pub struct Ping = kind::PING {
        /// The sender's clock, echoed back so it can measure the round trip.
        pub now_ms: u64,
    }
}

body! {
    /// The answer to a [`Ping`].
    pub struct Pong = kind::PONG {
        /// The `now_ms` of the ping.
        pub now_ms: u64,
    }
}

body! {
    /// A request failed.
    pub struct Refusal<'a> = kind::REFUSAL {
        /// The `seq` of the request.
        pub request_seq: u16,
        /// A stable [`Error`] code, so callers can branch on it.
        pub code: u16,
        /// A description for people.
        pub detail: &'a str,
    }
}

impl Refusal<'_> {
    /// The error, if this build knows the code.
    #[must_use]
    pub const fn error(&self) -> Option<Error> {
        Error::from_code(self.code)
    }
}

/// The revision a session runs at: the lower of both sides'.
#[must_use]
pub const fn session_revision(own: u16, peer: u16) -> u16 {
    if own < peer { own } else { peer }
}

#[cfg(test)]
mod tests {
    use super::*;
    use orion_wire::{Decode, INTERFACE_REVISION, body::encode_fields};

    #[test]
    fn a_newer_and_an_older_peer_both_negotiate() {
        let own = INTERFACE_REVISION;
        assert_eq!(session_revision(own, own + 4), own);
        assert_eq!(session_revision(own + 4, own), own);
        assert_eq!(session_revision(own, 0), 0);
    }

    /// A newer peer's `Hello` has more on the end, and is still understood.
    #[test]
    fn a_hello_from_a_newer_peer_still_decodes() {
        let hello = Hello {
            name: "cam",
            roles: Roles::CLIENT.and(Roles::PROVIDER),
            max_frame: 4096,
            revision: 9,
        };
        let mut buf = [0; 64];
        let len = encode_fields(&hello, &mut buf).unwrap();
        buf[len..len + 3].copy_from_slice(&[1, 2, 3]);
        assert_eq!(Hello::decode(&buf[..len + 3]).unwrap(), hello);
    }

    #[test]
    fn a_reject_reason_from_a_newer_peer_is_kept() {
        assert_eq!(
            Reject::decode(&[200]).unwrap().reason,
            RejectReason::Unknown(200)
        );
    }

    #[test]
    fn refusals_let_callers_branch() {
        let refusal = Refusal {
            request_seq: 1,
            code: Error::Busy.code(),
            detail: "",
        };
        assert_eq!(refusal.error(), Some(Error::Busy));
    }

    #[test]
    fn roles_combine() {
        let roles = Roles::CLIENT.and(Roles::EXECUTOR);
        assert!(roles.has(Roles::CLIENT) && roles.has(Roles::EXECUTOR));
        assert!(!roles.has(Roles::NODE));
    }
}
