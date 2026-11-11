//! Session bodies: the first and last things said on a connection.

#![no_std]

use orion_types::{Error, NodeName};
use orion_wire::{Bytes, List, body, kind, open_enum};
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

body! {
    /// Asks a node who it is. Answered with [`Identity`].
    pub struct Identify<'a> = kind::IDENTIFY {
        /// Fresh bytes of the asker's, up to 32, which the answer includes under its
        /// signature. That is what makes an answer one that was made for this question.
        pub nonce: Bytes<'a>,
    }
}

body! {
    /// One fact about the board, from whatever program published it.
    pub struct IdentityField<'a> {
        /// Its name.
        pub name: &'a str,
        /// The number of its field type.
        pub ty: u8,
        /// Its value, as that type's payload.
        pub value: Bytes<'a>,
    }
}

body! {
    /// Who a node is: the hello document.
    pub struct Identity<'a> = kind::IDENTITY {
        /// The `seq` of the request.
        pub request_seq: u16,
        /// The node.
        pub node: NodeName,
        /// Its cluster.
        pub cluster: &'a str,
        /// Its public key.
        pub public_key: Bytes<'a>,
        /// Its clock: milliseconds since 1970.
        pub time_ms: u64,
        /// The highest interface revision it speaks.
        pub revision: u16,
        /// Facts about the board that other programs published.
        pub fields: List<'a, IdentityField<'a>>,
        /// The `nonce` of the request.
        pub nonce: Bytes<'a>,
        /// The node's signature over everything before this field. Always last, so it can be
        /// cut off and the rest checked. Empty when the asker is on the board and gave no
        /// nonce.
        pub signature: Bytes<'a>,
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

open_enum! {
    /// What kind of handle a [`LinkOffer`] carries.
    pub enum LinkTransport: u8 {
        /// One descriptor that can be waited on and carries bytes both ways: a serial port
        /// that is already open, a gadget's serial function, a socket.
        Stream = 1,
        /// Two descriptors, one to read and one to write, that block: a gadget function's
        /// bulk endpoints.
        Endpoints = 2,
        /// One descriptor of a USB device, with the interface and bulk endpoints to use.
        Bulk = 3,
    }
}

body! {
    /// Hands the comm core an open device for one of its links. The descriptors travel with
    /// the frame. The comm core never opens hardware itself.
    pub struct LinkOffer<'a> = kind::LINK_OFFER {
        /// The link the device is for.
        pub link: &'a str,
        /// What the descriptors are.
        pub transport: LinkTransport,
        /// For [`LinkTransport::Bulk`]: the interface to claim.
        pub interface: u8,
        /// For [`LinkTransport::Bulk`]: the endpoint to read from.
        pub endpoint_in: u8,
        /// For [`LinkTransport::Bulk`]: the endpoint to write to.
        pub endpoint_out: u8,
    }
}

body! {
    /// Takes a [`LinkOffer`]: the link now runs over the device that was handed over.
    pub struct LinkTaken = kind::LINK_TAKEN {
        /// The `seq` of the offer.
        pub request_seq: u16,
    }
}

body! {
    /// Node logic tells the comm core it is ready, and which kinds it answers. From then on
    /// the comm core passes those requests to it as [`NodeRequest`]. Until then, and whenever
    /// node logic is away, they are refused with [`Error::Unavailable`].
    pub struct NodeAttach<'a> = kind::NODE_ATTACH {
        /// The kinds node logic answers, one byte each.
        pub kinds: Bytes<'a>,
    }
}

body! {
    /// A request the comm core passes to node logic, after the check. Node logic never sees
    /// one that was refused at the door.
    pub struct NodeRequest<'a> = kind::NODE_REQUEST {
        /// The session that asked, to name in the answer.
        pub session: u32,
        /// The `seq` of the request, which an answer to the caller names.
        pub seq: u16,
        /// The kind of the request.
        pub kind: u8,
        /// The caller's class: the bit of `orion_check::PrincipalClass`.
        pub class: u8,
        /// The caller's user id, for a program on this board. Zero otherwise.
        pub uid: u32,
        /// The caller's key, node or device name. Empty for a program on this board.
        pub caller: &'a str,
        /// The request's payload, as it came.
        pub payload: Bytes<'a>,
    }
}

body! {
    /// A frame from node logic for one session: an answer, a push, a refusal. The comm core
    /// sends it on as it is.
    pub struct NodeAnswer<'a> = kind::NODE_ANSWER {
        /// The session it is for.
        pub session: u32,
        /// The kind of the frame.
        pub kind: u8,
        /// Whether the frame is one of a stream that the next one supersedes, and may be
        /// dropped when the session is not reading. Otherwise the session is closed.
        pub may_drop: bool,
        /// The frame's payload.
        pub payload: Bytes<'a>,
    }
}

body! {
    /// A session that node logic was passed requests from is gone.
    pub struct NodeGone = kind::NODE_GONE {
        /// The session.
        pub session: u32,
    }
}
