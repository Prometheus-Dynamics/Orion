//! Peer, trust and time bodies.

#![no_std]

use orion_msg_records::RecordEntry;
use orion_types::{HlcTimestamp, KeyId, NodeName};
use orion_wire::{Bytes, List, body, kind, open_enum};

open_enum! {
    /// How far an operator is trusted.
    pub enum OperatorState: u8 {
        /// Enrolled.
        Enrolled = 1,
        /// Waiting for approval.
        Pending = 2,
        /// Revoked.
        Revoked = 3,
    }
}

open_enum! {
    /// What is being enrolled.
    pub enum EnrollRole: u8 {
        /// A node.
        Node = 1,
        /// An operator.
        Operator = 2,
    }
}

body! {
    /// An action an operator is granted.
    pub struct GrantedAction<'a> {
        /// The action's name.
        pub name: &'a str,
    }
}

body! {
    /// One operator.
    pub struct OperatorEntry<'a> {
        /// Their key.
        pub key: KeyId,
        /// Whether they're trusted.
        pub state: OperatorState,
        /// Whether they may read.
        pub read: bool,
        /// Actions they may run. Empty with `all_actions` means none.
        pub actions: List<'a, GrantedAction<'a>>,
        /// Whether they may run every action.
        pub all_actions: bool,
    }
}

body! {
    /// Compares desired state with a peer and carries what it's missing.
    pub struct PeerSync<'a> = kind::PEER_SYNC {
        /// Who is sending.
        pub node: NodeName,
        /// The newest stamp the sender has.
        pub newest: HlcTimestamp,
        /// A digest of the sender's whole desired state.
        pub fingerprint: u64,
        /// Records the peer is missing.
        pub records: List<'a, RecordEntry<'a>>,
    }
}

body! {
    /// What a node sees right now.
    pub struct PeerObserved<'a> = kind::PEER_OBSERVED {
        /// Who is sending.
        pub node: NodeName,
        /// The records.
        pub records: List<'a, RecordEntry<'a>>,
    }
}

body! {
    /// The first step of enrollment.
    pub struct EnrollHello<'a> = kind::ENROLL_HELLO {
        /// The cluster.
        pub cluster: &'a str,
        /// Who is enrolling.
        pub initiator: NodeName,
        /// Their public key.
        pub public_key: Bytes<'a>,
        /// Their nonce.
        pub nonce: Bytes<'a>,
        /// Who they want to enroll with.
        pub responder: NodeName,
        /// What is being enrolled.
        pub role: EnrollRole,
    }
}

body! {
    /// The second step of enrollment.
    pub struct EnrollChallenge<'a> = kind::ENROLL_CHALLENGE {
        /// Who is answering.
        pub responder: NodeName,
        /// Their public key.
        pub public_key: Bytes<'a>,
        /// Their nonce.
        pub nonce: Bytes<'a>,
        /// Proof they hold the shared enrollment key.
        pub proof: Bytes<'a>,
        /// Their signature.
        pub signature: Bytes<'a>,
    }
}

body! {
    /// The last step of enrollment.
    pub struct EnrollConfirm<'a> = kind::ENROLL_CONFIRM {
        /// Who is enrolling.
        pub initiator: NodeName,
        /// Who with.
        pub responder: NodeName,
        /// The responder's nonce.
        pub nonce: Bytes<'a>,
        /// Proof they hold the shared enrollment key.
        pub proof: Bytes<'a>,
        /// Their signature.
        pub signature: Bytes<'a>,
    }
}

body! {
    /// Stops trusting a key.
    pub struct PeerRevoke = kind::PEER_REVOKE {
        /// The key.
        pub key: KeyId,
    }
}

body! {
    /// Asks to be enrolled as an operator.
    pub struct OperatorEnroll<'a> = kind::OPERATOR_ENROLL {
        /// Their public key.
        pub public_key: Bytes<'a>,
        /// A name for people.
        pub name: &'a str,
        /// Proof they hold the enrollment key, if there is one.
        pub proof: Bytes<'a>,
    }
}

body! {
    /// The answer to an [`OperatorEnroll`].
    pub struct OperatorWelcome<'a> = kind::OPERATOR_WELCOME {
        /// The node answering.
        pub node: NodeName,
        /// Its public key.
        pub public_key: Bytes<'a>,
        /// The cluster.
        pub cluster: &'a str,
        /// What the operator was given: one entry.
        pub operator: List<'a, OperatorEntry<'a>>,
    }
}

body! {
    /// Asks for the operators. Answered with [`Operators`].
    pub struct OperatorList = kind::OPERATOR_LIST {
    }
}

body! {
    /// The operators.
    pub struct Operators<'a> = kind::OPERATORS {
        /// The `seq` of the request.
        pub request_seq: u16,
        /// The operators.
        pub operators: List<'a, OperatorEntry<'a>>,
    }
}

/// [`TimeRequest::clock`]: the wall clock, `CLOCK_REALTIME`, which time sync sets.
pub const CLOCK_REALTIME: u8 = 0;
/// [`TimeRequest::clock`]: `CLOCK_MONOTONIC`, which sample times are on.
pub const CLOCK_MONOTONIC: u8 = 1;

body! {
    /// Starts a clock comparison.
    pub struct TimeRequest = kind::TIME_REQUEST {
        /// When it was sent, on the sender's clock, in nanoseconds.
        pub sent_ns: u64,
        /// Which clock to answer with, [`CLOCK_REALTIME`] or [`CLOCK_MONOTONIC`]. Another is
        /// refused, `unsupported`.
        pub clock: u8,
    }
}

body! {
    /// Answers a clock comparison.
    pub struct TimeReply = kind::TIME_REPLY {
        /// The `sent_ns` of the request.
        pub sent_ns: u64,
        /// When it arrived, on the answerer's clock.
        pub received_ns: u64,
        /// When this was sent, on the answerer's clock.
        pub replied_ns: u64,
    }
}

body! {
    /// Offers the sender's clock as a time source. Whoever may send this may set the clock.
    pub struct TimeOffer = kind::TIME_OFFER {
        /// How good the sender's clock is: its NTP stratum, 1 to 15.
        pub stratum: u8,
    }
}

body! {
    /// Takes a [`TimeOffer`]: the receiver will ask with [`TimeRequest`] when it wants the time.
    pub struct TimeTurn = kind::TIME_TURN {
        /// The `seq` of the offer.
        pub request_seq: u16,
        /// How often it expects to ask, in milliseconds.
        pub poll_ms: u32,
    }
}
