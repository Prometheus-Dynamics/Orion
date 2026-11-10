//! Who is trusted.
//!
//! - **The node's identity**: an ed25519 key, made the first time the node starts and kept in
//!   the state directory. Its public half is how other nodes and people's tools know this
//!   board.
//! - **Peers**: other nodes, each pinned to one key.
//! - **Operators**: people's tools, each a key with what it may do: read, and which actions.
//!   An operator is never a node. It is known by its key, not by anything it says.
//! - **Revoked keys**: never trusted again, whoever presents them.
//!
//! **Enrollment** is how a key becomes trusted:
//!
//! - An administrator on the board says so (`OperatorEnroll` from root on the local socket).
//! - The newcomer proves it holds the cluster's enrollment key: an operator with a proof in
//!   its `OperatorEnroll`, a node with the three-step handshake ([`enroll`]). The key itself
//!   never crosses the wire.
//! - Otherwise an operator that asks is recorded as pending, for an administrator to approve.
//!
//! **A refused operator stays refused.** A key that was revoked, or a pending one that was
//! turned down, is not enrolled again by asking, or by a proof. Only an administrator can
//! bring it back.
//!
//! **With security off**, nothing here is consulted: the one check admits everyone, a tool
//! with no key is answered as an operator that may do everything, and nothing is recorded.
//! That is how a board on a closed network works today.

mod enroll_impl;
mod identity;
mod service;
mod store;

pub use identity::{Identity, fingerprint, random, verify};
pub use orion_msg_peers::OperatorState;
pub use service::KeysService;
pub use store::{Keys, KeysConfig, KeysError, Operator, SharedKeys};

/// The shared-key enrollment handshake between two nodes.
pub mod enroll {
    pub use crate::enroll_impl::{
        NONCE_LEN, Side, Transcript, operator_proof, proof, verify_proof,
    };
}
