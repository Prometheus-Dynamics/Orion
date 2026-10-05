//! Peer discovery and enrollment messages (`docs/discovery.md`).
//!
//! Discovery (mDNS/DNS-SD) only *finds* peers. A discovered peer is never trusted by itself: it
//! becomes a sync peer when an operator enrolls it ([`DiscoveredPeerEnrollment`]) or when both
//! nodes prove knowledge of a shared enrollment key with the [`EnrollmentHello`] →
//! [`EnrollmentChallenge`] → [`EnrollmentConfirm`] handshake.

use alloc::{string::String, vec::Vec};
use core::fmt;
use orion_core::{NodeId, PeerBaseUrl, PublicKeyHex};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};

/// Version of the shared-key enrollment handshake.
pub const ENROLLMENT_PROTOCOL_VERSION: u16 = 2;

/// Trust state of a discovered peer, as seen by the local node.
#[derive(
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum DiscoveredPeerState {
    /// Seen on the network; not trusted and not synced with.
    Discovered,
    /// Enrolled: the advertised key is the trusted key and the peer is registered for sync.
    Enrolled,
    /// Removed by an operator. Never enrolled again automatically.
    Revoked,
    /// Advertises a key that differs from the key this node trusts for that node id.
    KeyMismatch,
    /// Speaks another control protocol version; cannot be enrolled.
    Incompatible,
}

impl fmt::Display for DiscoveredPeerState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Discovered => "discovered",
            Self::Enrolled => "enrolled",
            Self::Revoked => "revoked",
            Self::KeyMismatch => "key_mismatch",
            Self::Incompatible => "incompatible",
        })
    }
}

/// One peer found by discovery.
#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct DiscoveredPeerRecord {
    pub node_id: NodeId,
    pub cluster: String,
    /// The advertised ed25519 public key (hex).
    pub public_key_hex: PublicKeyHex,
    /// `sha256:` + the first 16 bytes of SHA-256 over the public key, in hex. Compare it with
    /// the `local_key_fingerprint` the peer reports before enrolling it.
    pub key_fingerprint: String,
    /// Peer URLs built from the advertised addresses and ports (`orion+tcp://`, `http(s)://`).
    pub peer_urls: Vec<PeerBaseUrl>,
    pub control_protocol_version: u16,
    pub state: DiscoveredPeerState,
    pub first_seen_at_ms: u64,
    pub last_seen_at_ms: u64,
    pub expires_at_ms: u64,
    /// Last failed enrollment attempt with this peer, if any.
    pub last_enrollment_error: Option<String>,
}

/// Discovery and enrollment counters (also exported as Prometheus metrics).
#[derive(
    Clone,
    Debug,
    Default,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub struct DiscoveryMetricsSnapshot {
    /// Discovery is running on this node.
    pub enabled: bool,
    /// Peers of the local cluster currently in the discovered set (any state).
    pub discovered_peers: u64,
    /// Discovered peers that are enrolled.
    pub enrolled_peers: u64,
    /// Valid announcements of the local cluster received.
    pub announcements_received: u64,
    /// Announcements ignored: other cluster, malformed, or the node's own.
    pub announcements_ignored: u64,
    /// Discovered peers dropped because their announcement expired or was withdrawn.
    pub peers_expired: u64,
    /// Enrollments started (operator approvals, outbound and inbound shared-key handshakes).
    pub enrollment_attempts: u64,
    pub enrollment_successes: u64,
    pub enrollment_failures: u64,
}

/// Answer to [`crate::ControlMessage::QueryDiscovery`].
#[derive(
    Clone,
    Debug,
    Default,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub struct DiscoverySnapshot {
    pub metrics: DiscoveryMetricsSnapshot,
    /// Discovery backend (`mdns`, or `memory` in tests); empty when discovery is off.
    pub backend: String,
    pub cluster: String,
    /// Fingerprint of this node's own key, for out-of-band comparison on the other node.
    pub local_key_fingerprint: String,
    /// Whether a shared enrollment key is configured (automatic mutual enrollment).
    pub enrollment_key_configured: bool,
    pub peers: Vec<DiscoveredPeerRecord>,
}

/// Operator approval of a discovered peer (`orionctl peers enroll <node-id>`): pins the
/// advertised key and registers the peer for sync.
#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct DiscoveredPeerEnrollment {
    pub node_id: NodeId,
    /// When set, the enrollment fails unless the advertised key still has this fingerprint
    /// (the one the operator was shown).
    pub expected_key_fingerprint: Option<String>,
}

/// What the initiator of a shared-key enrollment wants to become on the responder.
#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum EnrollmentRole {
    /// A peer node (a sync peer and cluster member).
    #[default]
    Node,
    /// A remote operator (`operator:<name>`): never a cluster member, see
    /// `docs/remote-operator.md`.
    Operator,
}

impl EnrollmentRole {
    /// The byte bound into the handshake transcript.
    pub fn wire_byte(self) -> u8 {
        match self {
            Self::Node => 0,
            Self::Operator => 1,
        }
    }
}

/// Shared-key enrollment, step 1 (initiator → responder).
#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct EnrollmentHello {
    pub version: u16,
    pub cluster: String,
    pub initiator: NodeId,
    pub initiator_public_key: Vec<u8>,
    /// 32 random bytes.
    pub initiator_nonce: Vec<u8>,
    /// Where the responder can reach the initiator (`orion+tcp://...`), if it listens.
    pub initiator_url: Option<PeerBaseUrl>,
    /// The node the initiator wants to enroll with.
    pub responder: NodeId,
    /// Whether the initiator enrolls as a peer node or as a remote operator.
    pub role: EnrollmentRole,
}

/// Shared-key enrollment, step 2 (responder → initiator): the responder's key, nonce, and proofs
/// that it holds the enrollment key (`proof`) and its private key (`signature`).
#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct EnrollmentChallenge {
    pub responder: NodeId,
    pub responder_public_key: Vec<u8>,
    pub responder_nonce: Vec<u8>,
    pub proof: Vec<u8>,
    pub signature: Vec<u8>,
}

/// Shared-key enrollment, step 3 (initiator → responder): the initiator's proofs over the same
/// transcript. Answered with `Accepted` once the responder has enrolled the initiator.
#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct EnrollmentConfirm {
    pub initiator: NodeId,
    pub responder: NodeId,
    pub responder_nonce: Vec<u8>,
    pub proof: Vec<u8>,
    pub signature: Vec<u8>,
}
