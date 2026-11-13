//! The shared-key enrollment handshake, as on `dev`:
//!
//! ```text
//! I -> R  EnrollHello     { cluster, I, pk_I, n_I, R, role }
//! R -> I  EnrollChallenge { R, pk_R, n_R, HMAC(K, "responder\0" || T), Sig_R("responder\0" || T) }
//! I -> R  EnrollConfirm   { I, R, n_R,    HMAC(K, "initiator\0" || T), Sig_I("initiator\0" || T) }
//!
//! T = "orion-enroll-v1" || role || cluster || I || pk_I || n_I || R || pk_R || n_R
//!     (each field with its length in front)
//! ```
//!
//! The enrollment key `K` never crosses the wire. The HMACs prove each side knows it, the
//! signatures prove each side holds the private half of the key being pinned, and both are
//! bound to the role, both names, both keys and two fresh nonces, so a proof cannot be played
//! back into another handshake or used for another role.

/// Bytes in each side's nonce.
pub const NONCE_LEN: usize = 32;
const DOMAIN: &[u8] = b"orion-enroll-v1";

/// Which side of the handshake a proof is from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Side {
    /// The one that asked.
    Initiator,
    /// The one that answered.
    Responder,
}

impl Side {
    const fn label(self) -> &'static [u8] {
        match self {
            Self::Initiator => b"initiator\0",
            Self::Responder => b"responder\0",
        }
    }
}

/// What both sides bind their proofs to.
#[derive(Clone, Copy, Debug)]
pub struct Transcript<'a> {
    /// The role's number on the wire.
    pub role: u8,
    /// The cluster.
    pub cluster: &'a str,
    /// Who is enrolling.
    pub initiator: &'a str,
    /// Their public key.
    pub initiator_key: &'a [u8],
    /// Their nonce.
    pub initiator_nonce: &'a [u8],
    /// Who they enroll with.
    pub responder: &'a str,
    /// Its public key.
    pub responder_key: &'a [u8],
    /// Its nonce.
    pub responder_nonce: &'a [u8],
}

impl Transcript<'_> {
    /// The message `side` signs and MACs: its label, then every field with its length.
    #[must_use]
    pub fn message(&self, side: Side) -> Vec<u8> {
        let mut out = side.label().to_vec();
        let mut field = |bytes: &[u8]| {
            out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
            out.extend_from_slice(bytes);
        };
        field(DOMAIN);
        field(&[self.role]);
        field(self.cluster.as_bytes());
        field(self.initiator.as_bytes());
        field(self.initiator_key);
        field(self.initiator_nonce);
        field(self.responder.as_bytes());
        field(self.responder_key);
        field(self.responder_nonce);
        out
    }
}

// The operator's proof is computed by the client, so it lives with the identity.
pub use orion_identity::proof::{operator_proof, proof, verify_proof};
