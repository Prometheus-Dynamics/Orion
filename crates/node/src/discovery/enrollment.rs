//! Shared-key enrollment handshake (`docs/discovery.md`, "Shared-key enrollment").
//!
//! ```text
//! I -> R  EnrollmentHello     { cluster, I, pk_I, n_I (32 B), url_I, R }
//! R -> I  EnrollmentChallenge { R, pk_R, n_R (32 B),
//!                               HMAC(K, "responder" || T), Sig_R("responder" || T) }
//! I -> R  EnrollmentConfirm   { I, R, n_R, HMAC(K, "initiator" || T), Sig_I("initiator" || T) }
//! R -> I  Accepted            (R has enrolled I; I then enrolls R)
//!
//! T = "orion-enroll-v1" || version || cluster || I || pk_I || n_I || url_I
//!                        || R || pk_R || n_R                    (length-prefixed fields)
//! ```
//!
//! The enrollment key `K` never crosses the wire. The HMACs prove knowledge of `K`, the
//! signatures prove possession of the private key behind the key being pinned, and both are
//! bound to both node ids, both keys, the initiator's URL and two fresh nonces, so a proof cannot be
//! replayed into another handshake. `n_R` is single-use: the responder forgets it when the
//! confirmation arrives or after [`PENDING_TTL_MS`].

use super::config::EnrollmentKey;
use crate::NodeError;
use ed25519_dalek::{Signature, Verifier, VerifyingKey};
use hmac::{Hmac, Mac};
use orion::NodeId;
use orion_control_plane::{ENROLLMENT_PROTOCOL_VERSION, EnrollmentHello};
use rand_core::{OsRng, RngCore};
use sha2::Sha256;
use std::collections::VecDeque;

pub(crate) const NONCE_LEN: usize = 32;
/// How long a responder waits for the confirmation of a challenge.
pub(crate) const PENDING_TTL_MS: u64 = 30_000;
/// Most outstanding challenges; the oldest is dropped beyond this.
const MAX_PENDING: usize = 64;
const DOMAIN: &[u8] = b"orion-enroll-v1";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Role {
    Initiator,
    Responder,
}

impl Role {
    fn label(self) -> &'static [u8] {
        match self {
            Self::Initiator => b"initiator\0",
            Self::Responder => b"responder\0",
        }
    }
}

/// The fields both sides bind their proofs to.
pub(crate) struct Transcript<'a> {
    pub(crate) cluster: &'a str,
    pub(crate) initiator: &'a NodeId,
    pub(crate) initiator_key: &'a [u8],
    pub(crate) initiator_nonce: &'a [u8],
    pub(crate) initiator_url: &'a str,
    pub(crate) responder: &'a NodeId,
    pub(crate) responder_key: &'a [u8],
    pub(crate) responder_nonce: &'a [u8],
}

impl Transcript<'_> {
    pub(crate) fn bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(256);
        let mut field = |bytes: &[u8]| {
            out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
            out.extend_from_slice(bytes);
        };
        field(DOMAIN);
        field(&ENROLLMENT_PROTOCOL_VERSION.to_le_bytes());
        field(self.cluster.as_bytes());
        field(self.initiator.as_str().as_bytes());
        field(self.initiator_key);
        field(self.initiator_nonce);
        field(self.initiator_url.as_bytes());
        field(self.responder.as_str().as_bytes());
        field(self.responder_key);
        field(self.responder_nonce);
        out
    }
}

fn labelled(role: Role, transcript: &[u8]) -> Vec<u8> {
    let mut message = role.label().to_vec();
    message.extend_from_slice(transcript);
    message
}

fn mac(key: &EnrollmentKey) -> Hmac<Sha256> {
    // HMAC accepts keys of any length.
    <Hmac<Sha256> as Mac>::new_from_slice(key.as_bytes()).expect("HMAC accepts any key length")
}

/// `HMAC-SHA256(K, role || transcript)`.
pub(crate) fn proof(key: &EnrollmentKey, role: Role, transcript: &[u8]) -> Vec<u8> {
    let mut mac = mac(key);
    mac.update(&labelled(role, transcript));
    mac.finalize().into_bytes().to_vec()
}

/// Constant-time check of a peer's proof.
pub(crate) fn verify_proof(
    key: &EnrollmentKey,
    role: Role,
    transcript: &[u8],
    proof: &[u8],
) -> Result<(), NodeError> {
    let mut mac = mac(key);
    mac.update(&labelled(role, transcript));
    mac.verify_slice(proof).map_err(|_| {
        NodeError::Authentication(
            "enrollment proof does not verify (different enrollment key, or a tampered or \
             replayed handshake)"
                .into(),
        )
    })
}

/// The message a node signs with its identity key.
pub(crate) fn signed_message(role: Role, transcript: &[u8]) -> Vec<u8> {
    labelled(role, transcript)
}

pub(crate) fn verify_signature(
    public_key: &[u8; 32],
    role: Role,
    transcript: &[u8],
    signature: &[u8],
) -> Result<(), NodeError> {
    let signature: [u8; 64] = signature
        .try_into()
        .map_err(|_| NodeError::InvalidSignatureLength)?;
    VerifyingKey::from_bytes(public_key)
        .map_err(|err| NodeError::Authentication(err.to_string()))?
        .verify(
            &signed_message(role, transcript),
            &Signature::from_bytes(&signature),
        )
        .map_err(|_| {
            NodeError::Authentication("enrollment signature does not verify for the key".into())
        })
}

pub(crate) fn random_nonce() -> [u8; NONCE_LEN] {
    let mut nonce = [0u8; NONCE_LEN];
    OsRng.fill_bytes(&mut nonce);
    nonce
}

pub(crate) fn key_array(bytes: &[u8], what: &str) -> Result<[u8; 32], NodeError> {
    bytes
        .try_into()
        .map_err(|_| NodeError::Authentication(format!("{what} must be a 32-byte ed25519 key")))
}

/// A challenge the responder issued and has not seen confirmed yet.
#[derive(Clone, Debug)]
pub(crate) struct PendingChallenge {
    pub(crate) hello: EnrollmentHello,
    pub(crate) responder_nonce: [u8; NONCE_LEN],
    pub(crate) transcript: Vec<u8>,
    pub(crate) issued_at_ms: u64,
}

/// Outstanding challenges of a responder, keyed by their single-use responder nonce.
#[derive(Debug, Default)]
pub(crate) struct PendingChallenges {
    entries: VecDeque<PendingChallenge>,
}

impl PendingChallenges {
    pub(crate) fn insert(&mut self, challenge: PendingChallenge) {
        self.prune(challenge.issued_at_ms);
        while self.entries.len() >= MAX_PENDING {
            self.entries.pop_front();
        }
        self.entries.push_back(challenge);
    }

    /// Removes and returns the challenge with `responder_nonce`, if it is still valid.
    pub(crate) fn take(&mut self, responder_nonce: &[u8], now_ms: u64) -> Option<PendingChallenge> {
        self.prune(now_ms);
        let index = self
            .entries
            .iter()
            .position(|entry| entry.responder_nonce.as_slice() == responder_nonce)?;
        self.entries.remove(index)
    }

    fn prune(&mut self, now_ms: u64) {
        self.entries
            .retain(|entry| now_ms.saturating_sub(entry.issued_at_ms) < PENDING_TTL_MS);
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }
}
