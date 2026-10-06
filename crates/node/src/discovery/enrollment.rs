//! Shared-key enrollment handshake (`docs/discovery.md`, "Shared-key enrollment").
//!
//! The transcript, HMAC proofs and signatures are implemented once in
//! `orion_auth::enrollment` (shared with the remote operator client); this module adapts them to
//! the node's error type and keeps the responder's single-use challenges. `n_R` is single-use:
//! the responder forgets it when the confirmation arrives or after [`PENDING_TTL_MS`].

use super::config::EnrollmentKey;
use crate::NodeError;
use getrandom::{
    SysRng,
    rand_core::{Rng, UnwrapErr},
};
use orion_auth::enrollment::{
    ENROLLMENT_NONCE_LEN, EnrollmentSide, EnrollmentTranscript, enrollment_message,
    enrollment_proof, verify_enrollment_proof, verify_enrollment_signature,
};
use orion_control_plane::{EnrollmentHello, EnrollmentRole};
use std::collections::VecDeque;

pub(crate) const NONCE_LEN: usize = ENROLLMENT_NONCE_LEN;
/// How long a responder waits for the confirmation of a challenge.
pub(crate) const PENDING_TTL_MS: u64 = 30_000;
/// Most outstanding challenges; the oldest is dropped beyond this.
const MAX_PENDING: usize = 64;

/// Which side of the handshake produces a proof.
pub(crate) type Role = EnrollmentSide;

/// The fields both sides bind their proofs to.
pub(crate) type Transcript<'a> = EnrollmentTranscript<'a>;

fn auth_error(err: orion_auth::AuthProtocolError) -> NodeError {
    match err {
        orion_auth::AuthProtocolError::InvalidSignatureLength(_) => {
            NodeError::InvalidSignatureLength
        }
        other => NodeError::Authentication(other.to_string()),
    }
}

/// `HMAC-SHA256(K, role || transcript)`.
pub(crate) fn proof(key: &EnrollmentKey, role: Role, transcript: &[u8]) -> Vec<u8> {
    enrollment_proof(key.as_bytes(), role, transcript)
}

/// Constant-time check of a peer's proof.
pub(crate) fn verify_proof(
    key: &EnrollmentKey,
    role: Role,
    transcript: &[u8],
    proof: &[u8],
) -> Result<(), NodeError> {
    verify_enrollment_proof(key.as_bytes(), role, transcript, proof).map_err(auth_error)
}

/// The message a node signs with its identity key.
pub(crate) fn signed_message(role: Role, transcript: &[u8]) -> Vec<u8> {
    enrollment_message(role, transcript)
}

pub(crate) fn verify_signature(
    public_key: &[u8; 32],
    role: Role,
    transcript: &[u8],
    signature: &[u8],
) -> Result<(), NodeError> {
    verify_enrollment_signature(public_key, role, transcript, signature).map_err(auth_error)
}

pub(crate) fn random_nonce() -> [u8; NONCE_LEN] {
    let mut nonce = [0u8; NONCE_LEN];
    UnwrapErr(SysRng).fill_bytes(&mut nonce);
    nonce
}

pub(crate) fn key_array(bytes: &[u8], what: &str) -> Result<[u8; 32], NodeError> {
    bytes
        .try_into()
        .map_err(|_| NodeError::Authentication(format!("{what} must be a 32-byte ed25519 key")))
}

/// The URL bound into the transcript of `hello` (empty for operators, which do not listen).
pub(crate) fn transcript_url(hello: &EnrollmentHello) -> &str {
    match hello.role {
        EnrollmentRole::Node => hello
            .initiator_url
            .as_ref()
            .map(|url| url.as_str())
            .unwrap_or(""),
        EnrollmentRole::Operator => "",
    }
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
