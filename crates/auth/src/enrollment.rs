//! Shared-key enrollment handshake proofs (feature `enrollment`, `docs/discovery.md`).
//!
//! ```text
//! I -> R  EnrollmentHello     { role, cluster, I, pk_I, n_I (32 B), url_I, R }
//! R -> I  EnrollmentChallenge { R, pk_R, n_R (32 B),
//!                               HMAC(K, "responder\0" || T), Sig_R("responder\0" || T) }
//! I -> R  EnrollmentConfirm   { I, R, n_R, HMAC(K, "initiator\0" || T), Sig_I("initiator\0" || T) }
//! R -> I  Accepted            (R has enrolled I; a node initiator then enrolls R)
//!
//! T = "orion-enroll-v1" || version || role || cluster || I || pk_I || n_I || url_I
//!                        || R || pk_R || n_R                    (length-prefixed fields)
//! ```
//!
//! The enrollment key `K` never crosses the wire. The HMACs prove knowledge of `K`, the
//! signatures prove possession of the private key behind the key being pinned, and both are bound
//! to the role, both ids, both keys, the initiator's URL (empty for operators) and two fresh
//! nonces, so a proof cannot be replayed into another handshake or reused for another role.

use crate::{AuthProtocolError, crypto};
use alloc::{string::ToString, vec::Vec};
use hmac::{Hmac, Mac};
use orion_control_plane::{ENROLLMENT_PROTOCOL_VERSION, EnrollmentRole};
use orion_core::NodeId;
use sha2::Sha256;

/// Length of both handshake nonces.
pub const ENROLLMENT_NONCE_LEN: usize = 32;
const DOMAIN: &[u8] = b"orion-enroll-v1";

/// Which side of the handshake produces a proof.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EnrollmentSide {
    Initiator,
    Responder,
}

impl EnrollmentSide {
    fn label(self) -> &'static [u8] {
        match self {
            Self::Initiator => b"initiator\0",
            Self::Responder => b"responder\0",
        }
    }
}

/// The fields both sides bind their proofs to.
#[derive(Clone, Copy, Debug)]
pub struct EnrollmentTranscript<'a> {
    pub role: EnrollmentRole,
    pub cluster: &'a str,
    pub initiator: &'a NodeId,
    pub initiator_key: &'a [u8],
    pub initiator_nonce: &'a [u8],
    /// The initiator's `orion+tcp` URL; empty for operators, which do not listen.
    pub initiator_url: &'a str,
    pub responder: &'a NodeId,
    pub responder_key: &'a [u8],
    pub responder_nonce: &'a [u8],
}

impl EnrollmentTranscript<'_> {
    pub fn bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(256);
        let mut field = |bytes: &[u8]| {
            out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
            out.extend_from_slice(bytes);
        };
        field(DOMAIN);
        field(&ENROLLMENT_PROTOCOL_VERSION.to_le_bytes());
        field(&[self.role.wire_byte()]);
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

/// The message a side signs (and MACs): its label followed by the transcript.
pub fn enrollment_message(side: EnrollmentSide, transcript: &[u8]) -> Vec<u8> {
    let mut message = side.label().to_vec();
    message.extend_from_slice(transcript);
    message
}

fn mac(key: &[u8]) -> Hmac<Sha256> {
    // HMAC accepts keys of any length, so this never fails.
    match <Hmac<Sha256> as Mac>::new_from_slice(key) {
        Ok(mac) => mac,
        Err(_) => unreachable!("HMAC accepts any key length"),
    }
}

/// `HMAC-SHA256(K, side || transcript)`.
pub fn enrollment_proof(key: &[u8], side: EnrollmentSide, transcript: &[u8]) -> Vec<u8> {
    let mut mac = mac(key);
    mac.update(&enrollment_message(side, transcript));
    mac.finalize().into_bytes().to_vec()
}

/// Constant-time check of the other side's proof.
pub fn verify_enrollment_proof(
    key: &[u8],
    side: EnrollmentSide,
    transcript: &[u8],
    proof: &[u8],
) -> Result<(), AuthProtocolError> {
    let mut mac = mac(key);
    mac.update(&enrollment_message(side, transcript));
    mac.verify_slice(proof).map_err(|_| {
        AuthProtocolError::InvalidSignature(
            "enrollment proof does not verify (different enrollment key, or a tampered or \
             replayed handshake)"
                .to_string(),
        )
    })
}

/// Signs this side's message with its identity key.
pub fn sign_enrollment(
    key: &crypto::SigningKey,
    side: EnrollmentSide,
    transcript: &[u8],
) -> Vec<u8> {
    crypto::signature_bytes(key, &enrollment_message(side, transcript))
}

/// Verifies the other side's signature over its message.
pub fn verify_enrollment_signature(
    public_key: &[u8; 32],
    side: EnrollmentSide,
    transcript: &[u8],
    signature: &[u8],
) -> Result<(), AuthProtocolError> {
    crypto::verify_signature(public_key, &enrollment_message(side, transcript), signature).map_err(
        |err| match err {
            AuthProtocolError::InvalidSignatureLength(len) => {
                AuthProtocolError::InvalidSignatureLength(len)
            }
            _ => AuthProtocolError::InvalidSignature(
                "enrollment signature does not verify for the key".to_string(),
            ),
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn proofs_bind_role_and_side() {
        let initiator = NodeId::new("operator:alice");
        let responder = NodeId::new("node-a");
        let transcript = |role| {
            EnrollmentTranscript {
                role,
                cluster: "lab",
                initiator: &initiator,
                initiator_key: &[1; 32],
                initiator_nonce: &[2; 32],
                initiator_url: "",
                responder: &responder,
                responder_key: &[3; 32],
                responder_nonce: &[4; 32],
            }
            .bytes()
        };
        let key = [9u8; 32];
        let operator = transcript(EnrollmentRole::Operator);
        let proof = enrollment_proof(&key, EnrollmentSide::Initiator, &operator);
        verify_enrollment_proof(&key, EnrollmentSide::Initiator, &operator, &proof)
            .expect("genuine proof verifies");
        assert!(
            verify_enrollment_proof(&key, EnrollmentSide::Responder, &operator, &proof).is_err()
        );
        assert!(
            verify_enrollment_proof(
                &key,
                EnrollmentSide::Initiator,
                &transcript(EnrollmentRole::Node),
                &proof
            )
            .is_err(),
            "a node proof is not an operator proof"
        );
        let signer = crypto::SigningKey::from_bytes(&[5; 32]);
        let signature = sign_enrollment(&signer, EnrollmentSide::Initiator, &operator);
        let public = signer.verifying_key().to_bytes();
        verify_enrollment_signature(&public, EnrollmentSide::Initiator, &operator, &signature)
            .expect("signature verifies");
        assert!(
            verify_enrollment_signature(&public, EnrollmentSide::Responder, &operator, &signature)
                .is_err()
        );
    }
}
