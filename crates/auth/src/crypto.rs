//! ed25519 signing and verification of peer requests and `orion+tcp` responses, and key
//! fingerprints (feature `crypto`). Shared by `orion-node` and the remote operator client so both
//! sides sign exactly the same canonical bytes.

use crate::{
    AuthProtocolError, AuthenticatedPeerRequest, PEER_REQUEST_AUTH_VERSION,
    PEER_RESPONSE_AUTH_VERSION, PeerRequestAuth, PeerRequestPayload,
    canonical_peer_request_bytes, canonical_peer_response_bytes, hex::encode_hex,
    peer_tcp::PeerResponseSignature,
};
use alloc::{
    format,
    string::{String, ToString},
    vec::Vec,
};
pub use ed25519_dalek::{SigningKey, VerifyingKey};
use ed25519_dalek::{Signature, Signer, Verifier};
use orion_core::NodeId;
use sha2::{Digest, Sha256};

/// `sha256:` followed by the first 16 bytes of SHA-256 over `public_key`, in hex. This is the
/// fingerprint `orionctl` prints for node and operator keys.
pub fn key_fingerprint(public_key: &[u8]) -> String {
    let digest = Sha256::digest(public_key);
    format!("sha256:{}", encode_hex(&digest[..16]))
}

/// Interprets `bytes` as a 32-byte public key.
pub fn public_key_array(bytes: &[u8]) -> Result<[u8; 32], AuthProtocolError> {
    bytes
        .try_into()
        .map_err(|_| AuthProtocolError::InvalidPublicKeyLength(bytes.len()))
}

fn signature(bytes: &[u8]) -> Result<Signature, AuthProtocolError> {
    let bytes: [u8; 64] = bytes
        .try_into()
        .map_err(|_| AuthProtocolError::InvalidSignatureLength(bytes.len()))?;
    Ok(Signature::from_bytes(&bytes))
}

/// Verifies an ed25519 signature over `message`.
pub fn verify_signature(
    public_key: &[u8; 32],
    message: &[u8],
    signature_bytes: &[u8],
) -> Result<(), AuthProtocolError> {
    let signature = signature(signature_bytes)?;
    VerifyingKey::from_bytes(public_key)
        .map_err(|err| AuthProtocolError::InvalidSignature(err.to_string()))?
        .verify(message, &signature)
        .map_err(|err| AuthProtocolError::InvalidSignature(err.to_string()))
}

/// Signs `payload` as `principal` (a node id, or an `operator:<name>` id) with `nonce`.
///
/// The receiver keeps a window of recently seen nonces per principal; every request must use a
/// fresh one (a counter, or random 64-bit values).
pub fn sign_peer_request(
    key: &SigningKey,
    principal: &NodeId,
    nonce: u64,
    payload: PeerRequestPayload,
) -> Result<AuthenticatedPeerRequest, AuthProtocolError> {
    let public_key = key.verifying_key().to_bytes();
    let message = canonical_peer_request_bytes(
        PEER_REQUEST_AUTH_VERSION,
        principal,
        &public_key,
        nonce,
        &payload,
    )?;
    Ok(AuthenticatedPeerRequest {
        auth: PeerRequestAuth {
            version: PEER_REQUEST_AUTH_VERSION,
            node_id: principal.clone(),
            public_key: public_key.to_vec(),
            nonce,
            signature: key.sign(&message).to_bytes().to_vec(),
        },
        payload,
    })
}

/// Checks that a request is signed by the key it carries and returns that key. Whether the key
/// is trusted for `auth.node_id` is the caller's decision.
pub fn verify_peer_request(
    auth: &PeerRequestAuth,
    payload: &PeerRequestPayload,
) -> Result<[u8; 32], AuthProtocolError> {
    let public_key = public_key_array(&auth.public_key)?;
    let message = canonical_peer_request_bytes(
        auth.version,
        &auth.node_id,
        &public_key,
        auth.nonce,
        payload,
    )?;
    verify_signature(&public_key, &message, &auth.signature)?;
    Ok(public_key)
}

/// Signs the response to `request` (the exact request frame payload) as `responder`.
pub fn sign_peer_response(
    key: &SigningKey,
    responder: &NodeId,
    request: &[u8],
    status: u8,
    body: &[u8],
) -> Result<PeerResponseSignature, AuthProtocolError> {
    let public_key = key.verifying_key().to_bytes();
    let message = canonical_peer_response_bytes(
        PEER_RESPONSE_AUTH_VERSION,
        responder,
        &public_key,
        request,
        status,
        body,
    )?;
    Ok(PeerResponseSignature {
        signature: key.sign(&message).to_bytes().to_vec(),
        public_key: public_key.to_vec(),
    })
}

/// Verifies a response of `responder` to `request` against `expected_key`, the key the caller
/// trusts for `responder`.
pub fn verify_peer_response(
    responder: &NodeId,
    expected_key: &[u8; 32],
    request: &[u8],
    status: u8,
    body: &[u8],
    signature: &PeerResponseSignature,
) -> Result<(), AuthProtocolError> {
    let presented = public_key_array(&signature.public_key)?;
    if &presented != expected_key {
        return Err(AuthProtocolError::InvalidSignature(format!(
            "response of {responder} is signed with key {}, not the trusted key {}",
            key_fingerprint(&presented),
            key_fingerprint(expected_key)
        )));
    }
    let message = canonical_peer_response_bytes(
        PEER_RESPONSE_AUTH_VERSION,
        responder,
        expected_key,
        request,
        status,
        body,
    )?;
    verify_signature(expected_key, &message, &signature.signature)
}

/// Copies the bytes of a signature (for callers that keep raw `Vec<u8>` signatures).
pub fn signature_bytes(key: &SigningKey, message: &[u8]) -> Vec<u8> {
    key.sign(message).to_bytes().to_vec()
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::boxed::Box;
    use orion_control_plane::ControlMessage;

    fn key(seed: u8) -> SigningKey {
        SigningKey::from_bytes(&[seed; 32])
    }

    #[test]
    fn requests_and_responses_verify_and_reject_tampering() {
        let operator = key(1);
        let principal = NodeId::new("operator:alice");
        let request = sign_peer_request(
            &operator,
            &principal,
            7,
            PeerRequestPayload::Control(Box::new(ControlMessage::OperatorHello)),
        )
        .expect("request signs");
        assert_eq!(
            verify_peer_request(&request.auth, &request.payload),
            Ok(operator.verifying_key().to_bytes())
        );
        let mut tampered = request.auth.clone();
        tampered.nonce = 8;
        assert!(verify_peer_request(&tampered, &request.payload).is_err());

        let node = key(2);
        let responder = NodeId::new("node-a");
        let node_key = node.verifying_key().to_bytes();
        let signature = sign_peer_response(&node, &responder, b"req", 0, b"body").expect("signs");
        verify_peer_response(&responder, &node_key, b"req", 0, b"body", &signature)
            .expect("genuine response verifies");
        assert!(
            verify_peer_response(&responder, &node_key, b"req", 0, b"bodY", &signature).is_err()
        );
        assert!(
            verify_peer_response(
                &NodeId::new("node-b"),
                &node_key,
                b"req",
                0,
                b"body",
                &signature
            )
            .is_err()
        );
        let other = key(3).verifying_key().to_bytes();
        assert!(verify_peer_response(&responder, &other, b"req", 0, b"body", &signature).is_err());
        assert!(key_fingerprint(&node_key).starts_with("sha256:"));
        assert_eq!(key_fingerprint(&node_key).len(), "sha256:".len() + 32);
    }
}
