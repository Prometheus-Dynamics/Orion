use crate::NodeError;
use ed25519_dalek::Signature;
use orion_auth::{
    PeerRequestPayload, canonical_peer_request_bytes, canonical_transport_binding_bytes,
};
use orion_core::NodeId;

const ED25519_PUBLIC_KEY_HEX_LEN: usize = 64;
const ED25519_SIGNATURE_LEN: usize = 64;

pub(super) fn canonical_request_bytes(
    version: u16,
    node_id: &NodeId,
    public_key: &[u8; 32],
    nonce: u64,
    payload: &PeerRequestPayload,
) -> Result<Vec<u8>, NodeError> {
    canonical_peer_request_bytes(version, node_id, public_key, nonce, payload)
        .map_err(|err| NodeError::Authentication(err.to_string()))
}

pub(super) fn canonical_transport_binding(
    version: u16,
    node_id: &NodeId,
    public_key: &[u8; 32],
    tls_cert_pem: &[u8],
) -> Result<Vec<u8>, NodeError> {
    canonical_transport_binding_bytes(version, node_id, public_key, tls_cert_pem)
        .map_err(|err| NodeError::Authentication(err.to_string()))
}

pub(super) fn parse_public_key_bytes(bytes: &[u8]) -> Result<[u8; 32], NodeError> {
    bytes
        .try_into()
        .map_err(|_| NodeError::InvalidPublicKeyLength)
}

pub(super) fn parse_signature_bytes(bytes: &[u8]) -> Result<Signature, NodeError> {
    let signature_bytes: [u8; ED25519_SIGNATURE_LEN] = bytes
        .try_into()
        .map_err(|_| NodeError::InvalidSignatureLength)?;
    Ok(Signature::from_bytes(&signature_bytes))
}

pub(super) fn parse_hex_public_key(value: &str) -> Result<[u8; 32], NodeError> {
    let trimmed = value.trim();
    if trimmed.len() != ED25519_PUBLIC_KEY_HEX_LEN {
        return Err(NodeError::Storage(format!(
            "trusted peer public key hex must be {ED25519_PUBLIC_KEY_HEX_LEN} characters"
        )));
    }
    let mut bytes = [0_u8; 32];
    for (index, chunk) in trimmed.as_bytes().chunks_exact(2).enumerate() {
        bytes[index] = decode_hex_pair(chunk[0], chunk[1])?;
    }
    Ok(bytes)
}

pub(super) fn encode_hex(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(hex_char(byte >> 4));
        output.push(hex_char(byte & 0x0f));
    }
    output
}

fn decode_hex_pair(high: u8, low: u8) -> Result<u8, NodeError> {
    Ok((decode_hex_nibble(high)? << 4) | decode_hex_nibble(low)?)
}

fn decode_hex_nibble(value: u8) -> Result<u8, NodeError> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        b'A'..=b'F' => Ok(value - b'A' + 10),
        _ => Err(NodeError::InvalidHexPublicKey),
    }
}

fn hex_char(value: u8) -> char {
    match value {
        0..=9 => char::from(b'0' + value),
        10..=15 => char::from(b'a' + (value - 10)),
        _ => unreachable!("hex nibble must be in range 0..=15"),
    }
}

pub(super) fn current_effective_uid() -> u32 {
    // SAFETY: `geteuid` has no preconditions and simply returns the current process euid.
    unsafe { libc::geteuid() }
}

pub(super) fn current_effective_gid() -> u32 {
    // SAFETY: `getegid` has no preconditions and simply returns the current process egid.
    unsafe { libc::getegid() }
}
