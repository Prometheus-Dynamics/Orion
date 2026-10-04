#![cfg_attr(not(feature = "std"), no_std)]

extern crate alloc;

use alloc::{
    boxed::Box,
    string::{String, ToString},
    vec::Vec,
};
use orion_control_plane::{ControlMessage, ObservedStateUpdate};
use orion_core::{NodeId, encode_to_vec};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const PEER_REQUEST_AUTH_VERSION: u16 = 1;
pub const PEER_REQUEST_SIGNING_DOMAIN: &[u8] = b"orion.peer.http";
pub const TRANSPORT_BINDING_VERSION: u16 = 1;
pub const TRANSPORT_BINDING_SIGNING_DOMAIN: &[u8] = b"orion.transport.binding";
/// Version of the response signature used by the `orion+tcp` peer transport.
pub const PEER_RESPONSE_AUTH_VERSION: u16 = 1;
pub const PEER_RESPONSE_SIGNING_DOMAIN: &[u8] = b"orion.peer.tcp.response";

#[derive(Debug, Error)]
pub enum AuthProtocolError {
    #[error("unsupported peer auth version {0}")]
    UnsupportedVersion(u16),
    #[error("invalid public key length {0}, expected 32 bytes")]
    InvalidPublicKeyLength(usize),
    #[error("failed to encode peer auth payload: {0}")]
    Encode(String),
    #[error("invalid signature length {0}, expected 64 bytes")]
    InvalidSignatureLength(usize),
}

#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct PeerRequestAuth {
    pub version: u16,
    pub node_id: NodeId,
    pub public_key: Vec<u8>,
    pub nonce: u64,
    pub signature: Vec<u8>,
}

#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum PeerRequestPayload {
    Control(Box<ControlMessage>),
    ObservedUpdate(ObservedStateUpdate),
}

#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct AuthenticatedPeerRequest {
    pub auth: PeerRequestAuth,
    pub payload: PeerRequestPayload,
}

#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct NodeTransportBinding {
    pub version: u16,
    pub node_id: NodeId,
    pub public_key: Vec<u8>,
    pub tls_cert_pem: Vec<u8>,
    pub signature: Vec<u8>,
}

pub fn canonical_peer_request_bytes(
    version: u16,
    node_id: &NodeId,
    public_key: &[u8],
    nonce: u64,
    payload: &PeerRequestPayload,
) -> Result<Vec<u8>, AuthProtocolError> {
    if version != PEER_REQUEST_AUTH_VERSION {
        return Err(AuthProtocolError::UnsupportedVersion(version));
    }
    if public_key.len() != 32 {
        return Err(AuthProtocolError::InvalidPublicKeyLength(public_key.len()));
    }

    let payload_bytes =
        encode_to_vec(payload).map_err(|err| AuthProtocolError::Encode(err.to_string()))?;
    let mut bytes = Vec::with_capacity(
        PEER_REQUEST_SIGNING_DOMAIN.len()
            + core::mem::size_of::<u16>()
            + node_id.as_str().len()
            + public_key.len()
            + core::mem::size_of::<u64>()
            + payload_bytes.len(),
    );
    bytes.extend_from_slice(PEER_REQUEST_SIGNING_DOMAIN);
    bytes.extend_from_slice(&version.to_le_bytes());
    bytes.extend_from_slice(node_id.as_str().as_bytes());
    bytes.extend_from_slice(public_key);
    bytes.extend_from_slice(&nonce.to_le_bytes());
    bytes.extend_from_slice(&payload_bytes);
    Ok(bytes)
}

pub fn canonical_transport_binding_bytes(
    version: u16,
    node_id: &NodeId,
    public_key: &[u8],
    tls_cert_pem: &[u8],
) -> Result<Vec<u8>, AuthProtocolError> {
    if version != TRANSPORT_BINDING_VERSION {
        return Err(AuthProtocolError::UnsupportedVersion(version));
    }
    if public_key.len() != 32 {
        return Err(AuthProtocolError::InvalidPublicKeyLength(public_key.len()));
    }

    let mut bytes = Vec::with_capacity(
        TRANSPORT_BINDING_SIGNING_DOMAIN.len()
            + core::mem::size_of::<u16>()
            + node_id.as_str().len()
            + public_key.len()
            + tls_cert_pem.len(),
    );
    bytes.extend_from_slice(TRANSPORT_BINDING_SIGNING_DOMAIN);
    bytes.extend_from_slice(&version.to_le_bytes());
    bytes.extend_from_slice(node_id.as_str().as_bytes());
    bytes.extend_from_slice(public_key);
    bytes.extend_from_slice(tls_cert_pem);
    Ok(bytes)
}

/// Bytes a peer signs to authenticate a response on the `orion+tcp` transport.
///
/// The signature binds the responder's node id and public key, the exact request frame payload
/// it answers (which carries the request's nonce and signature), the response status and the
/// response body:
///
/// ```text
/// domain || version u16 LE || node_id_len u16 LE || node_id || public_key (32)
///        || request_len u32 LE || request || status u8 || body
/// ```
pub fn canonical_peer_response_bytes(
    version: u16,
    responder: &NodeId,
    public_key: &[u8],
    request: &[u8],
    status: u8,
    body: &[u8],
) -> Result<Vec<u8>, AuthProtocolError> {
    if version != PEER_RESPONSE_AUTH_VERSION {
        return Err(AuthProtocolError::UnsupportedVersion(version));
    }
    if public_key.len() != 32 {
        return Err(AuthProtocolError::InvalidPublicKeyLength(public_key.len()));
    }
    let node_id = responder.as_str().as_bytes();
    let node_id_len = u16::try_from(node_id.len())
        .map_err(|_| AuthProtocolError::Encode("responder node id is too long".to_string()))?;
    let request_len = u32::try_from(request.len())
        .map_err(|_| AuthProtocolError::Encode("request is too large".to_string()))?;
    let mut bytes = Vec::with_capacity(
        PEER_RESPONSE_SIGNING_DOMAIN.len()
            + 2
            + 2
            + node_id.len()
            + public_key.len()
            + 4
            + request.len()
            + 1
            + body.len(),
    );
    bytes.extend_from_slice(PEER_RESPONSE_SIGNING_DOMAIN);
    bytes.extend_from_slice(&version.to_le_bytes());
    bytes.extend_from_slice(&node_id_len.to_le_bytes());
    bytes.extend_from_slice(node_id);
    bytes.extend_from_slice(public_key);
    bytes.extend_from_slice(&request_len.to_le_bytes());
    bytes.extend_from_slice(request);
    bytes.push(status);
    bytes.extend_from_slice(body);
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use orion_control_plane::{ControlMessage, PeerHello};

    use orion_core::Revision;

    #[test]
    fn canonical_peer_response_bytes_bind_request_status_and_body() {
        let node = NodeId::new("node-b");
        let bytes = canonical_peer_response_bytes(
            PEER_RESPONSE_AUTH_VERSION,
            &node,
            &[7; 32],
            b"request",
            0,
            b"body",
        )
        .expect("canonical response bytes should encode");
        assert!(bytes.starts_with(PEER_RESPONSE_SIGNING_DOMAIN));
        assert!(bytes.ends_with(b"request\0body"));
        let other_status = canonical_peer_response_bytes(
            PEER_RESPONSE_AUTH_VERSION,
            &node,
            &[7; 32],
            b"request",
            1,
            b"body",
        )
        .expect("canonical response bytes should encode");
        assert_ne!(bytes, other_status);
        assert!(
            canonical_peer_response_bytes(9, &node, &[7; 32], b"", 0, b"").is_err(),
            "unknown versions are rejected"
        );
    }

    #[test]
    fn canonical_peer_request_bytes_include_version_and_domain() {
        let payload = PeerRequestPayload::Control(Box::new(ControlMessage::Hello(PeerHello {
            node_id: NodeId::new("node-a"),
            desired_revision: Revision::new(1),
            desired_fingerprint: 7,
            desired_section_fingerprints: orion_control_plane::DesiredStateSectionFingerprints {
                nodes: 1,
                artifacts: 2,
                workloads: 3,
                resources: 4,
                providers: 5,
                executors: 6,
                leases: 7,
            },
            observed_revision: Revision::ZERO,
            applied_revision: Revision::ZERO,
            transport_binding_version: None,
            transport_binding_public_key: None,
            transport_tls_cert_pem: None,
            transport_binding_signature: None,
        })));
        let bytes = canonical_peer_request_bytes(
            PEER_REQUEST_AUTH_VERSION,
            &NodeId::new("node-a"),
            &[9; 32],
            42,
            &payload,
        )
        .expect("canonical bytes should encode");

        assert!(bytes.starts_with(PEER_REQUEST_SIGNING_DOMAIN));
        assert_eq!(
            &bytes[PEER_REQUEST_SIGNING_DOMAIN.len()..PEER_REQUEST_SIGNING_DOMAIN.len() + 2],
            &PEER_REQUEST_AUTH_VERSION.to_le_bytes()
        );
    }

    #[test]
    fn canonical_transport_binding_bytes_include_version_and_domain() {
        let bytes = canonical_transport_binding_bytes(
            TRANSPORT_BINDING_VERSION,
            &NodeId::new("node-a"),
            &[9; 32],
            b"-----BEGIN CERTIFICATE-----\n...",
        )
        .expect("canonical transport binding bytes should encode");

        assert!(bytes.starts_with(TRANSPORT_BINDING_SIGNING_DOMAIN));
        assert_eq!(
            &bytes[TRANSPORT_BINDING_SIGNING_DOMAIN.len()
                ..TRANSPORT_BINDING_SIGNING_DOMAIN.len() + 2],
            &TRANSPORT_BINDING_VERSION.to_le_bytes()
        );
    }
}
