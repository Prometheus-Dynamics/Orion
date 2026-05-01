use orion_control_plane::{ControlMessage, ObservedStateUpdate};
use orion_core::{NodeId, encode_to_vec};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const PEER_REQUEST_AUTH_VERSION: u16 = 1;
pub const PEER_REQUEST_SIGNING_DOMAIN: &[u8] = b"orion.peer.http";
pub const TRANSPORT_BINDING_VERSION: u16 = 1;
pub const TRANSPORT_BINDING_SIGNING_DOMAIN: &[u8] = b"orion.transport.binding";

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
            + std::mem::size_of::<u16>()
            + node_id.as_str().len()
            + public_key.len()
            + std::mem::size_of::<u64>()
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
            + std::mem::size_of::<u16>()
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

#[cfg(test)]
mod tests {
    use super::*;
    use orion_control_plane::{ControlMessage, PeerHello};
    use orion_core::Revision;

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
