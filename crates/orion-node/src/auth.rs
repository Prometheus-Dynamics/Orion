mod crypto;
mod modes;
mod policy;
mod store;

use crate::lock::{read_rwlock, write_rwlock};
use crate::peer::PeerConfig;
use crate::{NodeError, NodeStorage};
use crypto::{
    canonical_request_bytes, canonical_transport_binding, encode_hex, parse_hex_public_key,
    parse_public_key_bytes, parse_signature_bytes,
};
use ed25519_dalek::{Signer, SigningKey, Verifier, VerifyingKey};
pub use modes::{LocalAuthenticationMode, PeerAuthenticationMode};
use orion_control_plane::{ClientSession, PeerHello};
use orion_core::{ExecutorId, NodeId, PublicKeyHex, ResourceId};
use orion_transport_ipc::LocalAddress;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    sync::{
        Arc, RwLock,
        atomic::{AtomicU64, Ordering},
    },
};

pub use orion_auth::{
    AuthenticatedPeerRequest, NodeTransportBinding, PEER_REQUEST_AUTH_VERSION, PeerRequestAuth,
    PeerRequestPayload, TRANSPORT_BINDING_VERSION,
};
pub use policy::PeerSecurityMiddleware;
use store::{
    AuthStateWorker, load_next_outbound_nonce, load_or_create_identity, load_seen_nonces,
    load_trusted_peer_state,
};

const IDENTITY_FILE: &str = "node-identity.ed25519";
const TRUST_STORE_FILE: &str = "trusted-peers.rkyv";
const NONCE_CACHE_LIMIT: usize = 256;
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuthenticatedPeer {
    pub node_id: NodeId,
    pub public_key_hex: PublicKeyHex,
}

pub trait LocalSessionLookup: Send + Sync {
    fn session_for(&self, source: &LocalAddress) -> Option<ClientSession>;
}

pub trait PeerPolicyLookup: Send + Sync {
    fn is_configured_peer(&self, node_id: &NodeId) -> bool;

    fn observed_scope_for(&self, node_id: &NodeId) -> PeerObservedScope;
}

pub trait AuthorizationLookup: LocalSessionLookup + PeerPolicyLookup {}

impl<T> AuthorizationLookup for T where T: LocalSessionLookup + PeerPolicyLookup {}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PeerObservedScope {
    pub executor_ids: BTreeSet<ExecutorId>,
    pub resource_ids: BTreeSet<ResourceId>,
}

#[derive(Clone)]
pub struct NodeSecurity {
    mode: PeerAuthenticationMode,
    local_node_id: NodeId,
    identity: Arc<SigningKey>,
    storage: Option<NodeStorage>,
    auth_state_worker: Option<Arc<AuthStateWorker>>,
    configured_peer_keys: Arc<RwLock<BTreeMap<NodeId, Option<[u8; 32]>>>>,
    trusted_peer_state: Arc<RwLock<TrustedPeerState>>,
    seen_nonces: Arc<RwLock<BTreeMap<NodeId, VecDeque<u64>>>>,
    next_nonce: Arc<AtomicU64>,
}

// Security-state locking contract:
// - configured_peer_keys, trusted_peer_state, and seen_nonces are updated in short synchronous
//   sections only.
// - Any persistence happens after the relevant guard is dropped so auth flows never await or block
//   durable I/O while holding an RwLock guard.

#[derive(Clone, Debug, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
struct TrustedPeerStore {
    peers: BTreeMap<NodeId, Vec<u8>>,
    tls_root_certs: BTreeMap<NodeId, Vec<u8>>,
    revoked: BTreeSet<NodeId>,
    seen_nonces: BTreeMap<NodeId, Vec<u64>>,
    next_outbound_nonce: u64,
}

#[derive(Clone, Debug, Default)]
struct TrustedPeerState {
    peers: BTreeMap<NodeId, [u8; 32]>,
    tls_root_certs: BTreeMap<NodeId, Vec<u8>>,
    revoked: BTreeSet<NodeId>,
}

impl NodeSecurity {
    pub fn load_or_create(
        local_node_id: NodeId,
        mode: PeerAuthenticationMode,
        peers: &[PeerConfig],
        storage: Option<NodeStorage>,
        auth_state_worker_queue_capacity: usize,
    ) -> Result<Self, NodeError> {
        let identity = Arc::new(load_or_create_identity(storage.as_ref())?);
        let configured_peer_keys = peers
            .iter()
            .map(|peer| {
                let parsed = peer
                    .trusted_public_key_hex
                    .as_deref()
                    .map(parse_hex_public_key)
                    .transpose()?;
                Ok((peer.node_id.clone(), parsed))
            })
            .collect::<Result<BTreeMap<_, _>, NodeError>>()?;

        let trusted_peer_state = Arc::new(RwLock::new(load_trusted_peer_state(storage.as_ref())?));
        let seen_nonces = Arc::new(RwLock::new(load_seen_nonces(storage.as_ref())?));
        let next_nonce = Arc::new(AtomicU64::new(load_next_outbound_nonce(storage.as_ref())?));
        let auth_state_worker = storage
            .as_ref()
            .map(|storage| AuthStateWorker::new(storage.clone(), auth_state_worker_queue_capacity))
            .transpose()?
            .map(Arc::new);

        Ok(Self {
            mode,
            local_node_id,
            identity,
            storage,
            auth_state_worker,
            configured_peer_keys: Arc::new(RwLock::new(configured_peer_keys)),
            trusted_peer_state,
            seen_nonces,
            next_nonce,
        })
    }

    pub fn mode(&self) -> PeerAuthenticationMode {
        self.mode
    }

    pub fn public_key_hex(&self) -> PublicKeyHex {
        PublicKeyHex::new(encode_hex(&self.identity.verifying_key().to_bytes()))
    }

    pub fn transport_binding(
        &self,
        tls_cert_pem: &[u8],
    ) -> Result<NodeTransportBinding, NodeError> {
        let public_key = self.identity.verifying_key().to_bytes();
        let signature = self.identity.sign(&canonical_transport_binding(
            TRANSPORT_BINDING_VERSION,
            &self.local_node_id,
            &public_key,
            tls_cert_pem,
        )?);
        Ok(NodeTransportBinding {
            version: TRANSPORT_BINDING_VERSION,
            node_id: self.local_node_id.clone(),
            public_key: public_key.to_vec(),
            tls_cert_pem: tls_cert_pem.to_vec(),
            signature: signature.to_bytes().to_vec(),
        })
    }

    pub fn trusted_peer_public_key_hex(
        &self,
        node_id: &NodeId,
    ) -> Result<Option<PublicKeyHex>, NodeError> {
        let trusted = self.trusted_peer_state.read();
        let trusted = read_rwlock(trusted, "trusted_peer_state");
        Ok(trusted
            .peers
            .get(node_id)
            .map(|key| PublicKeyHex::new(encode_hex(key))))
    }

    pub fn trusted_peer_tls_root_cert_pem(
        &self,
        node_id: &NodeId,
    ) -> Result<Option<Vec<u8>>, NodeError> {
        let trusted = self.trusted_peer_state.read();
        let trusted = read_rwlock(trusted, "trusted_peer_state");
        Ok(trusted.tls_root_certs.get(node_id).cloned())
    }

    pub fn trusted_peer_tls_root_certs(&self) -> Result<BTreeMap<NodeId, Vec<u8>>, NodeError> {
        let trusted = self.trusted_peer_state.read();
        let trusted = read_rwlock(trusted, "trusted_peer_state");
        Ok(trusted.tls_root_certs.clone())
    }

    pub fn validate_or_update_transport_binding(
        &self,
        expected_node_id: &NodeId,
        binding: &NodeTransportBinding,
    ) -> Result<bool, NodeError> {
        if &binding.node_id != expected_node_id {
            return Err(NodeError::Authentication(format!(
                "peer transport binding claimed node {}, expected {}",
                binding.node_id, expected_node_id
            )));
        }
        let public_key = parse_public_key_bytes(&binding.public_key)?;
        let signature = parse_signature_bytes(&binding.signature)?;
        self.ensure_peer_is_not_revoked(expected_node_id)?;
        self.validate_configured_or_trusted_peer_key(expected_node_id, public_key)?;
        VerifyingKey::from_bytes(&public_key)
            .map_err(|err| NodeError::Authentication(err.to_string()))?
            .verify(
                &canonical_transport_binding(
                    binding.version,
                    &binding.node_id,
                    &public_key,
                    &binding.tls_cert_pem,
                )?,
                &signature,
            )
            .map_err(|err| NodeError::Authentication(err.to_string()))?;

        let trusted = self.trusted_peer_state.write();
        let mut trusted = write_rwlock(trusted, "trusted_peer_state");
        let changed = trusted
            .tls_root_certs
            .get(expected_node_id)
            .map(|existing| existing != &binding.tls_cert_pem)
            .unwrap_or(true);
        trusted
            .tls_root_certs
            .insert(expected_node_id.clone(), binding.tls_cert_pem.clone());
        drop(trusted);
        self.persist_security_state()?;
        Ok(changed)
    }

    pub fn is_peer_revoked(&self, node_id: &NodeId) -> Result<bool, NodeError> {
        let trusted = self.trusted_peer_state.read();
        let trusted = read_rwlock(trusted, "trusted_peer_state");
        Ok(trusted.revoked.contains(node_id))
    }

    pub fn revoke_peer(&self, node_id: &NodeId) -> Result<bool, NodeError> {
        let changed = {
            let trusted = self.trusted_peer_state.write();
            let mut trusted = write_rwlock(trusted, "trusted_peer_state");
            let removed = trusted.peers.remove(node_id).is_some();
            let removed_cert = trusted.tls_root_certs.remove(node_id).is_some();
            let revoked = trusted.revoked.insert(node_id.clone());
            removed || removed_cert || revoked
        };
        self.clear_seen_nonces(node_id)?;
        if changed {
            self.persist_security_state()?;
        }
        Ok(changed)
    }

    pub fn replace_trusted_peer_key(
        &self,
        node_id: &NodeId,
        public_key: [u8; 32],
    ) -> Result<(), NodeError> {
        {
            let trusted = self.trusted_peer_state.write();
            let mut trusted = write_rwlock(trusted, "trusted_peer_state");
            trusted.revoked.remove(node_id);
            trusted.peers.insert(node_id.clone(), public_key);
            trusted.tls_root_certs.remove(node_id);
        }
        self.clear_seen_nonces(node_id)?;
        self.persist_security_state()
    }

    pub fn replace_trusted_peer_key_hex(
        &self,
        node_id: &NodeId,
        public_key_hex: &str,
    ) -> Result<(), NodeError> {
        self.replace_trusted_peer_key(node_id, parse_hex_public_key(public_key_hex)?)
    }

    pub fn trust_peer_tls_root_cert_pem(
        &self,
        node_id: &NodeId,
        tls_root_cert_pem: Vec<u8>,
    ) -> Result<(), NodeError> {
        {
            let trusted = self.trusted_peer_state.write();
            let mut trusted = write_rwlock(trusted, "trusted_peer_state");
            trusted
                .tls_root_certs
                .insert(node_id.clone(), tls_root_cert_pem);
        }
        self.persist_security_state()
    }

    pub fn configured_peer_public_key_hex(
        &self,
        node_id: &NodeId,
    ) -> Result<Option<PublicKeyHex>, NodeError> {
        let configured = self.configured_peer_keys.read();
        let configured = read_rwlock(configured, "configured_peer_keys");
        Ok(configured.get(node_id).and_then(|key| {
            key.as_ref()
                .map(|bytes| PublicKeyHex::new(encode_hex(bytes)))
        }))
    }

    pub fn configured_peer_node_ids(&self) -> Result<BTreeSet<NodeId>, NodeError> {
        let configured = self.configured_peer_keys.read();
        let configured = read_rwlock(configured, "configured_peer_keys");
        Ok(configured.keys().cloned().collect())
    }

    pub fn trust_store_node_ids(&self) -> Result<BTreeSet<NodeId>, NodeError> {
        let trusted = self.trusted_peer_state.read();
        let trusted = read_rwlock(trusted, "trusted_peer_state");
        Ok(trusted
            .peers
            .keys()
            .chain(trusted.revoked.iter())
            .cloned()
            .collect())
    }

    pub fn configure_peer(&self, peer: &PeerConfig) -> Result<(), NodeError> {
        let parsed = peer
            .trusted_public_key_hex
            .as_deref()
            .map(parse_hex_public_key)
            .transpose()?;
        let configured = self.configured_peer_keys.write();
        let mut configured = write_rwlock(configured, "configured_peer_keys");
        configured.insert(peer.node_id.clone(), parsed);
        Ok(())
    }

    pub fn wrap_http_payload(
        &self,
        payload: orion::transport::http::HttpRequestPayload,
    ) -> Result<orion::transport::http::HttpRequestPayload, NodeError> {
        if self.mode == PeerAuthenticationMode::Disabled {
            return Ok(payload);
        }

        let peer_payload = match payload {
            orion::transport::http::HttpRequestPayload::Control(message) => {
                PeerRequestPayload::Control(message)
            }
            orion::transport::http::HttpRequestPayload::ObservedUpdate(update) => {
                PeerRequestPayload::ObservedUpdate(update)
            }
            orion::transport::http::HttpRequestPayload::AuthenticatedPeer(request) => {
                return Ok(orion::transport::http::HttpRequestPayload::AuthenticatedPeer(request));
            }
        };

        let request = self.build_authenticated_http_request(peer_payload)?;
        self.persist_security_state()?;
        Ok(request)
    }

    pub async fn wrap_http_payload_async(
        &self,
        payload: orion::transport::http::HttpRequestPayload,
    ) -> Result<orion::transport::http::HttpRequestPayload, NodeError> {
        if self.mode == PeerAuthenticationMode::Disabled {
            return Ok(payload);
        }

        let peer_payload = match payload {
            orion::transport::http::HttpRequestPayload::Control(message) => {
                PeerRequestPayload::Control(message)
            }
            orion::transport::http::HttpRequestPayload::ObservedUpdate(update) => {
                PeerRequestPayload::ObservedUpdate(update)
            }
            orion::transport::http::HttpRequestPayload::AuthenticatedPeer(request) => {
                return Ok(orion::transport::http::HttpRequestPayload::AuthenticatedPeer(request));
            }
        };

        let request = self.build_authenticated_http_request(peer_payload)?;
        self.persist_security_state_async().await?;
        Ok(request)
    }

    fn build_authenticated_http_request(
        &self,
        peer_payload: PeerRequestPayload,
    ) -> Result<orion::transport::http::HttpRequestPayload, NodeError> {
        let nonce = self.next_nonce.fetch_add(1, Ordering::SeqCst);
        let public_key = self.identity.verifying_key().to_bytes();
        let signature = self.identity.sign(&canonical_request_bytes(
            PEER_REQUEST_AUTH_VERSION,
            &self.local_node_id,
            &public_key,
            nonce,
            &peer_payload,
        )?);

        Ok(
            orion::transport::http::HttpRequestPayload::AuthenticatedPeer(
                AuthenticatedPeerRequest {
                    auth: PeerRequestAuth {
                        version: PEER_REQUEST_AUTH_VERSION,
                        node_id: self.local_node_id.clone(),
                        public_key: public_key.to_vec(),
                        nonce,
                        signature: signature.to_bytes().to_vec(),
                    },
                    payload: peer_payload,
                },
            ),
        )
    }

    fn authenticate_request(
        &self,
        auth: &PeerRequestAuth,
        payload: &PeerRequestPayload,
    ) -> Result<AuthenticatedPeer, NodeError> {
        let public_key = parse_public_key_bytes(&auth.public_key)?;
        let signature = parse_signature_bytes(&auth.signature)?;
        self.ensure_peer_is_not_revoked(&auth.node_id)?;
        self.validate_configured_or_trusted_peer_key(&auth.node_id, public_key)?;

        VerifyingKey::from_bytes(&public_key)
            .map_err(|err| NodeError::Authentication(err.to_string()))?
            .verify(
                &canonical_request_bytes(
                    auth.version,
                    &auth.node_id,
                    &public_key,
                    auth.nonce,
                    payload,
                )?,
                &signature,
            )
            .map_err(|err| NodeError::Authentication(err.to_string()))?;

        self.record_and_validate_nonce(&auth.node_id, auth.nonce)?;

        Ok(AuthenticatedPeer {
            node_id: auth.node_id.clone(),
            public_key_hex: PublicKeyHex::new(encode_hex(&public_key)),
        })
    }

    fn validate_configured_or_trusted_peer_key(
        &self,
        node_id: &NodeId,
        public_key: [u8; 32],
    ) -> Result<(), NodeError> {
        let configured = self.configured_peer_keys.read();
        let configured = read_rwlock(configured, "configured_peer_keys");
        let configured_entry = configured.get(node_id).cloned();
        drop(configured);
        match configured_entry {
            Some(Some(expected)) => {
                if expected != public_key {
                    return Err(NodeError::Authentication(format!(
                        "peer {} public key mismatch",
                        node_id
                    )));
                }
                Ok(())
            }
            Some(None) => self.pin_or_validate_trusted_peer(node_id, public_key),
            None if self.mode == PeerAuthenticationMode::Required => Err(NodeError::Authorization(
                format!("unknown peer {}", node_id),
            )),
            None => self.pin_or_validate_trusted_peer(node_id, public_key),
        }
    }

    fn pin_or_validate_trusted_peer(
        &self,
        node_id: &NodeId,
        public_key: [u8; 32],
    ) -> Result<(), NodeError> {
        let trusted = self.trusted_peer_state.write();
        let mut trusted = write_rwlock(trusted, "trusted_peer_state");

        if trusted.revoked.contains(node_id) {
            return Err(NodeError::Authorization(format!(
                "peer {} is revoked",
                node_id
            )));
        }

        match trusted.peers.get(node_id) {
            Some(existing) if existing != &public_key => Err(NodeError::Authentication(format!(
                "peer {} presented a different trusted key",
                node_id
            ))),
            Some(_) => Ok(()),
            None => {
                trusted.peers.insert(node_id.clone(), public_key);
                drop(trusted);
                self.persist_security_state()
            }
        }
    }

    fn record_and_validate_nonce(&self, node_id: &NodeId, nonce: u64) -> Result<(), NodeError> {
        let nonces = self.seen_nonces.write();
        let mut nonces = write_rwlock(nonces, "seen_nonces");
        let peer_nonces = nonces.entry(node_id.clone()).or_default();
        if peer_nonces.contains(&nonce) {
            return Err(NodeError::Authentication(format!(
                "replayed nonce {nonce} from peer {}",
                node_id
            )));
        }
        peer_nonces.push_back(nonce);
        if peer_nonces.len() > NONCE_CACHE_LIMIT {
            peer_nonces.pop_front();
        }
        drop(nonces);
        self.persist_security_state()?;
        Ok(())
    }

    fn clear_seen_nonces(&self, node_id: &NodeId) -> Result<(), NodeError> {
        let nonces = self.seen_nonces.write();
        let mut nonces = write_rwlock(nonces, "seen_nonces");
        nonces.remove(node_id);
        Ok(())
    }

    fn ensure_peer_is_not_revoked(&self, node_id: &NodeId) -> Result<(), NodeError> {
        let trusted = self.trusted_peer_state.read();
        let trusted = read_rwlock(trusted, "trusted_peer_state");
        if trusted.revoked.contains(node_id) {
            return Err(NodeError::Authorization(format!(
                "peer {} is revoked",
                node_id
            )));
        }
        Ok(())
    }
}

pub(super) fn transport_binding_from_hello(
    hello: &PeerHello,
) -> Result<Option<NodeTransportBinding>, NodeError> {
    match (
        hello.transport_binding_version,
        hello.transport_binding_public_key.clone(),
        hello.transport_tls_cert_pem.clone(),
        hello.transport_binding_signature.clone(),
    ) {
        (None, None, None, None) => Ok(None),
        (Some(version), Some(public_key), Some(tls_cert_pem), Some(signature)) => {
            Ok(Some(NodeTransportBinding {
                version,
                node_id: hello.node_id.clone(),
                public_key,
                tls_cert_pem,
                signature,
            }))
        }
        _ => Err(NodeError::Authentication(
            "peer hello contained a partial transport binding".into(),
        )),
    }
}

#[cfg(test)]
mod tests;
