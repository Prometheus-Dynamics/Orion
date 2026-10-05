//! Turning a discovered peer into an enrolled sync peer, restoring enrollments at startup, and
//! forgetting them on removal.

use super::{
    advert::{hex, parse_key_hex},
    store::{self, EnrolledPeer, EnrollmentMethod},
};
use crate::{NodeApp, NodeError, PeerConfig, PeerTransportKind};
use orion::NodeId;
use orion_control_plane::{DiscoveredPeerEnrollment, DiscoveredPeerState};
use orion_core::{PeerBaseUrl, PublicKeyHex};
use tracing::{info, warn};

impl NodeApp {
    /// Pins `public_key` for `node_id`, registers the peer at `base_url` for sync and records the
    /// enrollment in `discovered-peers.json`.
    ///
    /// Shared-key enrollments never override an operator decision: they fail for removed
    /// (revoked) peers and for peers already trusted with another key. Operator enrollments
    /// replace the trusted key and lift a revocation.
    pub(crate) fn enroll_trusted_peer(
        &self,
        node_id: &NodeId,
        public_key: [u8; 32],
        base_url: PeerBaseUrl,
        method: EnrollmentMethod,
    ) -> Result<(), NodeError> {
        PeerTransportKind::check_supported(base_url.as_str()).map_err(NodeError::Config)?;
        let key_hex = hex(&public_key);
        let revoked = self.security.is_peer_revoked(node_id)?;
        let trusted = self.trusted_key_hex(node_id);
        let same_key = trusted
            .as_deref()
            .is_some_and(|trusted| trusted.eq_ignore_ascii_case(&key_hex));
        if method == EnrollmentMethod::EnrollmentKey {
            if revoked {
                return Err(NodeError::Authorization(format!(
                    "peer {node_id} was removed by an operator; enroll it with `orionctl peers \
                     enroll {node_id}` to trust it again"
                )));
            }
            if trusted.is_some() && !same_key {
                return Err(NodeError::Authentication(format!(
                    "peer {node_id} is already trusted with a different key"
                )));
            }
        }
        if revoked
            || !same_key
            || self
                .security
                .trusted_peer_public_key_hex(node_id)?
                .is_none()
        {
            self.security
                .replace_trusted_peer_key(node_id, public_key)?;
        }
        self.enroll_peer(
            PeerConfig::new(node_id.clone(), base_url.clone())
                .with_trusted_public_key_hex(PublicKeyHex::new(key_hex.clone())),
        )?;
        if let Some(state_dir) = self.state_dir() {
            store::upsert(
                state_dir,
                EnrolledPeer {
                    node_id: node_id.to_string(),
                    base_url: base_url.to_string(),
                    public_key_hex: key_hex,
                    method,
                    enrolled_at_ms: Self::current_time_ms(),
                },
            )?;
        }
        Ok(())
    }

    /// Re-registers the peers recorded in `discovered-peers.json`, skipping removed peers and
    /// peers that are already configured (for example in `ORION_NODE_PEERS`).
    pub fn restore_discovered_enrollments(&self) -> Result<usize, NodeError> {
        let Some(state_dir) = self.state_dir() else {
            return Ok(0);
        };
        let mut restored = 0;
        for record in store::load(state_dir)? {
            let node_id = NodeId::try_new(record.node_id.clone()).map_err(|err| {
                NodeError::Storage(format!("invalid node id in discovered peer store: {err}"))
            })?;
            if self.security.is_peer_revoked(&node_id)? || self.is_registered_peer(&node_id) {
                continue;
            }
            let key = parse_key_hex(&record.public_key_hex).map_err(NodeError::Storage)?;
            if let Some(trusted) = self.trusted_key_hex(&node_id)
                && !trusted.eq_ignore_ascii_case(&record.public_key_hex)
            {
                warn!(node = %self.config.node_id, peer = %node_id, "not restoring discovered enrollment: the trust store holds another key");
                continue;
            }
            if self
                .security
                .trusted_peer_public_key_hex(&node_id)?
                .is_none()
            {
                self.security.replace_trusted_peer_key(&node_id, key)?;
            }
            self.register_peer(
                PeerConfig::new(node_id.clone(), PeerBaseUrl::new(record.base_url.clone()))
                    .with_trusted_public_key_hex(PublicKeyHex::new(record.public_key_hex.clone())),
            )?;
            restored += 1;
        }
        if restored > 0 {
            info!(node = %self.config.node_id, restored, "restored peers enrolled through discovery");
        }
        Ok(restored)
    }

    /// Operator approval of a discovered peer (`orionctl peers enroll <node-id>`).
    pub(crate) fn enroll_discovered_peer(
        &self,
        request: DiscoveredPeerEnrollment,
    ) -> Result<(), NodeError> {
        let state = self.discovery_state().ok_or_else(|| {
            NodeError::Config(
                "peer discovery is not running on this node (ORION_NODE_DISCOVERY=mdns)".into(),
            )
        })?;
        state.record_attempt();
        let result = (|| {
            let peer = state
                .registry()
                .get(&request.node_id)
                .cloned()
                .ok_or_else(|| {
                    NodeError::Config(format!(
                        "peer {} has not been discovered (see `orionctl get discovered-peers`)",
                        request.node_id
                    ))
                })?;
            if self.classify_discovered_peer(&peer) == DiscoveredPeerState::Incompatible {
                return Err(NodeError::Config(format!(
                    "peer {} speaks control protocol v{}; upgrade it before enrolling",
                    request.node_id, peer.advertisement.control_protocol_version
                )));
            }
            let fingerprint = peer.key_fingerprint();
            if let Some(expected) = &request.expected_key_fingerprint
                && expected != &fingerprint
            {
                return Err(NodeError::Authentication(format!(
                    "the advertised key of {} changed: its fingerprint is now {fingerprint}, \
                     not {expected}",
                    request.node_id
                )));
            }
            let url = peer.preferred_url().cloned().ok_or_else(|| {
                NodeError::Config(format!(
                    "peer {} advertises no URL this build can sync with",
                    request.node_id
                ))
            })?;
            self.enroll_trusted_peer(
                &request.node_id,
                peer.advertisement.public_key,
                url,
                EnrollmentMethod::Operator,
            )
        })();
        state.record_outcome(result.is_ok());
        if result.is_ok() {
            info!(node = %self.config.node_id, peer = %request.node_id, "operator enrolled discovered peer");
        }
        result
    }

    /// Drops `node_id` from `discovered-peers.json`.
    pub(crate) fn forget_discovered_enrollment(&self, node_id: &NodeId) -> Result<(), NodeError> {
        if let Some(state_dir) = self.state_dir() {
            store::remove(state_dir, node_id.as_str())?;
        }
        Ok(())
    }

    /// Follows a new address of an enrolled peer (DHCP, a moved device). Safe because every
    /// response is verified against the pinned key: a spoofed address can only make sync fail.
    pub(crate) fn follow_enrolled_peer_address(&self, peer: &super::registry::DiscoveredPeer) {
        if self.classify_discovered_peer(peer) != DiscoveredPeerState::Enrolled {
            return;
        }
        let Some(url) = peer.preferred_url().cloned() else {
            return;
        };
        let node_id = peer.node_id();
        if self.registered_peer_base_url(node_id).as_ref() == Some(&url) {
            return;
        }
        let Some(state_dir) = self.state_dir() else {
            return;
        };
        // Only peers enrolled through discovery follow announcements; static peers keep their
        // configured URL.
        let Ok(records) = store::load(state_dir) else {
            return;
        };
        let Some(record) = records
            .into_iter()
            .find(|record| record.node_id == node_id.as_str())
        else {
            return;
        };
        let result = self.enroll_trusted_peer(
            node_id,
            peer.advertisement.public_key,
            url.clone(),
            record.method,
        );
        match result {
            Ok(()) => {
                info!(node = %self.config.node_id, peer = %node_id, url = %url, "enrolled peer moved to a new address")
            }
            Err(err) => {
                warn!(node = %self.config.node_id, peer = %node_id, error = %err, "failed to follow the new address of an enrolled peer")
            }
        }
    }
}
