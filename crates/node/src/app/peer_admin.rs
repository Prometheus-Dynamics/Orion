//! Peer removal and the control-message entry points of discovery and enrollment
//! (`docs/discovery.md`). Builds without the `discovery-mdns` feature answer the discovery
//! messages with a clear error.

use super::{AuditEventKind, NodeApp, NodeError};
use orion::{
    NodeId,
    control_plane::{
        DiscoveredPeerEnrollment, DiscoveryMetricsSnapshot, DiscoverySnapshot, EnrollmentChallenge,
        EnrollmentConfirm, EnrollmentHello,
    },
};
use orion_core::PeerBaseUrl;
use std::path::Path;

#[cfg(not(feature = "discovery-mdns"))]
fn discovery_not_compiled() -> NodeError {
    NodeError::Config(
        "peer discovery is not available: orion-node was built without the `discovery-mdns` \
         feature"
            .into(),
    )
}

impl NodeApp {
    /// Whether `node_id` is registered for peer sync.
    #[cfg_attr(not(feature = "discovery-mdns"), allow(dead_code))]
    pub(crate) fn is_registered_peer(&self, node_id: &NodeId) -> bool {
        self.peers_read().contains_key(node_id)
    }

    /// Base URL `node_id` is synced at, if registered.
    #[cfg_attr(not(feature = "discovery-mdns"), allow(dead_code))]
    pub(crate) fn registered_peer_base_url(&self, node_id: &NodeId) -> Option<PeerBaseUrl> {
        self.peers_read()
            .get(node_id)
            .map(|peer| peer.base_url.clone())
    }

    /// The node's state directory, when it persists state.
    #[cfg_attr(not(feature = "discovery-mdns"), allow(dead_code))]
    pub(crate) fn state_dir(&self) -> Option<&Path> {
        self.storage.as_ref().map(|storage| storage.root())
    }

    /// Removes a peer (`orionctl peers remove`): revokes its key (persisted, so neither a
    /// restart nor shared-key enrollment trusts it again), stops syncing with it and forgets a
    /// discovery enrollment. Enrolling it again with `orionctl peers enroll` lifts the revocation.
    /// Returns whether anything changed.
    pub fn remove_peer(&self, node_id: &NodeId) -> Result<bool, NodeError> {
        let revoked = self.security.revoke_peer(node_id)?;
        let unconfigured = self.security.unconfigure_peer(node_id)?;
        let unregistered = self.with_peers_mut(|peers| peers.remove(node_id).is_some());
        self.evict_peer_client(node_id);
        #[cfg(feature = "peer-tcp")]
        self.peer_tcp_clients_lock().remove(node_id);
        #[cfg(feature = "discovery-mdns")]
        self.forget_discovered_enrollment(node_id)?;
        let changed = revoked || unconfigured || unregistered;
        if changed {
            self.record_audit_event(
                AuditEventKind::PeerRemoved,
                Some(node_id.as_str().to_owned()),
                format!("removed peer `{node_id}` (key revoked, sync stopped)"),
            );
        }
        Ok(changed)
    }

    /// Answer to `QueryDiscovery`.
    pub(crate) fn query_discovery(&self) -> DiscoverySnapshot {
        #[cfg(feature = "discovery-mdns")]
        return self.discovery_snapshot();
        #[cfg(not(feature = "discovery-mdns"))]
        DiscoverySnapshot::default()
    }

    /// Discovery counters for the observability snapshot.
    pub(crate) fn discovery_metrics_snapshot(&self) -> DiscoveryMetricsSnapshot {
        #[cfg(feature = "discovery-mdns")]
        return self.discovery_metrics();
        #[cfg(not(feature = "discovery-mdns"))]
        DiscoveryMetricsSnapshot::default()
    }

    /// `EnrollDiscoveredPeer` from a local operator.
    pub(crate) fn approve_discovered_peer(
        &self,
        request: DiscoveredPeerEnrollment,
    ) -> Result<(), NodeError> {
        #[cfg(feature = "discovery-mdns")]
        return self.enroll_discovered_peer(request);
        #[cfg(not(feature = "discovery-mdns"))]
        {
            let _ = request;
            Err(discovery_not_compiled())
        }
    }

    /// `EnrollmentHello` from a peer.
    pub(crate) fn answer_enrollment_hello(
        &self,
        hello: EnrollmentHello,
    ) -> Result<EnrollmentChallenge, NodeError> {
        #[cfg(feature = "discovery-mdns")]
        return self.serve_enrollment_hello(hello);
        #[cfg(not(feature = "discovery-mdns"))]
        {
            let _ = hello;
            Err(discovery_not_compiled())
        }
    }

    /// `EnrollmentConfirm` from a peer.
    pub(crate) fn answer_enrollment_confirm(
        &self,
        confirm: EnrollmentConfirm,
    ) -> Result<(), NodeError> {
        #[cfg(feature = "discovery-mdns")]
        return self.serve_enrollment_confirm(confirm);
        #[cfg(not(feature = "discovery-mdns"))]
        {
            let _ = confirm;
            Err(discovery_not_compiled())
        }
    }
}
