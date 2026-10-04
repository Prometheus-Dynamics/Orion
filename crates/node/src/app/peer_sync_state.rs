use super::{NodeApp, NodeError, classify_peer_sync_error_kind};
use crate::peer::{PeerSyncBackoff, PeerSyncStatus};
use crate::storage_io::blocking_read_file;
use orion::{
    CompatibilityState, NodeId, Revision,
    control_plane::{DesiredStateSectionFingerprints, PeerHello, PeerSyncErrorKind},
};

/// Error text returned when peer sync is requested from a build without a peer transport.
#[cfg(not(peer_sync))]
pub(crate) const PEER_SYNC_REQUIRES_TRANSPORT: &str = "peer sync requires orion-node to be built \
     with the `transport-http` or `peer-tcp` feature";

fn read_file_bytes(path: &std::path::Path) -> Result<Vec<u8>, NodeError> {
    blocking_read_file(path, "failed to read file")
}

impl NodeApp {
    pub fn set_peer_sync_status(
        &self,
        node_id: &NodeId,
        status: PeerSyncStatus,
    ) -> Result<(), NodeError> {
        self.with_peer_mut(node_id, |peer| {
            peer.set_sync_status(status);
        })
    }

    pub fn record_peer_error(
        &self,
        node_id: &NodeId,
        error: impl Into<String>,
    ) -> Result<(), NodeError> {
        let error = error.into();
        self.record_peer_error_with_kind(
            node_id,
            error.clone(),
            Some(classify_peer_sync_error_kind(&error)),
        )
    }

    pub fn record_peer_error_with_kind(
        &self,
        node_id: &NodeId,
        error: impl Into<String>,
        error_kind: Option<PeerSyncErrorKind>,
    ) -> Result<(), NodeError> {
        self.with_peer_mut(node_id, |peer| {
            peer.note_sync_failure(
                error,
                error_kind,
                Self::current_time_ms(),
                PeerSyncBackoff {
                    base: self.config.runtime_tuning.peer_sync_backoff_base,
                    max: self.config.runtime_tuning.peer_sync_backoff_max,
                    max_jitter_ms: self.config.runtime_tuning.peer_sync_backoff_jitter_ms,
                    retry_scope: self.config.node_id.as_str(),
                },
            );
        })
    }

    pub fn record_peer_hello(
        &self,
        node_id: &NodeId,
        hello: &orion::control_plane::PeerHello,
        compatibility: CompatibilityState,
    ) -> Result<(), NodeError> {
        self.with_peer_mut(node_id, |peer| {
            peer.record_hello(
                hello.desired_revision,
                hello.desired_fingerprint,
                hello.desired_section_fingerprints.clone(),
                hello.observed_revision,
                hello.applied_revision,
                compatibility,
            );
        })
    }

    pub fn record_peer_assumed_desired_state(
        &self,
        node_id: &NodeId,
        desired_revision: Revision,
        desired_fingerprint: u64,
        desired_section_fingerprints: DesiredStateSectionFingerprints,
        compatibility: CompatibilityState,
    ) -> Result<(), NodeError> {
        self.with_peer_mut(node_id, |peer| {
            peer.record_assumed_desired_state(
                desired_revision,
                desired_fingerprint,
                desired_section_fingerprints,
                compatibility,
            );
        })
    }

    pub(super) fn evict_peer_client(&self, node_id: &NodeId) {
        self.with_peer_clients_mut(|peer_clients| {
            peer_clients.remove(node_id);
        });
    }

    pub(super) fn peer_hello(&self) -> Result<PeerHello, NodeError> {
        let revisions = self.current_revisions();
        let desired_metadata = self.desired_metadata()?;
        let (
            transport_binding_version,
            transport_binding_public_key,
            transport_tls_cert_pem,
            transport_binding_signature,
        ) = match self.http_tls_cert_path.as_deref() {
            Some(cert_path) => {
                let cert_pem = read_file_bytes(cert_path)?;
                let binding = self.security.transport_binding(&cert_pem)?;
                (
                    Some(binding.version),
                    Some(binding.public_key),
                    Some(binding.tls_cert_pem),
                    Some(binding.signature),
                )
            }
            None => (None, None, None, None),
        };
        Ok(PeerHello {
            node_id: self.config.node_id.clone(),
            desired_revision: revisions.desired,
            desired_fingerprint: desired_metadata.fingerprint,
            desired_section_fingerprints: desired_metadata.section_fingerprints.clone(),
            observed_revision: revisions.observed,
            applied_revision: revisions.applied,
            transport_binding_version,
            transport_binding_public_key,
            transport_tls_cert_pem,
            transport_binding_signature,
        })
    }
}

#[cfg(not(peer_sync))]
impl NodeApp {
    /// Builds without a peer transport cannot sync.
    pub async fn sync_peer(&self, node_id: &NodeId) -> Result<(), NodeError> {
        Err(NodeError::Config(format!(
            "cannot sync peer {node_id}: {PEER_SYNC_REQUIRES_TRANSPORT}"
        )))
    }
}
