use super::{NodeApp, NodeError, classify_peer_sync_error_kind};
use crate::peer::{PeerSyncBackoff, PeerSyncStatus};
use orion::{
    CompatibilityState, NodeId, Revision,
    control_plane::{DesiredStateSectionFingerprints, PeerSyncErrorKind},
};

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

    pub(super) fn infer_peer_matches_local_desired(
        &self,
        node_id: &NodeId,
        desired_revision: Revision,
        desired_fingerprint: u64,
        desired_section_fingerprints: DesiredStateSectionFingerprints,
    ) -> Result<(), NodeError> {
        self.record_peer_assumed_desired_state(
            node_id,
            desired_revision,
            desired_fingerprint,
            desired_section_fingerprints,
            CompatibilityState::Preferred,
        )
    }

    pub(super) fn infer_peer_matches_current_desired(
        &self,
        node_id: &NodeId,
    ) -> Result<(), NodeError> {
        let desired = self.desired_metadata()?;
        let revisions = self.current_revisions();
        self.infer_peer_matches_local_desired(
            node_id,
            revisions.desired,
            desired.fingerprint,
            desired.section_fingerprints,
        )
    }
}
