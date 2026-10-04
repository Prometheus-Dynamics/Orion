//! Transport-independent peer sync engine (`docs/peer-sync.md`, "Sync protocol").
//!
//! One round with a peer: exchange `Hello`; if the desired-state fingerprints differ, fetch the
//! peer's per-object versions for the differing sections, push the local versions that win the
//! merge rule and pull the peer's versions that win. Because the merge rule is idempotent and
//! commutative, rounds may fail half way, overlap with the peer's own round or race new local
//! writes without corrupting state.

use super::desired_sync::{
    all_desired_sections, changed_sections, plan_sync_exchange, sections_of_keys,
    selectors_for_keys, stamped_versions_for_keys,
};
use super::desired_writes::WriteOrigin;
use super::peer_transport::{PeerSyncTransport, unexpected_response};
use super::{NodeApp, NodeError, classify_peer_sync_error};
use crate::peer::{PeerState, PeerSyncStatus};
use orion::{
    CompatibilityState, NodeId,
    control_plane::{ControlMessage, SyncDiffRequest, SyncSummaryRequest},
    transport::http::HttpResponsePayload,
};
use std::time::Instant;

/// What one sync round exchanged.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct PeerSyncRound {
    pub(crate) pushed: usize,
    pub(crate) pulled: usize,
    /// `false` when some pulled versions were rejected (clock skew), so the peers still differ.
    pub(crate) converged: bool,
}

impl NodeApp {
    fn peer_ready_for_sync(&self, node_id: &NodeId) -> Result<Option<PeerState>, NodeError> {
        let now_ms = Self::current_time_ms();
        let peers = self.peers_read();
        let peer = peers
            .get(node_id)
            .cloned()
            .ok_or_else(|| NodeError::UnknownPeer(node_id.clone()))?;
        Ok(peer.can_attempt_sync_at(now_ms).then_some(peer))
    }

    /// Runs one sync round with `node_id` over the transport selected by its base URL.
    pub async fn sync_peer(&self, node_id: &NodeId) -> Result<(), NodeError> {
        if self.peer_sync_paused() {
            return Ok(());
        }
        let started = Instant::now();
        let Some(peer) = self.peer_ready_for_sync(node_id)? else {
            return Ok(());
        };
        let result = match self.open_peer_channel(node_id, &peer) {
            Ok(channel) => self.sync_peer_over(node_id, &peer, &channel).await,
            Err(err) => Err(err),
        };
        match result {
            Ok(_) => self.record_peer_sync_success(node_id, started.elapsed()),
            Err(err) => {
                let _ = self.record_peer_error_with_kind(
                    node_id,
                    err.to_string(),
                    Some(classify_peer_sync_error(&err)),
                );
                self.record_peer_sync_failure(Some(node_id), started.elapsed(), &err);
                Err(err)
            }
        }
    }

    /// One sync round over an explicit transport.
    pub(crate) async fn sync_peer_over<T: PeerSyncTransport>(
        &self,
        node_id: &NodeId,
        peer: &PeerState,
        transport: &T,
    ) -> Result<PeerSyncRound, NodeError> {
        self.collect_expired_tombstones();
        self.set_peer_sync_status(node_id, PeerSyncStatus::Negotiating)?;
        let remote = transport
            .hello(self, node_id, peer, self.peer_hello()?)
            .await?;
        self.record_peer_hello(node_id, &remote, CompatibilityState::Preferred)?;
        self.set_peer_sync_status(node_id, PeerSyncStatus::Syncing)?;

        let local = self.desired_metadata()?;
        if remote.desired_fingerprint == local.fingerprint {
            self.push_observed_slice(node_id, transport).await;
            return Ok(PeerSyncRound {
                converged: true,
                ..PeerSyncRound::default()
            });
        }
        let mut sections = changed_sections(
            &local.section_fingerprints,
            &remote.desired_section_fingerprints,
        );
        if sections.is_empty() {
            sections = all_desired_sections();
        }
        let remote_summary = match transport
            .send_control(
                self,
                node_id,
                ControlMessage::SyncSummaryRequest(SyncSummaryRequest {
                    node_id: self.config.node_id.clone(),
                    sections: sections.clone(),
                }),
            )
            .await?
        {
            HttpResponsePayload::Summary(summary) => summary,
            other => return Err(unexpected_response(transport.label(), "summary", &other)),
        };
        let local_summary = self.desired_state_summary_for_sections(&sections)?;
        let cutoff = self.tombstone_cutoff_ms(Self::wall_clock_ms());
        let (push, pull) = plan_sync_exchange(&local_summary, &remote_summary, cutoff);
        let mut round = PeerSyncRound {
            pushed: push.len(),
            pulled: pull.len(),
            converged: true,
        };

        if !push.is_empty() {
            let batch =
                self.with_desired_state_read(|desired| stamped_versions_for_keys(desired, &push));
            match transport
                .send_control(self, node_id, ControlMessage::Mutations(batch))
                .await?
            {
                HttpResponsePayload::Accepted => {}
                other => return Err(unexpected_response(transport.label(), "push", &other)),
            }
        }

        if !pull.is_empty() {
            let request = SyncDiffRequest {
                node_id: self.config.node_id.clone(),
                desired_revision: local_summary.revision,
                desired_summary: local_summary,
                sections: sections_of_keys(&pull),
                object_selectors: selectors_for_keys(&pull),
            };
            match transport
                .send_control(self, node_id, ControlMessage::SyncDiffRequest(request))
                .await?
            {
                HttpResponsePayload::Mutations(batch) => {
                    let outcome = self
                        .apply_mutation_batch_async(
                            &batch,
                            WriteOrigin::Peer(Some(node_id.clone())),
                        )
                        .await?;
                    round.converged = outcome.fully_merged();
                }
                HttpResponsePayload::Accepted => {}
                other => return Err(unexpected_response(transport.label(), "diff", &other)),
            }
        }

        if round.converged {
            let metadata = self.desired_metadata()?;
            self.record_peer_assumed_desired_state(
                node_id,
                self.current_desired_revision(),
                metadata.fingerprint,
                metadata.section_fingerprints,
                CompatibilityState::Preferred,
            )?;
        }
        self.push_observed_slice(node_id, transport).await;
        Ok(round)
    }
}
