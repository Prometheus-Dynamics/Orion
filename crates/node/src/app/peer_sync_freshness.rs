//! The stale-peer guard (`docs/peer-sync.md`, "Stale peers").
//!
//! A peer whose last completed sync round with this node is older than
//! `ORION_NODE_TOMBSTONE_RETENTION_MS` may hold objects that were deleted here after their
//! tombstones were collected. Neither side syncs desired state with it until an operator resets
//! it: `orionctl peers remove <node>` forgets its clock, and `orionctl peers enroll <node>` starts
//! a fresh one.

use super::{NodeApp, NodeError};
use crate::peer_sync_times;
use orion::{NodeId, control_plane::ControlMessage};
use tracing::warn;

/// Messages that move desired state between the peers (or ask for it). The stale-peer guard
/// refuses them from a stale peer. Observed state and queries are not refused.
pub(super) fn moves_desired_state(message: &ControlMessage) -> bool {
    matches!(
        message,
        ControlMessage::Hello(_)
            | ControlMessage::SyncRequest(_)
            | ControlMessage::SyncSummaryRequest(_)
            | ControlMessage::SyncDiffRequest(_)
            | ControlMessage::Snapshot(_)
            | ControlMessage::Mutations(_)
    )
}

fn stale_peer_reason(node_id: &NodeId, age_ms: u64, retention_ms: u64) -> String {
    format!(
        "peer {node_id} has not completed a sync round for {age_ms} ms, more than \
         ORION_NODE_TOMBSTONE_RETENTION_MS ({retention_ms} ms); deleted objects may already be \
         collected here, so it is refused until it is reset: run `orionctl peers remove \
         {node_id}`, then `orionctl peers enroll {node_id}`"
    )
}

impl NodeApp {
    /// Whether a peer whose last successful sync round was at `last_successful_sync_ms` is stale at
    /// `now`: older than the tombstone retention. A peer that never completed a round is not stale
    /// (it has nothing to be stale about yet).
    pub(super) fn peer_sync_is_stale(
        &self,
        last_successful_sync_ms: Option<u64>,
        now: u64,
    ) -> bool {
        last_successful_sync_ms
            .is_some_and(|at| now.saturating_sub(at) > self.tombstone_retention_ms())
    }

    pub(super) fn tombstone_retention_ms(&self) -> u64 {
        self.config
            .runtime_tuning
            .tombstone_retention
            .as_millis()
            .min(u128::from(u64::MAX)) as u64
    }

    /// Sync clock a peer starts with when it is registered now: its persisted record when it has
    /// one (a restart or a re-registration must not reset it), otherwise now.
    pub(super) fn sync_clock_start_for(&self, node_id: &NodeId) -> Result<u64, NodeError> {
        let now = Self::current_time_ms();
        let Some(state_dir) = self.state_dir() else {
            return Ok(now);
        };
        Ok(peer_sync_times::load(state_dir)?
            .get(node_id.as_str())
            .copied()
            .unwrap_or(now))
    }

    /// Writes the sync clocks of the registered peers into `peer-sync-times.json`, merged with
    /// the records already there. No-op without a state directory.
    pub(super) fn persist_peer_sync_times(&self) -> Result<(), NodeError> {
        let Some(state_dir) = self.state_dir() else {
            return Ok(());
        };
        let clocks: Vec<(String, u64)> = self
            .peers_read()
            .iter()
            .filter_map(|(node_id, peer)| {
                peer.last_successful_sync_ms
                    .map(|at| (node_id.to_string(), at))
            })
            .collect();
        peer_sync_times::update(state_dir, |records| records.extend(clocks))
    }

    /// Forgets a removed peer's clock, so a re-added peer starts fresh.
    pub(super) fn forget_peer_sync_time(&self, node_id: &NodeId) -> Result<(), NodeError> {
        let Some(state_dir) = self.state_dir() else {
            return Ok(());
        };
        peer_sync_times::update(state_dir, |records| {
            records.remove(node_id.as_str());
        })
    }

    /// Records a completed sync round with `node_id` and persists the clock (throttled).
    #[cfg(peer_sync)]
    pub(super) fn note_successful_sync_round(&self, node_id: &NodeId) {
        let now = Self::current_time_ms();
        let persist = self
            .with_peer_mut(node_id, |peer| peer.record_successful_sync(now))
            .unwrap_or(false);
        if persist && let Err(err) = self.persist_peer_sync_times() {
            warn!(
                node = %self.config.node_id,
                peer = %node_id,
                error = %err,
                "failed to persist the last successful sync time of a peer"
            );
        }
    }

    /// The stale-peer check. Returns the refusal reason when `node_id` is stale, after marking
    /// the peer with it (sync status and last error); `None` when the peer may sync.
    pub(super) fn refuse_stale_peer(&self, node_id: &NodeId) -> Option<String> {
        let now = Self::current_time_ms();
        let retention = self.tombstone_retention_ms();
        let (reason, warn_now) = self
            .with_peer_mut(node_id, |peer| {
                let last = peer.last_successful_sync_ms?;
                let age = now.saturating_sub(last);
                if age <= retention {
                    return None;
                }
                let reason = stale_peer_reason(node_id, age, retention);
                peer.mark_stale(reason.clone());
                Some((reason, peer.stale_warning_due(now)))
            })
            .ok()
            .flatten()?;
        if warn_now {
            warn!(
                node = %self.config.node_id,
                peer = %node_id,
                retention_ms = retention,
                "refusing to sync with a stale peer: {reason}"
            );
        }
        Some(reason)
    }
}
