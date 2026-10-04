//! The node's hybrid logical clock, tombstone collection and desired-state merge counters.
//! See `docs/peer-sync.md`.

use super::NodeApp;
use crate::lock::{lock_mutex, read_rwlock};
use orion::{
    HlcClockSkew, HlcTimestamp, HybridLogicalClock, NodeId,
    control_plane::{DesiredStateMergeSnapshot, DesiredStateMutation},
    hlc_node_tag,
};
use std::sync::MutexGuard;
use tracing::warn;

/// Counters reported as `desired_merge` in the observability snapshot.
#[derive(Clone, Debug, Default)]
pub(crate) struct DesiredMergeMetrics {
    pub(crate) local_writes: u64,
    pub(crate) remote_writes_applied: u64,
    pub(crate) stale_remote_writes_ignored: u64,
    pub(crate) clock_skew_rejections: u64,
    pub(crate) expired_tombstones_ignored: u64,
    pub(crate) tombstones_collected: u64,
    pub(crate) last_clock_skew: Option<String>,
}

/// What applying one stamped batch from a peer did.
#[derive(Clone, Debug, Default)]
pub(crate) struct RemoteApplyOutcome {
    /// Versions that replaced the local ones (recorded in the mutation history).
    pub(crate) applied: Vec<(DesiredStateMutation, HlcTimestamp)>,
    /// Versions that lost against the local version.
    pub(crate) stale: u64,
    /// Versions rejected because their timestamp was too far in the future.
    pub(crate) skewed: Vec<HlcClockSkew>,
    /// Deletes that were already past tombstone retention and had nothing left to delete.
    pub(crate) expired: u64,
}

impl RemoteApplyOutcome {
    /// `true` when every received version was either applied or lost to a newer local one, so the
    /// two nodes agree on these objects afterwards.
    #[cfg(peer_sync)]
    pub(crate) fn fully_merged(&self) -> bool {
        self.skewed.is_empty()
    }
}

pub(super) fn new_node_clock(node_id: &NodeId, max_drift_ms: u64) -> HybridLogicalClock {
    HybridLogicalClock::new(hlc_node_tag(node_id.as_str()), max_drift_ms)
}

impl NodeApp {
    pub(super) fn clock_lock(&self) -> MutexGuard<'_, HybridLogicalClock> {
        lock_mutex(
            self.state.persisted.clock.lock(),
            "node hybrid logical clock",
        )
    }

    pub(super) fn merge_metrics_lock(&self) -> MutexGuard<'_, DesiredMergeMetrics> {
        lock_mutex(
            self.state.persisted.merge_metrics.lock(),
            "desired merge metrics",
        )
    }

    /// Current wall clock in milliseconds since the Unix epoch, as used by the HLC.
    pub(crate) fn wall_clock_ms() -> u64 {
        Self::current_time_ms()
    }

    /// Stamps older than this (HLC physical milliseconds) belong to expired tombstones.
    pub(super) fn tombstone_cutoff_ms(&self, wall_ms: u64) -> u64 {
        let retention = self.config.runtime_tuning.tombstone_retention.as_millis();
        wall_ms.saturating_sub(retention.min(u128::from(u64::MAX)) as u64)
    }

    /// Advances the clock past every stamp in the current desired state, so local writes after a
    /// restart (or after the state was replaced) order after everything already stored.
    pub(super) fn seed_clock_from_desired(&self) {
        let max = {
            let store = read_rwlock(self.state.persisted.store.read(), "node runtime store");
            store.desired.max_stamp()
        };
        if let Some(max) = max {
            self.clock_lock().advance_to(max);
        }
    }

    /// The latest HLC timestamp this node has issued or accepted.
    pub fn hlc_now(&self) -> HlcTimestamp {
        self.clock_lock().last()
    }

    /// Drops tombstones older than `ORION_NODE_TOMBSTONE_RETENTION_MS`. Collection is not a write:
    /// it does not change the revision or the mutation history. Returns how many were dropped.
    pub fn collect_expired_tombstones(&self) -> usize {
        let cutoff = self.tombstone_cutoff_ms(Self::wall_clock_ms());
        let any_expired = {
            let store = self.store_read();
            store
                .desired
                .tombstones
                .entries()
                .iter()
                .any(|(_, stamp)| stamp.physical_ms < cutoff)
        };
        if !any_expired {
            return 0;
        }
        let collected = self.with_store_mut(|store| {
            if store.desired.tombstones.is_empty() {
                0
            } else {
                store
                    .desired
                    .collect_tombstones(|stamp| stamp.physical_ms >= cutoff)
            }
        });
        if collected > 0 {
            self.invalidate_desired_metadata_cache();
            let mut metrics = self.merge_metrics_lock();
            metrics.tombstones_collected = metrics
                .tombstones_collected
                .saturating_add(collected as u64);
        }
        collected
    }

    pub(super) fn record_local_writes(&self, count: usize) {
        if count > 0 {
            let mut metrics = self.merge_metrics_lock();
            metrics.local_writes = metrics.local_writes.saturating_add(count as u64);
        }
    }

    pub(super) fn record_remote_apply(&self, peer: Option<&NodeId>, outcome: &RemoteApplyOutcome) {
        {
            let mut metrics = self.merge_metrics_lock();
            metrics.remote_writes_applied = metrics
                .remote_writes_applied
                .saturating_add(outcome.applied.len() as u64);
            metrics.stale_remote_writes_ignored = metrics
                .stale_remote_writes_ignored
                .saturating_add(outcome.stale);
            metrics.expired_tombstones_ignored = metrics
                .expired_tombstones_ignored
                .saturating_add(outcome.expired);
            metrics.clock_skew_rejections = metrics
                .clock_skew_rejections
                .saturating_add(outcome.skewed.len() as u64);
            if let Some(skew) = outcome.skewed.last() {
                metrics.last_clock_skew = Some(match peer {
                    Some(peer) => format!("peer {peer}: {skew}"),
                    None => skew.to_string(),
                });
            }
        }
        if let Some(skew) = outcome.skewed.first() {
            warn!(
                node = %self.config.node_id,
                peer = peer.map(ToString::to_string).unwrap_or_else(|| "unknown".into()),
                rejected = outcome.skewed.len(),
                ahead_ms = skew.ahead_by_ms(),
                max_drift_ms = skew.max_drift_ms,
                "rejected desired-state versions stamped too far in the future; check the clocks \
                 (NTP/chrony/PTP) or raise ORION_NODE_HLC_MAX_DRIFT_MS"
            );
        }
    }

    /// Merge counters and clock reading for the observability snapshot.
    pub(crate) fn desired_merge_snapshot(&self) -> DesiredStateMergeSnapshot {
        let clock = self.clock_lock().clone();
        let tombstones = {
            let store = self.store_read();
            store.desired.tombstones.len() as u64
        };
        let metrics = self.merge_metrics_lock().clone();
        DesiredStateMergeSnapshot {
            hlc: clock.last(),
            max_drift_ms: clock.max_drift_ms(),
            tombstone_retention_ms: self
                .config
                .runtime_tuning
                .tombstone_retention
                .as_millis()
                .min(u128::from(u64::MAX)) as u64,
            tombstones,
            local_writes: metrics.local_writes,
            remote_writes_applied: metrics.remote_writes_applied,
            stale_remote_writes_ignored: metrics.stale_remote_writes_ignored,
            clock_skew_rejections: metrics.clock_skew_rejections,
            expired_tombstones_ignored: metrics.expired_tombstones_ignored,
            tombstones_collected: metrics.tombstones_collected,
            last_clock_skew: metrics.last_clock_skew,
        }
    }
}
