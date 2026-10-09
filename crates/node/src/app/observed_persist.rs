//! Coalesced persistence of observed and applied state.
//!
//! Provider and executor snapshots, peer observed updates, and reconcile results change observed
//! (and applied) state far more often than desired state. Writing every change durably costs one
//! fsynced snapshot rewrite each, so while a coalescer runs (it rides along with the reconcile
//! loop) those changes only mark the state dirty and the coalescer writes at most once per
//! `observed_persist_interval`. Desired-state commits are never deferred: they write the full
//! bundle, observed and applied sections included, so they also absorb any pending change.
//!
//! Without a running coalescer (embedded use without `spawn_reconcile_loop`), or with an interval
//! of zero, every change is written immediately as before.
//!
//! Crash semantics: after an unclean stop the persisted observed/applied state can be up to one
//! interval stale. That is safe because providers and executors republish full snapshots when they
//! reconnect, and reconcile re-derives applied state.

use super::{NodeApp, NodeError};
use orion::control_plane::ObservedPersistenceUsageSnapshot;
use std::sync::{
    Mutex, MutexGuard,
    atomic::{AtomicU64, Ordering},
};
use std::time::Duration;
use tokio::{sync::watch, time::Instant};
use tracing::warn;

#[derive(Debug, Default)]
struct CoalescerFlags {
    /// Running coalescers (one per reconcile loop).
    active: usize,
    /// Observed or applied state changed since the last write.
    dirty: bool,
}

#[derive(Debug, Default)]
pub(super) struct ObservedPersistState {
    flags: Mutex<CoalescerFlags>,
    wake: tokio::sync::Notify,
    /// Bumped on every observed change. Resource and workload changes do not always move
    /// `ObservedClusterState::revision`, so the revision alone cannot tell whether the observed
    /// section must be rewritten; a write re-encodes it while `content_generation` is ahead of
    /// `persisted_generation` (the generation captured by the last successful write).
    content_generation: AtomicU64,
    persisted_generation: AtomicU64,
    coalesced_total: AtomicU64,
    flushes_total: AtomicU64,
    absorbed_total: AtomicU64,
}

impl ObservedPersistState {
    fn flags(&self) -> MutexGuard<'_, CoalescerFlags> {
        self.flags
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn take_dirty(&self) -> bool {
        std::mem::take(&mut self.flags().dirty)
    }

    fn mark_dirty(&self) {
        self.flags().dirty = true;
    }
}

/// Decrements the active-coalescer count when the coalescer stops, even if it is cancelled.
struct ActiveCoalescer<'a>(&'a ObservedPersistState);

impl Drop for ActiveCoalescer<'_> {
    fn drop(&mut self) {
        let mut flags = self.0.flags();
        flags.active = flags.active.saturating_sub(1);
    }
}

impl NodeApp {
    fn observed_persist_interval(&self) -> Duration {
        self.config.runtime_tuning.observed_persist_interval
    }

    /// Defers an observed/applied write to the coalescer. Returns `false` when the change must be
    /// written now (no storage, coalescing disabled, or no coalescer running).
    fn defer_observed_persist(&self) -> bool {
        if self.storage.is_none() || self.observed_persist_interval().is_zero() {
            return false;
        }
        let state = &self.state.observed_persist;
        {
            let mut flags = state.flags();
            if flags.active == 0 {
                return false;
            }
            flags.dirty = true;
        }
        state.coalesced_total.fetch_add(1, Ordering::Relaxed);
        state.wake.notify_one();
        true
    }

    /// The observed generation to capture, and whether the observed section must be re-encoded.
    pub(super) fn observed_generation_to_capture(&self) -> (u64, bool) {
        let state = &self.state.observed_persist;
        let generation = state.content_generation.load(Ordering::SeqCst);
        (
            generation,
            generation != state.persisted_generation.load(Ordering::SeqCst),
        )
    }

    /// Records that a write captured observed generation `generation` and succeeded.
    pub(super) fn note_observed_generation_persisted(&self, generation: u64) {
        self.state
            .observed_persist
            .persisted_generation
            .fetch_max(generation, Ordering::SeqCst);
    }

    fn mark_observed_content_changed(&self) {
        self.state
            .observed_persist
            .content_generation
            .fetch_add(1, Ordering::SeqCst);
    }

    /// Persists an observed or applied state change, coalesced when a coalescer runs.
    pub(super) fn persist_observed_state(&self) -> Result<(), NodeError> {
        self.mark_observed_content_changed();
        self.notify_observed_watchers();
        if self.defer_observed_persist() {
            return Ok(());
        }
        self.persist_state()
    }

    /// Async variant of [`Self::persist_observed_state`].
    pub(super) async fn persist_observed_state_async(&self) -> Result<(), NodeError> {
        self.mark_observed_content_changed();
        self.notify_observed_watchers();
        if self.defer_observed_persist() {
            return Ok(());
        }
        self.persist_state_async().await
    }

    /// Called by every full-state write before it captures the state: the write carries any
    /// pending observed/applied change, so the pending flag is cleared (changes made after the
    /// capture set it again).
    pub(super) fn note_full_state_write(&self) {
        let state = &self.state.observed_persist;
        if state.take_dirty() {
            state.absorbed_total.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Writes pending observed/applied changes now. Returns whether anything was written.
    pub async fn flush_observed_state(&self) -> Result<bool, NodeError> {
        let state = &self.state.observed_persist;
        if !state.take_dirty() {
            return Ok(false);
        }
        state.flushes_total.fetch_add(1, Ordering::Relaxed);
        if let Err(error) = self.persist_state_async().await {
            state.mark_dirty();
            return Err(error);
        }
        Ok(true)
    }

    /// Coalescer loop: waits for a pending change, keeps writes at least one interval apart, and
    /// flushes once more when `shutdown` fires (or its sender is dropped).
    pub(super) async fn run_observed_persist_coalescer(&self, mut shutdown: watch::Receiver<bool>) {
        let interval = self.observed_persist_interval();
        if self.storage.is_none() || interval.is_zero() {
            return;
        }
        let state = &self.state.observed_persist;
        state.flags().active += 1;
        let active = ActiveCoalescer(state);
        let mut last_flush: Option<Instant> = None;
        'run: loop {
            if *shutdown.borrow() {
                break;
            }
            tokio::select! {
                _ = state.wake.notified() => {}
                changed = shutdown.changed() => {
                    if changed.is_err() {
                        break 'run;
                    }
                    continue;
                }
            }
            if let Some(last) = last_flush {
                tokio::select! {
                    _ = tokio::time::sleep_until(last + interval) => {}
                    changed = shutdown.changed() => {
                        let _ = changed;
                        break 'run;
                    }
                }
            }
            match self.flush_observed_state().await {
                Ok(true) => last_flush = Some(Instant::now()),
                Ok(false) => {}
                Err(error) => {
                    warn!(node = %self.config.node_id, %error, "coalesced observed state write failed; retrying after the interval");
                    last_flush = Some(Instant::now());
                    state.wake.notify_one();
                }
            }
        }
        // Stop deferring before the final flush so later changes are written immediately.
        drop(active);
        if let Err(error) = self.flush_observed_state().await {
            warn!(node = %self.config.node_id, %error, "final observed state flush failed");
        }
    }

    pub(super) fn observed_persistence_usage(&self) -> ObservedPersistenceUsageSnapshot {
        let state = &self.state.observed_persist;
        let (coalescing, pending) = {
            let flags = state.flags();
            (flags.active > 0, flags.dirty)
        };
        ObservedPersistenceUsageSnapshot {
            interval_ms: u64::try_from(self.observed_persist_interval().as_millis())
                .unwrap_or(u64::MAX),
            coalescing,
            pending,
            coalesced_changes_total: state.coalesced_total.load(Ordering::Relaxed),
            flushes_total: state.flushes_total.load(Ordering::Relaxed),
            absorbed_flushes_total: state.absorbed_total.load(Ordering::Relaxed),
        }
    }
}
