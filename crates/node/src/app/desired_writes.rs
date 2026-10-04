//! Entry points that commit desired-state writes: local client batches, stamped batches and
//! snapshots from peers, and whole-state replacement. Every path goes through
//! [`super::desired_state::DesiredStateTxn`], which stamps local writes with the node's hybrid
//! logical clock and merges remote versions with the last-writer-wins rule
//! (`docs/peer-sync.md`).

use super::{NodeApp, NodeError, desired_state::DesiredStateTxn, hlc_state::RemoteApplyOutcome};
use orion::{
    NodeId,
    control_plane::{
        DesiredClusterState, ExecutorRecord, MutationApplyError, MutationBatch, ProviderRecord,
        StateSnapshot,
    },
};
use tracing::info_span;

/// Where a mutation batch came from.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum WriteOrigin {
    /// A local client (`orionctl`, `orion-client`) or an in-process caller. The batch is checked
    /// against `base_revision` and stamped by this node.
    Local,
    /// A peer. Stamped batches are merged per object; unstamped ones are treated like local
    /// writes.
    Peer(Option<NodeId>),
}

/// What committing one batch did.
#[derive(Clone, Debug, Default)]
pub(crate) struct WriteOutcome {
    /// Set when the batch was merged as stamped peer versions.
    pub(crate) remote: Option<RemoteApplyOutcome>,
    /// Object versions written with this node's clock (local writes and re-asserted records).
    pub(crate) local_writes: usize,
}

impl WriteOutcome {
    /// `true` when nothing was rejected for clock skew.
    #[cfg(peer_sync)]
    pub(crate) fn fully_merged(&self) -> bool {
        self.remote
            .as_ref()
            .is_none_or(RemoteApplyOutcome::fully_merged)
    }
}

struct PreparedWrite {
    stamped: bool,
    providers: Vec<ProviderRecord>,
    executors: Vec<ExecutorRecord>,
}

impl NodeApp {
    fn ensure_desired_writes_allowed(&self) -> Result<(), NodeError> {
        if self.remote_desired_state_blocked() {
            return Err(NodeError::Authorization(
                "remote desired state mutations are blocked while maintenance isolation is active"
                    .into(),
            ));
        }
        Ok(())
    }

    fn local_integration_records(&self) -> (Vec<ProviderRecord>, Vec<ExecutorRecord>) {
        let providers = self
            .providers_read()
            .values()
            .map(|provider| provider.provider_record())
            .collect();
        let executors = self
            .executors_read()
            .values()
            .map(|executor| executor.executor_record())
            .collect();
        (providers, executors)
    }

    /// Checks the batch and validates the state it would produce before any lock is taken.
    fn prepare_write(
        &self,
        batch: &MutationBatch,
        origin: &WriteOrigin,
    ) -> Result<PreparedWrite, NodeError> {
        self.ensure_desired_writes_allowed()?;
        batch.check_stamps()?;
        let stamped = matches!(origin, WriteOrigin::Peer(_))
            && !batch.mutations.is_empty()
            && batch.is_stamped();
        let mut candidate = self.current_desired_state();
        if stamped {
            let wall_ms = Self::wall_clock_ms();
            let max_drift_ms = self.clock_lock().max_drift_ms();
            for (mutation, stamp) in batch.versions() {
                if !stamp.is_too_far_ahead(wall_ms, max_drift_ms) {
                    candidate.apply_stamped(mutation.clone(), stamp);
                }
            }
        } else {
            MutationBatch::new(batch.base_revision, batch.mutations.clone())
                .apply_to_checked(&mut candidate)?;
        }
        self.validate_desired_state(&candidate)?;
        let (providers, executors) = if stamped {
            self.local_integration_records()
        } else {
            (Vec::new(), Vec::new())
        };
        Ok(PreparedWrite {
            stamped,
            providers,
            executors,
        })
    }

    fn commit_prepared_write(
        txn: &mut DesiredStateTxn<'_>,
        batch: &MutationBatch,
        prepared: &PreparedWrite,
    ) -> Result<(WriteOutcome, bool), NodeError> {
        if prepared.stamped {
            let remote = txn.apply_remote(batch)?;
            let local_writes =
                txn.reassert_local_records(&prepared.providers, &prepared.executors)?;
            let changed = !remote.applied.is_empty() || local_writes > 0;
            return Ok((
                WriteOutcome {
                    remote: Some(remote),
                    local_writes,
                },
                changed,
            ));
        }
        let found = txn.store().desired.revision;
        if batch.base_revision != found {
            return Err(MutationApplyError::RevisionMismatch {
                expected: batch.base_revision,
                found,
            }
            .into());
        }
        let local_writes = txn.apply_local(batch.mutations.clone())?;
        Ok((
            WriteOutcome {
                remote: None,
                local_writes,
            },
            local_writes > 0,
        ))
    }

    fn record_write_outcome(&self, origin: &WriteOrigin, outcome: &WriteOutcome) {
        self.record_local_writes(outcome.local_writes);
        if let Some(remote) = &outcome.remote {
            let peer = match origin {
                WriteOrigin::Peer(peer) => peer.as_ref(),
                WriteOrigin::Local => None,
            };
            self.record_remote_apply(peer, remote);
        }
    }

    /// A local writer learns about a failing inline reconcile (as before). For peer writes the
    /// merge is already committed, so a reconcile failure must not fail the sync round; it is
    /// recorded by the reconcile metrics and log instead.
    fn reconcile_after_write(
        &self,
        origin: &WriteOrigin,
        reconciled: Result<(), NodeError>,
    ) -> Result<(), NodeError> {
        match (origin, reconciled) {
            (WriteOrigin::Peer(_), Err(err)) => {
                tracing::debug!(node = %self.config.node_id, error = %err, "reconcile after peer merge failed");
                Ok(())
            }
            (_, reconciled) => reconciled,
        }
    }

    /// Commits a mutation batch (see [`WriteOrigin`] for how it is interpreted).
    pub(crate) fn apply_mutation_batch(
        &self,
        batch: &MutationBatch,
        origin: WriteOrigin,
    ) -> Result<WriteOutcome, NodeError> {
        let _span = info_span!("mutation_apply", node = %self.config.node_id).entered();
        let started = std::time::Instant::now();
        let result = self.prepare_write(batch, &origin).and_then(|prepared| {
            let previous_revision = self.current_desired_revision();
            self.commit_desired_state_update_if_changed(previous_revision, |txn| {
                Self::commit_prepared_write(txn, batch, &prepared)
            })
        });
        let outcome = match result {
            Ok(outcome) => outcome,
            Err(err) => {
                self.record_mutation_apply_failure(started.elapsed(), &err);
                return Err(err);
            }
        };
        self.record_write_outcome(&origin, &outcome);
        self.record_mutation_apply_success(started.elapsed());
        self.reconcile_after_write(&origin, self.reconcile_after_change())?;
        Ok(outcome)
    }

    /// Async variant of [`Self::apply_mutation_batch`] (persists without blocking the runtime).
    #[cfg(any(test, peer_sync))]
    pub(crate) async fn apply_mutation_batch_async(
        &self,
        batch: &MutationBatch,
        origin: WriteOrigin,
    ) -> Result<WriteOutcome, NodeError> {
        let started = std::time::Instant::now();
        let result = match self.prepare_write(batch, &origin) {
            Ok(prepared) => {
                let previous_revision = self.current_desired_revision();
                self.commit_desired_state_update_async_if_changed(previous_revision, |txn| {
                    Self::commit_prepared_write(txn, batch, &prepared)
                })
                .await
            }
            Err(err) => Err(err),
        };
        let outcome = match result {
            Ok(outcome) => outcome,
            Err(err) => {
                self.record_mutation_apply_failure(started.elapsed(), &err);
                return Err(err);
            }
        };
        self.record_write_outcome(&origin, &outcome);
        self.record_mutation_apply_success(started.elapsed());
        let reconciled = self.reconcile_after_change_async().await;
        self.reconcile_after_write(&origin, reconciled)?;
        Ok(outcome)
    }

    /// Merges a peer's desired state into the local one, object by object. Only the desired part
    /// of the snapshot is used; observed state travels in `ObservedUpdate` pushes.
    pub(crate) fn merge_peer_snapshot(
        &self,
        peer: Option<NodeId>,
        snapshot: &StateSnapshot,
    ) -> Result<WriteOutcome, NodeError> {
        self.apply_mutation_batch(
            &snapshot.state.desired.stamped_batch(),
            WriteOrigin::Peer(peer),
        )
    }

    /// Async variant of [`Self::merge_peer_snapshot`].
    #[cfg(test)]
    pub(crate) async fn merge_peer_snapshot_async(
        &self,
        peer: Option<NodeId>,
        snapshot: &StateSnapshot,
    ) -> Result<WriteOutcome, NodeError> {
        self.apply_mutation_batch_async(
            &snapshot.state.desired.stamped_batch(),
            WriteOrigin::Peer(peer),
        )
        .await
    }

    /// Commits the difference between the current desired state and `desired` as local writes.
    pub(crate) fn replace_desired_tracked(
        &self,
        desired: DesiredClusterState,
    ) -> Result<(), NodeError> {
        let current = self.current_desired_state();
        let batch = super::desired_state::diff_desired_cluster_state(&current, &desired);
        if batch.mutations.is_empty() {
            return Ok(());
        }
        self.validate_desired_state(&desired)?;
        let previous_revision = current.revision;
        let written = self.commit_desired_state_update_if_changed(previous_revision, |txn| {
            let written = txn.apply_local(batch.mutations)?;
            Ok((written, written > 0))
        })?;
        self.record_local_writes(written);
        Ok(())
    }
}
