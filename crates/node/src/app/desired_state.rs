mod views;
mod watchers;

use super::{NodeApp, NodeError, hlc_state::RemoteApplyOutcome};
use orion::{
    HybridLogicalClock, Revision,
    control_plane::{
        ArtifactRecord, DesiredClusterState, DesiredStateMutation, ExecutorRecord, MutationBatch,
        ObservedClusterState, ProviderRecord,
    },
    encode_to_vec,
    runtime::LocalRuntimeStore,
};
pub(crate) use views::{
    diff_desired_cluster_state, entry_fingerprint, merge_observed_state, merge_peer_observed_state,
    stable_hash64, summarize_section,
};

/// One desired-state transaction. Holds the store, mutation history, history baseline and clock
/// locks (in that order) for its whole lifetime.
pub(super) struct DesiredStateTxn<'a> {
    store: &'a mut LocalRuntimeStore,
    history: &'a mut MutationHistory,
    baseline: &'a mut DesiredClusterState,
    clock: &'a mut HybridLogicalClock,
    wall_ms: u64,
    tombstone_cutoff_ms: u64,
    max_batches: usize,
    max_bytes: usize,
}

impl DesiredStateTxn<'_> {
    fn normalize_history(&mut self) -> Result<(), NodeError> {
        let _ = normalize_mutation_history_in_place(
            self.history,
            self.baseline,
            self.max_batches,
            self.max_bytes,
        )?;
        Ok(())
    }

    pub(super) fn replace_bundle(
        &mut self,
        desired: DesiredClusterState,
        observed: ObservedClusterState,
        applied: orion::control_plane::AppliedClusterState,
        history: MutationHistory,
        baseline: DesiredClusterState,
    ) {
        if let Some(max) = desired.max_stamp() {
            self.clock.advance_to(max);
        }
        self.store.replace_desired(desired);
        self.store.observed = observed;
        self.store.applied = applied;
        *self.history = history;
        *self.baseline = baseline;
    }

    pub(super) fn append_batch(&mut self, batch: MutationBatch) -> Result<(), NodeError> {
        if batch.mutations.is_empty() {
            return Ok(());
        }
        self.history.push(batch);
        self.normalize_history()
    }

    /// Commits local writes: every mutation gets a fresh timestamp from this node's clock (so it
    /// wins against everything this node has seen) and the stamped batch is appended to the
    /// mutation history. Returns the number of mutations written.
    pub(super) fn apply_local(
        &mut self,
        mutations: Vec<DesiredStateMutation>,
    ) -> Result<usize, NodeError> {
        if mutations.is_empty() {
            return Ok(0);
        }
        let base_revision = self.store.desired.revision;
        let mut versions = Vec::with_capacity(mutations.len());
        let mut removed_providers = Vec::new();
        for mutation in mutations {
            let stamp = self.clock.now(self.wall_ms);
            if let DesiredStateMutation::RemoveProvider(provider_id) = &mutation {
                removed_providers.push(provider_id.clone());
            }
            self.store.desired.force_stamped(mutation.clone(), stamp);
            versions.push((mutation, stamp));
        }
        let count = versions.len();
        self.append_batch(MutationBatch::stamped(base_revision, versions))?;
        self.store
            .prune_observed_for_removed_providers(&removed_providers);
        Ok(count)
    }

    /// Merges a stamped batch from a peer with the last-writer-wins rule. Versions stamped beyond
    /// the clock's maximum drift are rejected; deletes that are already past tombstone retention
    /// are dropped unless they still delete an older live copy. The versions that replaced the
    /// local ones are appended to the mutation history.
    pub(super) fn apply_remote(
        &mut self,
        batch: &MutationBatch,
    ) -> Result<RemoteApplyOutcome, NodeError> {
        let base_revision = self.store.desired.revision;
        let mut outcome = RemoteApplyOutcome::default();
        for (mutation, stamp) in batch.versions() {
            if let Err(skew) = self.clock.check(stamp, self.wall_ms) {
                outcome.skewed.push(skew);
                continue;
            }
            self.clock.advance_to(stamp);
            if mutation.is_remove() && stamp.physical_ms < self.tombstone_cutoff_ms {
                let deletes_older_copy = self
                    .store
                    .desired
                    .version_of(&mutation.key())
                    .is_some_and(|version| !version.deleted && version.stamp < stamp);
                if !deletes_older_copy {
                    outcome.expired += 1;
                    continue;
                }
            }
            if self.store.desired.apply_stamped(mutation.clone(), stamp) {
                outcome.applied.push((mutation.clone(), stamp));
            } else {
                outcome.stale += 1;
            }
        }
        if !outcome.applied.is_empty() {
            self.append_batch(MutationBatch::stamped(
                base_revision,
                outcome.applied.iter().cloned(),
            ))?;
            // A peer removed a provider: retract what it last reported here, too.
            let removed_providers: Vec<&_> = outcome
                .applied
                .iter()
                .filter_map(|(mutation, _)| match mutation {
                    DesiredStateMutation::RemoveProvider(provider_id) => Some(provider_id),
                    _ => None,
                })
                .collect();
            self.store
                .prune_observed_for_removed_providers(removed_providers);
        }
        Ok(outcome)
    }

    /// Writes back the records of providers and executors registered in-process on this node if
    /// a merge replaced them. Returns the number of records rewritten.
    pub(super) fn reassert_local_records(
        &mut self,
        providers: &[ProviderRecord],
        executors: &[ExecutorRecord],
    ) -> Result<usize, NodeError> {
        let desired = &self.store.desired;
        let mut mutations = Vec::new();
        for record in providers {
            if desired.providers.get(&record.provider_id) != Some(record) {
                mutations.push(DesiredStateMutation::PutProvider(record.clone()));
            }
        }
        for record in executors {
            if desired.executors.get(&record.executor_id) != Some(record) {
                mutations.push(DesiredStateMutation::PutExecutor(record.clone()));
            }
        }
        self.apply_local(mutations)
    }

    pub(super) fn store(&mut self) -> &mut LocalRuntimeStore {
        self.store
    }
}

impl NodeApp {
    pub(super) fn with_desired_state_transaction<T>(
        &self,
        mutate: impl FnOnce(&mut DesiredStateTxn<'_>) -> Result<T, NodeError>,
    ) -> Result<T, NodeError> {
        let wall_ms = Self::wall_clock_ms();
        let tombstone_cutoff_ms = self.tombstone_cutoff_ms(wall_ms);
        let mut store = self.store_lock();
        let mut history = self.mutation_history_lock();
        let mut baseline = self.mutation_history_baseline_lock();
        let mut clock = self.clock_lock();
        let mut txn = DesiredStateTxn {
            store: &mut store,
            history: &mut history,
            baseline: &mut baseline,
            clock: &mut clock,
            wall_ms,
            tombstone_cutoff_ms,
            max_batches: self.config.runtime_tuning.max_mutation_history_batches,
            max_bytes: self.config.runtime_tuning.max_mutation_history_bytes,
        };
        mutate(&mut txn)
    }

    pub(super) fn commit_desired_state_update_if_changed<T>(
        &self,
        previous_revision: Revision,
        mutate: impl FnOnce(&mut DesiredStateTxn<'_>) -> Result<(T, bool), NodeError>,
    ) -> Result<T, NodeError> {
        let (result, changed) = self.with_desired_state_transaction(mutate)?;
        if changed {
            self.finalize_desired_state_update(previous_revision)?;
        }
        Ok(result)
    }

    pub(super) async fn commit_desired_state_update_async_if_changed<T>(
        &self,
        previous_revision: Revision,
        mutate: impl FnOnce(&mut DesiredStateTxn<'_>) -> Result<(T, bool), NodeError>,
    ) -> Result<T, NodeError> {
        let (result, changed) = self.with_desired_state_transaction(mutate)?;
        if changed {
            self.finalize_desired_state_update_async(previous_revision)
                .await?;
        }
        Ok(result)
    }

    pub(super) fn restore_desired_state_bundle(
        &self,
        desired: DesiredClusterState,
        observed: ObservedClusterState,
        applied: orion::control_plane::AppliedClusterState,
        history: MutationHistory,
        baseline: DesiredClusterState,
    ) -> Result<(), NodeError> {
        self.with_desired_state_transaction(|txn| {
            txn.replace_bundle(desired, observed, applied, history, baseline);
            Ok(())
        })?;
        self.invalidate_desired_metadata_cache();
        self.request_reconcile();
        Ok(())
    }

    pub(super) fn with_mutation_history_mut<T>(
        &self,
        mutate: impl FnOnce(&mut MutationHistory, &mut DesiredClusterState) -> T,
    ) -> T {
        let mut history = self.mutation_history_lock();
        let mut baseline = self.mutation_history_baseline_lock();
        mutate(&mut history, &mut baseline)
    }

    pub(super) fn finalize_desired_state_update(
        &self,
        previous_revision: Revision,
    ) -> Result<(), NodeError> {
        self.invalidate_desired_metadata_cache();
        let persisted = self.persist_state();
        self.request_reconcile();
        persisted?;
        self.notify_desired_state_watchers(previous_revision);
        Ok(())
    }

    pub(super) async fn finalize_desired_state_update_async(
        &self,
        previous_revision: Revision,
    ) -> Result<(), NodeError> {
        self.invalidate_desired_metadata_cache();
        let persisted = self.persist_state_async().await;
        self.request_reconcile();
        persisted?;
        self.notify_desired_state_watchers(previous_revision);
        Ok(())
    }

    /// Replaces the desired state with `desired` as a local write: the difference to the current
    /// state is committed with fresh timestamps (objects missing from `desired` are deleted).
    pub fn replace_desired(&self, desired: DesiredClusterState) {
        let _ = self.replace_desired_tracked(desired);
    }

    /// Writes `mutation` locally unless `unchanged` says the current state already matches.
    fn put_record_tracked(
        &self,
        unchanged: impl FnOnce(&DesiredClusterState) -> bool,
        mutation: DesiredStateMutation,
    ) -> Result<(), NodeError> {
        let previous_revision = self.current_desired_revision();
        let written = self.commit_desired_state_update_if_changed(previous_revision, |txn| {
            if unchanged(&txn.store().desired) {
                return Ok((0, false));
            }
            let written = txn.apply_local(vec![mutation])?;
            Ok((written, true))
        })?;
        self.record_local_writes(written);
        Ok(())
    }

    pub(super) fn put_provider_record_tracked(
        &self,
        record: ProviderRecord,
    ) -> Result<(), NodeError> {
        let check = record.clone();
        self.put_record_tracked(
            move |desired| desired.providers.get(&check.provider_id) == Some(&check),
            DesiredStateMutation::PutProvider(record),
        )
    }

    pub(super) fn put_executor_record_tracked(
        &self,
        record: ExecutorRecord,
    ) -> Result<(), NodeError> {
        let check = record.clone();
        self.put_record_tracked(
            move |desired| desired.executors.get(&check.executor_id) == Some(&check),
            DesiredStateMutation::PutExecutor(record),
        )
    }

    pub(super) fn put_artifact_record_tracked(
        &self,
        record: ArtifactRecord,
    ) -> Result<(), NodeError> {
        let check = record.clone();
        self.put_record_tracked(
            move |desired| desired.artifacts.get(&check.artifact_id) == Some(&check),
            DesiredStateMutation::PutArtifact(record),
        )
    }

    pub(super) async fn put_artifact_record_tracked_async(
        &self,
        record: ArtifactRecord,
    ) -> Result<(), NodeError> {
        let previous_revision = self.current_desired_revision();
        let written = self
            .commit_desired_state_update_async_if_changed(previous_revision, |txn| {
                if txn.store().desired.artifacts.get(&record.artifact_id) == Some(&record) {
                    return Ok((0, false));
                }
                let written =
                    txn.apply_local(vec![DesiredStateMutation::PutArtifact(record.clone())])?;
                Ok((written, true))
            })
            .await?;
        self.record_local_writes(written);
        Ok(())
    }
}

/// Mutation history batches with the encoded size of each batch, kept in step with the batches.
///
/// The byte cap is checked against the sum of the batch sizes, so appending and trimming never
/// re-encode the history. The sum is a close estimate of the archived history (the history archive
/// adds one vector header on top of the batches), and the cap is a budget, not a file-size contract.
/// Encoding the history is still exact where the file is written (see the persistence path).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct MutationHistory {
    batches: Vec<MutationBatch>,
    encoded_sizes: Vec<usize>,
    encoded_total: usize,
}

impl MutationHistory {
    pub(crate) fn from_batches(batches: Vec<MutationBatch>) -> Self {
        let encoded_sizes: Vec<usize> = batches.iter().map(batch_encoded_len).collect();
        let encoded_total = encoded_sizes
            .iter()
            .fold(0usize, |sum, size| sum.saturating_add(*size));
        Self {
            batches,
            encoded_sizes,
            encoded_total,
        }
    }

    pub(crate) fn batches(&self) -> &[MutationBatch] {
        &self.batches
    }

    /// The batches as the `Vec` that is archived to disk (same archive layout as before).
    pub(crate) fn as_vec(&self) -> &Vec<MutationBatch> {
        &self.batches
    }

    pub(crate) fn len(&self) -> usize {
        self.batches.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.batches.is_empty()
    }

    /// Estimated archived size of the whole history, from the per-batch sizes.
    pub(crate) fn encoded_len(&self) -> usize {
        self.encoded_total
    }

    fn push(&mut self, batch: MutationBatch) {
        let size = batch_encoded_len(&batch);
        self.encoded_total = self.encoded_total.saturating_add(size);
        self.encoded_sizes.push(size);
        self.batches.push(batch);
    }

    /// Removes the oldest `count` batches and returns them, oldest first.
    fn drop_front(&mut self, count: usize) -> Vec<MutationBatch> {
        let dropped: Vec<MutationBatch> = self.batches.drain(0..count).collect();
        let dropped_size: usize = self
            .encoded_sizes
            .drain(0..count)
            .fold(0usize, |sum, size| sum.saturating_add(size));
        self.encoded_total = self.encoded_total.saturating_sub(dropped_size);
        dropped
    }

    /// Removes empty batches. Returns `true` when any were removed.
    fn retain_non_empty(&mut self) -> bool {
        if self.batches.iter().all(|batch| !batch.mutations.is_empty()) {
            return false;
        }
        let mut kept_batches = Vec::with_capacity(self.batches.len());
        let mut kept_sizes = Vec::with_capacity(self.batches.len());
        for (batch, size) in self.batches.drain(..).zip(self.encoded_sizes.drain(..)) {
            if !batch.mutations.is_empty() {
                kept_batches.push(batch);
                kept_sizes.push(size);
            }
        }
        self.batches = kept_batches;
        self.encoded_sizes = kept_sizes;
        self.encoded_total = self
            .encoded_sizes
            .iter()
            .fold(0usize, |sum, size| sum.saturating_add(*size));
        true
    }
}

fn batch_encoded_len(batch: &MutationBatch) -> usize {
    encode_to_vec(batch)
        .map(|bytes| bytes.len())
        .unwrap_or(usize::MAX)
}

fn mutation_history_contiguity_error(action: &str, err: impl std::fmt::Display) -> NodeError {
    NodeError::Storage(format!("{action}: {err}"))
}

fn fold_dropped_batches(
    dropped: Vec<MutationBatch>,
    baseline: &mut DesiredClusterState,
    action: &str,
) -> Result<(), NodeError> {
    for batch in dropped {
        batch
            .apply_to_checked(baseline)
            .map_err(|err| mutation_history_contiguity_error(action, err))?;
    }
    Ok(())
}

pub(super) fn normalize_mutation_history_with_baseline(
    mut history: MutationHistory,
    baseline: &mut DesiredClusterState,
    max_batches: usize,
    max_bytes: usize,
) -> Result<MutationHistory, NodeError> {
    let _ = normalize_mutation_history_in_place(&mut history, baseline, max_batches, max_bytes)?;
    Ok(history)
}

/// Enforces the batch and byte caps. Batches that leave the history are folded into `baseline`
/// in order, so the baseline plus the remaining batches always describe the same state. Returns
/// `true` when the history or the baseline changed.
pub(super) fn normalize_mutation_history_in_place(
    history: &mut MutationHistory,
    baseline: &mut DesiredClusterState,
    max_batches: usize,
    max_bytes: usize,
) -> Result<bool, NodeError> {
    let mut changed = history.retain_non_empty();
    if history.is_empty() {
        changed |= baseline.revision != Revision::ZERO;
        *baseline = DesiredClusterState::default();
        return Ok(changed);
    }
    let keep_batches = max_batches.max(1);
    if history.len() > keep_batches {
        let drain_count = history.len() - keep_batches;
        changed = true;
        let dropped = history.drop_front(drain_count);
        fold_dropped_batches(
            dropped,
            baseline,
            "failed to fold dropped mutation batch into history baseline",
        )?;
    }

    let keep_bytes = max_bytes.max(1);
    if history.len() > 1 && history.encoded_len() > keep_bytes {
        let drain_count = mutation_history_byte_trim_count(history, keep_bytes);
        if drain_count > 0 {
            changed = true;
            let dropped = history.drop_front(drain_count);
            fold_dropped_batches(
                dropped,
                baseline,
                "failed to fold trimmed mutation batch into history baseline",
            )?;
        }
    }
    Ok(changed)
}

/// Number of oldest batches to drop so that the newest batches fit in `keep_bytes`. The newest
/// batch is always kept. Walks back from the newest batch once, so it costs O(batches) additions.
fn mutation_history_byte_trim_count(history: &MutationHistory, keep_bytes: usize) -> usize {
    let mut kept = 0usize;
    let mut first_kept = history.len();
    for (index, size) in history.encoded_sizes.iter().enumerate().rev() {
        if kept.saturating_add(*size) > keep_bytes {
            break;
        }
        kept += size;
        first_kept = index;
    }
    first_kept.min(history.len().saturating_sub(1))
}

#[cfg(test)]
mod tests {
    use super::*;
    use orion::{HlcTimestamp, control_plane::ArtifactRecord};

    /// Batch `index` (based on revision `index`) puts one artifact with `label_len` bytes of label,
    /// so batches get different encoded sizes.
    fn batch(index: u64, label_len: usize) -> MutationBatch {
        let artifact = ArtifactRecord::builder(orion::ArtifactId::new(format!("artifact.{index}")))
            .label("x".repeat(label_len))
            .build();
        MutationBatch::stamped(
            Revision::new(index),
            [(
                DesiredStateMutation::PutArtifact(artifact),
                HlcTimestamp::new(index + 1, 0, 1),
            )],
        )
    }

    fn history_of(batches: &[MutationBatch]) -> MutationHistory {
        MutationHistory::from_batches(batches.to_vec())
    }

    fn encoded_size(batches: &[MutationBatch]) -> usize {
        encode_to_vec(&batches.to_vec())
            .expect("history encodes")
            .len()
    }

    #[test]
    fn tracked_sizes_match_per_batch_encoding() {
        let mut history = MutationHistory::default();
        let batches: Vec<_> = (0..12)
            .map(|index| batch(index, (index as usize) * 37))
            .collect();
        for batch in &batches {
            history.push(batch.clone());
        }
        let expected: usize = batches.iter().map(batch_encoded_len).sum();
        assert_eq!(history.encoded_len(), expected);
        assert_eq!(history, history_of(&batches));
    }

    #[test]
    fn byte_trim_keeps_longest_fitting_suffix_and_folds_the_rest_into_baseline() {
        let batches: Vec<_> = (0..10)
            .map(|index| batch(index, 200 + index as usize * 50))
            .collect();
        let total = history_of(&batches).encoded_len();
        // Budget for roughly the newest half; the exact boundary follows the tracked sizes.
        let keep_bytes = total / 2;
        let mut history = history_of(&batches);
        let mut baseline = DesiredClusterState::default();

        let changed =
            normalize_mutation_history_in_place(&mut history, &mut baseline, 256, keep_bytes)
                .expect("trim should succeed");

        assert!(changed);
        assert!(history.len() < batches.len());
        assert!(!history.is_empty());
        assert!(history.encoded_len() <= keep_bytes);
        // Adding the next older batch would exceed the budget (longest fitting suffix).
        let dropped = batches.len() - history.len();
        if dropped > 0 {
            let with_one_more = history.encoded_len() + batch_encoded_len(&batches[dropped - 1]);
            assert!(with_one_more > keep_bytes);
        }
        // The retained batches are the newest ones, in order, and the baseline is the state just
        // before the first retained batch.
        assert_eq!(history.batches(), &batches[dropped..]);
        assert_eq!(baseline.revision, Revision::new(dropped as u64));
        assert_eq!(baseline.artifacts.len(), dropped);
    }

    #[test]
    fn byte_trim_always_keeps_newest_batch_even_when_it_alone_exceeds_budget() {
        let batches: Vec<_> = (0..4).map(|index| batch(index, 500)).collect();
        let mut history = history_of(&batches);
        let mut baseline = DesiredClusterState::default();

        normalize_mutation_history_in_place(&mut history, &mut baseline, 256, 1)
            .expect("trim should succeed");

        assert_eq!(history.batches(), &batches[3..]);
        assert_eq!(baseline.revision, Revision::new(3));
    }

    #[test]
    fn batch_cap_folds_oldest_batches_into_baseline() {
        let batches: Vec<_> = (0..6).map(|index| batch(index, 8)).collect();
        let mut history = history_of(&batches);
        let mut baseline = DesiredClusterState::default();

        normalize_mutation_history_in_place(&mut history, &mut baseline, 2, usize::MAX)
            .expect("trim should succeed");

        assert_eq!(history.batches(), &batches[4..]);
        assert_eq!(baseline.revision, Revision::new(4));
    }

    #[test]
    fn tracked_total_stays_consistent_after_trimming() {
        let batches: Vec<_> = (0..20)
            .map(|index| batch(index, 40 + index as usize))
            .collect();
        let mut history = history_of(&batches);
        let mut baseline = DesiredClusterState::default();
        normalize_mutation_history_in_place(&mut history, &mut baseline, 7, 2_000)
            .expect("trim should succeed");

        let recomputed: usize = history.encoded_sizes.iter().sum();
        assert_eq!(history.encoded_len(), recomputed);
        assert_eq!(history.encoded_sizes.len(), history.len());
        // Estimate versus the archive of the whole history: the difference is only the per-vector
        // overhead, not a per-batch cost.
        let exact = encoded_size(history.batches());
        assert!(
            history.encoded_len().abs_diff(exact) <= 64,
            "estimate {} vs archive {}",
            history.encoded_len(),
            exact
        );
    }
}
