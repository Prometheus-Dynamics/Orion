mod views;
mod watchers;

use super::{NodeApp, NodeError};
use orion::{
    Revision,
    control_plane::{
        ArtifactRecord, DesiredClusterState, DesiredStateMutation, ExecutorRecord, MutationBatch,
        ObservedClusterState, ProviderRecord,
    },
    encode_to_vec,
    runtime::LocalRuntimeStore,
};
pub(crate) use views::{
    diff_desired_cluster_state, entry_fingerprint, merge_observed_state, summarize_section,
};

pub(super) struct DesiredStateTxn<'a> {
    store: &'a mut LocalRuntimeStore,
    history: &'a mut Vec<MutationBatch>,
    baseline: &'a mut DesiredClusterState,
    max_batches: usize,
    max_bytes: usize,
}

impl DesiredStateTxn<'_> {
    fn normalize_history(&mut self) -> Result<(), NodeError> {
        normalize_mutation_history_in_place(
            self.history,
            self.baseline,
            self.max_batches,
            self.max_bytes,
        )
    }

    pub(super) fn replace_desired(&mut self, desired: DesiredClusterState) {
        self.store.replace_desired(desired);
    }

    pub(super) fn replace_bundle(
        &mut self,
        desired: DesiredClusterState,
        observed: ObservedClusterState,
        applied: orion::control_plane::AppliedClusterState,
        history: Vec<MutationBatch>,
        baseline: DesiredClusterState,
    ) {
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

    pub(super) fn replace_history(
        &mut self,
        baseline_state: DesiredClusterState,
        local_delta: Option<MutationBatch>,
    ) -> Result<(), NodeError> {
        self.history.clear();
        *self.baseline = baseline_state;
        if let Some(local_delta) = local_delta.filter(|delta| !delta.mutations.is_empty()) {
            self.history.push(local_delta);
        }
        self.normalize_history()
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
        let mut store = self.store_lock();
        let mut history = self.mutation_history_lock();
        let mut baseline = self.mutation_history_baseline_lock();
        let mut txn = DesiredStateTxn {
            store: &mut store,
            history: &mut history,
            baseline: &mut baseline,
            max_batches: self.config.runtime_tuning.max_mutation_history_batches,
            max_bytes: self.config.runtime_tuning.max_mutation_history_bytes,
        };
        mutate(&mut txn)
    }

    pub(super) fn commit_desired_state_update<T>(
        &self,
        previous_revision: Revision,
        mutate: impl FnOnce(&mut DesiredStateTxn<'_>) -> Result<T, NodeError>,
    ) -> Result<T, NodeError> {
        let result = self.with_desired_state_transaction(mutate)?;
        self.finalize_desired_state_update(previous_revision)?;
        Ok(result)
    }

    pub(super) async fn commit_desired_state_update_async<T>(
        &self,
        previous_revision: Revision,
        mutate: impl FnOnce(&mut DesiredStateTxn<'_>) -> Result<T, NodeError>,
    ) -> Result<T, NodeError> {
        let result = self.with_desired_state_transaction(mutate)?;
        self.finalize_desired_state_update_async(previous_revision)
            .await?;
        Ok(result)
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
        history: Vec<MutationBatch>,
        baseline: DesiredClusterState,
    ) -> Result<(), NodeError> {
        self.with_desired_state_transaction(|txn| {
            txn.replace_bundle(desired, observed, applied, history, baseline);
            Ok(())
        })?;
        self.invalidate_desired_metadata_cache();
        Ok(())
    }

    pub(super) fn with_mutation_history_mut<T>(
        &self,
        mutate: impl FnOnce(&mut Vec<MutationBatch>, &mut DesiredClusterState) -> T,
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
        self.persist_state()?;
        self.notify_desired_state_watchers(previous_revision);
        Ok(())
    }

    pub(super) async fn finalize_desired_state_update_async(
        &self,
        previous_revision: Revision,
    ) -> Result<(), NodeError> {
        self.invalidate_desired_metadata_cache();
        self.persist_state_async().await?;
        self.notify_desired_state_watchers(previous_revision);
        Ok(())
    }

    pub fn replace_desired(&self, desired: DesiredClusterState) {
        let _ = self.replace_desired_tracked(desired);
    }

    pub(super) fn put_provider_record_tracked(
        &self,
        record: ProviderRecord,
    ) -> Result<(), NodeError> {
        let previous_revision = self.current_desired_revision();
        self.commit_desired_state_update_if_changed(previous_revision, |txn| {
            if txn.store().desired.providers.get(&record.provider_id) == Some(&record) {
                return Ok(((), false));
            }
            txn.store().desired.put_provider(record.clone());
            txn.append_batch(MutationBatch {
                base_revision: previous_revision,
                mutations: vec![DesiredStateMutation::PutProvider(record)],
            })?;
            Ok(((), true))
        })
    }

    pub(super) fn put_executor_record_tracked(
        &self,
        record: ExecutorRecord,
    ) -> Result<(), NodeError> {
        let previous_revision = self.current_desired_revision();
        self.commit_desired_state_update_if_changed(previous_revision, |txn| {
            if txn.store().desired.executors.get(&record.executor_id) == Some(&record) {
                return Ok(((), false));
            }
            txn.store().desired.put_executor(record.clone());
            txn.append_batch(MutationBatch {
                base_revision: previous_revision,
                mutations: vec![DesiredStateMutation::PutExecutor(record)],
            })?;
            Ok(((), true))
        })
    }

    pub(super) fn put_artifact_record_tracked(
        &self,
        record: ArtifactRecord,
    ) -> Result<(), NodeError> {
        let previous_revision = self.current_desired_revision();
        self.commit_desired_state_update_if_changed(previous_revision, |txn| {
            if txn.store().desired.artifacts.get(&record.artifact_id) == Some(&record) {
                return Ok(((), false));
            }
            txn.store().desired.put_artifact(record.clone());
            txn.append_batch(MutationBatch {
                base_revision: previous_revision,
                mutations: vec![DesiredStateMutation::PutArtifact(record)],
            })?;
            Ok(((), true))
        })
    }

    pub(super) async fn put_artifact_record_tracked_async(
        &self,
        record: ArtifactRecord,
    ) -> Result<(), NodeError> {
        let previous_revision = self.current_desired_revision();
        self.commit_desired_state_update_async_if_changed(previous_revision, |txn| {
            if txn.store().desired.artifacts.get(&record.artifact_id) == Some(&record) {
                return Ok(((), false));
            }
            txn.store().desired.put_artifact(record.clone());
            txn.append_batch(MutationBatch {
                base_revision: previous_revision,
                mutations: vec![DesiredStateMutation::PutArtifact(record)],
            })?;
            Ok(((), true))
        })
        .await
    }
}

fn mutation_history_contiguity_error(action: &str, err: impl std::fmt::Display) -> NodeError {
    NodeError::Storage(format!("{action}: {err}"))
}

pub(super) fn normalize_mutation_history_with_baseline(
    mut history: Vec<MutationBatch>,
    baseline: &mut DesiredClusterState,
    max_batches: usize,
    max_bytes: usize,
) -> Result<Vec<MutationBatch>, NodeError> {
    normalize_mutation_history_in_place(&mut history, baseline, max_batches, max_bytes)?;
    Ok(history)
}

pub(super) fn normalize_mutation_history_in_place(
    history: &mut Vec<MutationBatch>,
    baseline: &mut DesiredClusterState,
    max_batches: usize,
    max_bytes: usize,
) -> Result<(), NodeError> {
    history.retain(|batch| !batch.mutations.is_empty());
    if history.is_empty() {
        *baseline = DesiredClusterState::default();
        return Ok(());
    }
    let keep_batches = max_batches.max(1);
    if history.len() > keep_batches {
        let drain_count = history.len() - keep_batches;
        for dropped in history.drain(0..drain_count) {
            dropped.apply_to_checked(baseline).map_err(|err| {
                mutation_history_contiguity_error(
                    "failed to fold dropped mutation batch into history baseline",
                    err,
                )
            })?;
        }
    }

    let keep_bytes = max_bytes.max(1);
    let mut encoded_len = mutation_history_encoded_len(history);
    while history.len() > 1 && encoded_len > keep_bytes {
        let dropped = history.remove(0);
        dropped.apply_to_checked(baseline).map_err(|err| {
            mutation_history_contiguity_error(
                "failed to fold trimmed mutation batch into history baseline",
                err,
            )
        })?;
        encoded_len = mutation_history_encoded_len(history);
    }
    Ok(())
}

fn mutation_history_encoded_len(history: &[MutationBatch]) -> usize {
    encode_to_vec(&history.to_vec())
        .map(|bytes| bytes.len())
        .unwrap_or(usize::MAX)
}
