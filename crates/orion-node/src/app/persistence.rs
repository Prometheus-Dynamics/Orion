mod worker;

use super::{
    NodeApp, NodeError,
    desired_state::{
        normalize_mutation_history_in_place, normalize_mutation_history_with_baseline,
    },
};
use crate::blocking::run_possibly_blocking;
use crate::storage::{
    EncodedDesiredSnapshot, NodeStorage, PersistedSnapshotSections, encode_archive_to_vec,
    encode_desired_snapshot,
};
use orion::{
    ArtifactId, Revision,
    control_plane::{
        AppliedClusterState, ArtifactRecord, ClusterStateEnvelope, DesiredClusterState,
        MutationBatch, ObservedClusterState,
    },
};
use std::{fs::File, io::Read};
use tracing::info_span;

pub(crate) use worker::PersistenceWorker;
#[cfg(test)]
pub(crate) use worker::{clear_test_persist_delay, set_test_persist_delay};

struct PersistedStateBundle {
    snapshot: ClusterStateEnvelope,
    history: Vec<MutationBatch>,
    baseline: DesiredClusterState,
}

pub(super) struct EncodedPersistedStateBundle {
    desired: EncodedDesiredSnapshot,
    observed_revision: Revision,
    observed_bytes: Vec<u8>,
    applied_revision: Revision,
    applied_bytes: Vec<u8>,
    history_bytes: Vec<u8>,
    baseline_revision: Revision,
    baseline_bytes: Option<Vec<u8>>,
    snapshot_rewrite_cadence: u64,
}

pub(super) fn persist_bundle_to_storage(
    storage: &NodeStorage,
    bundle: &EncodedPersistedStateBundle,
) -> Result<(), NodeError> {
    worker::delay_before_persist_for_tests();
    storage.save_mutation_history_bytes(&bundle.history_bytes)?;
    storage.save_mutation_history_baseline_bytes(
        bundle.baseline_revision,
        bundle.baseline_bytes.as_deref(),
    )?;
    let rewrite_desired = should_rewrite_desired_snapshot(storage, bundle)?;
    storage.save_encoded_snapshot(
        &bundle.desired,
        bundle.observed_revision,
        &bundle.observed_bytes,
        bundle.applied_revision,
        &bundle.applied_bytes,
        rewrite_desired,
    )
}

fn should_rewrite_desired_snapshot(
    storage: &NodeStorage,
    bundle: &EncodedPersistedStateBundle,
) -> Result<bool, NodeError> {
    let Some(manifest) = storage.load_snapshot_manifest()? else {
        return Ok(true);
    };
    if !storage.snapshot_desired_path().exists() {
        return Ok(true);
    }
    if bundle.desired.revision <= manifest.desired_snapshot_revision {
        return Ok(false);
    }
    Ok(bundle
        .desired
        .revision
        .get()
        .saturating_sub(manifest.desired_snapshot_revision.get())
        >= bundle.snapshot_rewrite_cadence.max(1))
}

impl NodeApp {
    async fn persist_bundle_async(
        storage: NodeStorage,
        bundle: EncodedPersistedStateBundle,
    ) -> Result<(), NodeError> {
        tokio::task::spawn_blocking(move || persist_bundle_to_storage(&storage, &bundle))
            .await
            .map_err(|err| NodeError::Storage(format!("persistence task join failed: {err}")))?
    }

    fn replay_bundle_from_snapshot(
        &self,
        snapshot: PersistedSnapshotSections,
        desired: DesiredClusterState,
        history: Vec<MutationBatch>,
        baseline: DesiredClusterState,
    ) -> PersistedStateBundle {
        PersistedStateBundle {
            snapshot: ClusterStateEnvelope::new(desired, snapshot.observed, snapshot.applied),
            history,
            baseline,
        }
    }

    fn restore_replayed_snapshot_state(
        &self,
        snapshot: PersistedSnapshotSections,
        desired: DesiredClusterState,
        history: Vec<MutationBatch>,
        baseline: DesiredClusterState,
    ) -> Result<(), NodeError> {
        self.restore_persisted_state_bundle(
            self.replay_bundle_from_snapshot(snapshot, desired, history, baseline),
        )
    }

    fn capture_encoded_persisted_state_bundle(
        &self,
    ) -> Result<EncodedPersistedStateBundle, NodeError> {
        self.with_persisted_state_read(|store, history, baseline| {
            Ok(EncodedPersistedStateBundle {
                desired: encode_desired_snapshot(&store.desired)?,
                observed_revision: store.observed.revision,
                observed_bytes: encode_archive_to_vec(&store.observed)?,
                applied_revision: store.applied.revision,
                applied_bytes: encode_archive_to_vec(&store.applied)?,
                history_bytes: encode_archive_to_vec(history)?,
                baseline_revision: baseline.revision,
                baseline_bytes: if baseline.revision == Revision::ZERO {
                    None
                } else {
                    Some(encode_archive_to_vec(baseline)?)
                },
                snapshot_rewrite_cadence: self.config.runtime_tuning.snapshot_rewrite_cadence,
            })
        })
    }

    fn restore_persisted_state_bundle(
        &self,
        bundle: PersistedStateBundle,
    ) -> Result<(), NodeError> {
        self.restore_desired_state_bundle(
            bundle.snapshot.desired,
            bundle.snapshot.observed,
            bundle.snapshot.applied,
            bundle.history,
            bundle.baseline,
        )
    }

    pub fn replay_state(&self) -> Result<bool, NodeError> {
        let _span = info_span!("replay", node = %self.config.node_id).entered();
        let started = std::time::Instant::now();
        let Some(storage) = &self.storage else {
            return Ok(false);
        };
        let snapshot_sections_result = storage.load_snapshot_sections();
        let history_result = storage.load_mutation_history();
        let baseline_result = storage.load_mutation_history_baseline();

        let result = match (snapshot_sections_result, history_result, baseline_result) {
            (Ok(Some(snapshot)), Ok(history), Ok(baseline)) => {
                let mut baseline = baseline.unwrap_or_default();
                let history = normalize_mutation_history_with_baseline(
                    history,
                    &mut baseline,
                    self.config.runtime_tuning.max_mutation_history_batches,
                    self.config.runtime_tuning.max_mutation_history_bytes,
                )?;
                let desired =
                    self.replay_desired_from_snapshot(storage, &snapshot, &baseline, &history)?;
                self.restore_replayed_snapshot_state(snapshot, desired, history, baseline)?;
                Ok(true)
            }
            (Ok(Some(snapshot)), Err(history_err), Ok(_)) => {
                if snapshot.manifest.latest_desired_revision
                    != snapshot.manifest.desired_snapshot_revision
                {
                    Err(history_err)
                } else {
                    let desired = storage.load_snapshot_desired(&snapshot.manifest)?;
                    self.restore_replayed_snapshot_state(
                        snapshot,
                        desired,
                        Vec::new(),
                        DesiredClusterState::default(),
                    )?;
                    Ok(true)
                }
            }
            (Ok(Some(snapshot)), Err(history_err), Err(_)) => {
                if snapshot.manifest.latest_desired_revision
                    != snapshot.manifest.desired_snapshot_revision
                {
                    Err(history_err)
                } else {
                    let desired = storage.load_snapshot_desired(&snapshot.manifest)?;
                    self.restore_replayed_snapshot_state(
                        snapshot,
                        desired,
                        Vec::new(),
                        DesiredClusterState::default(),
                    )?;
                    Ok(true)
                }
            }
            (Ok(Some(snapshot)), Ok(history), Err(_)) => {
                let mut baseline = DesiredClusterState::default();
                let history = normalize_mutation_history_with_baseline(
                    history,
                    &mut baseline,
                    self.config.runtime_tuning.max_mutation_history_batches,
                    self.config.runtime_tuning.max_mutation_history_bytes,
                )?;
                let desired =
                    self.replay_desired_from_snapshot(storage, &snapshot, &baseline, &history)?;
                self.restore_replayed_snapshot_state(snapshot, desired, history, baseline)?;
                Ok(true)
            }
            (Ok(None), Ok(history), Ok(baseline)) | (Err(_), Ok(history), Ok(baseline))
                if !history.is_empty() =>
            {
                let mut baseline = baseline.unwrap_or_default();
                let history = normalize_mutation_history_with_baseline(
                    history,
                    &mut baseline,
                    self.config.runtime_tuning.max_mutation_history_batches,
                    self.config.runtime_tuning.max_mutation_history_bytes,
                )?;
                let desired = reconstruct_desired_from_history(&baseline, &history)?;
                self.restore_persisted_state_bundle(PersistedStateBundle {
                    snapshot: ClusterStateEnvelope::new(
                        desired,
                        ObservedClusterState::default(),
                        AppliedClusterState::default(),
                    ),
                    history,
                    baseline,
                })?;
                Ok(true)
            }
            (Ok(None), Ok(_), Ok(_)) => Ok(false),
            (Err(snapshot_err), Ok(_), Ok(_)) => Err(snapshot_err),
            (Ok(None), Err(history_err), Ok(_)) => Err(history_err),
            (Err(snapshot_err), Err(_), Ok(_)) => Err(snapshot_err),
            (Err(snapshot_err), _, Err(_)) => Err(snapshot_err),
            (Ok(None), Err(history_err), Err(_)) => Err(history_err),
            (Ok(None), Ok(history), Err(baseline_err)) if !history.is_empty() => Err(baseline_err),
            (Ok(None), Ok(_), Err(_)) => Ok(false),
        };

        match &result {
            Ok(_) => self.record_replay_success(started.elapsed()),
            Err(err) => self.record_replay_failure(started.elapsed(), err),
        }

        result
    }

    pub async fn persist_state_async(&self) -> Result<(), NodeError> {
        let Some(storage) = &self.storage else {
            return Ok(());
        };
        let started = std::time::Instant::now();
        let result = async {
            self.compact_mutation_history()?;
            let bundle = self.capture_encoded_persisted_state_bundle()?;
            if let Some(worker) = &self.persistence_worker {
                return worker.persist_bundle_async(bundle).await;
            }
            Self::persist_bundle_async(storage.clone(), bundle).await
        }
        .await;
        match &result {
            Ok(()) => self.record_persistence_success(started.elapsed()),
            Err(error) => self.record_persistence_failure(started.elapsed(), error),
        }
        result
    }

    pub fn persist_state(&self) -> Result<(), NodeError> {
        // Keep the synchronous entrypoint for startup/admin and other non-async callers. The
        // request-serving path should prefer `persist_state_async()` so runtime threads do not
        // inherit this durable write boundary unnecessarily.
        let Some(storage) = &self.storage else {
            return Ok(());
        };
        let started = std::time::Instant::now();
        let result = (|| {
            self.compact_mutation_history()?;
            let bundle = self.capture_encoded_persisted_state_bundle()?;
            if let Some(worker) = &self.persistence_worker {
                return worker.persist_bundle_blocking(bundle);
            }
            let storage = storage.clone();
            run_possibly_blocking(|| persist_bundle_to_storage(&storage, &bundle))
        })();
        match &result {
            Ok(()) => self.record_persistence_success(started.elapsed()),
            Err(error) => self.record_persistence_failure(started.elapsed(), error),
        }
        result
    }

    pub async fn put_artifact_content_async(
        &self,
        artifact: &ArtifactRecord,
        content: &[u8],
    ) -> Result<(), NodeError> {
        self.put_artifact_content_stream_async(
            artifact.clone(),
            std::io::Cursor::new(content.to_vec()),
        )
        .await
    }

    pub async fn put_artifact_content_stream_async<R>(
        &self,
        artifact: ArtifactRecord,
        content: R,
    ) -> Result<(), NodeError>
    where
        R: Read + Send + 'static,
    {
        self.put_artifact_record_tracked_async(artifact.clone())
            .await?;
        if self.storage.is_some() {
            let started = std::time::Instant::now();
            let result = async {
                if let Some(worker) = &self.persistence_worker {
                    worker
                        .put_artifact_stream_async(artifact.clone(), Box::new(content))
                        .await?;
                } else if let Some(storage) = &self.storage {
                    let storage = storage.clone();
                    tokio::task::spawn_blocking(move || {
                        storage.put_artifact_stream(&artifact, content)
                    })
                    .await
                    .map_err(|err| {
                        NodeError::Storage(format!("artifact persistence task join failed: {err}"))
                    })??;
                }
                Ok(())
            }
            .await;
            match &result {
                Ok(()) => self.record_artifact_write_success(started.elapsed()),
                Err(error) => self.record_artifact_write_failure(started.elapsed(), error),
            }
            result?;
        }
        self.persist_state_async().await
    }

    pub fn put_artifact_content(
        &self,
        artifact: &ArtifactRecord,
        content: &[u8],
    ) -> Result<(), NodeError> {
        // This synchronous variant remains for call sites that do not own an async runtime. The
        // async path is the preferred hot-path surface for release builds.
        self.put_artifact_record_tracked(artifact.clone())?;
        if self.storage.is_some() {
            let started = std::time::Instant::now();
            let result = (|| {
                if let Some(worker) = &self.persistence_worker {
                    worker.put_artifact_stream_blocking(
                        artifact.clone(),
                        Box::new(std::io::Cursor::new(content.to_vec())),
                    )?;
                } else if let Some(storage) = &self.storage {
                    let storage = storage.clone();
                    run_possibly_blocking(|| storage.put_artifact(artifact, content))?;
                }
                Ok(())
            })();
            match &result {
                Ok(()) => self.record_artifact_write_success(started.elapsed()),
                Err(error) => self.record_artifact_write_failure(started.elapsed(), error),
            }
            result?;
        }
        self.persist_state()
    }

    pub fn put_artifact_content_stream<R>(
        &self,
        artifact: &ArtifactRecord,
        content: R,
    ) -> Result<(), NodeError>
    where
        R: Read + Send + 'static,
    {
        self.put_artifact_record_tracked(artifact.clone())?;
        if self.storage.is_some() {
            let started = std::time::Instant::now();
            let result = (|| {
                if let Some(worker) = &self.persistence_worker {
                    worker.put_artifact_stream_blocking(artifact.clone(), Box::new(content))?;
                } else if let Some(storage) = &self.storage {
                    let storage = storage.clone();
                    run_possibly_blocking(|| storage.put_artifact_stream(artifact, content))?;
                }
                Ok(())
            })();
            match &result {
                Ok(()) => self.record_artifact_write_success(started.elapsed()),
                Err(error) => self.record_artifact_write_failure(started.elapsed(), error),
            }
            result?;
        }
        self.persist_state()
    }

    pub fn artifact_content_stream(
        &self,
        artifact_id: &ArtifactId,
    ) -> Result<Option<(ArtifactRecord, File)>, NodeError> {
        match &self.storage {
            Some(storage) => storage.open_artifact(artifact_id),
            None => Ok(None),
        }
    }

    pub fn artifact_content(
        &self,
        artifact_id: &ArtifactId,
    ) -> Result<Option<(ArtifactRecord, Vec<u8>)>, NodeError> {
        match &self.storage {
            Some(storage) => storage.get_artifact(artifact_id),
            None => Ok(None),
        }
    }

    fn compact_mutation_history(&self) -> Result<(), NodeError> {
        self.with_mutation_history_mut(|history, baseline| {
            normalize_mutation_history_in_place(
                history,
                baseline,
                self.config.runtime_tuning.max_mutation_history_batches,
                self.config.runtime_tuning.max_mutation_history_bytes,
            )
        })?;
        Ok(())
    }

    fn replay_desired_from_snapshot(
        &self,
        storage: &NodeStorage,
        snapshot: &PersistedSnapshotSections,
        baseline: &DesiredClusterState,
        history: &[MutationBatch],
    ) -> Result<DesiredClusterState, NodeError> {
        if snapshot.manifest.latest_desired_revision == snapshot.manifest.desired_snapshot_revision
        {
            return storage.load_snapshot_desired(&snapshot.manifest);
        }

        if baseline.revision > snapshot.manifest.desired_snapshot_revision {
            return reconstruct_desired_from_history(baseline, history);
        }

        let desired_snapshot = storage.load_snapshot_desired(&snapshot.manifest)?;
        reconstruct_desired_from_checkpoint_and_history(
            &desired_snapshot,
            snapshot.manifest.latest_desired_revision,
            history,
        )
    }
}

fn reconstruct_desired_from_history(
    baseline: &DesiredClusterState,
    history: &[MutationBatch],
) -> Result<DesiredClusterState, NodeError> {
    let mut desired = baseline.clone();
    for batch in history.iter().cloned() {
        batch.apply_to_checked(&mut desired)?;
    }
    Ok(desired)
}

fn reconstruct_desired_from_checkpoint_and_history(
    checkpoint: &DesiredClusterState,
    latest_desired_revision: orion::Revision,
    history: &[MutationBatch],
) -> Result<DesiredClusterState, NodeError> {
    let mut desired = checkpoint.clone();
    let mut saw_newer_batch = false;

    for batch in history.iter().cloned() {
        if batch.base_revision < desired.revision {
            continue;
        }
        if batch.base_revision > desired.revision {
            saw_newer_batch = true;
            break;
        }
        saw_newer_batch = true;
        batch.apply_to_checked(&mut desired)?;
    }

    if saw_newer_batch && desired.revision == checkpoint.revision {
        return Err(NodeError::Storage(format!(
            "persisted desired snapshot at revision {} is stale and history does not bridge it",
            checkpoint.revision
        )));
    }

    if desired.revision != latest_desired_revision {
        return Err(NodeError::Storage(format!(
            "persisted desired state replay stopped at revision {} but manifest expects {}",
            desired.revision, latest_desired_revision
        )));
    }

    Ok(desired)
}
