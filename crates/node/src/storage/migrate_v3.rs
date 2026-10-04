//! Upgrade of state directories written in snapshot format 3 (control protocol v2 and earlier).
//!
//! Format 4 adds per-object HLC stamps and general tombstones to the desired state and stamps to
//! mutation batches, and control protocol v3 added `NodeRecord::clock`, so format-3 desired
//! snapshots, histories and observed snapshots cannot be decoded with the current types. This
//! module decodes them with frozen copies of the old layouts, replays the old history, and
//! rewrites the desired state in format 4. See "Upgrading from protocol v2" in
//! `docs/peer-sync.md`.

use super::{
    DesiredStateSectionCounts, NodeStorage, SNAPSHOT_FORMAT_VERSION, SnapshotManifest,
    decode_from_slice, encode_to_vec,
};
use crate::NodeError;
use orion::{
    ArtifactId, ExecutorId, HlcTimestamp, NodeId, ProviderId, ResourceId, Revision, WorkloadId,
    control_plane::{
        AppliedClusterState, ArtifactRecord, DesiredClusterState, DesiredStateSectionFingerprints,
        ExecutorRecord, HealthState, LeaseRecord, NodeRecord, ObservedClusterState, ProviderRecord,
        ResourceRecord, WorkloadRecord,
    },
};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use std::{collections::BTreeMap, path::PathBuf};

pub(super) const LEGACY_SNAPSHOT_FORMAT_VERSION: u32 = 3;
/// Directory (inside the state directory) that keeps the format-3 files after a migration.
pub(crate) const LEGACY_BACKUP_DIR: &str = "legacy-format-3";

/// `NodeRecord` before control protocol v3 (no `clock`).
#[derive(Clone, Debug, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub(crate) struct LegacyNodeRecord {
    pub(crate) node_id: NodeId,
    pub(crate) health: HealthState,
    pub(crate) schedulable: bool,
    pub(crate) labels: Vec<String>,
}

/// `DesiredClusterState` in snapshot format 3.
#[derive(Clone, Debug, Default, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub(crate) struct LegacyDesiredClusterState {
    pub(crate) revision: Revision,
    pub(crate) nodes: BTreeMap<NodeId, LegacyNodeRecord>,
    pub(crate) artifacts: BTreeMap<ArtifactId, ArtifactRecord>,
    pub(crate) workloads: BTreeMap<WorkloadId, WorkloadRecord>,
    pub(crate) workload_tombstones: BTreeMap<WorkloadId, Revision>,
    pub(crate) resources: BTreeMap<ResourceId, ResourceRecord>,
    pub(crate) providers: BTreeMap<ProviderId, ProviderRecord>,
    pub(crate) executors: BTreeMap<ExecutorId, ExecutorRecord>,
    pub(crate) leases: BTreeMap<ResourceId, LeaseRecord>,
}

/// `DesiredStateMutation` in snapshot format 3.
#[derive(Clone, Debug, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub(crate) enum LegacyDesiredStateMutation {
    PutNode(LegacyNodeRecord),
    PutArtifact(ArtifactRecord),
    PutWorkload(WorkloadRecord),
    PutResource(ResourceRecord),
    PutProvider(ProviderRecord),
    PutExecutor(ExecutorRecord),
    PutLease(LeaseRecord),
    RemoveNode(NodeId),
    RemoveArtifact(ArtifactId),
    RemoveWorkload(WorkloadId),
    RemoveResource(ResourceId),
    RemoveProvider(ProviderId),
    RemoveExecutor(ExecutorId),
    RemoveLease(ResourceId),
}

/// `MutationBatch` in snapshot format 3 (no stamps).
#[derive(Clone, Debug, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub(crate) struct LegacyMutationBatch {
    pub(crate) base_revision: Revision,
    pub(crate) mutations: Vec<LegacyDesiredStateMutation>,
}

#[derive(Clone, Debug, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub(crate) struct LegacyDesiredStateSectionCounts {
    pub(crate) nodes: u64,
    pub(crate) artifacts: u64,
    pub(crate) workloads: u64,
    pub(crate) workload_tombstones: u64,
    pub(crate) resources: u64,
    pub(crate) providers: u64,
    pub(crate) executors: u64,
    pub(crate) leases: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub(crate) struct LegacySnapshotManifest {
    pub(crate) format_version: u32,
    pub(crate) desired_snapshot_revision: Revision,
    pub(crate) latest_desired_revision: Revision,
    pub(crate) observed_revision: Revision,
    pub(crate) applied_revision: Revision,
    pub(crate) desired_section_fingerprints: DesiredStateSectionFingerprints,
    pub(crate) desired_section_counts: LegacyDesiredStateSectionCounts,
}

/// What a migration did.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StateMigrationReport {
    pub from_format: u32,
    pub to_format: u32,
    pub desired_revision: Revision,
    pub objects: usize,
    pub tombstones: usize,
    /// Where the format-3 files were kept.
    pub backup_dir: PathBuf,
}

impl LegacyDesiredClusterState {
    fn bump(&mut self) {
        self.revision = self.revision.next();
    }

    /// Format-3 mutation semantics, so the old history replays exactly as it was written.
    fn apply(&mut self, mutation: LegacyDesiredStateMutation) {
        use LegacyDesiredStateMutation as M;
        macro_rules! put {
            ($map:ident, $record:expr, $id:ident) => {{
                let record = $record;
                self.$map.insert(record.$id.clone(), record);
                self.bump();
            }};
        }
        macro_rules! remove {
            ($map:ident, $id:expr) => {{
                if self.$map.remove(&$id).is_some() {
                    self.bump();
                }
            }};
        }
        match mutation {
            M::PutNode(record) => put!(nodes, record, node_id),
            M::PutArtifact(record) => put!(artifacts, record, artifact_id),
            M::PutWorkload(record) => {
                self.workload_tombstones.remove(&record.workload_id);
                put!(workloads, record, workload_id)
            }
            M::PutResource(record) => put!(resources, record, resource_id),
            M::PutProvider(record) => put!(providers, record, provider_id),
            M::PutExecutor(record) => put!(executors, record, executor_id),
            M::PutLease(record) => put!(leases, record, resource_id),
            M::RemoveNode(id) => remove!(nodes, id),
            M::RemoveArtifact(id) => remove!(artifacts, id),
            M::RemoveWorkload(id) => {
                let removed = self.workloads.remove(&id).is_some();
                if removed || !self.workload_tombstones.contains_key(&id) {
                    self.bump();
                    self.workload_tombstones.insert(id, self.revision);
                }
            }
            M::RemoveResource(id) => remove!(resources, id),
            M::RemoveProvider(id) => remove!(providers, id),
            M::RemoveExecutor(id) => remove!(executors, id),
            M::RemoveLease(id) => remove!(leases, id),
        }
    }

    /// The current layout, with every object and workload tombstone stamped `stamp`.
    fn into_current(self, stamp: HlcTimestamp) -> DesiredClusterState {
        let mut desired = DesiredClusterState {
            revision: self.revision,
            ..DesiredClusterState::default()
        };
        for (id, record) in self.nodes {
            let mut builder = NodeRecord::builder(record.node_id)
                .health(record.health)
                .schedulable(record.schedulable);
            for label in record.labels {
                builder = builder.label(label);
            }
            desired.stamps.nodes.insert(id.clone(), stamp);
            desired.nodes.insert(id, builder.build());
        }
        macro_rules! carry {
            ($field:ident) => {
                for (id, record) in self.$field {
                    desired.stamps.$field.insert(id.clone(), stamp);
                    desired.$field.insert(id, record);
                }
            };
        }
        carry!(artifacts);
        carry!(workloads);
        carry!(resources);
        carry!(providers);
        carry!(executors);
        carry!(leases);
        for id in self.workload_tombstones.into_keys() {
            desired.tombstones.workloads.insert(id, stamp);
        }
        desired
    }
}

fn unreadable(storage: &NodeStorage, detail: impl std::fmt::Display) -> NodeError {
    NodeError::Storage(format!(
        "state directory {} holds snapshot format {LEGACY_SNAPSHOT_FORMAT_VERSION} (written by \
         orion-node before control protocol v3) that could not be migrated: {detail}. Nothing was \
         overwritten. To start with an empty desired state that the node pulls from its peers, \
         move snapshot-*.rkyv and mutation-history*.rkyv out of the state directory; see \
         docs/peer-sync.md",
        storage.root().display()
    ))
}

impl NodeStorage {
    /// Migrates a format-3 state directory to the current format in place, keeping the old files
    /// in [`LEGACY_BACKUP_DIR`]. Returns `None` when there is nothing to migrate (no state, or
    /// already current). A format-3 directory that cannot be migrated is an error, so the node
    /// never starts on (and later overwrites) state it could not read.
    pub fn migrate_legacy_state(
        &self,
        local_node_tag: u64,
    ) -> Result<Option<StateMigrationReport>, NodeError> {
        let manifest_path = self.snapshot_manifest_path();
        if !manifest_path.exists() {
            return Ok(None);
        }
        let bytes =
            std::fs::read(&manifest_path).map_err(|err| NodeError::Storage(err.to_string()))?;
        if decode_from_slice::<SnapshotManifest>(&bytes)
            .is_ok_and(|manifest| manifest.format_version == SNAPSHOT_FORMAT_VERSION)
        {
            return Ok(None);
        }
        let Ok(legacy) = decode_from_slice::<LegacySnapshotManifest>(&bytes) else {
            // Neither layout: leave it to replay, which reports the decode error.
            return Ok(None);
        };
        if legacy.format_version != LEGACY_SNAPSHOT_FORMAT_VERSION {
            return Ok(None);
        }
        // Read the old files from the backup when an earlier migration was interrupted after
        // making it, so a half-written directory never becomes the source. Nothing is written
        // before the old state has been read completely.
        let interrupted_backup = self.root().join(LEGACY_BACKUP_DIR);
        let source_dir = if interrupted_backup.exists() {
            interrupted_backup
        } else {
            self.root().to_path_buf()
        };
        let desired = self
            .reconstruct_legacy_desired(&legacy, &source_dir)
            .map_err(|err| unreadable(self, err))?;
        let backup_dir = self.backup_legacy_files()?;
        let stamp = HlcTimestamp::new(0, 0, local_node_tag);
        let desired = desired.into_current(stamp);
        let objects = desired.stamps.len();
        let tombstones = desired.tombstones.len();
        let revision = desired.revision;

        // Observed state is rebuilt from live reports; keep only its revision.
        let observed = ObservedClusterState {
            revision: legacy.observed_revision,
            ..ObservedClusterState::default()
        };
        let applied = AppliedClusterState {
            revision: legacy.applied_revision,
        };
        self.atomic_write(&self.snapshot_desired_path(), &encode_to_vec(&desired)?)?;
        self.atomic_write(&self.snapshot_observed_path(), &encode_to_vec(&observed)?)?;
        self.atomic_write(&self.snapshot_applied_path(), &encode_to_vec(&applied)?)?;
        for path in [
            self.mutation_history_path(),
            self.mutation_history_baseline_path(),
        ] {
            if path.exists() {
                std::fs::remove_file(&path).map_err(|err| NodeError::Storage(err.to_string()))?;
            }
        }
        let manifest = SnapshotManifest {
            format_version: SNAPSHOT_FORMAT_VERSION,
            desired_snapshot_revision: revision,
            latest_desired_revision: revision,
            observed_revision: legacy.observed_revision,
            applied_revision: legacy.applied_revision,
            desired_section_fingerprints: crate::app::desired_section_fingerprints(&desired)?,
            desired_section_counts: DesiredStateSectionCounts::of(&desired),
        };
        self.atomic_write(&manifest_path, &encode_to_vec(&manifest)?)?;
        Ok(Some(StateMigrationReport {
            from_format: LEGACY_SNAPSHOT_FORMAT_VERSION,
            to_format: SNAPSHOT_FORMAT_VERSION,
            desired_revision: revision,
            objects,
            tombstones,
            backup_dir,
        }))
    }

    /// Copies the format-3 files into the backup directory once (rename makes it atomic), so an
    /// interrupted migration restarts from the untouched originals.
    fn backup_legacy_files(&self) -> Result<PathBuf, NodeError> {
        let backup_dir = self.root().join(LEGACY_BACKUP_DIR);
        if backup_dir.exists() {
            return Ok(backup_dir);
        }
        let staging = self.root().join(format!("{LEGACY_BACKUP_DIR}.tmp"));
        if staging.exists() {
            std::fs::remove_dir_all(&staging).map_err(|err| NodeError::Storage(err.to_string()))?;
        }
        std::fs::create_dir_all(&staging).map_err(|err| NodeError::Storage(err.to_string()))?;
        for path in [
            self.snapshot_manifest_path(),
            self.snapshot_desired_path(),
            self.snapshot_observed_path(),
            self.snapshot_applied_path(),
            self.mutation_history_path(),
            self.mutation_history_baseline_path(),
        ] {
            if let Some(name) = path.file_name().filter(|_| path.exists()) {
                std::fs::copy(&path, staging.join(name))
                    .map_err(|err| NodeError::Storage(err.to_string()))?;
            }
        }
        std::fs::rename(&staging, &backup_dir)
            .map_err(|err| NodeError::Storage(err.to_string()))?;
        super::sync_parent_directory(self.root())?;
        Ok(backup_dir)
    }

    fn reconstruct_legacy_desired(
        &self,
        manifest: &LegacySnapshotManifest,
        source_dir: &std::path::Path,
    ) -> Result<LegacyDesiredClusterState, String> {
        let read = |name: &str| -> Result<Option<Vec<u8>>, String> {
            let path = source_dir.join(name);
            if !path.exists() {
                return Ok(None);
            }
            std::fs::read(&path)
                .map(Some)
                .map_err(|err| format!("failed to read {}: {err}", path.display()))
        };
        let file_name = |path: PathBuf| {
            path.file_name()
                .map(|name| name.to_string_lossy().into_owned())
                .unwrap_or_default()
        };
        let checkpoint: LegacyDesiredClusterState =
            match read(&file_name(self.snapshot_desired_path()))? {
                Some(bytes) => decode_from_slice(&bytes)
                    .map_err(|err| format!("desired snapshot did not decode: {err}"))?,
                None => return Err("the desired snapshot file is missing".into()),
            };
        if checkpoint.revision != manifest.desired_snapshot_revision {
            return Err("the desired snapshot does not match the manifest".into());
        }
        if manifest.latest_desired_revision == manifest.desired_snapshot_revision {
            return Ok(checkpoint);
        }
        let history: Vec<LegacyMutationBatch> =
            match read(&file_name(self.mutation_history_path()))? {
                Some(bytes) => decode_from_slice(&bytes)
                    .map_err(|err| format!("mutation history did not decode: {err}"))?,
                None => Vec::new(),
            };
        let baseline: Option<LegacyDesiredClusterState> =
            match read(&file_name(self.mutation_history_baseline_path()))? {
                Some(bytes) => Some(
                    decode_from_slice(&bytes)
                        .map_err(|err| format!("history baseline did not decode: {err}"))?,
                ),
                None => None,
            };
        let mut desired = match baseline {
            Some(baseline) if baseline.revision > checkpoint.revision => baseline,
            _ => checkpoint,
        };
        for batch in history {
            if batch.base_revision < desired.revision {
                continue;
            }
            if batch.base_revision > desired.revision {
                break;
            }
            for mutation in batch.mutations {
                desired.apply(mutation);
            }
        }
        if desired.revision != manifest.latest_desired_revision {
            return Err(format!(
                "the mutation history stops at revision {} but the manifest expects {}",
                desired.revision, manifest.latest_desired_revision
            ));
        }
        Ok(desired)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{NodeApp, NodeConfig};
    use orion::control_plane::{DesiredState, WorkloadRecord};

    fn temp_dir(label: &str) -> PathBuf {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time should be after epoch")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "orion-migrate-{label}-{}-{unique}",
            std::process::id()
        ))
    }

    fn legacy_manifest(snapshot: u64, latest: u64) -> LegacySnapshotManifest {
        LegacySnapshotManifest {
            format_version: LEGACY_SNAPSHOT_FORMAT_VERSION,
            desired_snapshot_revision: Revision::new(snapshot),
            latest_desired_revision: Revision::new(latest),
            observed_revision: Revision::new(7),
            applied_revision: Revision::ZERO,
            desired_section_fingerprints: DesiredStateSectionFingerprints::default(),
            desired_section_counts: LegacyDesiredStateSectionCounts {
                nodes: 1,
                artifacts: 1,
                workloads: 0,
                workload_tombstones: 0,
                resources: 0,
                providers: 0,
                executors: 0,
                leases: 0,
            },
        }
    }

    /// Writes a format-3 state directory: a checkpoint at revision 2 plus history up to 4.
    fn write_legacy_state(storage: &NodeStorage) {
        storage.ensure_layout().expect("layout should be created");
        let mut checkpoint = LegacyDesiredClusterState {
            revision: Revision::new(2),
            ..LegacyDesiredClusterState::default()
        };
        checkpoint.nodes.insert(
            NodeId::new("node-a"),
            LegacyNodeRecord {
                node_id: NodeId::new("node-a"),
                health: HealthState::Healthy,
                schedulable: true,
                labels: vec!["camera".into()],
            },
        );
        checkpoint.artifacts.insert(
            ArtifactId::new("artifact.pose"),
            ArtifactRecord::builder("artifact.pose").build(),
        );
        let history = vec![LegacyMutationBatch {
            base_revision: Revision::new(2),
            mutations: vec![
                LegacyDesiredStateMutation::PutWorkload(
                    WorkloadRecord::builder("workload.pose", "graph.exec.v1", "artifact.pose")
                        .desired_state(DesiredState::Stopped)
                        .assigned_to("node-a")
                        .build(),
                ),
                // Format 3 tombstoned a missing workload and advanced the revision.
                LegacyDesiredStateMutation::RemoveWorkload(WorkloadId::new("workload.old")),
            ],
        }];
        let write = |path: PathBuf, bytes: Vec<u8>| {
            std::fs::write(path, bytes).expect("legacy file should be written")
        };
        write(
            storage.snapshot_manifest_path(),
            encode_to_vec(&legacy_manifest(2, 4)).expect("manifest should encode"),
        );
        write(
            storage.snapshot_desired_path(),
            encode_to_vec(&checkpoint).expect("desired should encode"),
        );
        write(
            storage.mutation_history_path(),
            encode_to_vec(&history).expect("history should encode"),
        );
        write(
            storage.snapshot_observed_path(),
            b"format-3 observed".to_vec(),
        );
        write(
            storage.snapshot_applied_path(),
            encode_to_vec(&AppliedClusterState::default()).expect("applied should encode"),
        );
    }

    #[test]
    fn format_3_state_is_migrated_once_and_kept_as_a_backup() {
        let dir = temp_dir("ok");
        let storage = NodeStorage::new(&dir);
        write_legacy_state(&storage);
        let tag = orion::hlc_node_tag("node-a");

        let report = storage
            .migrate_legacy_state(tag)
            .expect("migration should succeed")
            .expect("a format-3 directory is migrated");
        assert_eq!(report.from_format, 3);
        assert_eq!(report.to_format, SNAPSHOT_FORMAT_VERSION);
        assert_eq!(report.desired_revision, Revision::new(4));
        assert_eq!(report.objects, 3);
        assert_eq!(report.tombstones, 1);
        assert!(report.backup_dir.join("snapshot-desired.rkyv").exists());
        assert!(!storage.mutation_history_path().exists());
        assert_eq!(
            storage.migrate_legacy_state(tag).expect("second call"),
            None,
            "an upgraded directory is left alone"
        );

        let app = NodeApp::try_new(
            NodeConfig::for_local_node("node-a")
                .with_state_dir(&dir)
                .with_ipc_socket_path(dir.join("control.sock")),
        )
        .expect("node should start on the migrated state");
        let desired = app.state_snapshot().state.desired;
        assert_eq!(desired.revision, Revision::new(4));
        assert_eq!(desired.nodes[&NodeId::new("node-a")].labels, vec!["camera"]);
        assert!(
            desired
                .workloads
                .contains_key(&WorkloadId::new("workload.pose"))
        );
        let migrated = HlcTimestamp::new(0, 0, tag);
        assert_eq!(
            desired.stamps.workloads[&WorkloadId::new("workload.pose")],
            migrated
        );
        assert_eq!(
            desired.tombstones.workloads[&WorkloadId::new("workload.old")],
            migrated
        );
        assert_eq!(
            app.state_snapshot().state.observed.revision,
            Revision::new(7)
        );
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn unreadable_format_3_state_fails_startup_without_overwriting_it() {
        let dir = temp_dir("bad");
        let storage = NodeStorage::new(&dir);
        write_legacy_state(&storage);
        std::fs::write(storage.snapshot_desired_path(), b"not an archive")
            .expect("file should be written");
        let manifest_before =
            std::fs::read(storage.snapshot_manifest_path()).expect("manifest should be readable");

        let err = NodeApp::try_new(
            NodeConfig::for_local_node("node-a")
                .with_state_dir(&dir)
                .with_ipc_socket_path(dir.join("control.sock")),
        )
        .err()
        .expect("startup must fail on state it cannot migrate");
        let message = err.to_string();
        assert!(message.contains("could not be migrated"), "{message}");
        assert!(message.contains("docs/peer-sync.md"), "{message}");
        assert_eq!(
            std::fs::read(storage.snapshot_manifest_path()).expect("manifest should be readable"),
            manifest_before,
            "the old manifest is not overwritten"
        );
        let _ = std::fs::remove_dir_all(dir);
    }
}
