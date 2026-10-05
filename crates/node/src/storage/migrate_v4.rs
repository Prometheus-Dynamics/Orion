//! Upgrade of state directories written in snapshot format 4 (control protocol v3).
//!
//! Control protocol v4 added `NodeRecord::host`, which changes the archived layout of every file
//! that holds node records: the desired and observed snapshots, the mutation history, and its
//! baseline. This module decodes them with frozen copies of the format-4 layouts and rewrites
//! them in the current format, keeping every record, stamp, tombstone and revision as it was
//! (node records get `host: None`; the running node reports its host facts again). The old files
//! are kept in `legacy-format-4/`. See "Upgrading from protocol v3" in `docs/host-facts.md`.

use super::{
    NodeStorage, SNAPSHOT_FORMAT_VERSION, SnapshotManifest, StateMigrationReport,
    decode_from_slice, encode_to_vec,
};
use crate::NodeError;
use orion::{
    ArtifactId, ExecutorId, HlcTimestamp, NodeId, ProviderId, ResourceId, Revision, WorkloadId,
    control_plane::{
        ArtifactRecord, DesiredClusterState, DesiredObjectStamps, DesiredStateMutation,
        ExecutorRecord, HealthState, LeaseRecord, MutationBatch, NodeClockFacts, NodeRecord,
        ObservedClusterState, ProviderRecord, ResourceRecord, WorkloadRecord,
    },
};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use std::{collections::BTreeMap, path::Path};

pub(super) const FORMAT_4: u32 = 4;
/// Directory (inside the state directory) that keeps the format-4 files after a migration.
pub(crate) const FORMAT_4_BACKUP_DIR: &str = "legacy-format-4";

/// `NodeRecord` in control protocol v3 (no `host`).
#[derive(Clone, Debug, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub(crate) struct V4NodeRecord {
    pub(crate) node_id: NodeId,
    pub(crate) health: HealthState,
    pub(crate) schedulable: bool,
    pub(crate) labels: Vec<String>,
    pub(crate) clock: Option<NodeClockFacts>,
}

impl V4NodeRecord {
    fn into_current(self) -> NodeRecord {
        NodeRecord {
            node_id: self.node_id,
            health: self.health,
            schedulable: self.schedulable,
            labels: self.labels,
            clock: self.clock,
            host: None,
        }
    }
}

fn convert_nodes(nodes: BTreeMap<NodeId, V4NodeRecord>) -> BTreeMap<NodeId, NodeRecord> {
    nodes
        .into_iter()
        .map(|(id, record)| (id, record.into_current()))
        .collect()
}

/// `DesiredClusterState` in snapshot format 4.
#[derive(Clone, Debug, Default, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub(crate) struct V4DesiredClusterState {
    pub(crate) revision: Revision,
    pub(crate) nodes: BTreeMap<NodeId, V4NodeRecord>,
    pub(crate) artifacts: BTreeMap<ArtifactId, ArtifactRecord>,
    pub(crate) workloads: BTreeMap<WorkloadId, WorkloadRecord>,
    pub(crate) resources: BTreeMap<ResourceId, ResourceRecord>,
    pub(crate) providers: BTreeMap<ProviderId, ProviderRecord>,
    pub(crate) executors: BTreeMap<ExecutorId, ExecutorRecord>,
    pub(crate) leases: BTreeMap<ResourceId, LeaseRecord>,
    pub(crate) stamps: DesiredObjectStamps,
    pub(crate) tombstones: DesiredObjectStamps,
}

impl V4DesiredClusterState {
    fn into_current(self) -> DesiredClusterState {
        DesiredClusterState {
            revision: self.revision,
            nodes: convert_nodes(self.nodes),
            artifacts: self.artifacts,
            workloads: self.workloads,
            resources: self.resources,
            providers: self.providers,
            executors: self.executors,
            leases: self.leases,
            stamps: self.stamps,
            tombstones: self.tombstones,
        }
    }
}

/// `ObservedClusterState` in snapshot format 4.
#[derive(Clone, Debug, Default, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub(crate) struct V4ObservedClusterState {
    pub(crate) revision: Revision,
    pub(crate) nodes: BTreeMap<NodeId, V4NodeRecord>,
    pub(crate) workloads: BTreeMap<WorkloadId, WorkloadRecord>,
    pub(crate) resources: BTreeMap<ResourceId, ResourceRecord>,
    pub(crate) leases: BTreeMap<ResourceId, LeaseRecord>,
}

/// `DesiredStateMutation` in snapshot format 4.
#[derive(Clone, Debug, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub(crate) enum V4DesiredStateMutation {
    PutNode(V4NodeRecord),
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

impl V4DesiredStateMutation {
    fn into_current(self) -> DesiredStateMutation {
        use DesiredStateMutation as M;
        use V4DesiredStateMutation as V4;
        match self {
            V4::PutNode(record) => M::PutNode(record.into_current()),
            V4::PutArtifact(record) => M::PutArtifact(record),
            V4::PutWorkload(record) => M::PutWorkload(record),
            V4::PutResource(record) => M::PutResource(record),
            V4::PutProvider(record) => M::PutProvider(record),
            V4::PutExecutor(record) => M::PutExecutor(record),
            V4::PutLease(record) => M::PutLease(record),
            V4::RemoveNode(id) => M::RemoveNode(id),
            V4::RemoveArtifact(id) => M::RemoveArtifact(id),
            V4::RemoveWorkload(id) => M::RemoveWorkload(id),
            V4::RemoveResource(id) => M::RemoveResource(id),
            V4::RemoveProvider(id) => M::RemoveProvider(id),
            V4::RemoveExecutor(id) => M::RemoveExecutor(id),
            V4::RemoveLease(id) => M::RemoveLease(id),
        }
    }
}

/// `MutationBatch` in snapshot format 4.
#[derive(Clone, Debug, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub(crate) struct V4MutationBatch {
    pub(crate) base_revision: Revision,
    pub(crate) mutations: Vec<V4DesiredStateMutation>,
    pub(crate) stamps: Vec<HlcTimestamp>,
}

impl V4MutationBatch {
    fn into_current(self) -> MutationBatch {
        MutationBatch {
            base_revision: self.base_revision,
            mutations: self
                .mutations
                .into_iter()
                .map(V4DesiredStateMutation::into_current)
                .collect(),
            stamps: self.stamps,
        }
    }
}

/// Converted files, ready to be written.
struct ConvertedState {
    desired: DesiredClusterState,
    observed: Option<ObservedClusterState>,
    history: Option<Vec<MutationBatch>>,
    baseline: Option<DesiredClusterState>,
}

fn read_optional(dir: &Path, name: &str) -> Result<Option<Vec<u8>>, String> {
    let path = dir.join(name);
    if !path.exists() {
        return Ok(None);
    }
    std::fs::read(&path)
        .map(Some)
        .map_err(|err| format!("failed to read {}: {err}", path.display()))
}

fn file_name(path: std::path::PathBuf) -> String {
    path.file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_default()
}

fn unreadable(storage: &NodeStorage, detail: impl std::fmt::Display) -> NodeError {
    NodeError::Storage(format!(
        "state directory {} holds snapshot format {FORMAT_4} (written by orion-node with control \
         protocol v3) that could not be migrated: {detail}. Nothing was overwritten. To start \
         with an empty desired state that the node pulls from its peers, move snapshot-*.rkyv \
         and mutation-history*.rkyv out of the state directory; see docs/host-facts.md",
        storage.root().display()
    ))
}

impl NodeStorage {
    /// Rewrites a format-4 state directory in the current format, keeping the old files in
    /// `legacy-format-4/`.
    pub(super) fn migrate_format_4(
        &self,
        manifest: SnapshotManifest,
    ) -> Result<Option<StateMigrationReport>, NodeError> {
        // Read from the backup when an earlier migration was interrupted after making it.
        let interrupted_backup = self.root().join(FORMAT_4_BACKUP_DIR);
        let source_dir = if interrupted_backup.exists() {
            interrupted_backup
        } else {
            self.root().to_path_buf()
        };
        let converted = self
            .convert_format_4(&manifest, &source_dir)
            .map_err(|err| unreadable(self, err))?;
        let backup_dir = self.backup_state_files(FORMAT_4_BACKUP_DIR)?;

        let desired = &converted.desired;
        let objects = desired.stamps.len();
        let tombstones = desired.tombstones.len();
        self.atomic_write(&self.snapshot_desired_path(), &encode_to_vec(desired)?)?;
        if let Some(observed) = &converted.observed {
            self.atomic_write(&self.snapshot_observed_path(), &encode_to_vec(observed)?)?;
        }
        if let Some(history) = &converted.history {
            self.atomic_write(&self.mutation_history_path(), &encode_to_vec(history)?)?;
        }
        if let Some(baseline) = &converted.baseline {
            self.atomic_write(
                &self.mutation_history_baseline_path(),
                &encode_to_vec(baseline)?,
            )?;
        }
        let latest_desired_revision = manifest.latest_desired_revision;
        let manifest = SnapshotManifest {
            format_version: SNAPSHOT_FORMAT_VERSION,
            desired_section_fingerprints: crate::app::desired_section_fingerprints(desired)?,
            ..manifest
        };
        self.atomic_write(&self.snapshot_manifest_path(), &encode_to_vec(&manifest)?)?;
        Ok(Some(StateMigrationReport {
            from_format: FORMAT_4,
            to_format: SNAPSHOT_FORMAT_VERSION,
            desired_revision: latest_desired_revision,
            objects,
            tombstones,
            backup_dir,
        }))
    }

    fn convert_format_4(
        &self,
        manifest: &SnapshotManifest,
        source_dir: &Path,
    ) -> Result<ConvertedState, String> {
        let desired: V4DesiredClusterState =
            match read_optional(source_dir, &file_name(self.snapshot_desired_path()))? {
                Some(bytes) => decode_from_slice(&bytes)
                    .map_err(|err| format!("desired snapshot did not decode: {err}"))?,
                None => return Err("the desired snapshot file is missing".into()),
            };
        if desired.revision != manifest.desired_snapshot_revision {
            return Err("the desired snapshot does not match the manifest".into());
        }
        let observed = read_optional(source_dir, &file_name(self.snapshot_observed_path()))?
            .map(|bytes| {
                decode_from_slice::<V4ObservedClusterState>(&bytes)
                    .map_err(|err| format!("observed snapshot did not decode: {err}"))
            })
            .transpose()?
            .map(|observed| ObservedClusterState {
                revision: observed.revision,
                nodes: convert_nodes(observed.nodes),
                workloads: observed.workloads,
                resources: observed.resources,
                leases: observed.leases,
            });
        let history = read_optional(source_dir, &file_name(self.mutation_history_path()))?
            .map(|bytes| {
                decode_from_slice::<Vec<V4MutationBatch>>(&bytes)
                    .map_err(|err| format!("mutation history did not decode: {err}"))
            })
            .transpose()?
            .map(|history| {
                history
                    .into_iter()
                    .map(V4MutationBatch::into_current)
                    .collect()
            });
        let baseline = read_optional(
            source_dir,
            &file_name(self.mutation_history_baseline_path()),
        )?
        .map(|bytes| {
            decode_from_slice::<V4DesiredClusterState>(&bytes)
                .map_err(|err| format!("history baseline did not decode: {err}"))
        })
        .transpose()?
        .map(V4DesiredClusterState::into_current);
        Ok(ConvertedState {
            desired: desired.into_current(),
            observed,
            history,
            baseline,
        })
    }
}

#[cfg(test)]
mod tests;
