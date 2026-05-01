use crate::NodeError;
use crate::storage_io::{
    atomic_write_file, atomic_write_stream, sync_parent_directory as sync_directory,
};
use orion::{
    ArtifactId, Revision,
    control_plane::{
        AppliedClusterState, ArtifactRecord, ClusterStateEnvelope, DesiredClusterState,
        DesiredStateSectionFingerprints, MaintenanceState, MutationBatch, ObservedClusterState,
    },
};
use rkyv::{
    Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize,
    bytecheck::CheckBytes,
    de::pooling::Pool,
    rancor::{Error as ArchiveError, Strategy},
    util::AlignedVec,
};
use std::{
    fs::File,
    io::Read,
    path::{Path, PathBuf},
};

const SNAPSHOT_MANIFEST_FILE: &str = "snapshot-manifest.rkyv";
const SNAPSHOT_DESIRED_FILE: &str = "snapshot-desired.rkyv";
const SNAPSHOT_OBSERVED_FILE: &str = "snapshot-observed.rkyv";
const SNAPSHOT_APPLIED_FILE: &str = "snapshot-applied.rkyv";
const MUTATION_HISTORY_FILE: &str = "mutation-history.rkyv";
const MUTATION_HISTORY_BASELINE_FILE: &str = "mutation-history-baseline.rkyv";
const MAINTENANCE_STATE_FILE: &str = "maintenance-state.rkyv";
const ARTIFACT_METADATA_FILE: &str = "metadata.rkyv";
const ARTIFACT_PAYLOAD_FILE: &str = "payload.bin";
const SNAPSHOT_FORMAT_VERSION: u32 = 3;

#[derive(Clone, Debug, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct DesiredStateSectionCounts {
    pub nodes: u64,
    pub artifacts: u64,
    pub workloads: u64,
    pub workload_tombstones: u64,
    pub resources: u64,
    pub providers: u64,
    pub executors: u64,
    pub leases: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct SnapshotManifest {
    pub format_version: u32,
    pub desired_snapshot_revision: Revision,
    pub latest_desired_revision: Revision,
    pub observed_revision: Revision,
    pub applied_revision: Revision,
    pub desired_section_fingerprints: DesiredStateSectionFingerprints,
    pub desired_section_counts: DesiredStateSectionCounts,
}

#[derive(Clone, Debug)]
pub(crate) struct EncodedDesiredSnapshot {
    pub(crate) revision: Revision,
    pub(crate) bytes: Option<Vec<u8>>,
    pub(crate) section_fingerprints: Option<DesiredStateSectionFingerprints>,
    pub(crate) section_counts: Option<DesiredStateSectionCounts>,
}

#[derive(Clone, Debug)]
pub struct PersistedSnapshot {
    pub manifest: SnapshotManifest,
    pub state: ClusterStateEnvelope,
}

#[derive(Clone, Debug)]
pub struct PersistedSnapshotSections {
    pub manifest: SnapshotManifest,
    pub observed: ObservedClusterState,
    pub applied: AppliedClusterState,
}

#[derive(Clone, Debug)]
pub struct NodeStorage {
    root: PathBuf,
}

impl NodeStorage {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn snapshot_manifest_path(&self) -> PathBuf {
        self.root.join(SNAPSHOT_MANIFEST_FILE)
    }

    pub fn snapshot_desired_path(&self) -> PathBuf {
        self.root.join(SNAPSHOT_DESIRED_FILE)
    }

    pub fn snapshot_observed_path(&self) -> PathBuf {
        self.root.join(SNAPSHOT_OBSERVED_FILE)
    }

    pub fn snapshot_applied_path(&self) -> PathBuf {
        self.root.join(SNAPSHOT_APPLIED_FILE)
    }

    pub fn mutation_history_path(&self) -> PathBuf {
        self.root.join(MUTATION_HISTORY_FILE)
    }

    pub fn mutation_history_baseline_path(&self) -> PathBuf {
        self.root.join(MUTATION_HISTORY_BASELINE_FILE)
    }

    pub fn maintenance_state_path(&self) -> PathBuf {
        self.root.join(MAINTENANCE_STATE_FILE)
    }

    pub fn ensure_layout(&self) -> Result<(), NodeError> {
        std::fs::create_dir_all(self.root.join("artifacts"))
            .map_err(|err| NodeError::Storage(err.to_string()))?;
        Ok(())
    }

    pub fn load_snapshot_manifest(&self) -> Result<Option<SnapshotManifest>, NodeError> {
        let manifest_path = self.snapshot_manifest_path();
        if !manifest_path.exists() {
            return Ok(None);
        }

        let manifest_bytes =
            std::fs::read(&manifest_path).map_err(|err| NodeError::Storage(err.to_string()))?;
        let manifest: SnapshotManifest = decode_from_slice(&manifest_bytes)?;
        if manifest.format_version != SNAPSHOT_FORMAT_VERSION {
            return Err(NodeError::Storage(format!(
                "unsupported snapshot format version {}",
                manifest.format_version
            )));
        }
        Ok(Some(manifest))
    }

    pub fn save_snapshot(
        &self,
        snapshot: &ClusterStateEnvelope,
        rewrite_desired: bool,
    ) -> Result<(), NodeError> {
        self.ensure_layout()?;
        let existing_manifest = self.load_snapshot_manifest()?;
        let has_existing_manifest = existing_manifest.is_some();
        let rewrite_desired = rewrite_desired || !has_existing_manifest;
        let (desired_snapshot_revision, desired_section_fingerprints, desired_section_counts) =
            if rewrite_desired {
                let desired_bytes = encode_to_vec(&snapshot.desired)?;
                self.atomic_write(&self.snapshot_desired_path(), &desired_bytes)?;
                (
                    snapshot.desired.revision,
                    desired_section_fingerprints(&snapshot.desired)?,
                    desired_section_counts(&snapshot.desired),
                )
            } else {
                let manifest = existing_manifest.as_ref().ok_or_else(|| {
                    NodeError::Storage(
                        "cannot reuse desired snapshot metadata without an existing manifest"
                            .into(),
                    )
                })?;
                if !self.snapshot_desired_path().exists() {
                    return Err(NodeError::Storage(
                        "cannot reuse desired snapshot metadata when desired snapshot file is missing"
                            .into(),
                    ));
                }
                (
                    manifest.desired_snapshot_revision,
                    manifest.desired_section_fingerprints.clone(),
                    manifest.desired_section_counts.clone(),
                )
            };

        let manifest = SnapshotManifest {
            format_version: SNAPSHOT_FORMAT_VERSION,
            desired_snapshot_revision,
            latest_desired_revision: snapshot.desired.revision,
            observed_revision: snapshot.observed.revision,
            applied_revision: snapshot.applied.revision,
            desired_section_fingerprints,
            desired_section_counts,
        };
        let observed_bytes = encode_to_vec(&snapshot.observed)?;
        let applied_bytes = encode_to_vec(&snapshot.applied)?;
        let manifest_bytes = encode_to_vec(&manifest)?;

        self.atomic_write(&self.snapshot_observed_path(), &observed_bytes)?;
        self.atomic_write(&self.snapshot_applied_path(), &applied_bytes)?;
        self.atomic_write(&self.snapshot_manifest_path(), &manifest_bytes)
    }

    pub(crate) fn save_encoded_snapshot(
        &self,
        desired: &EncodedDesiredSnapshot,
        observed_revision: Revision,
        observed_bytes: Option<&[u8]>,
        applied_revision: Revision,
        applied_bytes: Option<&[u8]>,
        rewrite_desired: bool,
    ) -> Result<(), NodeError> {
        self.ensure_layout()?;
        let existing_manifest = self.load_snapshot_manifest()?;
        let has_existing_manifest = existing_manifest.is_some();
        let rewrite_desired = rewrite_desired || !has_existing_manifest;
        let (desired_snapshot_revision, desired_section_fingerprints, desired_section_counts) =
            if rewrite_desired {
                let desired_bytes = desired.bytes.as_ref().ok_or_else(|| {
                    NodeError::Storage(
                        "cannot rewrite desired snapshot without encoded desired bytes".into(),
                    )
                })?;
                self.atomic_write(&self.snapshot_desired_path(), desired_bytes)?;
                (
                    desired.revision,
                    desired.section_fingerprints.clone().ok_or_else(|| {
                        NodeError::Storage(
                            "cannot rewrite desired snapshot without section fingerprints".into(),
                        )
                    })?,
                    desired.section_counts.clone().ok_or_else(|| {
                        NodeError::Storage(
                            "cannot rewrite desired snapshot without section counts".into(),
                        )
                    })?,
                )
            } else {
                let manifest = existing_manifest.ok_or_else(|| {
                    NodeError::Storage(
                        "cannot reuse desired snapshot metadata without an existing manifest"
                            .into(),
                    )
                })?;
                if !self.snapshot_desired_path().exists() {
                    return Err(NodeError::Storage(
                        "cannot reuse desired snapshot metadata when desired snapshot file is missing"
                            .into(),
                    ));
                }
                (
                    manifest.desired_snapshot_revision,
                    manifest.desired_section_fingerprints,
                    manifest.desired_section_counts,
                )
            };

        let manifest = SnapshotManifest {
            format_version: SNAPSHOT_FORMAT_VERSION,
            desired_snapshot_revision,
            latest_desired_revision: desired.revision,
            observed_revision,
            applied_revision,
            desired_section_fingerprints,
            desired_section_counts,
        };
        let manifest_bytes = encode_archive_to_vec(&manifest)?;

        match observed_bytes {
            Some(observed_bytes) => {
                self.atomic_write(&self.snapshot_observed_path(), observed_bytes)?
            }
            None => {
                if !has_existing_manifest || !self.snapshot_observed_path().exists() {
                    return Err(NodeError::Storage(
                        "cannot reuse observed snapshot bytes without an existing observed snapshot"
                            .into(),
                    ));
                }
            }
        }
        match applied_bytes {
            Some(applied_bytes) => {
                self.atomic_write(&self.snapshot_applied_path(), applied_bytes)?
            }
            None => {
                if !has_existing_manifest || !self.snapshot_applied_path().exists() {
                    return Err(NodeError::Storage(
                        "cannot reuse applied snapshot bytes without an existing applied snapshot"
                            .into(),
                    ));
                }
            }
        }
        self.atomic_write(&self.snapshot_manifest_path(), &manifest_bytes)
    }

    pub fn save_mutation_history(&self, history: &[MutationBatch]) -> Result<(), NodeError> {
        self.ensure_layout()?;
        let bytes = encode_to_vec(&history.to_vec())?;
        self.atomic_write(&self.mutation_history_path(), &bytes)
    }

    pub(crate) fn save_mutation_history_bytes(&self, bytes: &[u8]) -> Result<(), NodeError> {
        self.ensure_layout()?;
        self.atomic_write(&self.mutation_history_path(), bytes)
    }

    pub fn save_mutation_history_baseline(
        &self,
        baseline: &DesiredClusterState,
    ) -> Result<(), NodeError> {
        self.ensure_layout()?;
        let path = self.mutation_history_baseline_path();
        if baseline.revision == orion::Revision::ZERO {
            if path.exists() {
                std::fs::remove_file(&path).map_err(|err| NodeError::Storage(err.to_string()))?;
                if let Some(parent) = path.parent() {
                    sync_parent_directory(parent)?;
                }
            }
            return Ok(());
        }
        let bytes = encode_to_vec(baseline)?;
        self.atomic_write(&path, &bytes)
    }

    pub fn load_maintenance_state(&self) -> Result<Option<MaintenanceState>, NodeError> {
        let path = self.maintenance_state_path();
        if !path.exists() {
            return Ok(None);
        }
        let bytes = std::fs::read(&path).map_err(|err| NodeError::Storage(err.to_string()))?;
        decode_from_slice(&bytes).map(Some)
    }

    pub fn save_maintenance_state(&self, state: &MaintenanceState) -> Result<(), NodeError> {
        self.ensure_layout()?;
        let bytes = encode_to_vec(state)?;
        self.atomic_write(&self.maintenance_state_path(), &bytes)
    }

    pub(crate) fn save_mutation_history_baseline_bytes(
        &self,
        baseline_revision: Revision,
        bytes: Option<&[u8]>,
    ) -> Result<(), NodeError> {
        self.ensure_layout()?;
        let path = self.mutation_history_baseline_path();
        if baseline_revision == orion::Revision::ZERO {
            if path.exists() {
                std::fs::remove_file(&path).map_err(|err| NodeError::Storage(err.to_string()))?;
                if let Some(parent) = path.parent() {
                    sync_parent_directory(parent)?;
                }
            }
            return Ok(());
        }
        let bytes = bytes.ok_or_else(|| {
            NodeError::Storage("missing encoded mutation history baseline bytes".into())
        })?;
        self.atomic_write(&path, bytes)
    }

    pub fn load_snapshot(&self) -> Result<Option<PersistedSnapshot>, NodeError> {
        let Some(sections) = self.load_snapshot_sections()? else {
            return Ok(None);
        };

        let desired_bytes = std::fs::read(self.snapshot_desired_path())
            .map_err(|err| NodeError::Storage(err.to_string()))?;
        let desired: DesiredClusterState = decode_from_slice(&desired_bytes)?;

        if desired.revision != sections.manifest.desired_snapshot_revision {
            return Err(NodeError::Storage(
                "snapshot manifest revisions do not match section files".into(),
            ));
        }

        Ok(Some(PersistedSnapshot {
            manifest: sections.manifest,
            state: ClusterStateEnvelope {
                desired,
                observed: sections.observed,
                applied: sections.applied,
            },
        }))
    }

    pub fn load_snapshot_sections(&self) -> Result<Option<PersistedSnapshotSections>, NodeError> {
        let Some(manifest) = self.load_snapshot_manifest()? else {
            return Ok(None);
        };

        let observed_bytes = std::fs::read(self.snapshot_observed_path())
            .map_err(|err| NodeError::Storage(err.to_string()))?;
        let applied_bytes = std::fs::read(self.snapshot_applied_path())
            .map_err(|err| NodeError::Storage(err.to_string()))?;

        let observed: ObservedClusterState = decode_from_slice(&observed_bytes)?;
        let applied: AppliedClusterState = decode_from_slice(&applied_bytes)?;

        if observed.revision != manifest.observed_revision
            || applied.revision != manifest.applied_revision
        {
            return Err(NodeError::Storage(
                "snapshot manifest revisions do not match section files".into(),
            ));
        }

        Ok(Some(PersistedSnapshotSections {
            manifest,
            observed,
            applied,
        }))
    }

    pub fn load_snapshot_desired(
        &self,
        manifest: &SnapshotManifest,
    ) -> Result<DesiredClusterState, NodeError> {
        let desired_bytes = std::fs::read(self.snapshot_desired_path())
            .map_err(|err| NodeError::Storage(err.to_string()))?;
        let desired: DesiredClusterState = decode_from_slice(&desired_bytes)?;
        if desired.revision != manifest.desired_snapshot_revision {
            return Err(NodeError::Storage(
                "snapshot manifest revisions do not match section files".into(),
            ));
        }
        Ok(desired)
    }

    pub fn load_mutation_history(&self) -> Result<Vec<MutationBatch>, NodeError> {
        let path = self.mutation_history_path();
        if !path.exists() {
            return Ok(Vec::new());
        }

        let bytes = std::fs::read(&path).map_err(|err| NodeError::Storage(err.to_string()))?;
        decode_from_slice(&bytes)
    }

    pub fn load_mutation_history_baseline(&self) -> Result<Option<DesiredClusterState>, NodeError> {
        let path = self.mutation_history_baseline_path();
        if !path.exists() {
            return Ok(None);
        }

        let bytes = std::fs::read(&path).map_err(|err| NodeError::Storage(err.to_string()))?;
        let baseline = decode_from_slice(&bytes)?;
        Ok(Some(baseline))
    }

    pub fn put_artifact(&self, artifact: &ArtifactRecord, content: &[u8]) -> Result<(), NodeError> {
        self.put_artifact_stream(artifact, content)
    }

    pub fn put_artifact_stream<R: Read>(
        &self,
        artifact: &ArtifactRecord,
        mut content: R,
    ) -> Result<(), NodeError> {
        self.ensure_layout()?;
        let artifact_dir = self
            .root
            .join("artifacts")
            .join(artifact.artifact_id.as_str());
        std::fs::create_dir_all(&artifact_dir)
            .map_err(|err| NodeError::Storage(err.to_string()))?;

        let metadata = encode_to_vec(artifact)?;
        self.atomic_write(&artifact_dir.join(ARTIFACT_METADATA_FILE), &metadata)?;
        self.atomic_write_stream(&artifact_dir.join(ARTIFACT_PAYLOAD_FILE), &mut content)?;
        Ok(())
    }

    pub fn get_artifact(
        &self,
        artifact_id: &ArtifactId,
    ) -> Result<Option<(ArtifactRecord, Vec<u8>)>, NodeError> {
        let Some((artifact, mut payload)) = self.open_artifact(artifact_id)? else {
            return Ok(None);
        };
        let mut content = Vec::new();
        payload
            .read_to_end(&mut content)
            .map_err(|err| NodeError::Storage(err.to_string()))?;
        Ok(Some((artifact, content)))
    }

    pub fn open_artifact(
        &self,
        artifact_id: &ArtifactId,
    ) -> Result<Option<(ArtifactRecord, File)>, NodeError> {
        let artifact_dir = self.root.join("artifacts").join(artifact_id.as_str());
        if !artifact_dir.exists() {
            return Ok(None);
        }

        let metadata = std::fs::read(artifact_dir.join(ARTIFACT_METADATA_FILE))
            .map_err(|err| NodeError::Storage(err.to_string()))?;
        let payload = File::open(artifact_dir.join(ARTIFACT_PAYLOAD_FILE))
            .map_err(|err| NodeError::Storage(err.to_string()))?;
        let artifact = decode_from_slice(&metadata)?;
        Ok(Some((artifact, payload)))
    }

    fn atomic_write(&self, path: &Path, bytes: &[u8]) -> Result<(), NodeError> {
        atomic_write_file(
            path,
            bytes,
            "failed to create storage directory",
            "failed to write storage file",
            "failed to install storage file",
        )
    }

    fn atomic_write_stream(&self, path: &Path, content: &mut dyn Read) -> Result<(), NodeError> {
        atomic_write_stream(
            path,
            content,
            "failed to create storage directory",
            "failed to write storage file",
            "failed to install storage file",
        )
    }
}

fn sync_parent_directory(path: &Path) -> Result<(), NodeError> {
    sync_directory(path, "failed to sync storage directory")
}

fn desired_section_counts(desired: &DesiredClusterState) -> DesiredStateSectionCounts {
    DesiredStateSectionCounts {
        nodes: desired.nodes.len() as u64,
        artifacts: desired.artifacts.len() as u64,
        workloads: desired.workloads.len() as u64,
        workload_tombstones: desired.workload_tombstones.len() as u64,
        resources: desired.resources.len() as u64,
        providers: desired.providers.len() as u64,
        executors: desired.executors.len() as u64,
        leases: desired.leases.len() as u64,
    }
}

fn desired_section_fingerprints(
    desired: &DesiredClusterState,
) -> Result<DesiredStateSectionFingerprints, NodeError> {
    Ok(DesiredStateSectionFingerprints {
        nodes: entry_fingerprint(&desired.nodes)?,
        artifacts: entry_fingerprint(&desired.artifacts)?,
        workloads: combined_fingerprint([
            entry_fingerprint(&desired.workloads)?,
            entry_fingerprint(&desired.workload_tombstones)?,
        ]),
        resources: entry_fingerprint(&desired.resources)?,
        providers: entry_fingerprint(&desired.providers)?,
        executors: entry_fingerprint(&desired.executors)?,
        leases: entry_fingerprint(&desired.leases)?,
    })
}

pub(crate) fn encode_desired_snapshot(
    desired: &DesiredClusterState,
) -> Result<EncodedDesiredSnapshot, NodeError> {
    Ok(EncodedDesiredSnapshot {
        revision: desired.revision,
        bytes: Some(encode_archive_to_vec(desired)?),
        section_fingerprints: Some(desired_section_fingerprints(desired)?),
        section_counts: Some(desired_section_counts(desired)),
    })
}

pub(crate) fn desired_snapshot_revision_only(
    desired: &DesiredClusterState,
) -> EncodedDesiredSnapshot {
    EncodedDesiredSnapshot {
        revision: desired.revision,
        bytes: None,
        section_fingerprints: None,
        section_counts: None,
    }
}

fn entry_fingerprint<T>(value: &T) -> Result<u64, NodeError>
where
    T: for<'a> RkyvSerialize<
        rkyv::api::high::HighSerializer<
            AlignedVec,
            rkyv::ser::allocator::ArenaHandle<'a>,
            ArchiveError,
        >,
    >,
{
    use std::hash::{Hash, Hasher};

    let bytes = encode_to_vec(value)?;
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    bytes.hash(&mut hasher);
    Ok(hasher.finish())
}

fn combined_fingerprint(parts: impl IntoIterator<Item = u64>) -> u64 {
    use std::hash::{Hash, Hasher};

    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    for part in parts {
        part.hash(&mut hasher);
    }
    hasher.finish()
}

type DecodeStrategy = Strategy<Pool, ArchiveError>;

pub(crate) fn encode_archive_to_vec<T>(value: &T) -> Result<Vec<u8>, NodeError>
where
    T: for<'a> RkyvSerialize<
        rkyv::api::high::HighSerializer<
            AlignedVec,
            rkyv::ser::allocator::ArenaHandle<'a>,
            ArchiveError,
        >,
    >,
{
    let bytes =
        rkyv::to_bytes::<ArchiveError>(value).map_err(|err| NodeError::Storage(err.to_string()))?;
    Ok(bytes.as_ref().to_vec())
}

fn encode_to_vec<T>(value: &T) -> Result<Vec<u8>, NodeError>
where
    T: for<'a> RkyvSerialize<
        rkyv::api::high::HighSerializer<
            AlignedVec,
            rkyv::ser::allocator::ArenaHandle<'a>,
            ArchiveError,
        >,
    >,
{
    encode_archive_to_vec(value)
}

fn decode_from_slice<T>(payload: &[u8]) -> Result<T, NodeError>
where
    T: Archive,
    T::Archived: for<'a> CheckBytes<rkyv::api::high::HighValidator<'a, ArchiveError>>
        + RkyvDeserialize<T, DecodeStrategy>,
{
    if payload.is_empty()
        || payload
            .as_ptr()
            .align_offset(std::mem::align_of::<T::Archived>())
            == 0
    {
        return rkyv::from_bytes::<T, ArchiveError>(payload)
            .map_err(|err| NodeError::Storage(err.to_string()));
    }

    let mut aligned = AlignedVec::<16>::with_capacity(payload.len());
    aligned.extend_from_slice(payload);
    rkyv::from_bytes::<T, ArchiveError>(&aligned).map_err(|err| NodeError::Storage(err.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{fs, time::SystemTime};

    fn temp_state_dir(label: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("system time should be after unix epoch")
            .as_nanos();
        let path = std::env::temp_dir().join(format!("orion-storage-{label}-{unique}"));
        fs::create_dir_all(&path).expect("temporary storage directory should be created");
        path
    }

    #[test]
    fn atomic_write_replaces_target_and_cleans_temp_file() {
        let state_dir = temp_state_dir("atomic-write");
        let storage = NodeStorage::new(&state_dir);
        let path = storage.snapshot_manifest_path();

        storage
            .atomic_write(&path, b"first")
            .expect("first atomic write should succeed");
        storage
            .atomic_write(&path, b"second")
            .expect("second atomic write should succeed");

        assert_eq!(
            fs::read(&path).expect("target file should be readable"),
            b"second"
        );
        let entries = fs::read_dir(&state_dir)
            .expect("state directory should be readable")
            .map(|entry| {
                entry
                    .expect("directory entry should be readable")
                    .file_name()
                    .to_string_lossy()
                    .into_owned()
            })
            .collect::<Vec<_>>();
        assert!(
            entries.iter().all(|entry| !entry.ends_with(".tmp")),
            "temporary files should not remain after atomic write: {entries:?}"
        );

        let _ = fs::remove_dir_all(state_dir);
    }

    #[test]
    fn artifact_writes_roundtrip_through_hardened_atomic_path() {
        let state_dir = temp_state_dir("artifact-write");
        let storage = NodeStorage::new(&state_dir);
        let artifact = ArtifactRecord::builder("artifact.video").build();

        storage
            .put_artifact(&artifact, b"payload")
            .expect("artifact write should succeed");

        let stored = storage
            .get_artifact(&ArtifactId::new("artifact.video"))
            .expect("artifact lookup should succeed")
            .expect("artifact should exist");
        assert_eq!(stored.0, artifact);
        assert_eq!(stored.1, b"payload");

        let _ = fs::remove_dir_all(state_dir);
    }

    #[test]
    fn artifact_stream_writes_and_opens_payload_without_vec_contract() {
        let state_dir = temp_state_dir("artifact-stream");
        let storage = NodeStorage::new(&state_dir);
        let artifact = ArtifactRecord::builder("artifact.stream").build();

        storage
            .put_artifact_stream(&artifact, std::io::Cursor::new(b"streamed-payload"))
            .expect("artifact stream write should succeed");

        let (stored, mut payload) = storage
            .open_artifact(&ArtifactId::new("artifact.stream"))
            .expect("artifact stream lookup should succeed")
            .expect("artifact should exist");
        let mut content = Vec::new();
        payload
            .read_to_end(&mut content)
            .expect("payload stream should be readable");
        assert_eq!(stored, artifact);
        assert_eq!(content, b"streamed-payload");

        let _ = fs::remove_dir_all(state_dir);
    }

    #[test]
    fn mutation_history_roundtrips_through_hardened_atomic_path() {
        let state_dir = temp_state_dir("mutation-history");
        let storage = NodeStorage::new(&state_dir);
        let history = vec![MutationBatch {
            base_revision: Revision::ZERO,
            mutations: Vec::new(),
        }];

        storage
            .save_mutation_history(&history)
            .expect("mutation history write should succeed");

        assert_eq!(
            storage
                .load_mutation_history()
                .expect("mutation history should load"),
            history
        );

        let _ = fs::remove_dir_all(state_dir);
    }
}
