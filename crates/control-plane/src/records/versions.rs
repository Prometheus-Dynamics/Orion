//! Per-object versions of desired state and the last-writer-wins merge rule.
//!
//! Every live desired-state object has the [`HlcTimestamp`] of its last write in
//! [`DesiredClusterState::stamps`]; every deleted object has the timestamp of its delete in
//! [`DesiredClusterState::tombstones`]. [`DesiredClusterState::apply_stamped`] is the single merge
//! rule all nodes apply, so merging is deterministic, commutative and idempotent. See
//! `docs/peer-sync.md`.

use super::DesiredClusterState;
use crate::{DesiredStateMutation, DesiredStateSection, MutationBatch};
use alloc::{collections::BTreeMap, vec::Vec};
use core::cmp::Ordering;
use orion_core::{
    ArchiveEncode, ArtifactId, ExecutorId, HlcTimestamp, NodeId, ProviderId, ResourceId,
    WorkloadId, encode_to_vec,
};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};

/// HLC timestamps keyed by object, with one map per desired-state section.
#[derive(
    Clone,
    Debug,
    Default,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub struct DesiredObjectStamps {
    pub nodes: BTreeMap<NodeId, HlcTimestamp>,
    pub artifacts: BTreeMap<ArtifactId, HlcTimestamp>,
    pub workloads: BTreeMap<WorkloadId, HlcTimestamp>,
    pub resources: BTreeMap<ResourceId, HlcTimestamp>,
    pub providers: BTreeMap<ProviderId, HlcTimestamp>,
    pub executors: BTreeMap<ExecutorId, HlcTimestamp>,
    pub leases: BTreeMap<ResourceId, HlcTimestamp>,
}

/// Identity of one desired-state object.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum DesiredObjectKey {
    Node(NodeId),
    Artifact(ArtifactId),
    Workload(WorkloadId),
    Resource(ResourceId),
    Provider(ProviderId),
    Executor(ExecutorId),
    Lease(ResourceId),
}

/// Current version of one object: the stamp of its last write and whether that write deleted it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DesiredObjectVersion {
    pub stamp: HlcTimestamp,
    pub deleted: bool,
}

impl DesiredObjectKey {
    pub fn section(&self) -> DesiredStateSection {
        match self {
            Self::Node(_) => DesiredStateSection::Nodes,
            Self::Artifact(_) => DesiredStateSection::Artifacts,
            Self::Workload(_) => DesiredStateSection::Workloads,
            Self::Resource(_) => DesiredStateSection::Resources,
            Self::Provider(_) => DesiredStateSection::Providers,
            Self::Executor(_) => DesiredStateSection::Executors,
            Self::Lease(_) => DesiredStateSection::Leases,
        }
    }

    /// The mutation that deletes this object.
    pub fn remove_mutation(&self) -> DesiredStateMutation {
        match self.clone() {
            Self::Node(id) => DesiredStateMutation::RemoveNode(id),
            Self::Artifact(id) => DesiredStateMutation::RemoveArtifact(id),
            Self::Workload(id) => DesiredStateMutation::RemoveWorkload(id),
            Self::Resource(id) => DesiredStateMutation::RemoveResource(id),
            Self::Provider(id) => DesiredStateMutation::RemoveProvider(id),
            Self::Executor(id) => DesiredStateMutation::RemoveExecutor(id),
            Self::Lease(id) => DesiredStateMutation::RemoveLease(id),
        }
    }
}

impl DesiredStateMutation {
    /// The object this mutation writes.
    pub fn key(&self) -> DesiredObjectKey {
        match self {
            Self::PutNode(record) => DesiredObjectKey::Node(record.node_id.clone()),
            Self::PutArtifact(record) => DesiredObjectKey::Artifact(record.artifact_id.clone()),
            Self::PutWorkload(record) => DesiredObjectKey::Workload(record.workload_id.clone()),
            Self::PutResource(record) => DesiredObjectKey::Resource(record.resource_id.clone()),
            Self::PutProvider(record) => DesiredObjectKey::Provider(record.provider_id.clone()),
            Self::PutExecutor(record) => DesiredObjectKey::Executor(record.executor_id.clone()),
            Self::PutLease(record) => DesiredObjectKey::Lease(record.resource_id.clone()),
            Self::RemoveNode(id) => DesiredObjectKey::Node(id.clone()),
            Self::RemoveArtifact(id) => DesiredObjectKey::Artifact(id.clone()),
            Self::RemoveWorkload(id) => DesiredObjectKey::Workload(id.clone()),
            Self::RemoveResource(id) => DesiredObjectKey::Resource(id.clone()),
            Self::RemoveProvider(id) => DesiredObjectKey::Provider(id.clone()),
            Self::RemoveExecutor(id) => DesiredObjectKey::Executor(id.clone()),
            Self::RemoveLease(id) => DesiredObjectKey::Lease(id.clone()),
        }
    }

    pub fn is_remove(&self) -> bool {
        matches!(
            self,
            Self::RemoveNode(_)
                | Self::RemoveArtifact(_)
                | Self::RemoveWorkload(_)
                | Self::RemoveResource(_)
                | Self::RemoveProvider(_)
                | Self::RemoveExecutor(_)
                | Self::RemoveLease(_)
        )
    }
}

macro_rules! for_each_section {
    ($stamps:expr, $map:ident => $body:expr) => {{
        {
            let $map = &$stamps.nodes;
            $body;
        }
        {
            let $map = &$stamps.artifacts;
            $body;
        }
        {
            let $map = &$stamps.workloads;
            $body;
        }
        {
            let $map = &$stamps.resources;
            $body;
        }
        {
            let $map = &$stamps.providers;
            $body;
        }
        {
            let $map = &$stamps.executors;
            $body;
        }
        {
            let $map = &$stamps.leases;
            $body;
        }
    }};
}

impl DesiredObjectStamps {
    pub fn len(&self) -> usize {
        let mut total = 0;
        for_each_section!(self, map => total += map.len());
        total
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Largest timestamp in any section.
    pub fn max_stamp(&self) -> Option<HlcTimestamp> {
        let mut max = None;
        for_each_section!(self, map => {
            if let Some(stamp) = map.values().max() {
                max = max.max(Some(*stamp));
            }
        });
        max
    }

    pub fn get(&self, key: &DesiredObjectKey) -> Option<HlcTimestamp> {
        match key {
            DesiredObjectKey::Node(id) => self.nodes.get(id),
            DesiredObjectKey::Artifact(id) => self.artifacts.get(id),
            DesiredObjectKey::Workload(id) => self.workloads.get(id),
            DesiredObjectKey::Resource(id) => self.resources.get(id),
            DesiredObjectKey::Provider(id) => self.providers.get(id),
            DesiredObjectKey::Executor(id) => self.executors.get(id),
            DesiredObjectKey::Lease(id) => self.leases.get(id),
        }
        .copied()
    }

    pub fn insert(&mut self, key: DesiredObjectKey, stamp: HlcTimestamp) -> Option<HlcTimestamp> {
        match key {
            DesiredObjectKey::Node(id) => self.nodes.insert(id, stamp),
            DesiredObjectKey::Artifact(id) => self.artifacts.insert(id, stamp),
            DesiredObjectKey::Workload(id) => self.workloads.insert(id, stamp),
            DesiredObjectKey::Resource(id) => self.resources.insert(id, stamp),
            DesiredObjectKey::Provider(id) => self.providers.insert(id, stamp),
            DesiredObjectKey::Executor(id) => self.executors.insert(id, stamp),
            DesiredObjectKey::Lease(id) => self.leases.insert(id, stamp),
        }
    }

    pub fn remove(&mut self, key: &DesiredObjectKey) -> Option<HlcTimestamp> {
        match key {
            DesiredObjectKey::Node(id) => self.nodes.remove(id),
            DesiredObjectKey::Artifact(id) => self.artifacts.remove(id),
            DesiredObjectKey::Workload(id) => self.workloads.remove(id),
            DesiredObjectKey::Resource(id) => self.resources.remove(id),
            DesiredObjectKey::Provider(id) => self.providers.remove(id),
            DesiredObjectKey::Executor(id) => self.executors.remove(id),
            DesiredObjectKey::Lease(id) => self.leases.remove(id),
        }
    }

    /// All entries in section order (nodes, artifacts, workloads, resources, providers,
    /// executors, leases), then key order.
    pub fn entries(&self) -> Vec<(DesiredObjectKey, HlcTimestamp)> {
        let mut entries = Vec::with_capacity(self.len());
        entries.extend(
            self.nodes
                .iter()
                .map(|(id, stamp)| (DesiredObjectKey::Node(id.clone()), *stamp)),
        );
        entries.extend(
            self.artifacts
                .iter()
                .map(|(id, stamp)| (DesiredObjectKey::Artifact(id.clone()), *stamp)),
        );
        entries.extend(
            self.workloads
                .iter()
                .map(|(id, stamp)| (DesiredObjectKey::Workload(id.clone()), *stamp)),
        );
        entries.extend(
            self.resources
                .iter()
                .map(|(id, stamp)| (DesiredObjectKey::Resource(id.clone()), *stamp)),
        );
        entries.extend(
            self.providers
                .iter()
                .map(|(id, stamp)| (DesiredObjectKey::Provider(id.clone()), *stamp)),
        );
        entries.extend(
            self.executors
                .iter()
                .map(|(id, stamp)| (DesiredObjectKey::Executor(id.clone()), *stamp)),
        );
        entries.extend(
            self.leases
                .iter()
                .map(|(id, stamp)| (DesiredObjectKey::Lease(id.clone()), *stamp)),
        );
        entries
    }

    /// Copy restricted to `sections` (all sections when `sections` is empty).
    pub fn for_sections(&self, sections: &[DesiredStateSection]) -> Self {
        fn pick<K: Ord + Clone>(
            sections: &[DesiredStateSection],
            section: DesiredStateSection,
            map: &BTreeMap<K, HlcTimestamp>,
        ) -> BTreeMap<K, HlcTimestamp> {
            if sections.is_empty() || sections.contains(&section) {
                map.clone()
            } else {
                BTreeMap::new()
            }
        }
        Self {
            nodes: pick(sections, DesiredStateSection::Nodes, &self.nodes),
            artifacts: pick(sections, DesiredStateSection::Artifacts, &self.artifacts),
            workloads: pick(sections, DesiredStateSection::Workloads, &self.workloads),
            resources: pick(sections, DesiredStateSection::Resources, &self.resources),
            providers: pick(sections, DesiredStateSection::Providers, &self.providers),
            executors: pick(sections, DesiredStateSection::Executors, &self.executors),
            leases: pick(sections, DesiredStateSection::Leases, &self.leases),
        }
    }

    /// Removes every entry for which `keep` returns `false`; returns how many were removed.
    pub fn retain(&mut self, mut keep: impl FnMut(HlcTimestamp) -> bool) -> usize {
        let before = self.len();
        self.nodes.retain(|_, stamp| keep(*stamp));
        self.artifacts.retain(|_, stamp| keep(*stamp));
        self.workloads.retain(|_, stamp| keep(*stamp));
        self.resources.retain(|_, stamp| keep(*stamp));
        self.providers.retain(|_, stamp| keep(*stamp));
        self.executors.retain(|_, stamp| keep(*stamp));
        self.leases.retain(|_, stamp| keep(*stamp));
        before - self.len()
    }
}

/// Outcome of comparing a candidate version against the current one.
fn candidate_wins<V: PartialEq + ArchiveEncode>(
    candidate_stamp: HlcTimestamp,
    candidate: Option<&V>,
    current: Option<DesiredObjectVersion>,
    current_record: Option<&V>,
) -> bool {
    let Some(current) = current else {
        return true;
    };
    match candidate_stamp.cmp(&current.stamp) {
        Ordering::Greater => true,
        Ordering::Less => false,
        // Ties: a delete beats a put, and between two records the larger encoding wins.
        Ordering::Equal => match (candidate, current.deleted) {
            (None, deleted) => !deleted,
            (Some(_), true) => false,
            (Some(candidate), false) => match current_record {
                None => true,
                Some(current) if current == candidate => false,
                Some(current) => encode_to_vec(candidate).ok() > encode_to_vec(current).ok(),
            },
        },
    }
}

struct SectionMut<'a, K, V> {
    records: &'a mut BTreeMap<K, V>,
    stamps: &'a mut BTreeMap<K, HlcTimestamp>,
    tombstones: &'a mut BTreeMap<K, HlcTimestamp>,
}

impl<K: Ord + Clone, V: PartialEq + ArchiveEncode> SectionMut<'_, K, V> {
    fn version(&self, key: &K) -> Option<DesiredObjectVersion> {
        version_in(self.records, self.stamps, self.tombstones, key)
    }

    fn write(&mut self, key: K, value: Option<V>, stamp: HlcTimestamp, force: bool) -> bool {
        if !force
            && !candidate_wins(
                stamp,
                value.as_ref(),
                self.version(&key),
                self.records.get(&key),
            )
        {
            return false;
        }
        match value {
            Some(value) => {
                self.tombstones.remove(&key);
                self.stamps.insert(key.clone(), stamp);
                self.records.insert(key, value);
            }
            None => {
                self.records.remove(&key);
                self.stamps.remove(&key);
                self.tombstones.insert(key, stamp);
            }
        }
        true
    }
}

fn version_in<K: Ord, V>(
    records: &BTreeMap<K, V>,
    stamps: &BTreeMap<K, HlcTimestamp>,
    tombstones: &BTreeMap<K, HlcTimestamp>,
    key: &K,
) -> Option<DesiredObjectVersion> {
    if let Some(stamp) = stamps.get(key) {
        return Some(DesiredObjectVersion {
            stamp: *stamp,
            deleted: false,
        });
    }
    if let Some(stamp) = tombstones.get(key) {
        return Some(DesiredObjectVersion {
            stamp: *stamp,
            deleted: true,
        });
    }
    // A record written without a stamp (built with the plain `put_*` helpers) predates every
    // stamped write.
    records.contains_key(key).then_some(DesiredObjectVersion {
        stamp: HlcTimestamp::ZERO,
        deleted: false,
    })
}

macro_rules! section_mut {
    ($state:expr, $field:ident) => {
        SectionMut {
            records: &mut $state.$field,
            stamps: &mut $state.stamps.$field,
            tombstones: &mut $state.tombstones.$field,
        }
    };
}

impl DesiredClusterState {
    /// Current version of `key`: live (with the stamp of its last write) or deleted (with the
    /// stamp of the delete). `None` when this state has never seen the object or its tombstone
    /// has been collected.
    pub fn version_of(&self, key: &DesiredObjectKey) -> Option<DesiredObjectVersion> {
        match key {
            DesiredObjectKey::Node(id) => {
                version_in(&self.nodes, &self.stamps.nodes, &self.tombstones.nodes, id)
            }
            DesiredObjectKey::Artifact(id) => version_in(
                &self.artifacts,
                &self.stamps.artifacts,
                &self.tombstones.artifacts,
                id,
            ),
            DesiredObjectKey::Workload(id) => version_in(
                &self.workloads,
                &self.stamps.workloads,
                &self.tombstones.workloads,
                id,
            ),
            DesiredObjectKey::Resource(id) => version_in(
                &self.resources,
                &self.stamps.resources,
                &self.tombstones.resources,
                id,
            ),
            DesiredObjectKey::Provider(id) => version_in(
                &self.providers,
                &self.stamps.providers,
                &self.tombstones.providers,
                id,
            ),
            DesiredObjectKey::Executor(id) => version_in(
                &self.executors,
                &self.stamps.executors,
                &self.tombstones.executors,
                id,
            ),
            DesiredObjectKey::Lease(id) => version_in(
                &self.leases,
                &self.stamps.leases,
                &self.tombstones.leases,
                id,
            ),
        }
    }

    /// Applies one object version with the last-writer-wins rule.
    ///
    /// The version replaces the current one when it has a later stamp, or the same stamp and
    /// wins the tie (a delete beats a put; between two records the larger rkyv encoding wins).
    /// Returns `true` and advances the revision by one when the state changed.
    pub fn apply_stamped(&mut self, mutation: DesiredStateMutation, stamp: HlcTimestamp) -> bool {
        self.write_stamped(mutation, stamp, false)
    }

    /// Applies one object version unconditionally (mutation-history replay). Always advances the
    /// revision by one.
    pub fn force_stamped(&mut self, mutation: DesiredStateMutation, stamp: HlcTimestamp) {
        self.write_stamped(mutation, stamp, true);
    }

    /// Returns `true` when [`Self::apply_stamped`] would change the state.
    pub fn would_apply(&self, mutation: &DesiredStateMutation, stamp: HlcTimestamp) -> bool {
        let key = mutation.key();
        let current = self.version_of(&key);
        macro_rules! check {
            ($field:ident, $record:expr, $id:expr) => {
                candidate_wins(stamp, $record, current, self.$field.get($id))
            };
        }
        match mutation {
            DesiredStateMutation::PutNode(record) => check!(nodes, Some(record), &record.node_id),
            DesiredStateMutation::PutArtifact(record) => {
                check!(artifacts, Some(record), &record.artifact_id)
            }
            DesiredStateMutation::PutWorkload(record) => {
                check!(workloads, Some(record), &record.workload_id)
            }
            DesiredStateMutation::PutResource(record) => {
                check!(resources, Some(record), &record.resource_id)
            }
            DesiredStateMutation::PutProvider(record) => {
                check!(providers, Some(record), &record.provider_id)
            }
            DesiredStateMutation::PutExecutor(record) => {
                check!(executors, Some(record), &record.executor_id)
            }
            DesiredStateMutation::PutLease(record) => {
                check!(leases, Some(record), &record.resource_id)
            }
            DesiredStateMutation::RemoveNode(id) => check!(nodes, None, id),
            DesiredStateMutation::RemoveArtifact(id) => check!(artifacts, None, id),
            DesiredStateMutation::RemoveWorkload(id) => check!(workloads, None, id),
            DesiredStateMutation::RemoveResource(id) => check!(resources, None, id),
            DesiredStateMutation::RemoveProvider(id) => check!(providers, None, id),
            DesiredStateMutation::RemoveExecutor(id) => check!(executors, None, id),
            DesiredStateMutation::RemoveLease(id) => check!(leases, None, id),
        }
    }

    fn write_stamped(
        &mut self,
        mutation: DesiredStateMutation,
        stamp: HlcTimestamp,
        force: bool,
    ) -> bool {
        let changed = match mutation {
            DesiredStateMutation::PutNode(record) => {
                section_mut!(self, nodes).write(record.node_id.clone(), Some(record), stamp, force)
            }
            DesiredStateMutation::PutArtifact(record) => section_mut!(self, artifacts).write(
                record.artifact_id.clone(),
                Some(record),
                stamp,
                force,
            ),
            DesiredStateMutation::PutWorkload(record) => section_mut!(self, workloads).write(
                record.workload_id.clone(),
                Some(record),
                stamp,
                force,
            ),
            DesiredStateMutation::PutResource(record) => section_mut!(self, resources).write(
                record.resource_id.clone(),
                Some(record),
                stamp,
                force,
            ),
            DesiredStateMutation::PutProvider(record) => section_mut!(self, providers).write(
                record.provider_id.clone(),
                Some(record),
                stamp,
                force,
            ),
            DesiredStateMutation::PutExecutor(record) => section_mut!(self, executors).write(
                record.executor_id.clone(),
                Some(record),
                stamp,
                force,
            ),
            DesiredStateMutation::PutLease(record) => section_mut!(self, leases).write(
                record.resource_id.clone(),
                Some(record),
                stamp,
                force,
            ),
            DesiredStateMutation::RemoveNode(id) => {
                section_mut!(self, nodes).write(id, None, stamp, force)
            }
            DesiredStateMutation::RemoveArtifact(id) => {
                section_mut!(self, artifacts).write(id, None, stamp, force)
            }
            DesiredStateMutation::RemoveWorkload(id) => {
                section_mut!(self, workloads).write(id, None, stamp, force)
            }
            DesiredStateMutation::RemoveResource(id) => {
                section_mut!(self, resources).write(id, None, stamp, force)
            }
            DesiredStateMutation::RemoveProvider(id) => {
                section_mut!(self, providers).write(id, None, stamp, force)
            }
            DesiredStateMutation::RemoveExecutor(id) => {
                section_mut!(self, executors).write(id, None, stamp, force)
            }
            DesiredStateMutation::RemoveLease(id) => {
                section_mut!(self, leases).write(id, None, stamp, force)
            }
        };
        if changed {
            self.bump_revision();
        }
        changed
    }

    /// The current version of `key` as a stamped mutation (`Put` for a live object, `Remove`
    /// for a tombstone).
    pub fn stamped_mutation_for(
        &self,
        key: &DesiredObjectKey,
    ) -> Option<(DesiredStateMutation, HlcTimestamp)> {
        let version = self.version_of(key)?;
        if version.deleted {
            return Some((key.remove_mutation(), version.stamp));
        }
        let mutation = match key {
            DesiredObjectKey::Node(id) => {
                DesiredStateMutation::PutNode(self.nodes.get(id)?.clone())
            }
            DesiredObjectKey::Artifact(id) => {
                DesiredStateMutation::PutArtifact(self.artifacts.get(id)?.clone())
            }
            DesiredObjectKey::Workload(id) => {
                DesiredStateMutation::PutWorkload(self.workloads.get(id)?.clone())
            }
            DesiredObjectKey::Resource(id) => {
                DesiredStateMutation::PutResource(self.resources.get(id)?.clone())
            }
            DesiredObjectKey::Provider(id) => {
                DesiredStateMutation::PutProvider(self.providers.get(id)?.clone())
            }
            DesiredObjectKey::Executor(id) => {
                DesiredStateMutation::PutExecutor(self.executors.get(id)?.clone())
            }
            DesiredObjectKey::Lease(id) => {
                DesiredStateMutation::PutLease(self.leases.get(id)?.clone())
            }
        };
        Some((mutation, version.stamp))
    }

    /// Keys of every live object and tombstone, in section then key order.
    pub fn object_keys(&self) -> Vec<DesiredObjectKey> {
        let mut keys = Vec::new();
        keys.extend(self.nodes.keys().cloned().map(DesiredObjectKey::Node));
        keys.extend(
            self.artifacts
                .keys()
                .cloned()
                .map(DesiredObjectKey::Artifact),
        );
        keys.extend(
            self.workloads
                .keys()
                .cloned()
                .map(DesiredObjectKey::Workload),
        );
        keys.extend(
            self.resources
                .keys()
                .cloned()
                .map(DesiredObjectKey::Resource),
        );
        keys.extend(
            self.providers
                .keys()
                .cloned()
                .map(DesiredObjectKey::Provider),
        );
        keys.extend(
            self.executors
                .keys()
                .cloned()
                .map(DesiredObjectKey::Executor),
        );
        keys.extend(self.leases.keys().cloned().map(DesiredObjectKey::Lease));
        keys.extend(self.tombstones.entries().into_iter().map(|(key, _)| key));
        keys
    }

    /// Every live object and tombstone as one stamped batch (based at revision zero). Applying it
    /// to another state with the merge rule merges this whole state into it.
    pub fn stamped_batch(&self) -> MutationBatch {
        let mut versions = Vec::new();
        for key in self.object_keys() {
            if let Some(version) = self.stamped_mutation_for(&key) {
                versions.push(version);
            }
        }
        MutationBatch::stamped(orion_core::Revision::ZERO, versions)
    }

    /// Largest stamp of any live object or tombstone.
    pub fn max_stamp(&self) -> Option<HlcTimestamp> {
        self.stamps.max_stamp().max(self.tombstones.max_stamp())
    }

    /// Drops tombstones for which `keep` returns `false`. Does not change the revision: a
    /// collected tombstone is not a write. Returns how many were dropped.
    pub fn collect_tombstones(&mut self, keep: impl FnMut(HlcTimestamp) -> bool) -> usize {
        self.tombstones.retain(keep)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ArtifactRecord, NodeRecord};

    fn stamp(physical_ms: u64, node: u64) -> HlcTimestamp {
        HlcTimestamp::new(physical_ms, 0, node)
    }

    fn put_artifact(id: &'static str, size: u64) -> DesiredStateMutation {
        DesiredStateMutation::PutArtifact(ArtifactRecord::builder(id).size_bytes(size).build())
    }

    fn artifact_size(state: &DesiredClusterState, id: &'static str) -> Option<u64> {
        state
            .artifacts
            .get(&ArtifactId::new(id))
            .and_then(|record| record.size_bytes)
    }

    #[test]
    fn later_stamp_wins_regardless_of_order() {
        let early = (put_artifact("a", 1), stamp(10, 1));
        let late = (put_artifact("a", 2), stamp(20, 2));

        let mut forward = DesiredClusterState::default();
        assert!(forward.apply_stamped(early.0.clone(), early.1));
        assert!(forward.apply_stamped(late.0.clone(), late.1));
        let mut backward = DesiredClusterState::default();
        assert!(backward.apply_stamped(late.0.clone(), late.1));
        assert!(!backward.apply_stamped(early.0, early.1));

        assert_eq!(artifact_size(&forward, "a"), Some(2));
        assert_eq!(forward.artifacts, backward.artifacts);
        assert_eq!(forward.stamps, backward.stamps);
        assert_eq!(forward.revision.get(), 2);
        assert_eq!(backward.revision.get(), 1);
    }

    #[test]
    fn delete_and_update_resolve_by_stamp_and_delete_wins_ties() {
        let key = DesiredObjectKey::Artifact(ArtifactId::new("a"));
        let mut state = DesiredClusterState::default();
        state.apply_stamped(put_artifact("a", 1), stamp(10, 1));
        assert!(state.apply_stamped(key.remove_mutation(), stamp(20, 2)));
        assert!(state.artifacts.is_empty());
        assert_eq!(state.tombstones.get(&key), Some(stamp(20, 2)));
        assert!(state.stamps.get(&key).is_none());

        // An older update does not resurrect the object.
        assert!(!state.apply_stamped(put_artifact("a", 3), stamp(15, 3)));
        // Same stamp: the delete wins.
        assert!(!state.apply_stamped(put_artifact("a", 3), stamp(20, 2)));
        // A later update brings it back and clears the tombstone.
        assert!(state.apply_stamped(put_artifact("a", 4), stamp(30, 3)));
        assert_eq!(artifact_size(&state, "a"), Some(4));
        assert!(state.tombstones.get(&key).is_none());

        // A put and a delete with the same stamp: the delete wins in either order.
        let mut put_first = DesiredClusterState::default();
        put_first.apply_stamped(put_artifact("b", 1), stamp(5, 1));
        put_first.apply_stamped(
            DesiredObjectKey::Artifact(ArtifactId::new("b")).remove_mutation(),
            stamp(5, 1),
        );
        assert!(put_first.artifacts.is_empty());
    }

    #[test]
    fn equal_stamps_with_different_records_pick_the_larger_encoding() {
        let left = put_artifact("a", 1);
        let right = put_artifact("a", 2);
        let tie = stamp(10, 1);
        let mut one = DesiredClusterState::default();
        one.apply_stamped(left.clone(), tie);
        one.apply_stamped(right.clone(), tie);
        let mut two = DesiredClusterState::default();
        two.apply_stamped(right, tie);
        two.apply_stamped(left, tie);
        assert_eq!(one.artifacts, two.artifacts);
    }

    #[test]
    fn replaying_a_stamped_batch_reproduces_the_state() {
        let mut state = DesiredClusterState::default();
        state.apply_stamped(
            DesiredStateMutation::PutNode(NodeRecord::builder("node-a").build()),
            stamp(1, 1),
        );
        state.apply_stamped(put_artifact("a", 1), stamp(2, 1));
        state.apply_stamped(
            DesiredObjectKey::Artifact(ArtifactId::new("gone")).remove_mutation(),
            stamp(3, 1),
        );
        let mut merged = DesiredClusterState::default();
        state.stamped_batch().apply_to(&mut merged);
        assert_eq!(merged.nodes, state.nodes);
        assert_eq!(merged.artifacts, state.artifacts);
        assert_eq!(merged.stamps, state.stamps);
        assert_eq!(merged.tombstones, state.tombstones);
        assert_eq!(state.max_stamp(), Some(stamp(3, 1)));

        assert_eq!(merged.collect_tombstones(|stamp| stamp.physical_ms > 3), 1);
        assert!(merged.tombstones.is_empty());
    }
}
