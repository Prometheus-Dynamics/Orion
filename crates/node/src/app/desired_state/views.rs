use super::{NodeApp, NodeError};
use crate::app::{
    ClusterRevisionState, DesiredStateMetadataCache, DesiredStateSummaryCache,
    desired_sync::{section_fingerprints, section_mask, summarize_desired_state_for_sections},
};
use orion::{
    ArchiveEncode, NodeId, Revision,
    control_plane::{
        ClusterStateEnvelope, DesiredClusterState, DesiredStateMutation, DesiredStateSection,
        DesiredStateSummary, ObservedClusterState, StateSnapshot,
    },
    encode_to_vec,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    hash::{Hash, Hasher},
};

impl NodeApp {
    pub(crate) fn invalidate_desired_metadata_cache(&self) {
        *self.desired_metadata_cache_lock() = None;
        *self.desired_summary_cache_lock() = None;
    }

    pub(crate) fn current_revisions(&self) -> ClusterRevisionState {
        let store = self.store_read();
        ClusterRevisionState {
            desired: store.desired.revision,
            observed: store.observed.revision,
            applied: store.applied.revision,
        }
    }

    pub(crate) fn current_desired_revision(&self) -> Revision {
        self.store_read().desired.revision
    }

    pub(crate) fn with_desired_state_read<T>(
        &self,
        read: impl FnOnce(&DesiredClusterState) -> T,
    ) -> T {
        let store = self.store_read();
        read(&store.desired)
    }

    pub(crate) fn current_desired_state(&self) -> DesiredClusterState {
        self.store_read().desired.clone()
    }

    pub fn state_snapshot(&self) -> StateSnapshot {
        let store = self.store_read();
        StateSnapshot {
            state: ClusterStateEnvelope::new(
                store.desired.clone(),
                store.observed.clone(),
                store.applied.clone(),
            ),
        }
    }

    pub(crate) fn desired_metadata(&self) -> Result<DesiredStateMetadataCache, NodeError> {
        let desired_revision = self.current_desired_revision();
        if let Some(cached) = self.desired_metadata_cache_read().as_ref()
            && cached.revision == desired_revision
        {
            return Ok(cached.clone());
        }

        let metadata = self.with_desired_state_read(
            |desired| -> Result<DesiredStateMetadataCache, NodeError> {
                Ok(DesiredStateMetadataCache {
                    revision: desired.revision,
                    section_fingerprints: section_fingerprints(desired)?,
                    fingerprint: desired_fingerprint(desired)?,
                })
            },
        )?;
        *self.desired_metadata_cache_lock() = Some(metadata.clone());
        Ok(metadata)
    }

    pub(crate) fn desired_state_summary_for_sections(
        &self,
        sections: &[DesiredStateSection],
    ) -> Result<DesiredStateSummary, NodeError> {
        let metadata = self.desired_metadata()?;
        let mask = section_mask(sections);
        if let Some(cached) = self.desired_summary_cache_read().as_ref()
            && cached.revision == metadata.revision
            && let Some(summary) = cached.summaries_by_mask.get(&mask)
        {
            return Ok(summary.clone());
        }

        let summary = self.with_desired_state_read(|desired| {
            summarize_desired_state_for_sections(desired, &metadata.section_fingerprints, sections)
        })?;
        let mut cache = self.desired_summary_cache_lock();
        match cache.as_mut() {
            Some(cached) if cached.revision == metadata.revision => {
                cached.summaries_by_mask.insert(mask, summary.clone());
            }
            _ => {
                let mut summaries_by_mask = BTreeMap::new();
                summaries_by_mask.insert(mask, summary.clone());
                *cache = Some(DesiredStateSummaryCache {
                    revision: metadata.revision,
                    summaries_by_mask,
                });
            }
        }
        Ok(summary)
    }

    #[cfg(test)]
    pub(crate) fn desired_metadata_for_test(
        &self,
    ) -> Result<
        (
            Revision,
            u64,
            orion::control_plane::DesiredStateSectionFingerprints,
        ),
        NodeError,
    > {
        let metadata = self.desired_metadata()?;
        Ok((
            metadata.revision,
            metadata.fingerprint,
            metadata.section_fingerprints,
        ))
    }
}

pub(crate) fn desired_fingerprint(desired: &DesiredClusterState) -> Result<u64, NodeError> {
    let bytes = encode_to_vec(&section_fingerprints(desired)?)
        .map_err(|err| NodeError::Storage(err.to_string()))?;
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    bytes.hash(&mut hasher);
    Ok(hasher.finish())
}

pub(crate) fn entry_fingerprint<T: ArchiveEncode>(value: &T) -> Result<u64, NodeError> {
    let bytes = encode_to_vec(value).map_err(|err| NodeError::Storage(err.to_string()))?;
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    bytes.hash(&mut hasher);
    Ok(hasher.finish())
}

pub(crate) fn summarize_section<K, V>(
    section: &BTreeMap<K, V>,
) -> Result<BTreeMap<K, u64>, NodeError>
where
    K: Ord + Clone,
    V: ArchiveEncode,
{
    section
        .iter()
        .map(|(key, value)| Ok((key.clone(), entry_fingerprint(value)?)))
        .collect()
}

pub(crate) fn diff_desired_cluster_state(
    current: &DesiredClusterState,
    desired: &DesiredClusterState,
) -> orion::control_plane::MutationBatch {
    let base_revision = current.revision;
    let mut mutations = Vec::new();

    diff_section(
        &current.nodes,
        &desired.nodes,
        |record| DesiredStateMutation::PutNode(record.clone()),
        |node_id| DesiredStateMutation::RemoveNode(node_id.clone()),
        &mut mutations,
    );
    diff_section(
        &current.artifacts,
        &desired.artifacts,
        |record| DesiredStateMutation::PutArtifact(record.clone()),
        |artifact_id| DesiredStateMutation::RemoveArtifact(artifact_id.clone()),
        &mut mutations,
    );
    diff_section(
        &current.workloads,
        &desired.workloads,
        |record| DesiredStateMutation::PutWorkload(record.clone()),
        |workload_id| DesiredStateMutation::RemoveWorkload(workload_id.clone()),
        &mut mutations,
    );
    diff_section(
        &current.resources,
        &desired.resources,
        |record| DesiredStateMutation::PutResource(record.clone()),
        |resource_id| DesiredStateMutation::RemoveResource(resource_id.clone()),
        &mut mutations,
    );
    diff_section(
        &current.providers,
        &desired.providers,
        |record| DesiredStateMutation::PutProvider(record.clone()),
        |provider_id| DesiredStateMutation::RemoveProvider(provider_id.clone()),
        &mut mutations,
    );
    diff_section(
        &current.executors,
        &desired.executors,
        |record| DesiredStateMutation::PutExecutor(record.clone()),
        |executor_id| DesiredStateMutation::RemoveExecutor(executor_id.clone()),
        &mut mutations,
    );
    diff_section(
        &current.leases,
        &desired.leases,
        |record| DesiredStateMutation::PutLease(record.clone()),
        |resource_id| DesiredStateMutation::RemoveLease(resource_id.clone()),
        &mut mutations,
    );

    orion::control_plane::MutationBatch {
        base_revision,
        mutations,
    }
}

fn diff_section<K, V, Put, Remove>(
    current: &BTreeMap<K, V>,
    desired: &BTreeMap<K, V>,
    put: Put,
    remove: Remove,
    mutations: &mut Vec<DesiredStateMutation>,
) where
    K: Ord + Clone,
    V: PartialEq + Clone,
    Put: Fn(&V) -> DesiredStateMutation,
    Remove: Fn(&K) -> DesiredStateMutation,
{
    for (key, current_value) in current {
        match desired.get(key) {
            Some(desired_value) if desired_value == current_value => {}
            Some(desired_value) => mutations.push(put(desired_value)),
            None => mutations.push(remove(key)),
        }
    }

    for (key, desired_value) in desired {
        if !current.contains_key(key) {
            mutations.push(put(desired_value));
        }
    }
}

pub(crate) fn merge_observed_state(
    target: &mut ObservedClusterState,
    source: ObservedClusterState,
) -> bool {
    let mut changed = false;
    if target.revision != source.revision {
        target.revision = source.revision;
        changed = true;
    }
    for (key, value) in source.nodes {
        if target.nodes.get(&key) != Some(&value) {
            target.nodes.insert(key, value);
            changed = true;
        }
    }
    for (key, value) in source.workloads {
        if target.workloads.get(&key) != Some(&value) {
            target.workloads.insert(key, value);
            changed = true;
        }
    }
    for (key, value) in source.resources {
        if target.resources.get(&key) != Some(&value) {
            target.resources.insert(key, value);
            changed = true;
        }
    }
    for (key, value) in source.leases {
        if target.leases.get(&key) != Some(&value) {
            target.leases.insert(key, value);
            changed = true;
        }
    }
    changed
}

pub(crate) fn merge_peer_observed_state(
    target: &mut ObservedClusterState,
    desired: &DesiredClusterState,
    peer_node_id: &NodeId,
    source: ObservedClusterState,
) -> bool {
    let mut changed = false;
    if target.revision != source.revision {
        target.revision = source.revision;
        changed = true;
    }

    let source_node_ids: BTreeSet<_> = source.nodes.keys().cloned().collect();
    let source_workload_ids: BTreeSet<_> = source.workloads.keys().cloned().collect();
    let source_resource_ids: BTreeSet<_> = source.resources.keys().cloned().collect();
    let source_lease_ids: BTreeSet<_> = source.leases.keys().cloned().collect();
    let peer_provider_ids: BTreeSet<_> = desired
        .providers
        .values()
        .filter(|provider| &provider.node_id == peer_node_id)
        .map(|provider| provider.provider_id.clone())
        .collect();
    let peer_executor_ids: BTreeSet<_> = desired
        .executors
        .values()
        .filter(|executor| &executor.node_id == peer_node_id)
        .map(|executor| executor.executor_id.clone())
        .collect();
    let peer_desired_resource_ids: BTreeSet<_> = desired
        .resources
        .iter()
        .filter(|(_, resource)| {
            peer_provider_ids.contains(&resource.provider_id)
                || resource
                    .realized_by_executor_id
                    .as_ref()
                    .is_some_and(|executor_id| peer_executor_ids.contains(executor_id))
        })
        .map(|(resource_id, _)| resource_id.clone())
        .collect();

    changed |= retain_with_change(&mut target.nodes, |node_id, _| {
        node_id != peer_node_id || source_node_ids.contains(node_id)
    });
    changed |= retain_with_change(&mut target.workloads, |_, workload| {
        workload.assigned_node_id.as_ref() != Some(peer_node_id)
            || source_workload_ids.contains(&workload.workload_id)
    });
    changed |= retain_with_change(&mut target.resources, |resource_id, resource| {
        let peer_owned = peer_provider_ids.contains(&resource.provider_id)
            || resource
                .realized_by_executor_id
                .as_ref()
                .is_some_and(|executor_id| peer_executor_ids.contains(executor_id))
            || peer_desired_resource_ids.contains(resource_id);
        !peer_owned || source_resource_ids.contains(resource_id)
    });
    changed |= retain_with_change(&mut target.leases, |resource_id, lease| {
        let peer_owned = peer_desired_resource_ids.contains(resource_id)
            || source_resource_ids.contains(resource_id)
            || lease.holder_node_id.as_ref() == Some(peer_node_id);
        !peer_owned || source_lease_ids.contains(resource_id)
    });

    merge_observed_state(target, source) || changed
}

fn retain_with_change<K, V>(map: &mut BTreeMap<K, V>, mut keep: impl FnMut(&K, &V) -> bool) -> bool
where
    K: Ord,
{
    let before = map.len();
    map.retain(|key, value| keep(key, value));
    map.len() != before
}

#[cfg(test)]
mod tests {
    use super::merge_peer_observed_state;
    use orion::control_plane::{
        DesiredClusterState, DesiredState, ExecutorRecord, LeaseRecord, NodeRecord,
        ObservedClusterState, ProviderRecord, ResourceRecord, WorkloadObservedState,
        WorkloadRecord,
    };
    use orion_core::{ExecutorId, NodeId, ProviderId, ResourceType, RuntimeType};

    #[test]
    fn peer_observed_merge_prunes_records_missing_from_peer_snapshot() {
        let peer_node_id = NodeId::new("node-b");
        let mut desired = DesiredClusterState::default();
        desired.put_node(NodeRecord::builder("node-b").build());
        desired.put_provider(ProviderRecord {
            provider_id: ProviderId::new("provider.peer"),
            node_id: peer_node_id.clone(),
            resource_types: vec![ResourceType::new("camera")],
        });
        desired.put_executor(ExecutorRecord {
            executor_id: ExecutorId::new("executor.peer"),
            node_id: peer_node_id.clone(),
            runtime_types: vec![RuntimeType::new("camera.runtime")],
        });

        let mut target = ObservedClusterState::default();
        target.put_node(NodeRecord::builder("node-b").build());
        target.put_workload(
            WorkloadRecord::builder("workload.old", "camera.runtime", "artifact.old")
                .desired_state(DesiredState::Running)
                .observed_state(WorkloadObservedState::Running)
                .assigned_to("node-b")
                .build(),
        );
        target.put_resource(
            ResourceRecord::builder("resource.old", "camera", "provider.peer")
                .endpoint("tcp://127.0.0.1:1234")
                .build(),
        );
        target.put_lease(
            LeaseRecord::builder("resource.old")
                .holder_node("node-b")
                .build(),
        );

        let source = ObservedClusterState::default();
        let changed = merge_peer_observed_state(&mut target, &desired, &peer_node_id, source);

        assert!(changed);
        assert!(target.nodes.is_empty());
        assert!(target.workloads.is_empty());
        assert!(target.resources.is_empty());
        assert!(target.leases.is_empty());
    }
}
