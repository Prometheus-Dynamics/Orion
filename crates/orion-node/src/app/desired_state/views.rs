use super::{NodeApp, NodeError};
use crate::app::{
    ClusterRevisionState, DesiredStateMetadataCache, DesiredStateSummaryCache,
    desired_sync::{section_fingerprints, section_mask, summarize_desired_state_for_sections},
};
use orion::{
    ArchiveEncode, Revision,
    control_plane::{
        ClusterStateEnvelope, DesiredClusterState, DesiredStateMutation, DesiredStateSection,
        DesiredStateSummary, ObservedClusterState, StateSnapshot,
    },
    encode_to_vec,
};
use std::{
    collections::BTreeMap,
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
) {
    target.revision = source.revision;
    target.nodes.extend(source.nodes);
    target.workloads.extend(source.workloads);
    target.resources.extend(source.resources);
    target.leases.extend(source.leases);
}
