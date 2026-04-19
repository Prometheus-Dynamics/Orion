use super::desired_state::{entry_fingerprint, summarize_section};
use super::*;

pub(super) fn summarize_desired_state_for_sections(
    desired: &DesiredClusterState,
    section_fingerprints: &DesiredStateSectionFingerprints,
    sections: &[DesiredStateSection],
) -> Result<DesiredStateSummary, NodeError> {
    let include_all = sections.is_empty();
    let include = |target: DesiredStateSection| include_all || sections.contains(&target);
    Ok(DesiredStateSummary {
        revision: desired.revision,
        section_fingerprints: section_fingerprints.clone(),
        nodes: if include(DesiredStateSection::Nodes) {
            summarize_section(&desired.nodes)?
        } else {
            BTreeMap::new()
        },
        artifacts: if include(DesiredStateSection::Artifacts) {
            summarize_section(&desired.artifacts)?
        } else {
            BTreeMap::new()
        },
        workloads: if include(DesiredStateSection::Workloads) {
            summarize_section(&desired.workloads)?
        } else {
            BTreeMap::new()
        },
        workload_tombstones: if include(DesiredStateSection::Workloads) {
            desired.workload_tombstones.clone()
        } else {
            BTreeMap::new()
        },
        resources: if include(DesiredStateSection::Resources) {
            summarize_section(&desired.resources)?
        } else {
            BTreeMap::new()
        },
        providers: if include(DesiredStateSection::Providers) {
            summarize_section(&desired.providers)?
        } else {
            BTreeMap::new()
        },
        executors: if include(DesiredStateSection::Executors) {
            summarize_section(&desired.executors)?
        } else {
            BTreeMap::new()
        },
        leases: if include(DesiredStateSection::Leases) {
            summarize_section(&desired.leases)?
        } else {
            BTreeMap::new()
        },
    })
}

pub(super) fn section_fingerprints(
    desired: &DesiredClusterState,
) -> Result<DesiredStateSectionFingerprints, NodeError> {
    Ok(DesiredStateSectionFingerprints {
        nodes: section_fingerprint(&desired.nodes)?,
        artifacts: section_fingerprint(&desired.artifacts)?,
        workloads: workload_section_fingerprint(&desired.workloads, &desired.workload_tombstones)?,
        resources: section_fingerprint(&desired.resources)?,
        providers: section_fingerprint(&desired.providers)?,
        executors: section_fingerprint(&desired.executors)?,
        leases: section_fingerprint(&desired.leases)?,
    })
}

fn section_fingerprint<T: ArchiveEncode>(value: &T) -> Result<u64, NodeError> {
    entry_fingerprint(value)
}

fn workload_section_fingerprint(
    workloads: &BTreeMap<WorkloadId, WorkloadRecord>,
    tombstones: &BTreeMap<WorkloadId, Revision>,
) -> Result<u64, NodeError> {
    let bytes = encode_to_vec(&(workloads.clone(), tombstones.clone()))
        .map_err(|err| NodeError::Storage(err.to_string()))?;
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    bytes.hash(&mut hasher);
    Ok(hasher.finish())
}

pub(super) fn changed_sections(
    local: &DesiredStateSectionFingerprints,
    remote: &DesiredStateSectionFingerprints,
) -> Vec<DesiredStateSection> {
    let mut sections = Vec::new();
    if local.nodes != remote.nodes {
        sections.push(DesiredStateSection::Nodes);
    }
    if local.artifacts != remote.artifacts {
        sections.push(DesiredStateSection::Artifacts);
    }
    if local.workloads != remote.workloads {
        sections.push(DesiredStateSection::Workloads);
    }
    if local.resources != remote.resources {
        sections.push(DesiredStateSection::Resources);
    }
    if local.providers != remote.providers {
        sections.push(DesiredStateSection::Providers);
    }
    if local.executors != remote.executors {
        sections.push(DesiredStateSection::Executors);
    }
    if local.leases != remote.leases {
        sections.push(DesiredStateSection::Leases);
    }
    sections
}

pub(super) fn empty_summary_for_sections(
    revision: Revision,
    _sections: &[DesiredStateSection],
) -> DesiredStateSummary {
    DesiredStateSummary {
        revision,
        section_fingerprints: DesiredStateSectionFingerprints {
            nodes: 0,
            artifacts: 0,
            workloads: 0,
            resources: 0,
            providers: 0,
            executors: 0,
            leases: 0,
        },
        nodes: BTreeMap::new(),
        artifacts: BTreeMap::new(),
        workloads: BTreeMap::new(),
        workload_tombstones: BTreeMap::new(),
        resources: BTreeMap::new(),
        providers: BTreeMap::new(),
        executors: BTreeMap::new(),
        leases: BTreeMap::new(),
    }
}

pub(super) fn all_desired_sections() -> Vec<DesiredStateSection> {
    vec![
        DesiredStateSection::Nodes,
        DesiredStateSection::Artifacts,
        DesiredStateSection::Workloads,
        DesiredStateSection::Resources,
        DesiredStateSection::Providers,
        DesiredStateSection::Executors,
        DesiredStateSection::Leases,
    ]
}

pub(super) fn section_mask(sections: &[DesiredStateSection]) -> u8 {
    if sections.is_empty() {
        return u8::MAX;
    }
    let mut mask = 0u8;
    for section in sections {
        mask |= match section {
            DesiredStateSection::Nodes => 1 << 0,
            DesiredStateSection::Artifacts => 1 << 1,
            DesiredStateSection::Workloads => 1 << 2,
            DesiredStateSection::Resources => 1 << 3,
            DesiredStateSection::Providers => 1 << 4,
            DesiredStateSection::Executors => 1 << 5,
            DesiredStateSection::Leases => 1 << 6,
        };
    }
    mask
}

pub(super) fn diff_desired_against_summary_sections(
    desired: &DesiredClusterState,
    local_summary: &DesiredStateSummary,
    summary: &DesiredStateSummary,
    sections: &[DesiredStateSection],
    object_selectors: &[DesiredStateObjectSelector],
    base_revision: Revision,
) -> Result<MutationBatch, NodeError> {
    let mut mutations = Vec::new();
    let selector_map = selector_map(object_selectors);

    for section in sections {
        match section {
            DesiredStateSection::Nodes => diff_section_against_summary_or_selected(
                &desired.nodes,
                &local_summary.nodes,
                &summary.nodes,
                selector_map.nodes.as_ref(),
                |record| DesiredStateMutation::PutNode(record.clone()),
                |node_id| DesiredStateMutation::RemoveNode(node_id.clone()),
                &mut mutations,
            )?,
            DesiredStateSection::Artifacts => diff_section_against_summary_or_selected(
                &desired.artifacts,
                &local_summary.artifacts,
                &summary.artifacts,
                selector_map.artifacts.as_ref(),
                |record| DesiredStateMutation::PutArtifact(record.clone()),
                |artifact_id| DesiredStateMutation::RemoveArtifact(artifact_id.clone()),
                &mut mutations,
            )?,
            DesiredStateSection::Workloads => diff_workloads_against_summary_or_selected(
                &desired.workloads,
                &desired.workload_tombstones,
                &WorkloadSummaryViews {
                    local_workloads: &local_summary.workloads,
                    local_tombstones: &local_summary.workload_tombstones,
                    remote_workloads: &summary.workloads,
                    remote_tombstones: &summary.workload_tombstones,
                },
                selector_map.workloads.as_ref(),
                &mut mutations,
            )?,
            DesiredStateSection::Resources => diff_section_against_summary_or_selected(
                &desired.resources,
                &local_summary.resources,
                &summary.resources,
                selector_map.resources.as_ref(),
                |record| DesiredStateMutation::PutResource(record.clone()),
                |resource_id| DesiredStateMutation::RemoveResource(resource_id.clone()),
                &mut mutations,
            )?,
            DesiredStateSection::Providers => diff_section_against_summary_or_selected(
                &desired.providers,
                &local_summary.providers,
                &summary.providers,
                selector_map.providers.as_ref(),
                |record| DesiredStateMutation::PutProvider(record.clone()),
                |provider_id| DesiredStateMutation::RemoveProvider(provider_id.clone()),
                &mut mutations,
            )?,
            DesiredStateSection::Executors => diff_section_against_summary_or_selected(
                &desired.executors,
                &local_summary.executors,
                &summary.executors,
                selector_map.executors.as_ref(),
                |record| DesiredStateMutation::PutExecutor(record.clone()),
                |executor_id| DesiredStateMutation::RemoveExecutor(executor_id.clone()),
                &mut mutations,
            )?,
            DesiredStateSection::Leases => diff_section_against_summary_or_selected(
                &desired.leases,
                &local_summary.leases,
                &summary.leases,
                selector_map.leases.as_ref(),
                |record| DesiredStateMutation::PutLease(record.clone()),
                |resource_id| DesiredStateMutation::RemoveLease(resource_id.clone()),
                &mut mutations,
            )?,
        }
    }

    Ok(MutationBatch {
        base_revision,
        mutations,
    })
}

#[derive(Default)]
struct SelectorMap {
    nodes: Option<Vec<NodeId>>,
    artifacts: Option<Vec<ArtifactId>>,
    workloads: Option<Vec<WorkloadId>>,
    resources: Option<Vec<ResourceId>>,
    providers: Option<Vec<ProviderId>>,
    executors: Option<Vec<ExecutorId>>,
    leases: Option<Vec<ResourceId>>,
}

struct WorkloadSummaryViews<'a> {
    local_workloads: &'a BTreeMap<WorkloadId, u64>,
    local_tombstones: &'a BTreeMap<WorkloadId, Revision>,
    remote_workloads: &'a BTreeMap<WorkloadId, u64>,
    remote_tombstones: &'a BTreeMap<WorkloadId, Revision>,
}

fn selector_map(selectors: &[DesiredStateObjectSelector]) -> SelectorMap {
    let mut map = SelectorMap::default();
    for selector in selectors {
        match selector {
            DesiredStateObjectSelector::Nodes(ids) => map.nodes = Some(ids.clone()),
            DesiredStateObjectSelector::Artifacts(ids) => map.artifacts = Some(ids.clone()),
            DesiredStateObjectSelector::Workloads(ids) => map.workloads = Some(ids.clone()),
            DesiredStateObjectSelector::Resources(ids) => map.resources = Some(ids.clone()),
            DesiredStateObjectSelector::Providers(ids) => map.providers = Some(ids.clone()),
            DesiredStateObjectSelector::Executors(ids) => map.executors = Some(ids.clone()),
            DesiredStateObjectSelector::Leases(ids) => map.leases = Some(ids.clone()),
        }
    }
    map
}

fn diff_section_against_summary_or_selected<K, V, Put, Remove>(
    desired: &BTreeMap<K, V>,
    local_summary: &BTreeMap<K, u64>,
    summary: &BTreeMap<K, u64>,
    selected_keys: Option<&Vec<K>>,
    put: Put,
    remove: Remove,
    mutations: &mut Vec<DesiredStateMutation>,
) -> Result<(), NodeError>
where
    K: Ord + Clone,
    V: ArchiveEncode + Clone,
    Put: Fn(&V) -> DesiredStateMutation,
    Remove: Fn(&K) -> DesiredStateMutation,
{
    match selected_keys {
        Some(keys) => {
            for key in keys {
                match desired.get(key) {
                    Some(value) => {
                        if summary.get(key) != local_summary.get(key) {
                            mutations.push(put(value));
                        }
                    }
                    None => {
                        if summary.contains_key(key) {
                            mutations.push(remove(key));
                        }
                    }
                }
            }
        }
        None => {
            if summary.is_empty() {
                mutations.extend(desired.values().map(put));
                return Ok(());
            }

            for (key, value) in desired {
                if summary.get(key) != local_summary.get(key) {
                    mutations.push(put(value));
                }
            }

            for key in summary.keys() {
                if !desired.contains_key(key) {
                    mutations.push(remove(key));
                }
            }
        }
    }

    Ok(())
}

fn diff_workloads_against_summary_or_selected(
    desired_workloads: &BTreeMap<WorkloadId, WorkloadRecord>,
    desired_tombstones: &BTreeMap<WorkloadId, Revision>,
    summary_views: &WorkloadSummaryViews<'_>,
    selected_ids: Option<&Vec<WorkloadId>>,
    mutations: &mut Vec<DesiredStateMutation>,
) -> Result<(), NodeError> {
    match selected_ids {
        Some(ids) => {
            for workload_id in ids {
                if let Some(record) = desired_workloads.get(workload_id) {
                    if summary_views.remote_workloads.get(workload_id)
                        != summary_views.local_workloads.get(workload_id)
                    {
                        mutations.push(DesiredStateMutation::PutWorkload(record.clone()));
                    }
                    continue;
                }
                if desired_tombstones.get(workload_id).is_some() {
                    if summary_views.remote_tombstones.get(workload_id)
                        != summary_views.local_tombstones.get(workload_id)
                        || summary_views.remote_workloads.contains_key(workload_id)
                    {
                        mutations.push(DesiredStateMutation::RemoveWorkload(workload_id.clone()));
                    }
                    continue;
                }
                if summary_views.remote_workloads.contains_key(workload_id)
                    || summary_views.remote_tombstones.contains_key(workload_id)
                {
                    mutations.push(DesiredStateMutation::RemoveWorkload(workload_id.clone()));
                }
            }
        }
        None => {
            if summary_views.remote_workloads.is_empty()
                && summary_views.remote_tombstones.is_empty()
            {
                mutations.extend(
                    desired_workloads
                        .values()
                        .cloned()
                        .map(DesiredStateMutation::PutWorkload),
                );
                mutations.extend(
                    desired_tombstones
                        .keys()
                        .cloned()
                        .map(DesiredStateMutation::RemoveWorkload),
                );
                return Ok(());
            }

            for (workload_id, record) in desired_workloads {
                if summary_views.remote_workloads.get(workload_id)
                    != summary_views.local_workloads.get(workload_id)
                {
                    mutations.push(DesiredStateMutation::PutWorkload(record.clone()));
                }
            }

            for workload_id in desired_tombstones.keys() {
                if summary_views.remote_tombstones.get(workload_id)
                    != summary_views.local_tombstones.get(workload_id)
                    || summary_views.remote_workloads.contains_key(workload_id)
                {
                    mutations.push(DesiredStateMutation::RemoveWorkload(workload_id.clone()));
                }
            }

            for workload_id in summary_views.remote_workloads.keys() {
                if !desired_workloads.contains_key(workload_id)
                    && !desired_tombstones.contains_key(workload_id)
                {
                    mutations.push(DesiredStateMutation::RemoveWorkload(workload_id.clone()));
                }
            }
        }
    }

    Ok(())
}

type WorkloadMergeResult = (
    BTreeMap<WorkloadId, WorkloadRecord>,
    BTreeMap<WorkloadId, Revision>,
);

pub(super) fn merge_desired_cluster_state(
    local: &DesiredClusterState,
    remote: &DesiredClusterState,
) -> Result<DesiredClusterState, NodeError> {
    let nodes = merge_section(&local.nodes, &remote.nodes)?;
    let artifacts = merge_section(&local.artifacts, &remote.artifacts)?;
    let (workloads, workload_tombstones) = merge_workload_section(local, remote)?;
    let resources = merge_section(&local.resources, &remote.resources)?;
    let providers = merge_section(&local.providers, &remote.providers)?;
    let executors = merge_section(&local.executors, &remote.executors)?;
    let leases = merge_section(&local.leases, &remote.leases)?;
    let differs_from_local = local.nodes != nodes
        || local.artifacts != artifacts
        || local.workloads != workloads
        || local.workload_tombstones != workload_tombstones
        || local.resources != resources
        || local.providers != providers
        || local.executors != executors
        || local.leases != leases;
    let differs_from_remote = remote.nodes != nodes
        || remote.artifacts != artifacts
        || remote.workloads != workloads
        || remote.workload_tombstones != workload_tombstones
        || remote.resources != resources
        || remote.providers != providers
        || remote.executors != executors
        || remote.leases != leases;
    let mut merged = DesiredClusterState {
        revision: std::cmp::max(local.revision, remote.revision),
        nodes,
        artifacts,
        workloads,
        workload_tombstones,
        resources,
        providers,
        executors,
        leases,
    };

    if differs_from_local && differs_from_remote {
        merged.revision = merged.revision.next();
    }

    Ok(merged)
}

fn merge_workload_section(
    local: &DesiredClusterState,
    remote: &DesiredClusterState,
) -> Result<WorkloadMergeResult, NodeError> {
    let mut workloads = BTreeMap::new();
    let mut workload_tombstones = BTreeMap::new();

    for workload_id in local
        .workloads
        .keys()
        .chain(local.workload_tombstones.keys())
        .chain(remote.workloads.keys())
        .chain(remote.workload_tombstones.keys())
    {
        if workloads.contains_key(workload_id) || workload_tombstones.contains_key(workload_id) {
            continue;
        }

        let local_record = local.workloads.get(workload_id);
        let remote_record = remote.workloads.get(workload_id);
        let local_tombstone = local.workload_tombstones.get(workload_id).copied();
        let remote_tombstone = remote.workload_tombstones.get(workload_id).copied();

        let winner = select_workload_entry(
            workload_id,
            local_record,
            local.revision,
            local_tombstone,
            remote_record,
            remote.revision,
            remote_tombstone,
        )?;

        match winner {
            WorkloadMergeEntry::Record(record) => {
                workloads.insert(workload_id.clone(), record);
            }
            WorkloadMergeEntry::Tombstone(revision) => {
                workload_tombstones.insert(workload_id.clone(), revision);
            }
            WorkloadMergeEntry::Absent => {}
        }
    }

    Ok((workloads, workload_tombstones))
}

enum WorkloadMergeEntry {
    Record(WorkloadRecord),
    Tombstone(Revision),
    Absent,
}

fn select_workload_entry(
    workload_id: &WorkloadId,
    local_record: Option<&WorkloadRecord>,
    local_record_revision: Revision,
    local_tombstone: Option<Revision>,
    remote_record: Option<&WorkloadRecord>,
    remote_record_revision: Revision,
    remote_tombstone: Option<Revision>,
) -> Result<WorkloadMergeEntry, NodeError> {
    let winning_record = match (local_record, remote_record) {
        (Some(left), Some(right)) if left == right => Some((
            left.clone(),
            local_record_revision.max(remote_record_revision),
        )),
        (Some(left), Some(right)) => {
            let left_bytes =
                encode_to_vec(left).map_err(|err| NodeError::Storage(err.to_string()))?;
            let right_bytes =
                encode_to_vec(right).map_err(|err| NodeError::Storage(err.to_string()))?;
            if left_bytes >= right_bytes {
                Some((left.clone(), local_record_revision))
            } else {
                Some((right.clone(), remote_record_revision))
            }
        }
        (Some(left), None) => Some((left.clone(), local_record_revision)),
        (None, Some(right)) => Some((right.clone(), remote_record_revision)),
        (None, None) => None,
    };

    let winning_tombstone = match (local_tombstone, remote_tombstone) {
        (Some(left), Some(right)) => Some(std::cmp::max(left, right)),
        (Some(left), None) => Some(left),
        (None, Some(right)) => Some(right),
        (None, None) => None,
    };

    match (winning_record, winning_tombstone) {
        (Some((record, record_revision)), Some(tombstone_revision)) => {
            if tombstone_revision > record_revision {
                Ok(WorkloadMergeEntry::Tombstone(tombstone_revision))
            } else if record_revision > tombstone_revision {
                Ok(WorkloadMergeEntry::Record(record))
            } else {
                let _ = workload_id;
                Ok(WorkloadMergeEntry::Record(record))
            }
        }
        (Some((record, _)), None) => Ok(WorkloadMergeEntry::Record(record)),
        (None, Some(tombstone_revision)) => Ok(WorkloadMergeEntry::Tombstone(tombstone_revision)),
        (None, None) => Ok(WorkloadMergeEntry::Absent),
    }
}

fn merge_section<K, V>(
    local: &BTreeMap<K, V>,
    remote: &BTreeMap<K, V>,
) -> Result<BTreeMap<K, V>, NodeError>
where
    K: Ord + Clone,
    V: Clone + Eq + ArchiveEncode,
{
    let mut merged = BTreeMap::new();

    for key in local.keys().chain(remote.keys()) {
        if merged.contains_key(key) {
            continue;
        }

        match (local.get(key), remote.get(key)) {
            (Some(left), Some(right)) if left == right => {
                merged.insert(key.clone(), left.clone());
            }
            (Some(left), Some(right)) => {
                let left_bytes =
                    encode_to_vec(left).map_err(|err| NodeError::Storage(err.to_string()))?;
                let right_bytes =
                    encode_to_vec(right).map_err(|err| NodeError::Storage(err.to_string()))?;
                if left_bytes >= right_bytes {
                    merged.insert(key.clone(), left.clone());
                } else {
                    merged.insert(key.clone(), right.clone());
                }
            }
            (Some(left), None) => {
                merged.insert(key.clone(), left.clone());
            }
            (None, Some(right)) => {
                merged.insert(key.clone(), right.clone());
            }
            (None, None) => {}
        }
    }

    Ok(merged)
}
