//! Desired-state fingerprints, summaries and per-object version diffs used by peer sync.
//!
//! Peers compare versions with the same last-writer-wins rule that
//! [`DesiredClusterState::apply_stamped`] applies, so the initiator of a sync round can split the
//! differing objects into the ones it pushes and the ones it pulls. See `docs/peer-sync.md`.

use super::desired_state::{entry_fingerprint, summarize_section};
use super::*;
use orion::{HlcTimestamp, control_plane::DesiredObjectKey};

pub(super) fn summarize_desired_state_for_sections(
    desired: &DesiredClusterState,
    section_fingerprints: &DesiredStateSectionFingerprints,
    sections: &[DesiredStateSection],
) -> Result<DesiredStateSummary, NodeError> {
    let include_all = sections.is_empty();
    let include = |target: DesiredStateSection| include_all || sections.contains(&target);
    fn summarize_if<K: Ord + Clone, V: ArchiveEncode>(
        include: bool,
        section: &BTreeMap<K, V>,
    ) -> Result<BTreeMap<K, u64>, NodeError> {
        if include {
            summarize_section(section)
        } else {
            Ok(BTreeMap::new())
        }
    }
    Ok(DesiredStateSummary {
        revision: desired.revision,
        section_fingerprints: section_fingerprints.clone(),
        nodes: summarize_if(include(DesiredStateSection::Nodes), &desired.nodes)?,
        artifacts: summarize_if(include(DesiredStateSection::Artifacts), &desired.artifacts)?,
        workloads: summarize_if(include(DesiredStateSection::Workloads), &desired.workloads)?,
        resources: summarize_if(include(DesiredStateSection::Resources), &desired.resources)?,
        providers: summarize_if(include(DesiredStateSection::Providers), &desired.providers)?,
        executors: summarize_if(include(DesiredStateSection::Executors), &desired.executors)?,
        leases: summarize_if(include(DesiredStateSection::Leases), &desired.leases)?,
        stamps: desired.stamps.for_sections(sections),
        tombstones: desired.tombstones.for_sections(sections),
    })
}

/// Per-section fingerprints over records, their stamps and the section's tombstones. Two nodes
/// with equal fingerprints hold the same desired state, whatever their local revisions are.
pub(crate) fn section_fingerprints(
    desired: &DesiredClusterState,
) -> Result<DesiredStateSectionFingerprints, NodeError> {
    fn section<K, V>(
        records: &BTreeMap<K, V>,
        stamps: &BTreeMap<K, HlcTimestamp>,
        tombstones: &BTreeMap<K, HlcTimestamp>,
    ) -> Result<u64, NodeError>
    where
        BTreeMap<K, V>: ArchiveEncode,
        BTreeMap<K, HlcTimestamp>: ArchiveEncode,
    {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        entry_fingerprint(records)?.hash(&mut hasher);
        entry_fingerprint(stamps)?.hash(&mut hasher);
        entry_fingerprint(tombstones)?.hash(&mut hasher);
        Ok(hasher.finish())
    }
    let (stamps, tombstones) = (&desired.stamps, &desired.tombstones);
    Ok(DesiredStateSectionFingerprints {
        nodes: section(&desired.nodes, &stamps.nodes, &tombstones.nodes)?,
        artifacts: section(&desired.artifacts, &stamps.artifacts, &tombstones.artifacts)?,
        workloads: section(&desired.workloads, &stamps.workloads, &tombstones.workloads)?,
        resources: section(&desired.resources, &stamps.resources, &tombstones.resources)?,
        providers: section(&desired.providers, &stamps.providers, &tombstones.providers)?,
        executors: section(&desired.executors, &stamps.executors, &tombstones.executors)?,
        leases: section(&desired.leases, &stamps.leases, &tombstones.leases)?,
    })
}

#[cfg(peer_sync)]
pub(super) fn changed_sections(
    local: &DesiredStateSectionFingerprints,
    remote: &DesiredStateSectionFingerprints,
) -> Vec<DesiredStateSection> {
    let pairs = [
        (DesiredStateSection::Nodes, local.nodes, remote.nodes),
        (
            DesiredStateSection::Artifacts,
            local.artifacts,
            remote.artifacts,
        ),
        (
            DesiredStateSection::Workloads,
            local.workloads,
            remote.workloads,
        ),
        (
            DesiredStateSection::Resources,
            local.resources,
            remote.resources,
        ),
        (
            DesiredStateSection::Providers,
            local.providers,
            remote.providers,
        ),
        (
            DesiredStateSection::Executors,
            local.executors,
            remote.executors,
        ),
        (DesiredStateSection::Leases, local.leases, remote.leases),
    ];
    pairs
        .into_iter()
        .filter(|(_, local, remote)| local != remote)
        .map(|(section, _, _)| section)
        .collect()
}

#[cfg(peer_sync)]
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

/// The version of one object as seen in a summary (stamp plus content hash).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct SummaryVersion {
    stamp: HlcTimestamp,
    /// `None` for a tombstone.
    content: Option<u64>,
}

fn summary_content(summary: &DesiredStateSummary, key: &DesiredObjectKey) -> Option<u64> {
    match key {
        DesiredObjectKey::Node(id) => summary.nodes.get(id),
        DesiredObjectKey::Artifact(id) => summary.artifacts.get(id),
        DesiredObjectKey::Workload(id) => summary.workloads.get(id),
        DesiredObjectKey::Resource(id) => summary.resources.get(id),
        DesiredObjectKey::Provider(id) => summary.providers.get(id),
        DesiredObjectKey::Executor(id) => summary.executors.get(id),
        DesiredObjectKey::Lease(id) => summary.leases.get(id),
    }
    .copied()
}

fn summary_version(
    summary: &DesiredStateSummary,
    key: &DesiredObjectKey,
) -> Option<SummaryVersion> {
    if let Some(content) = summary_content(summary, key) {
        return Some(SummaryVersion {
            stamp: summary.stamps.get(key).unwrap_or(HlcTimestamp::ZERO),
            content: Some(content),
        });
    }
    summary.tombstones.get(key).map(|stamp| SummaryVersion {
        stamp,
        content: None,
    })
}

/// Returns `true` when `ours` should replace `theirs` under the merge rule, judged from
/// summaries. Equal stamps with different content count as a win for both sides, so both versions
/// are exchanged and the receiver resolves the tie with the full records.
fn summary_wins(ours: SummaryVersion, theirs: Option<SummaryVersion>) -> bool {
    let Some(theirs) = theirs else {
        return true;
    };
    match ours.stamp.cmp(&theirs.stamp) {
        std::cmp::Ordering::Greater => true,
        std::cmp::Ordering::Less => false,
        std::cmp::Ordering::Equal => match (ours.content, theirs.content) {
            (None, None) => false,
            (None, Some(_)) => true,
            (Some(_), None) => false,
            (Some(left), Some(right)) => left != right,
        },
    }
}

#[cfg(peer_sync)]
fn summary_keys(summary: &DesiredStateSummary) -> Vec<DesiredObjectKey> {
    let mut keys = Vec::new();
    keys.extend(summary.nodes.keys().cloned().map(DesiredObjectKey::Node));
    keys.extend(
        summary
            .artifacts
            .keys()
            .cloned()
            .map(DesiredObjectKey::Artifact),
    );
    keys.extend(
        summary
            .workloads
            .keys()
            .cloned()
            .map(DesiredObjectKey::Workload),
    );
    keys.extend(
        summary
            .resources
            .keys()
            .cloned()
            .map(DesiredObjectKey::Resource),
    );
    keys.extend(
        summary
            .providers
            .keys()
            .cloned()
            .map(DesiredObjectKey::Provider),
    );
    keys.extend(
        summary
            .executors
            .keys()
            .cloned()
            .map(DesiredObjectKey::Executor),
    );
    keys.extend(summary.leases.keys().cloned().map(DesiredObjectKey::Lease));
    keys.extend(summary.tombstones.entries().into_iter().map(|(key, _)| key));
    keys
}

/// Tombstones whose stamp is older than `cutoff_ms` are expired: they are neither sent nor
/// pulled (see "Tombstones and garbage collection" in `docs/peer-sync.md`).
fn is_expired_tombstone(version: SummaryVersion, cutoff_ms: u64) -> bool {
    version.content.is_none() && version.stamp.physical_ms < cutoff_ms
}

/// Keys whose version in `local` wins against `remote` (push set) and keys whose version in
/// `remote` wins against `local` (pull set), restricted to `sections`.
#[cfg(peer_sync)]
pub(super) fn plan_sync_exchange(
    local: &DesiredStateSummary,
    remote: &DesiredStateSummary,
    tombstone_cutoff_ms: u64,
) -> (Vec<DesiredObjectKey>, Vec<DesiredObjectKey>) {
    let mut keys = summary_keys(local);
    keys.extend(summary_keys(remote));
    keys.sort();
    keys.dedup();
    let mut push = Vec::new();
    let mut pull = Vec::new();
    for key in keys {
        let ours = summary_version(local, &key)
            .filter(|version| !is_expired_tombstone(*version, tombstone_cutoff_ms));
        let theirs = summary_version(remote, &key)
            .filter(|version| !is_expired_tombstone(*version, tombstone_cutoff_ms));
        if let Some(ours) = ours
            && summary_wins(ours, theirs)
        {
            push.push(key.clone());
        }
        if let Some(theirs) = theirs
            && summary_wins(theirs, ours)
        {
            pull.push(key);
        }
    }
    (push, pull)
}

/// Stamped versions of `keys` from `desired` (keys without a version are skipped).
#[cfg(peer_sync)]
pub(super) fn stamped_versions_for_keys(
    desired: &DesiredClusterState,
    keys: &[DesiredObjectKey],
) -> MutationBatch {
    MutationBatch::stamped(
        desired.revision,
        keys.iter()
            .filter_map(|key| desired.stamped_mutation_for(key)),
    )
}

/// Versions in `desired` (restricted to `sections`, or to the selected objects) that win against
/// the versions described by `summary`, as a stamped batch.
pub(super) fn versions_newer_than_summary(
    desired: &DesiredClusterState,
    summary: &DesiredStateSummary,
    sections: &[DesiredStateSection],
    object_selectors: &[DesiredStateObjectSelector],
    tombstone_cutoff_ms: u64,
) -> MutationBatch {
    // A section with a selector is restricted to the selected objects; other requested sections
    // (all sections when none are listed) are compared in full.
    let selected = selected_keys(object_selectors);
    let selected_sections: Vec<_> = selected.iter().map(DesiredObjectKey::section).collect();
    let mut candidates: Vec<_> = desired
        .object_keys()
        .into_iter()
        .filter(|key| {
            let section = key.section();
            (sections.is_empty() || sections.contains(&section))
                && !selected_sections.contains(&section)
        })
        .collect();
    candidates.extend(selected);
    let mut versions = Vec::new();
    for key in candidates {
        let Some((mutation, stamp)) = desired.stamped_mutation_for(&key) else {
            continue;
        };
        let content = if mutation.is_remove() {
            None
        } else {
            let local_summary_hash = match &mutation {
                DesiredStateMutation::PutNode(record) => entry_fingerprint(record),
                DesiredStateMutation::PutArtifact(record) => entry_fingerprint(record),
                DesiredStateMutation::PutWorkload(record) => entry_fingerprint(record),
                DesiredStateMutation::PutResource(record) => entry_fingerprint(record),
                DesiredStateMutation::PutProvider(record) => entry_fingerprint(record),
                DesiredStateMutation::PutExecutor(record) => entry_fingerprint(record),
                DesiredStateMutation::PutLease(record) => entry_fingerprint(record),
                _ => Ok(0),
            };
            Some(local_summary_hash.unwrap_or(0))
        };
        let ours = SummaryVersion { stamp, content };
        if is_expired_tombstone(ours, tombstone_cutoff_ms) {
            continue;
        }
        if summary_wins(ours, summary_version(summary, &key)) {
            versions.push((mutation, stamp));
        }
    }
    MutationBatch::stamped(desired.revision, versions)
}

fn selected_keys(selectors: &[DesiredStateObjectSelector]) -> Vec<DesiredObjectKey> {
    let mut keys = Vec::new();
    for selector in selectors {
        match selector {
            DesiredStateObjectSelector::Nodes(ids) => {
                keys.extend(ids.iter().cloned().map(DesiredObjectKey::Node))
            }
            DesiredStateObjectSelector::Artifacts(ids) => {
                keys.extend(ids.iter().cloned().map(DesiredObjectKey::Artifact))
            }
            DesiredStateObjectSelector::Workloads(ids) => {
                keys.extend(ids.iter().cloned().map(DesiredObjectKey::Workload))
            }
            DesiredStateObjectSelector::Resources(ids) => {
                keys.extend(ids.iter().cloned().map(DesiredObjectKey::Resource))
            }
            DesiredStateObjectSelector::Providers(ids) => {
                keys.extend(ids.iter().cloned().map(DesiredObjectKey::Provider))
            }
            DesiredStateObjectSelector::Executors(ids) => {
                keys.extend(ids.iter().cloned().map(DesiredObjectKey::Executor))
            }
            DesiredStateObjectSelector::Leases(ids) => {
                keys.extend(ids.iter().cloned().map(DesiredObjectKey::Lease))
            }
        }
    }
    keys
}

/// Groups keys into one selector per section.
#[cfg(peer_sync)]
pub(super) fn selectors_for_keys(keys: &[DesiredObjectKey]) -> Vec<DesiredStateObjectSelector> {
    let mut nodes = Vec::new();
    let mut artifacts = Vec::new();
    let mut workloads = Vec::new();
    let mut resources = Vec::new();
    let mut providers = Vec::new();
    let mut executors = Vec::new();
    let mut leases = Vec::new();
    for key in keys {
        match key.clone() {
            DesiredObjectKey::Node(id) => nodes.push(id),
            DesiredObjectKey::Artifact(id) => artifacts.push(id),
            DesiredObjectKey::Workload(id) => workloads.push(id),
            DesiredObjectKey::Resource(id) => resources.push(id),
            DesiredObjectKey::Provider(id) => providers.push(id),
            DesiredObjectKey::Executor(id) => executors.push(id),
            DesiredObjectKey::Lease(id) => leases.push(id),
        }
    }
    let mut selectors = Vec::new();
    if !nodes.is_empty() {
        selectors.push(DesiredStateObjectSelector::Nodes(nodes));
    }
    if !artifacts.is_empty() {
        selectors.push(DesiredStateObjectSelector::Artifacts(artifacts));
    }
    if !workloads.is_empty() {
        selectors.push(DesiredStateObjectSelector::Workloads(workloads));
    }
    if !resources.is_empty() {
        selectors.push(DesiredStateObjectSelector::Resources(resources));
    }
    if !providers.is_empty() {
        selectors.push(DesiredStateObjectSelector::Providers(providers));
    }
    if !executors.is_empty() {
        selectors.push(DesiredStateObjectSelector::Executors(executors));
    }
    if !leases.is_empty() {
        selectors.push(DesiredStateObjectSelector::Leases(leases));
    }
    selectors
}

/// Sections touched by `keys`, in canonical order.
#[cfg(peer_sync)]
pub(super) fn sections_of_keys(keys: &[DesiredObjectKey]) -> Vec<DesiredStateSection> {
    all_desired_sections()
        .into_iter()
        .filter(|section| keys.iter().any(|key| key.section() == *section))
        .collect()
}
