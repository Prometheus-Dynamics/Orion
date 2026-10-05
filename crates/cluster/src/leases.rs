//! Cross-node binding: resolving, authorizing and leasing another node's resource
//! (`docs/placement.md`, "Cross-node binding").
//!
//! A lease is an ordinary desired-state record keyed by resource id, so every node agrees on it
//! through the HLC last-writer-wins merge. The consumer node adds its workload to the lease's
//! `holders` when the resource's ownership mode leaves room; concurrent adds to the same lease
//! are whole-record writes, so exactly one survives and the losers see a full lease on their next
//! pass. The owner node evicts holders beyond capacity (its own local bindings count first) and
//! holders whose workload no longer runs there, so an Exclusive resource never ends up shared.

use alloc::{collections::BTreeSet, vec::Vec};
use orion_control_plane::{
    AvailabilityState, DesiredClusterState, DesiredState, LeaseHolder, LeaseRecord,
    ObservedClusterState, ResourceOwnershipMode, ResourceRecord, WorkloadObservedState,
    WorkloadRecord, WorkloadRequirement,
};
use orion_core::{NodeId, ResourceId, WorkloadId};

use crate::placement::{rendezvous_score, resource_host};

/// One lease change a node wants to commit.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LeaseEdit {
    Put(LeaseRecord),
    Remove(ResourceId),
}

impl LeaseEdit {
    pub fn resource_id(&self) -> &ResourceId {
        match self {
            Self::Put(lease) => &lease.resource_id,
            Self::Remove(resource_id) => resource_id,
        }
    }
}

/// Consumers a resource admits: `None` for unlimited (`SharedRead`).
pub fn capacity(mode: &ResourceOwnershipMode) -> Option<u32> {
    match mode {
        ResourceOwnershipMode::Exclusive => Some(1),
        ResourceOwnershipMode::SharedRead => None,
        ResourceOwnershipMode::SharedLimited { max_consumers } => Some(*max_consumers),
    }
}

/// Whether `resource` matches `requirement` apart from capacity (type, ownership mode,
/// capabilities).
pub fn resource_matches(resource: &ResourceRecord, requirement: &WorkloadRequirement) -> bool {
    resource.resource_type == requirement.resource_type
        && requirement
            .ownership_mode
            .as_ref()
            .is_none_or(|mode| mode == &resource.ownership_mode)
        && requirement
            .required_capabilities
            .iter()
            .all(|capability_id| {
                resource
                    .capabilities
                    .iter()
                    .any(|capability| &capability.capability_id == capability_id)
            })
}

/// A holder is live while its workload still exists, should run, and is assigned to the
/// holder's node.
pub fn holder_is_current(desired: &DesiredClusterState, holder: &LeaseHolder) -> bool {
    desired
        .workloads
        .get(&holder.workload_id)
        .is_some_and(|workload| {
            workload.desired_state == DesiredState::Running
                && workload.assigned_node_id.as_ref() == Some(&holder.node_id)
        })
}

fn is_active(state: WorkloadObservedState) -> bool {
    matches!(
        state,
        WorkloadObservedState::Pending
            | WorkloadObservedState::Assigned
            | WorkloadObservedState::Starting
            | WorkloadObservedState::Running
    )
}

/// Local (non-leased) bindings of `owner`'s active workloads to `resource_id`, as reported in
/// `owner`'s observed slice.
pub fn owner_local_claims(
    observed: &ObservedClusterState,
    owner: &NodeId,
    resource_id: &ResourceId,
) -> u32 {
    let count = observed
        .workloads
        .values()
        .filter(|workload| {
            workload.assigned_node_id.as_ref() == Some(owner) && is_active(workload.observed_state)
        })
        .flat_map(|workload| workload.resource_bindings.iter())
        .filter(|binding| {
            &binding.resource_id == resource_id && &binding.node_id == owner && !binding.is_remote()
        })
        .count();
    u32::try_from(count).unwrap_or(u32::MAX)
}

/// The current holders of `lease` (dropping holders whose workload no longer runs there).
pub fn current_holders(desired: &DesiredClusterState, lease: &LeaseRecord) -> Vec<LeaseHolder> {
    lease
        .all_holders()
        .into_iter()
        .filter(|holder| holder_is_current(desired, holder))
        .collect()
}

/// Whether `holder` can be added to the lease of `resource` owned by `owner`.
pub fn has_room(
    desired: &DesiredClusterState,
    observed: &ObservedClusterState,
    resource: &ResourceRecord,
    owner: &NodeId,
    holder: &LeaseHolder,
) -> bool {
    let Some(limit) = capacity(&resource.ownership_mode) else {
        return true;
    };
    let others = desired
        .leases
        .get(&resource.resource_id)
        .map(|lease| {
            current_holders(desired, lease)
                .into_iter()
                .filter(|existing| existing != holder)
                .count()
        })
        .unwrap_or(0);
    let used = u32::try_from(others)
        .unwrap_or(u32::MAX)
        .saturating_add(owner_local_claims(observed, owner, &resource.resource_id));
    used < limit
}

/// Remote resources `workload` (assigned to `local`) could lease for `requirement`, best first.
///
/// Candidates are owned by another reachable node, available, matching, not in `exclude`, and
/// have room. They are ranked by rendezvous score of the workload id over resource ids, so
/// every node ranks them the same way and different workloads spread over equal resources.
pub fn remote_candidates(
    local: &NodeId,
    workload: &WorkloadRecord,
    requirement: &WorkloadRequirement,
    desired: &DesiredClusterState,
    observed: &ObservedClusterState,
    is_reachable: impl Fn(&NodeId) -> bool,
    exclude: &BTreeSet<ResourceId>,
) -> Vec<(ResourceId, NodeId)> {
    let holder = LeaseHolder::new(local.clone(), workload.workload_id.clone());
    let mut candidates: Vec<(ResourceId, NodeId)> = known_resources(desired, observed)
        .filter(|resource| {
            resource.availability == AvailabilityState::Available
                && !exclude.contains(&resource.resource_id)
                && resource_matches(resource, requirement)
        })
        .filter_map(|resource| {
            let owner = resource_host(desired, resource)?;
            (&owner != local
                && is_reachable(&owner)
                && has_room(desired, observed, resource, &owner, &holder))
            .then(|| (resource.resource_id.clone(), owner))
        })
        .collect();
    candidates.sort_by(|(left, _), (right, _)| {
        let key = workload.workload_id.as_str();
        rendezvous_score(key, right.as_str())
            .cmp(&rendezvous_score(key, left.as_str()))
            .then_with(|| left.cmp(right))
    });
    candidates.dedup_by(|a, b| a.0 == b.0);
    candidates
}

/// Every resource record known locally: observed reports (fresher) first, then desired records
/// that no node reports.
pub fn known_resources<'a>(
    desired: &'a DesiredClusterState,
    observed: &'a ObservedClusterState,
) -> impl Iterator<Item = &'a ResourceRecord> {
    observed.resources.values().chain(
        desired
            .resources
            .values()
            .filter(|resource| !observed.resources.contains_key(&resource.resource_id)),
    )
}

/// Looks up a resource by id, preferring the observed report.
pub fn known_resource<'a>(
    desired: &'a DesiredClusterState,
    observed: &'a ObservedClusterState,
    resource_id: &ResourceId,
) -> Option<&'a ResourceRecord> {
    observed
        .resources
        .get(resource_id)
        .or_else(|| desired.resources.get(resource_id))
}

/// `lease` (possibly absent) with `holder` added.
pub fn with_holder(
    resource_id: &ResourceId,
    lease: Option<&LeaseRecord>,
    holder: LeaseHolder,
) -> LeaseRecord {
    let mut holders = lease.map(|lease| lease.holders.clone()).unwrap_or_default();
    holders.push(holder);
    LeaseRecord::held_by(resource_id.clone(), holders)
}

/// `lease` without the holders `drop` selects: `None` when nothing changes, a removal when no
/// holder is left.
pub fn without_holders(
    lease: &LeaseRecord,
    drop: impl Fn(&LeaseHolder) -> bool,
) -> Option<LeaseEdit> {
    let kept: Vec<LeaseHolder> = lease
        .holders
        .iter()
        .filter(|holder| !drop(holder))
        .cloned()
        .collect();
    if kept.len() == lease.holders.len() {
        return None;
    }
    Some(if kept.is_empty() {
        LeaseEdit::Remove(lease.resource_id.clone())
    } else {
        LeaseEdit::Put(LeaseRecord::held_by(lease.resource_id.clone(), kept))
    })
}

/// Owner-side arbitration for leases on resources hosted by `local`: drops holders whose
/// workload no longer runs on the holder's node, then holders beyond the resource's capacity
/// (local bindings count first; the remaining holders keep their sorted order).
pub fn arbitrate_owned_leases(
    local: &NodeId,
    desired: &DesiredClusterState,
    observed: &ObservedClusterState,
) -> Vec<LeaseEdit> {
    let mut edits = Vec::new();
    for lease in desired
        .leases
        .values()
        .filter(|lease| !lease.holders.is_empty())
    {
        let Some(resource) = known_resource(desired, observed, &lease.resource_id) else {
            continue;
        };
        if resource_host(desired, resource).as_ref() != Some(local) {
            continue;
        }
        let mut keep: Vec<LeaseHolder> = current_holders(desired, lease);
        if let Some(limit) = capacity(&resource.ownership_mode) {
            let room =
                limit.saturating_sub(owner_local_claims(observed, local, &lease.resource_id));
            keep.truncate(usize::try_from(room).unwrap_or(usize::MAX));
        }
        if let Some(edit) = without_holders(lease, |holder| !keep.contains(holder)) {
            edits.push(edit);
        }
    }
    edits
}

/// Leases (by resource id) held by `node`'s workload `workload_id`.
pub fn leases_held_by<'a>(
    desired: &'a DesiredClusterState,
    node: &'a NodeId,
    workload_id: &'a WorkloadId,
) -> impl Iterator<Item = &'a LeaseRecord> {
    desired.leases.values().filter(move |lease| {
        lease
            .holders
            .iter()
            .any(|holder| &holder.node_id == node && &holder.workload_id == workload_id)
    })
}
