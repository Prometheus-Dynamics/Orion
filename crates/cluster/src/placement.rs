//! Deterministic, leaderless workload placement (`docs/placement.md`).
//!
//! Every node builds a [`ClusterView`] from its converged desired state, the observed slices its
//! peers push, and its own liveness judgement, and evaluates the same pure functions over it:
//! [`eligibility`] filters nodes by schedulability, liveness, runtime support, node selector and
//! co-location, and [`choose_node`] picks among the eligible nodes with rendezvous (highest
//! random weight) hashing of the workload id over node ids, ties broken by the smaller node id.
//! Nodes that see the same view therefore choose the same node without talking to each other.

use alloc::{
    collections::{BTreeMap, BTreeSet},
    string::String,
    vec::Vec,
};
use orion_control_plane::{
    DesiredClusterState, LabelRequirement, ObservedClusterState, ResourceRecord, WorkloadRecord,
    split_label,
};
use orion_core::{NodeId, ResourceId, RuntimeType};

/// What placement knows about one node.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct NodeCandidate {
    /// Effective labels (`key=value` or `key`): the node's self-reported labels, overridden per
    /// key by labels set on its desired node record.
    pub labels: Vec<String>,
    /// `false` when the desired node record or the node's own report marks it unschedulable
    /// (cordoned, draining, maintenance).
    pub schedulable: bool,
    /// The local node is always live; other nodes are live while the local node hears from them.
    pub live: bool,
    /// Runtime types of the executors registered on the node.
    pub runtime_types: BTreeSet<RuntimeType>,
}

/// The cluster as one node sees it, the input of every placement decision.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ClusterView {
    pub nodes: BTreeMap<NodeId, NodeCandidate>,
    /// Node hosting each known resource.
    pub resource_hosts: BTreeMap<ResourceId, NodeId>,
}

impl ClusterView {
    /// Builds the view from converged state. `is_live` judges peers (the local node is always
    /// live); `local_schedulable` reflects local maintenance state.
    pub fn from_state(
        local_node_id: &NodeId,
        desired: &DesiredClusterState,
        observed: &ObservedClusterState,
        local_schedulable: bool,
        is_live: impl Fn(&NodeId) -> bool,
    ) -> Self {
        let mut node_ids: BTreeSet<NodeId> = desired.nodes.keys().cloned().collect();
        node_ids.extend(observed.nodes.keys().cloned());
        node_ids.extend(desired.executors.values().map(|e| e.node_id.clone()));
        node_ids.insert(local_node_id.clone());

        let nodes = node_ids
            .into_iter()
            .map(|node_id| {
                let reported = observed.nodes.get(&node_id);
                let declared = desired.nodes.get(&node_id);
                let mut labels = BTreeMap::<String, String>::new();
                for label in reported
                    .into_iter()
                    .chain(declared)
                    .flat_map(|record| record.labels.iter())
                {
                    if let Some((key, _)) = split_label(label) {
                        labels.insert(key.into(), label.trim().into());
                    }
                }
                let local = &node_id == local_node_id;
                let candidate = NodeCandidate {
                    labels: labels.into_values().collect(),
                    schedulable: declared.is_none_or(|record| record.schedulable)
                        && reported.is_none_or(|record| record.schedulable)
                        && (!local || local_schedulable),
                    live: local || is_live(&node_id),
                    runtime_types: desired
                        .executors
                        .values()
                        .filter(|executor| executor.node_id == node_id)
                        .flat_map(|executor| executor.runtime_types.iter().cloned())
                        .collect(),
                };
                (node_id, candidate)
            })
            .collect();

        let mut resource_hosts = BTreeMap::new();
        for resource in desired
            .resources
            .values()
            .chain(observed.resources.values())
        {
            if let Some(host) = resource_host(desired, resource) {
                resource_hosts.insert(resource.resource_id.clone(), host);
            }
        }
        Self {
            nodes,
            resource_hosts,
        }
    }
}

/// Node that hosts `resource`: its provider's node, or the node of the executor that realizes it.
pub fn resource_host(desired: &DesiredClusterState, resource: &ResourceRecord) -> Option<NodeId> {
    if let Some(executor_id) = resource.realized_by_executor_id.as_ref()
        && let Some(executor) = desired.executors.get(executor_id)
    {
        return Some(executor.node_id.clone());
    }
    desired
        .providers
        .get(&resource.provider_id)
        .map(|provider| provider.node_id.clone())
}

/// Why a node cannot run a workload.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Ineligibility {
    UnknownNode,
    Unreachable,
    Unschedulable,
    RuntimeUnsupported,
    SelectorMismatch(LabelRequirement),
    /// The co-location resource is hosted elsewhere (or unknown, `host: None`).
    NotColocated {
        resource_id: ResourceId,
        host: Option<NodeId>,
    },
}

impl core::fmt::Display for Ineligibility {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::UnknownNode => f.write_str("unknown node"),
            Self::Unreachable => f.write_str("unreachable"),
            Self::Unschedulable => f.write_str("unschedulable"),
            Self::RuntimeUnsupported => f.write_str("no executor for the runtime type"),
            Self::SelectorMismatch(term) => write!(f, "label {term} does not match"),
            Self::NotColocated {
                resource_id,
                host: Some(host),
            } => write!(f, "resource {resource_id} is hosted on {host}"),
            Self::NotColocated {
                resource_id,
                host: None,
            } => write!(f, "resource {resource_id} is not known"),
        }
    }
}

/// Whether `node_id` may run `workload` under its placement constraints.
pub fn eligibility(
    workload: &WorkloadRecord,
    node_id: &NodeId,
    view: &ClusterView,
) -> Result<(), Ineligibility> {
    let node = view.nodes.get(node_id).ok_or(Ineligibility::UnknownNode)?;
    if !node.live {
        return Err(Ineligibility::Unreachable);
    }
    if !node.schedulable {
        return Err(Ineligibility::Unschedulable);
    }
    if !node.runtime_types.contains(&workload.runtime_type) {
        return Err(Ineligibility::RuntimeUnsupported);
    }
    let Some(placement) = workload.placement.as_ref() else {
        return Ok(());
    };
    if let Some(term) = placement
        .node_selector
        .iter()
        .find(|term| !term.matches(node.labels.iter().map(String::as_str)))
    {
        return Err(Ineligibility::SelectorMismatch(term.clone()));
    }
    if let Some(resource_id) = placement.colocate_with_resource.as_ref() {
        let host = view.resource_hosts.get(resource_id);
        if host != Some(node_id) {
            return Err(Ineligibility::NotColocated {
                resource_id: resource_id.clone(),
                host: host.cloned(),
            });
        }
    }
    Ok(())
}

/// Every node eligible for `workload`, in node id order.
pub fn eligible_nodes(workload: &WorkloadRecord, view: &ClusterView) -> Vec<NodeId> {
    view.nodes
        .keys()
        .filter(|node_id| eligibility(workload, node_id, view).is_ok())
        .cloned()
        .collect()
}

/// Rendezvous weight of `node` for `key`: a 64-bit FNV-1a hash of both ids, finalized with the
/// SplitMix64 mixer. Stable across platforms, releases and processes.
pub fn rendezvous_score(key: &str, node: &str) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in key.bytes().chain([0xff]).chain(node.bytes()) {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash = (hash ^ (hash >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    hash = (hash ^ (hash >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    hash ^ (hash >> 31)
}

/// Picks the candidate with the highest rendezvous score for `key`; equal scores go to the
/// smaller id. Adding or removing a candidate only moves the keys that candidate wins or loses.
pub fn rendezvous_choice<'a, T>(
    key: &str,
    candidates: impl IntoIterator<Item = &'a T>,
) -> Option<&'a T>
where
    T: AsRef<str> + Ord + 'a,
{
    candidates.into_iter().max_by(|left, right| {
        rendezvous_score(key, left.as_ref())
            .cmp(&rendezvous_score(key, right.as_ref()))
            .then_with(|| right.cmp(left))
    })
}

/// The node every observer of `view` chooses for `workload`, if any node is eligible.
pub fn choose_node(workload: &WorkloadRecord, view: &ClusterView) -> Option<NodeId> {
    let eligible = eligible_nodes(workload, view);
    rendezvous_choice(workload.workload_id.as_str(), &eligible).cloned()
}
