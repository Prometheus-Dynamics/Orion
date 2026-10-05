//! Turning placement choices into `assigned_node_id` writes (`docs/placement.md`).
//!
//! Only the chosen node writes an assignment, and only to itself, so a node never claims work
//! for another node it may not be able to reach. The write is an ordinary desired-state write, so
//! concurrent writers converge by the HLC last-writer-wins rule. Hysteresis: a workload whose
//! assignee is still eligible is never moved (not even when a "better" node appears); a workload
//! whose assignee is gone or ineligible is moved only after that has lasted longer than the
//! grace period, as judged by the node that would take it over.

use alloc::{collections::BTreeMap, vec::Vec};
use orion_control_plane::{
    DesiredClusterState, DesiredState, PlacementDecision, PlacementReason, WorkloadRecord,
};
use orion_core::{NodeId, WorkloadId};

use crate::placement::{ClusterView, Ineligibility, choose_node, eligibility};

/// Where a placement-managed (or manual) workload stands from one node's point of view.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PlacementStatus {
    /// `placement: None` and no assignment: nothing will run it until a user assigns it.
    Manual,
    /// Explicitly assigned by a user; placement never moves it.
    Explicit { node_id: NodeId },
    /// Placed on an eligible node.
    Placed {
        node_id: NodeId,
        reason: Option<PlacementReason>,
    },
    /// The assignee is gone or ineligible; it moves once that has lasted the grace period.
    Degraded { node_id: NodeId, why: Ineligibility },
    /// Waiting for `chosen` to write the assignment, or for any eligible node (`None`).
    Pending { chosen: Option<NodeId> },
}

/// Leaderless placement coordinator run by every node.
///
/// It keeps only local hysteresis bookkeeping (when this node first saw a workload's assignee
/// as gone or ineligible); every decision is otherwise a pure function of the converged state and
/// the [`ClusterView`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClusterCoordinator {
    local_node_id: NodeId,
    grace_ms: u64,
    degraded_since: BTreeMap<WorkloadId, (NodeId, u64)>,
}

impl ClusterCoordinator {
    pub fn new(local_node_id: NodeId, grace_ms: u64) -> Self {
        Self {
            local_node_id,
            grace_ms,
            degraded_since: BTreeMap::new(),
        }
    }

    pub fn local_node_id(&self) -> &NodeId {
        &self.local_node_id
    }

    pub fn grace_ms(&self) -> u64 {
        self.grace_ms
    }

    /// Placement status of `workload` as seen through `view`.
    pub fn status(&self, workload: &WorkloadRecord, view: &ClusterView) -> PlacementStatus {
        if workload.has_explicit_assignment() {
            let node_id = workload
                .assigned_node_id
                .clone()
                .unwrap_or_else(|| self.local_node_id.clone());
            return PlacementStatus::Explicit { node_id };
        }
        let Some(placement) = workload.placement.as_ref() else {
            return PlacementStatus::Manual;
        };
        match workload.assigned_node_id.as_ref() {
            Some(node_id) => match eligibility(workload, node_id, view) {
                Ok(()) => PlacementStatus::Placed {
                    node_id: node_id.clone(),
                    reason: placement
                        .decision
                        .as_ref()
                        .map(|decision| decision.reason.clone()),
                },
                Err(why) => PlacementStatus::Degraded {
                    node_id: node_id.clone(),
                    why,
                },
            },
            None => PlacementStatus::Pending {
                chosen: choose_node(workload, view),
            },
        }
    }

    /// Evaluates every running, placement-managed workload and returns the records this node
    /// should write: assignments of workloads to the local node. `now_ms` drives the grace period.
    pub fn plan(
        &mut self,
        desired: &DesiredClusterState,
        view: &ClusterView,
        now_ms: u64,
    ) -> Vec<WorkloadRecord> {
        let mut writes = Vec::new();
        let mut still_degraded = BTreeMap::new();
        for workload in desired.workloads.values() {
            if workload.desired_state != DesiredState::Running || !workload.is_placement_managed() {
                continue;
            }
            let reason = match workload.assigned_node_id.as_ref() {
                None => PlacementReason::Placed,
                Some(current) => {
                    if eligibility(workload, current, view).is_ok() {
                        continue;
                    }
                    let since = match self.degraded_since.get(&workload.workload_id) {
                        Some((node_id, since)) if node_id == current => *since,
                        _ => now_ms,
                    };
                    still_degraded.insert(workload.workload_id.clone(), (current.clone(), since));
                    if now_ms.saturating_sub(since) < self.grace_ms {
                        continue;
                    }
                    PlacementReason::Failover {
                        from: current.clone(),
                    }
                }
            };
            if choose_node(workload, view).as_ref() == Some(&self.local_node_id) {
                writes.push(self.assign_in_place(workload.clone(), reason));
            }
        }
        self.degraded_since = still_degraded;
        writes
    }

    /// Assigns `workload` to the local node, recording the placement decision.
    pub fn assign_in_place(
        &self,
        mut workload: WorkloadRecord,
        reason: PlacementReason,
    ) -> WorkloadRecord {
        workload.assigned_node_id = Some(self.local_node_id.clone());
        let placement = workload.placement.get_or_insert_with(Default::default);
        placement.decision = Some(PlacementDecision {
            node_id: self.local_node_id.clone(),
            reason,
        });
        workload
    }
}
