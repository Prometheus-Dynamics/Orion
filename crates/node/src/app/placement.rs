//! The node's share of leaderless placement and cross-node binding (`docs/placement.md`).
//!
//! Every reconcile pass, before the runtime plans local workloads, the node publishes its own
//! node facts, refreshes peer liveness, and lets [`ClusterCoordinator`] decide which
//! placement-managed workloads it should take (only ever assigning them to itself). After the
//! runtime planned, the node maintains the cross-node leases of its workloads: it releases leases
//! its workloads no longer need or whose owner has been gone longer than the grace period,
//! acquires leases on remote resources for requirements nothing local satisfies, and, as the
//! owner of resources, evicts lease holders beyond capacity. All of these are ordinary local
//! desired-state writes, stamped by the HLC and merged by peers with last writer wins.

use super::{NodeApp, NodeError};
use orion::{
    NodeId, ResourceId,
    cluster::{
        ClusterCoordinator, ClusterView, LeaseEdit,
        leases::{
            arbitrate_owned_leases, holder_is_current, known_resource, remote_candidates,
            with_holder, without_holders,
        },
        resource_host,
    },
    control_plane::{
        DesiredClusterState, DesiredStateMutation, LeaseHolder, MaintenanceMode, MutationBatch,
        ObservedClusterState,
    },
    runtime::UnsatisfiedRequirement,
};
use std::collections::{BTreeMap, BTreeSet};
use tracing::{debug, info};

impl NodeApp {
    fn cluster_inputs(&self) -> (DesiredClusterState, ObservedClusterState, bool) {
        let store = self.store_read();
        (
            store.desired.clone(),
            store.observed.clone(),
            store.maintenance().mode == MaintenanceMode::Normal,
        )
    }

    /// The cluster as this node sees it now (converged state plus local liveness).
    pub fn cluster_view(&self) -> ClusterView {
        let (desired, observed, schedulable) = self.cluster_inputs();
        let now_ms = Self::current_time_ms();
        ClusterView::from_state(
            &self.config.node_id,
            &desired,
            &observed,
            schedulable,
            |node_id| self.is_node_live(node_id, now_ms),
        )
    }

    /// Runs before the runtime plans: node facts, liveness, placement. Returns whether observed
    /// state or binding availability changed.
    pub(super) fn run_placement_pass(&self) -> bool {
        let facts_changed = self.publish_local_node_facts();
        let liveness_changed = self.refresh_unreachable_nodes();
        let (desired, observed, schedulable) = self.cluster_inputs();
        if desired
            .workloads
            .values()
            .any(|workload| workload.placement.is_some())
        {
            let now_ms = Self::current_time_ms();
            let view = ClusterView::from_state(
                &self.config.node_id,
                &desired,
                &observed,
                schedulable,
                |node_id| self.is_node_live(node_id, now_ms),
            );
            let writes = {
                let mut cluster = self.state.cluster.lock();
                let grace_ms = self.placement_grace_ms();
                cluster
                    .coordinator
                    .get_or_insert_with(|| {
                        ClusterCoordinator::new(self.config.node_id.clone(), grace_ms)
                    })
                    .plan(&desired, &view, now_ms)
            };
            for workload in &writes {
                info!(
                    node = %self.config.node_id,
                    workload = %workload.workload_id,
                    reason = ?workload.placement.as_ref().and_then(|p| p.decision.as_ref()).map(|d| &d.reason),
                    "placement assigns workload to this node"
                );
            }
            let mutations = writes
                .into_iter()
                .map(DesiredStateMutation::PutWorkload)
                .collect();
            if let Err(err) = self.commit_cluster_writes(mutations) {
                debug!(node = %self.config.node_id, error = %err, "placement write rejected; retrying next pass");
            }
        }
        facts_changed || liveness_changed
    }

    /// Runs after the runtime planned: releases, acquires and arbitrates cross-node leases.
    pub(super) fn run_lease_pass(&self, unsatisfied: &[UnsatisfiedRequirement]) {
        let (desired, observed, _) = self.cluster_inputs();
        let local = self.config.node_id.clone();
        let has_remote_leases = desired
            .leases
            .values()
            .any(|lease| !lease.holders.is_empty());
        if unsatisfied.is_empty() && !has_remote_leases {
            return;
        }
        let now_ms = Self::current_time_ms();
        let mut edits = BTreeMap::<ResourceId, LeaseEdit>::new();
        let released = self.release_leases(&desired, &observed, now_ms, &mut edits);
        for edit in arbitrate_owned_leases(&local, &desired, &observed) {
            edits.entry(edit.resource_id().clone()).or_insert(edit);
        }
        self.acquire_leases(
            &desired,
            &observed,
            unsatisfied,
            &released,
            now_ms,
            &mut edits,
        );

        let mutations: Vec<_> = edits
            .into_values()
            .map(|edit| match edit {
                LeaseEdit::Put(lease) => DesiredStateMutation::PutLease(lease),
                LeaseEdit::Remove(resource_id) => DesiredStateMutation::RemoveLease(resource_id),
            })
            .collect();
        if let Err(err) = self.commit_cluster_writes(mutations) {
            debug!(node = %self.config.node_id, error = %err, "lease write rejected; retrying next pass");
        }
    }

    /// Drops this node's holders that are no longer current, and those whose owner has been
    /// unreachable for longer than the grace period. Returns the resources released for the
    /// second reason (not re-acquired in the same pass).
    fn release_leases(
        &self,
        desired: &DesiredClusterState,
        observed: &ObservedClusterState,
        now_ms: u64,
        edits: &mut BTreeMap<ResourceId, LeaseEdit>,
    ) -> BTreeSet<ResourceId> {
        let local = &self.config.node_id;
        let grace_ms = self.placement_grace_ms();
        let mut released = BTreeSet::new();
        let mut gone_since = BTreeMap::new();
        let previous = std::mem::take(&mut self.state.cluster.lock().owner_gone_since);
        for lease in desired.leases.values() {
            if !lease.holders.iter().any(|holder| &holder.node_id == local) {
                continue;
            }
            let owner = known_resource(desired, observed, &lease.resource_id)
                .and_then(|resource| resource_host(desired, resource));
            let owner_expired = match owner.as_ref() {
                Some(owner) if !self.is_node_live(owner, now_ms) => {
                    let since = previous.get(&lease.resource_id).copied().unwrap_or(now_ms);
                    gone_since.insert(lease.resource_id.clone(), since);
                    now_ms.saturating_sub(since) >= grace_ms
                }
                _ => false,
            };
            if owner_expired {
                released.insert(lease.resource_id.clone());
            }
            let edit = without_holders(lease, |holder| {
                &holder.node_id == local && (owner_expired || !holder_is_current(desired, holder))
            });
            if let Some(edit) = edit {
                info!(node = %local, resource = %lease.resource_id, owner_gone = owner_expired, "releasing cross-node lease");
                gone_since.remove(&lease.resource_id);
                edits.insert(lease.resource_id.clone(), edit);
            }
        }
        self.state.cluster.lock().owner_gone_since = gone_since;
        released
    }

    fn acquire_leases(
        &self,
        desired: &DesiredClusterState,
        observed: &ObservedClusterState,
        unsatisfied: &[UnsatisfiedRequirement],
        released: &BTreeSet<ResourceId>,
        now_ms: u64,
        edits: &mut BTreeMap<ResourceId, LeaseEdit>,
    ) {
        let local = &self.config.node_id;
        let mut taken: BTreeSet<ResourceId> = released.clone();
        for need in unsatisfied {
            let Some(workload) = desired.workloads.get(&need.workload_id) else {
                continue;
            };
            let mut exclude = taken.clone();
            exclude.extend(
                desired
                    .leases
                    .values()
                    .filter(|lease| lease.is_held_by(local, &need.workload_id))
                    .map(|lease| lease.resource_id.clone()),
            );
            let candidates = remote_candidates(
                local,
                workload,
                &need.requirement,
                desired,
                observed,
                |node_id: &NodeId| self.is_node_live(node_id, now_ms),
                &exclude,
            );
            for (resource_id, owner) in candidates.into_iter().take(need.missing as usize) {
                let current = match edits.get(&resource_id) {
                    Some(LeaseEdit::Put(lease)) => Some(lease.clone()),
                    Some(LeaseEdit::Remove(_)) => None,
                    None => desired.leases.get(&resource_id).cloned(),
                };
                let holder = LeaseHolder::new(local.clone(), need.workload_id.clone());
                info!(node = %local, workload = %need.workload_id, resource = %resource_id, owner = %owner, "leasing remote resource");
                edits.insert(
                    resource_id.clone(),
                    LeaseEdit::Put(with_holder(&resource_id, current.as_ref(), holder)),
                );
                taken.insert(resource_id);
            }
        }
    }

    /// Commits placement or lease writes as local desired-state writes (validated, stamped,
    /// persisted, watchers notified). Does not reconcile inline; it requests a pass.
    fn commit_cluster_writes(&self, mutations: Vec<DesiredStateMutation>) -> Result<(), NodeError> {
        if mutations.is_empty() {
            return Ok(());
        }
        let mut candidate = self.current_desired_state();
        MutationBatch::new(candidate.revision, mutations.clone()).apply_to(&mut candidate);
        self.validate_desired_state(&candidate)?;
        let previous_revision = self.current_desired_revision();
        let written = self.commit_desired_state_update_if_changed(previous_revision, |txn| {
            let written = txn.apply_local(mutations)?;
            Ok((written, written > 0))
        })?;
        self.record_local_writes(written);
        Ok(())
    }
}
