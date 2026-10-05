use crate::{
    ExecutorCommand, LocalRuntimeStore, RuntimeError, WorkloadPlan,
    provider::validate_requirement_against_resource,
    state::{RemoteLease, remote_lease_matches},
};
use alloc::collections::{BTreeMap, BTreeSet};
use alloc::vec::Vec;
use orion_control_plane::{
    AvailabilityState, DesiredState, ExecutorRecord, ResourceBinding, ResourceRecord,
    WorkloadObservedState, WorkloadRecord, WorkloadRequirement,
};
use orion_core::{ExecutorId, NodeId, ResourceId, WorkloadId};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReconcileReport {
    pub local_node_id: NodeId,
    pub desired_revision: orion_core::Revision,
    pub commands: Vec<ExecutorCommand>,
    /// Requirements of local running workloads that neither local resources nor held
    /// cross-node leases satisfy. The node resolves them against remote resources.
    pub unsatisfied: Vec<UnsatisfiedRequirement>,
}

/// `missing` units of `requirement` of `workload_id` could not be bound.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UnsatisfiedRequirement {
    pub workload_id: WorkloadId,
    pub requirement: WorkloadRequirement,
    pub missing: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Runtime {
    pub local_node_id: NodeId,
}

impl Runtime {
    pub fn new(local_node_id: NodeId) -> Self {
        Self { local_node_id }
    }

    pub fn plan_workload(
        &self,
        workload: &WorkloadRecord,
        executors: &[&ExecutorRecord],
        resources: &[&ResourceRecord],
    ) -> Result<Option<WorkloadPlan>, RuntimeError> {
        self.plan_workload_with_reserved(
            workload,
            executors,
            resources,
            &[],
            &BTreeMap::new(),
            &BTreeMap::new(),
            &mut Vec::new(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn plan_workload_with_reserved(
        &self,
        workload: &WorkloadRecord,
        executors: &[&ExecutorRecord],
        resources: &[&ResourceRecord],
        remote: &[RemoteLease<'_>],
        reserved_resource_ids: &BTreeMap<ResourceId, u32>,
        released_resource_ids: &BTreeMap<ResourceId, u32>,
        unsatisfied: &mut Vec<UnsatisfiedRequirement>,
    ) -> Result<Option<WorkloadPlan>, RuntimeError> {
        if workload.desired_state != DesiredState::Running {
            return Ok(None);
        }

        let Some(assigned_node_id) = workload.assigned_node_id.as_ref() else {
            return Err(RuntimeError::UnassignedWorkload(
                workload.workload_id.clone(),
            ));
        };

        if assigned_node_id != &self.local_node_id {
            return Ok(None);
        }

        let executor = executors
            .iter()
            .find(|executor| executor.runtime_types.contains(&workload.runtime_type))
            .ok_or_else(|| RuntimeError::UnsupportedRuntimeType(workload.runtime_type.clone()))?;

        let mut planned_resource_ids = BTreeMap::<ResourceId, u32>::new();
        let mut used_remote = BTreeSet::<ResourceId>::new();
        let mut resource_bindings = Vec::new();
        let mut first_missing = None;

        for requirement in &workload.requirements {
            let mut missing = 0_u32;
            for _ in 0..requirement.count {
                let local = resources.iter().find(|resource| {
                    if resource.resource_type != requirement.resource_type
                        || resource.availability != AvailabilityState::Available
                    {
                        return false;
                    }

                    let existing_claims = reserved_claim_count(
                        &resource.resource_id,
                        reserved_resource_ids,
                        released_resource_ids,
                        &planned_resource_ids,
                    );
                    validate_requirement_against_resource(resource, requirement, existing_claims)
                        .is_ok()
                });
                if let Some(resource) = local {
                    resource_bindings.push(ResourceBinding::new(
                        resource.resource_id.clone(),
                        self.local_node_id.clone(),
                    ));
                    *planned_resource_ids
                        .entry(resource.resource_id.clone())
                        .or_default() += 1;
                    continue;
                }
                // Cross-node binding: a remote resource this workload holds a lease on.
                let leased = remote.iter().find(|lease| {
                    !used_remote.contains(&lease.resource.resource_id)
                        && remote_lease_matches(lease.resource, requirement)
                });
                if let Some(lease) = leased {
                    used_remote.insert(lease.resource.resource_id.clone());
                    resource_bindings.push(ResourceBinding::remote(
                        lease.resource.resource_id.clone(),
                        lease.owner.clone(),
                        lease.resource.endpoints.clone(),
                        lease.available,
                    ));
                    continue;
                }
                missing += 1;
            }
            if missing > 0 {
                first_missing.get_or_insert_with(|| requirement.resource_type.clone());
                unsatisfied.push(UnsatisfiedRequirement {
                    workload_id: workload.workload_id.clone(),
                    requirement: requirement.clone(),
                    missing,
                });
            }
        }

        if let Some(resource_type) = first_missing {
            return Err(RuntimeError::UnsupportedResourceType(resource_type));
        }

        Ok(Some(WorkloadPlan {
            executor_id: executor.executor_id.clone(),
            workload: workload.clone(),
            resource_bindings,
        }))
    }

    pub fn reconcile(&self, store: &LocalRuntimeStore) -> Result<ReconcileReport, RuntimeError> {
        let local_executors = store.local_executors();
        let local_resources = store.local_resource_refs();
        let mut commands = Vec::new();
        let mut desired_ids = BTreeSet::<WorkloadId>::new();
        let mut reserved_resource_ids = observed_active_resource_claim_counts(store);
        let mut unsatisfied = Vec::new();

        for workload in store.local_desired_workloads_iter() {
            desired_ids.insert(workload.workload_id.clone());
            // Only this node's own report counts: after a failover the previous assignee's
            // report of the same workload may still be in observed state.
            let observed = store
                .observed_workload(&workload.workload_id)
                .filter(|observed| observed.assigned_node_id.as_ref() == Some(&self.local_node_id));

            match workload.desired_state {
                DesiredState::Running => {
                    let released_resource_ids = observed_resource_claim_counts(
                        observed.map(|record| record.resource_bindings.as_slice()),
                    );

                    let remote = store.remote_leases_for(&workload.workload_id);
                    let plan = match self.plan_workload_with_reserved(
                        workload,
                        &local_executors,
                        &local_resources,
                        &remote,
                        &reserved_resource_ids,
                        &released_resource_ids,
                        &mut unsatisfied,
                    ) {
                        Ok(plan) => plan,
                        Err(RuntimeError::UnsupportedResourceType(_)) => None,
                        Err(err) => return Err(err),
                    };

                    if let Some(plan) = plan {
                        let needs_start = observed
                            .map(|observed| {
                                observed.observed_state != WorkloadObservedState::Running
                                    || observed.resource_bindings != plan.resource_bindings
                            })
                            .unwrap_or(true);

                        if needs_start {
                            for binding in &plan.resource_bindings {
                                *reserved_resource_ids
                                    .entry(binding.resource_id.clone())
                                    .or_default() += 1;
                            }
                            commands.push(ExecutorCommand::Start(plan));
                        }
                    } else if let Some(observed) = observed
                        && is_active(observed.observed_state)
                        && lost_remote_binding(observed, &remote)
                    {
                        // A cross-node lease this workload ran with is gone (released after the
                        // owner disappeared, or lost to a competing holder): the workload must
                        // stop using the resource. It starts again once it is bound again.
                        commands.push(ExecutorCommand::Stop {
                            executor_id: self
                                .select_executor_id(&observed.runtime_type, &local_executors)?,
                            workload_id: observed.workload_id.clone(),
                        });
                    }
                }
                DesiredState::Stopped => {
                    if let Some(observed) = observed
                        && is_active(observed.observed_state)
                    {
                        commands.push(ExecutorCommand::Stop {
                            executor_id: self
                                .select_executor_id(&observed.runtime_type, &local_executors)?,
                            workload_id: observed.workload_id.clone(),
                        });
                    }
                }
            }
        }

        for observed in store.local_observed_workloads_iter() {
            if desired_ids.contains(&observed.workload_id) {
                continue;
            }

            if is_active(observed.observed_state) {
                commands.push(ExecutorCommand::Stop {
                    executor_id: self
                        .select_executor_id(&observed.runtime_type, &local_executors)?,
                    workload_id: observed.workload_id.clone(),
                });
            }
        }

        Ok(ReconcileReport {
            local_node_id: self.local_node_id.clone(),
            desired_revision: store.desired.revision,
            commands,
            unsatisfied,
        })
    }

    fn select_executor_id(
        &self,
        runtime_type: &orion_core::RuntimeType,
        executors: &[&ExecutorRecord],
    ) -> Result<ExecutorId, RuntimeError> {
        executors
            .iter()
            .find(|executor| executor.runtime_types.contains(runtime_type))
            .map(|executor| executor.executor_id.clone())
            .ok_or_else(|| RuntimeError::UnsupportedRuntimeType(runtime_type.clone()))
    }
}

fn reserved_claim_count(
    resource_id: &ResourceId,
    reserved_resource_ids: &BTreeMap<ResourceId, u32>,
    released_resource_ids: &BTreeMap<ResourceId, u32>,
    planned_resource_ids: &BTreeMap<ResourceId, u32>,
) -> u32 {
    reserved_resource_ids
        .get(resource_id)
        .copied()
        .unwrap_or(0)
        .saturating_sub(released_resource_ids.get(resource_id).copied().unwrap_or(0))
        .saturating_add(planned_resource_ids.get(resource_id).copied().unwrap_or(0))
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

/// Claims on resources: bindings of active local workloads plus cross-node lease holders on
/// other nodes (their claims on local resources count against local capacity).
fn observed_active_resource_claim_counts(store: &LocalRuntimeStore) -> BTreeMap<ResourceId, u32> {
    let mut counts = store
        .local_observed_workloads_iter()
        .filter(|workload| is_active(workload.observed_state))
        .fold(BTreeMap::new(), |mut counts, workload| {
            for binding in workload.resource_bindings.iter().filter(|b| !b.is_remote()) {
                *counts.entry(binding.resource_id.clone()).or_default() += 1;
            }
            counts
        });
    for lease in store.desired.leases.values() {
        let remote_holders = lease
            .holders
            .iter()
            .filter(|holder| holder.node_id != store.local_node_id)
            .count();
        if remote_holders > 0 {
            *counts.entry(lease.resource_id.clone()).or_default() +=
                u32::try_from(remote_holders).unwrap_or(u32::MAX);
        }
    }
    counts
}

fn observed_resource_claim_counts(
    resource_bindings: Option<&[ResourceBinding]>,
) -> BTreeMap<ResourceId, u32> {
    let mut counts = BTreeMap::new();

    if let Some(resource_bindings) = resource_bindings {
        for binding in resource_bindings
            .iter()
            .filter(|binding| !binding.is_remote())
        {
            *counts.entry(binding.resource_id.clone()).or_default() += 1;
        }
    }

    counts
}

/// Whether `observed` runs with a cross-node binding the workload no longer holds a lease for.
fn lost_remote_binding(observed: &WorkloadRecord, held: &[RemoteLease<'_>]) -> bool {
    observed.resource_bindings.iter().any(|binding| {
        binding.is_remote()
            && !held
                .iter()
                .any(|lease| lease.resource.resource_id == binding.resource_id)
    })
}
