use crate::{
    ExecutorCommand, LocalRuntimeStore, RuntimeError, WorkloadPlan,
    provider::validate_requirement_against_resource,
};
use orion_control_plane::{
    AvailabilityState, DesiredState, ExecutorRecord, ResourceBinding, ResourceRecord,
    WorkloadObservedState, WorkloadRecord,
};
use orion_core::{ExecutorId, NodeId, ResourceId, WorkloadId};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReconcileReport {
    pub local_node_id: NodeId,
    pub desired_revision: orion_core::Revision,
    pub commands: Vec<ExecutorCommand>,
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
            &BTreeMap::new(),
            &BTreeMap::new(),
        )
    }

    fn plan_workload_with_reserved(
        &self,
        workload: &WorkloadRecord,
        executors: &[&ExecutorRecord],
        resources: &[&ResourceRecord],
        reserved_resource_ids: &BTreeMap<ResourceId, u32>,
        released_resource_ids: &BTreeMap<ResourceId, u32>,
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
        let mut resource_bindings = Vec::new();

        for requirement in &workload.requirements {
            for _ in 0..requirement.count {
                let resource = resources
                    .iter()
                    .find(|resource| {
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
                        validate_requirement_against_resource(
                            resource,
                            requirement,
                            existing_claims,
                        )
                        .is_ok()
                    })
                    .ok_or_else(|| {
                        RuntimeError::UnsupportedResourceType(requirement.resource_type.clone())
                    })?;

                resource_bindings.push(ResourceBinding {
                    resource_id: resource.resource_id.clone(),
                    node_id: self.local_node_id.clone(),
                });
                *planned_resource_ids
                    .entry(resource.resource_id.clone())
                    .or_default() += 1;
            }
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

        for workload in store.local_desired_workloads_iter() {
            desired_ids.insert(workload.workload_id.clone());
            let observed = store.observed_workload(&workload.workload_id);

            match workload.desired_state {
                DesiredState::Running => {
                    let released_resource_ids = observed_resource_claim_counts(
                        observed.map(|record| record.resource_bindings.as_slice()),
                    );

                    let plan = match self.plan_workload_with_reserved(
                        workload,
                        &local_executors,
                        &local_resources,
                        &reserved_resource_ids,
                        &released_resource_ids,
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

fn observed_active_resource_claim_counts(store: &LocalRuntimeStore) -> BTreeMap<ResourceId, u32> {
    store
        .local_observed_workloads_iter()
        .filter(|workload| is_active(workload.observed_state))
        .fold(BTreeMap::new(), |mut counts, workload| {
            for binding in &workload.resource_bindings {
                *counts.entry(binding.resource_id.clone()).or_default() += 1;
            }
            counts
        })
}

fn observed_resource_claim_counts(
    resource_bindings: Option<&[ResourceBinding]>,
) -> BTreeMap<ResourceId, u32> {
    let mut counts = BTreeMap::new();

    if let Some(resource_bindings) = resource_bindings {
        for binding in resource_bindings {
            *counts.entry(binding.resource_id.clone()).or_default() += 1;
        }
    }

    counts
}
