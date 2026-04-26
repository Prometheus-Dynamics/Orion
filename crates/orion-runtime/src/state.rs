use crate::{ExecutorSnapshot, ProviderSnapshot, RuntimeError};
use orion_control_plane::{
    AppliedClusterState, DesiredClusterState, ExecutorRecord, MaintenanceMode, MaintenanceState,
    ObservedClusterState, ProviderRecord, ResourceRecord, WorkloadRecord,
};
use orion_core::{ExecutorId, NodeId, ProviderId, ResourceId, Revision, WorkloadId};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RuntimeSnapshot {
    pub local_node_id: NodeId,
    pub desired_revision: Revision,
    pub observed_revision: Revision,
    pub applied_revision: Revision,
    pub local_desired_workloads: usize,
    pub local_observed_workloads: usize,
    pub local_resources: usize,
    pub local_providers: usize,
    pub local_executors: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LocalRuntimeStore {
    pub local_node_id: NodeId,
    pub desired: DesiredClusterState,
    pub observed: ObservedClusterState,
    pub applied: AppliedClusterState,
    maintenance: MaintenanceState,
    provider_sync_revisions: BTreeMap<ProviderId, Revision>,
    executor_sync_revisions: BTreeMap<ExecutorId, Revision>,
}

impl LocalRuntimeStore {
    pub fn new(local_node_id: NodeId) -> Self {
        Self {
            local_node_id,
            desired: DesiredClusterState::default(),
            observed: ObservedClusterState::default(),
            applied: AppliedClusterState::default(),
            maintenance: MaintenanceState::default(),
            provider_sync_revisions: BTreeMap::new(),
            executor_sync_revisions: BTreeMap::new(),
        }
    }

    pub fn maintenance(&self) -> &MaintenanceState {
        &self.maintenance
    }

    pub fn set_maintenance(&mut self, maintenance: MaintenanceState) {
        self.maintenance = maintenance;
    }

    pub fn allows_local_workload(&self, workload: &WorkloadRecord) -> bool {
        match self.maintenance.mode {
            MaintenanceMode::Normal => true,
            MaintenanceMode::Cordoned | MaintenanceMode::Draining => false,
            MaintenanceMode::Maintenance | MaintenanceMode::Isolated => {
                self.maintenance
                    .allow_workload_ids
                    .contains(&workload.workload_id)
                    || self
                        .maintenance
                        .allow_runtime_types
                        .contains(&workload.runtime_type)
            }
        }
    }

    pub fn replace_desired(&mut self, desired: DesiredClusterState) {
        self.desired = desired;
        self.provider_sync_revisions
            .retain(|provider_id, _| self.desired.providers.contains_key(provider_id));
        self.executor_sync_revisions
            .retain(|executor_id, _| self.desired.executors.contains_key(executor_id));
        self.refresh_observed_revision();
    }

    pub fn apply_provider_snapshot(
        &mut self,
        snapshot: ProviderSnapshot,
    ) -> Result<(), RuntimeError> {
        if snapshot.provider.node_id != self.local_node_id {
            return Err(RuntimeError::ProviderNodeMismatch {
                expected: self.local_node_id.clone(),
                found: snapshot.provider.node_id,
            });
        }

        snapshot.validate()?;

        let provider_id = snapshot.provider.provider_id.clone();
        if !self.local_provider_exists(&provider_id) {
            return Err(RuntimeError::UnknownProvider(provider_id));
        }

        self.observed
            .resources
            .retain(|_, resource| resource.provider_id != provider_id);

        for resource in snapshot.resources {
            self.observed.put_resource(resource);
        }

        self.provider_sync_revisions
            .insert(snapshot.provider.provider_id, self.desired.revision);
        self.refresh_observed_revision();

        Ok(())
    }

    pub fn apply_executor_snapshot(
        &mut self,
        snapshot: ExecutorSnapshot,
    ) -> Result<(), RuntimeError> {
        if snapshot.executor.node_id != self.local_node_id {
            return Err(RuntimeError::ExecutorNodeMismatch {
                expected: self.local_node_id.clone(),
                found: snapshot.executor.node_id,
            });
        }

        if !self.local_executor_exists(&snapshot.executor.executor_id) {
            return Err(RuntimeError::UnknownExecutor(snapshot.executor.executor_id));
        }

        let workload_ids: BTreeSet<_> = snapshot
            .workloads
            .iter()
            .map(|workload| workload.workload_id.clone())
            .collect();

        self.observed.workloads.retain(|_, workload| {
            workload.assigned_node_id.as_ref() != Some(&self.local_node_id)
                || !workload_ids.contains(&workload.workload_id)
        });
        self.observed.resources.retain(|_, resource| {
            resource.realized_by_executor_id.as_ref() != Some(&snapshot.executor.executor_id)
        });

        for workload in snapshot.workloads {
            self.observed.put_workload(workload);
        }
        for resource in snapshot.resources {
            if resource.realized_by_executor_id.as_ref() != Some(&snapshot.executor.executor_id) {
                return Err(RuntimeError::InvalidDerivedResource {
                    resource_id: resource.resource_id,
                    reason: "realized resource must declare its realizing executor".into(),
                });
            }
            if resource.source_workload_id.is_none() {
                return Err(RuntimeError::InvalidDerivedResource {
                    resource_id: resource.resource_id,
                    reason: "realized resource must declare its source workload".into(),
                });
            }
            if let Some(realized_for_workload_id) = resource.realized_for_workload_id.as_ref()
                && !workload_ids.contains(realized_for_workload_id)
            {
                return Err(RuntimeError::InvalidDerivedResource {
                    resource_id: resource.resource_id,
                    reason: "realized resource owner must exist in executor snapshot".into(),
                });
            }
            if let Some(source_workload_id) = resource.source_workload_id.as_ref()
                && !workload_ids.contains(source_workload_id)
            {
                return Err(RuntimeError::InvalidDerivedResource {
                    resource_id: resource.resource_id,
                    reason: "realized resource source workload must exist in executor snapshot"
                        .into(),
                });
            }
            self.observed.put_resource(resource);
        }

        self.executor_sync_revisions
            .insert(snapshot.executor.executor_id, self.desired.revision);
        self.refresh_observed_revision();
        Ok(())
    }

    pub fn mark_applied_revision(&mut self, revision: Revision) {
        self.applied.mark_applied(revision);
    }

    fn refresh_observed_revision(&mut self) {
        let desired_revision = self.desired.revision;
        let providers_synced = self.local_providers_iter().all(|provider| {
            self.provider_sync_revisions
                .get(&provider.provider_id)
                .copied()
                == Some(desired_revision)
        });
        let executors_synced = self.local_executors_iter().all(|executor| {
            self.executor_sync_revisions
                .get(&executor.executor_id)
                .copied()
                == Some(desired_revision)
        });

        self.observed.revision = if providers_synced && executors_synced {
            desired_revision
        } else {
            Revision::ZERO
        };
    }

    pub fn local_providers(&self) -> Vec<&ProviderRecord> {
        self.local_providers_iter().collect()
    }

    pub fn local_executors(&self) -> Vec<&ExecutorRecord> {
        self.local_executors_iter().collect()
    }

    pub fn local_resources(&self) -> Vec<ResourceRecord> {
        self.local_resource_refs().into_iter().cloned().collect()
    }

    pub fn local_providers_iter(&self) -> impl Iterator<Item = &ProviderRecord> {
        self.desired
            .providers
            .values()
            .filter(|provider| provider.node_id == self.local_node_id)
    }

    pub fn local_executors_iter(&self) -> impl Iterator<Item = &ExecutorRecord> {
        self.desired
            .executors
            .values()
            .filter(|executor| executor.node_id == self.local_node_id)
    }

    pub fn local_resource_refs(&self) -> Vec<&ResourceRecord> {
        let selectors = self.local_resource_selectors();
        let mut resources = Vec::new();
        self.extend_local_resource_refs(&selectors, &mut resources);
        resources
    }

    fn extend_local_resource_refs<'a>(
        &'a self,
        selectors: &LocalResourceSelectors,
        resources: &mut Vec<&'a ResourceRecord>,
    ) {
        let mut observed_ids = BTreeSet::<ResourceId>::new();
        for resource in self
            .observed
            .resources
            .values()
            .filter(|resource| selectors.includes(resource))
        {
            observed_ids.insert(resource.resource_id.clone());
            resources.push(resource);
        }
        resources.extend(self.desired.resources.values().filter(|resource| {
            selectors.includes(resource) && !observed_ids.contains(&resource.resource_id)
        }));
    }

    fn local_resource_count(&self) -> usize {
        let selectors = self.local_resource_selectors();
        let mut observed_ids = BTreeSet::<ResourceId>::new();
        let observed_count = self
            .observed
            .resources
            .values()
            .filter(|resource| selectors.includes(resource))
            .inspect(|resource| {
                observed_ids.insert(resource.resource_id.clone());
            })
            .count();
        let desired_count = self
            .desired
            .resources
            .values()
            .filter(|resource| {
                selectors.includes(resource) && !observed_ids.contains(&resource.resource_id)
            })
            .count();
        observed_count + desired_count
    }

    fn local_resource_selectors(&self) -> LocalResourceSelectors {
        LocalResourceSelectors {
            provider_ids: self
                .local_providers_iter()
                .map(|provider| provider.provider_id.clone())
                .collect(),
            executor_ids: self
                .local_executors_iter()
                .map(|executor| executor.executor_id.clone())
                .collect(),
        }
    }

    fn local_provider_exists(&self, provider_id: &ProviderId) -> bool {
        self.local_providers_iter()
            .any(|provider| &provider.provider_id == provider_id)
    }

    fn local_executor_exists(&self, executor_id: &ExecutorId) -> bool {
        self.local_executors_iter()
            .any(|executor| &executor.executor_id == executor_id)
    }

    pub fn local_desired_workloads(&self) -> Vec<WorkloadRecord> {
        self.local_desired_workloads_iter().cloned().collect()
    }

    pub fn local_desired_workloads_iter(&self) -> impl Iterator<Item = &WorkloadRecord> {
        self.desired
            .workloads
            .values()
            .filter(|workload| workload.assigned_node_id.as_ref() == Some(&self.local_node_id))
            .filter(|workload| self.allows_local_workload(workload))
    }

    pub fn local_observed_workloads(&self) -> Vec<WorkloadRecord> {
        self.local_observed_workloads_iter().cloned().collect()
    }

    pub fn local_observed_workloads_iter(&self) -> impl Iterator<Item = &WorkloadRecord> {
        self.observed
            .workloads
            .values()
            .filter(|workload| workload.assigned_node_id.as_ref() == Some(&self.local_node_id))
    }

    pub fn observed_workload(&self, workload_id: &WorkloadId) -> Option<&WorkloadRecord> {
        self.observed.workloads.get(workload_id)
    }

    pub fn snapshot(&self) -> RuntimeSnapshot {
        RuntimeSnapshot {
            local_node_id: self.local_node_id.clone(),
            desired_revision: self.desired.revision,
            observed_revision: self.observed.revision,
            applied_revision: self.applied.revision,
            local_desired_workloads: self.local_desired_workloads_iter().count(),
            local_observed_workloads: self.local_observed_workloads_iter().count(),
            local_resources: self.local_resource_count(),
            local_providers: self.local_providers_iter().count(),
            local_executors: self.local_executors_iter().count(),
        }
    }
}

#[derive(Clone, Debug, Default)]
struct LocalResourceSelectors {
    provider_ids: BTreeSet<ProviderId>,
    executor_ids: BTreeSet<ExecutorId>,
}

impl LocalResourceSelectors {
    fn includes(&self, resource: &ResourceRecord) -> bool {
        self.provider_ids.contains(&resource.provider_id)
            || resource
                .realized_by_executor_id
                .as_ref()
                .is_some_and(|executor_id| self.executor_ids.contains(executor_id))
    }
}
