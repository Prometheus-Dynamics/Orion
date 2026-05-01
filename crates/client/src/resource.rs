use orion_control_plane::{
    AvailabilityState, HealthState, LeaseState, ResourceCapability, ResourceOwnershipMode,
    ResourceRecord, ResourceState, WorkloadRequirement,
};
use orion_core::{
    CapabilityDef, CapabilityId, ExecutorId, ProviderId, ResourceId, ResourceType, WorkloadId,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResourceClaim {
    requirement: WorkloadRequirement,
}

impl ResourceClaim {
    pub fn new(resource_type: impl Into<ResourceType>, count: u32) -> Self {
        Self {
            requirement: WorkloadRequirement::new(resource_type, count),
        }
    }

    pub fn ownership_mode(mut self, ownership_mode: ResourceOwnershipMode) -> Self {
        self.requirement = self.requirement.with_ownership_mode(ownership_mode);
        self
    }

    pub fn requires_capability(mut self, capability_id: impl Into<CapabilityId>) -> Self {
        self.requirement = self.requirement.require_capability(capability_id);
        self
    }

    pub fn requires_capability_of<T: CapabilityDef>(mut self) -> Self {
        self.requirement = self.requirement.require_capability_of::<T>();
        self
    }

    pub fn build(self) -> WorkloadRequirement {
        self.requirement
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProviderResource {
    record: ResourceRecord,
}

impl ProviderResource {
    pub fn new(
        resource_id: impl Into<ResourceId>,
        resource_type: impl Into<ResourceType>,
        provider_id: impl Into<ProviderId>,
    ) -> Self {
        Self {
            record: ResourceRecord::builder(resource_id, resource_type, provider_id).build(),
        }
    }

    pub fn ownership_mode(mut self, ownership_mode: ResourceOwnershipMode) -> Self {
        self.record.ownership_mode = ownership_mode;
        self
    }

    pub fn capability(mut self, capability: ResourceCapability) -> Self {
        self.record.capabilities.push(capability);
        self
    }

    pub fn supports_capability(mut self, capability_id: impl Into<CapabilityId>) -> Self {
        self.record
            .capabilities
            .push(ResourceCapability::new(capability_id));
        self
    }

    pub fn supports_capability_of<T: CapabilityDef>(mut self) -> Self {
        self.record.capabilities.push(ResourceCapability::of::<T>());
        self
    }

    pub fn health(mut self, health: HealthState) -> Self {
        self.record.health = health;
        self
    }

    pub fn availability(mut self, availability: AvailabilityState) -> Self {
        self.record.availability = availability;
        self
    }

    pub fn lease_state(mut self, lease_state: LeaseState) -> Self {
        self.record.lease_state = lease_state;
        self
    }

    pub fn label(mut self, label: impl Into<String>) -> Self {
        self.record.labels.push(label.into());
        self
    }

    pub fn endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.record.endpoints.push(endpoint.into());
        self
    }

    pub fn state(mut self, state: ResourceState) -> Self {
        self.record.state = Some(state);
        self
    }

    pub fn build(self) -> ResourceRecord {
        self.record
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DerivedResource {
    record: ResourceRecord,
}

impl DerivedResource {
    pub fn new(
        resource_id: impl Into<ResourceId>,
        resource_type: impl Into<ResourceType>,
        provider_id: impl Into<ProviderId>,
    ) -> Self {
        Self {
            record: ResourceRecord::builder(resource_id, resource_type, provider_id).build(),
        }
    }

    pub fn realized_by_executor(mut self, executor_id: impl Into<ExecutorId>) -> Self {
        self.record.realized_by_executor_id = Some(executor_id.into());
        self
    }

    pub fn realized_for_workload(mut self, workload_id: impl Into<WorkloadId>) -> Self {
        self.record.realized_for_workload_id = Some(workload_id.into());
        self
    }

    pub fn source_resource(mut self, resource_id: impl Into<ResourceId>) -> Self {
        self.record.source_resource_id = Some(resource_id.into());
        self
    }

    pub fn source_workload(mut self, workload_id: impl Into<WorkloadId>) -> Self {
        self.record.source_workload_id = Some(workload_id.into());
        self
    }

    pub fn ownership_mode(mut self, ownership_mode: ResourceOwnershipMode) -> Self {
        self.record.ownership_mode = ownership_mode;
        self
    }

    pub fn capability(mut self, capability: ResourceCapability) -> Self {
        self.record.capabilities.push(capability);
        self
    }

    pub fn supports_capability(mut self, capability_id: impl Into<CapabilityId>) -> Self {
        self.record
            .capabilities
            .push(ResourceCapability::new(capability_id));
        self
    }

    pub fn supports_capability_of<T: CapabilityDef>(mut self) -> Self {
        self.record.capabilities.push(ResourceCapability::of::<T>());
        self
    }

    pub fn health(mut self, health: HealthState) -> Self {
        self.record.health = health;
        self
    }

    pub fn availability(mut self, availability: AvailabilityState) -> Self {
        self.record.availability = availability;
        self
    }

    pub fn lease_state(mut self, lease_state: LeaseState) -> Self {
        self.record.lease_state = lease_state;
        self
    }

    pub fn label(mut self, label: impl Into<String>) -> Self {
        self.record.labels.push(label.into());
        self
    }

    pub fn endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.record.endpoints.push(endpoint.into());
        self
    }

    pub fn state(mut self, state: ResourceState) -> Self {
        self.record.state = Some(state);
        self
    }

    pub fn build(self) -> ResourceRecord {
        self.record
    }
}
