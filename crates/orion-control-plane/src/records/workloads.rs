use crate::{DesiredState, RestartPolicy, WorkloadObservedState};
use orion_core::{
    ArtifactId, CapabilityDef, CapabilityId, ConfigSchemaDef, ConfigSchemaId, NodeId, ResourceType,
    ResourceTypeDef, RuntimeType, RuntimeTypeDef, WorkloadId,
};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use super::resources::ResourceOwnershipMode;

#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct WorkloadRequirement {
    pub resource_type: ResourceType,
    pub count: u32,
    pub ownership_mode: Option<ResourceOwnershipMode>,
    pub required_capabilities: Vec<CapabilityId>,
}

impl WorkloadRequirement {
    pub fn new(resource_type: impl Into<ResourceType>, count: u32) -> Self {
        Self {
            resource_type: resource_type.into(),
            count,
            ownership_mode: None,
            required_capabilities: Vec::new(),
        }
    }

    pub fn of<T: ResourceTypeDef>(count: u32) -> Self {
        Self::new(ResourceType::of::<T>(), count)
    }

    pub fn with_ownership_mode(mut self, ownership_mode: ResourceOwnershipMode) -> Self {
        self.ownership_mode = Some(ownership_mode);
        self
    }

    pub fn require_capability(mut self, capability_id: impl Into<CapabilityId>) -> Self {
        self.required_capabilities.push(capability_id.into());
        self
    }

    pub fn require_capability_of<T: CapabilityDef>(mut self) -> Self {
        self.required_capabilities.push(CapabilityId::of::<T>());
        self
    }
}

#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct ResourceBinding {
    pub resource_id: orion_core::ResourceId,
    pub node_id: NodeId,
}

impl ResourceBinding {
    pub fn new(resource_id: impl Into<orion_core::ResourceId>, node_id: impl Into<NodeId>) -> Self {
        Self {
            resource_id: resource_id.into(),
            node_id: node_id.into(),
        }
    }
}

#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum TypedConfigValue {
    Bool(bool),
    Int(i64),
    UInt(u64),
    String(String),
    Bytes(Vec<u8>),
}

impl TypedConfigValue {
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Self::Bool(value) => Some(*value),
            _ => None,
        }
    }

    pub fn as_int(&self) -> Option<i64> {
        match self {
            Self::Int(value) => Some(*value),
            _ => None,
        }
    }

    pub fn as_uint(&self) -> Option<u64> {
        match self {
            Self::UInt(value) => Some(*value),
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::String(value) => Some(value.as_str()),
            _ => None,
        }
    }

    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Self::Bytes(value) => Some(value.as_slice()),
            _ => None,
        }
    }
}

#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct WorkloadConfig {
    pub schema_id: ConfigSchemaId,
    pub payload: BTreeMap<String, TypedConfigValue>,
}

impl WorkloadConfig {
    pub fn new(schema_id: impl Into<ConfigSchemaId>) -> Self {
        Self {
            schema_id: schema_id.into(),
            payload: BTreeMap::new(),
        }
    }

    pub fn of<T: ConfigSchemaDef>() -> Self {
        Self::new(ConfigSchemaId::of::<T>())
    }

    pub fn field(mut self, key: impl Into<String>, value: TypedConfigValue) -> Self {
        self.payload.insert(key.into(), value);
        self
    }

    pub fn value(&self, key: &str) -> Option<&TypedConfigValue> {
        self.payload.get(key)
    }

    pub fn bool(&self, key: &str) -> Option<bool> {
        self.value(key).and_then(TypedConfigValue::as_bool)
    }

    pub fn int(&self, key: &str) -> Option<i64> {
        self.value(key).and_then(TypedConfigValue::as_int)
    }

    pub fn uint(&self, key: &str) -> Option<u64> {
        self.value(key).and_then(TypedConfigValue::as_uint)
    }

    pub fn string(&self, key: &str) -> Option<&str> {
        self.value(key).and_then(TypedConfigValue::as_str)
    }

    pub fn bytes(&self, key: &str) -> Option<&[u8]> {
        self.value(key).and_then(TypedConfigValue::as_bytes)
    }
}

#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct WorkloadRecord {
    pub workload_id: WorkloadId,
    pub runtime_type: RuntimeType,
    pub artifact_id: ArtifactId,
    pub config: Option<WorkloadConfig>,
    pub desired_state: DesiredState,
    pub observed_state: WorkloadObservedState,
    pub assigned_node_id: Option<NodeId>,
    pub requirements: Vec<WorkloadRequirement>,
    pub resource_bindings: Vec<ResourceBinding>,
    pub restart_policy: RestartPolicy,
}

impl WorkloadRecord {
    pub fn builder(
        workload_id: impl Into<WorkloadId>,
        runtime_type: impl Into<RuntimeType>,
        artifact_id: impl Into<ArtifactId>,
    ) -> WorkloadRecordBuilder {
        WorkloadRecordBuilder {
            workload_id: workload_id.into(),
            runtime_type: runtime_type.into(),
            artifact_id: artifact_id.into(),
            config: None,
            desired_state: DesiredState::Stopped,
            observed_state: WorkloadObservedState::Pending,
            assigned_node_id: None,
            requirements: Vec::new(),
            resource_bindings: Vec::new(),
            restart_policy: RestartPolicy::Never,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkloadRecordBuilder {
    workload_id: WorkloadId,
    runtime_type: RuntimeType,
    artifact_id: ArtifactId,
    config: Option<WorkloadConfig>,
    desired_state: DesiredState,
    observed_state: WorkloadObservedState,
    assigned_node_id: Option<NodeId>,
    requirements: Vec<WorkloadRequirement>,
    resource_bindings: Vec<ResourceBinding>,
    restart_policy: RestartPolicy,
}

impl WorkloadRecordBuilder {
    pub fn with_runtime<T: RuntimeTypeDef>(mut self) -> Self {
        self.runtime_type = RuntimeType::of::<T>();
        self
    }

    pub fn desired_state(mut self, desired_state: DesiredState) -> Self {
        self.desired_state = desired_state;
        self
    }

    pub fn config(mut self, config: WorkloadConfig) -> Self {
        self.config = Some(config);
        self
    }

    pub fn observed_state(mut self, observed_state: WorkloadObservedState) -> Self {
        self.observed_state = observed_state;
        self
    }

    pub fn assigned_to(mut self, node_id: impl Into<NodeId>) -> Self {
        self.assigned_node_id = Some(node_id.into());
        self
    }

    pub fn require_resource(mut self, resource_type: impl Into<ResourceType>, count: u32) -> Self {
        self.requirements
            .push(WorkloadRequirement::new(resource_type, count));
        self
    }

    pub fn require_resource_with_ownership(
        mut self,
        resource_type: impl Into<ResourceType>,
        count: u32,
        ownership_mode: ResourceOwnershipMode,
    ) -> Self {
        self.requirements.push(
            WorkloadRequirement::new(resource_type, count).with_ownership_mode(ownership_mode),
        );
        self
    }

    pub fn require_resource_with_capability(
        mut self,
        resource_type: impl Into<ResourceType>,
        count: u32,
        capability_id: impl Into<CapabilityId>,
    ) -> Self {
        self.requirements
            .push(WorkloadRequirement::new(resource_type, count).require_capability(capability_id));
        self
    }

    pub fn require_resource_with_ownership_and_capability(
        mut self,
        resource_type: impl Into<ResourceType>,
        count: u32,
        ownership_mode: ResourceOwnershipMode,
        capability_id: impl Into<CapabilityId>,
    ) -> Self {
        self.requirements.push(
            WorkloadRequirement::new(resource_type, count)
                .with_ownership_mode(ownership_mode)
                .require_capability(capability_id),
        );
        self
    }

    pub fn require<T: ResourceTypeDef>(mut self, count: u32) -> Self {
        self.requirements.push(WorkloadRequirement::of::<T>(count));
        self
    }

    pub fn require_claim(mut self, requirement: WorkloadRequirement) -> Self {
        self.requirements.push(requirement);
        self
    }

    pub fn bind_resource(
        mut self,
        resource_id: impl Into<orion_core::ResourceId>,
        node_id: impl Into<NodeId>,
    ) -> Self {
        self.resource_bindings
            .push(ResourceBinding::new(resource_id, node_id));
        self
    }

    pub fn restart_policy(mut self, restart_policy: RestartPolicy) -> Self {
        self.restart_policy = restart_policy;
        self
    }

    pub fn build(self) -> WorkloadRecord {
        WorkloadRecord {
            workload_id: self.workload_id,
            runtime_type: self.runtime_type,
            artifact_id: self.artifact_id,
            config: self.config,
            desired_state: self.desired_state,
            observed_state: self.observed_state,
            assigned_node_id: self.assigned_node_id,
            requirements: self.requirements,
            resource_bindings: self.resource_bindings,
            restart_policy: self.restart_policy,
        }
    }
}
