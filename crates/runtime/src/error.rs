use orion_control_plane::ResourceOwnershipMode;
use orion_core::{
    CapabilityId, ConfigSchemaId, ExecutorId, NodeId, ProviderId, ResourceId, ResourceType,
    RuntimeType, WorkloadId,
};
use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum RuntimeError {
    #[error("workload {0} is not assigned to a node")]
    UnassignedWorkload(WorkloadId),
    #[error("no local executor supports runtime type {0}")]
    UnsupportedRuntimeType(RuntimeType),
    #[error("no available local resource satisfies resource type {0}")]
    UnsupportedResourceType(ResourceType),
    #[error(
        "workload {workload_id} does not support config schema {schema_id} for runtime type {runtime_type}"
    )]
    UnsupportedWorkloadConfigSchema {
        workload_id: WorkloadId,
        runtime_type: RuntimeType,
        schema_id: ConfigSchemaId,
    },
    #[error("workload {workload_id} has invalid config: {reason}")]
    InvalidWorkloadConfig {
        workload_id: WorkloadId,
        reason: String,
    },
    #[error(
        "resource {resource_id} ownership mode mismatch: expected {expected:?}, found {found:?}"
    )]
    OwnershipModeMismatch {
        resource_id: ResourceId,
        expected: ResourceOwnershipMode,
        found: ResourceOwnershipMode,
    },
    #[error(
        "resource {resource_id} does not allow additional consumers under ownership mode {mode:?}"
    )]
    ResourceOwnershipConflict {
        resource_id: ResourceId,
        mode: ResourceOwnershipMode,
    },
    #[error("resource {resource_id} reached shared consumer limit {max_consumers}")]
    ResourceConsumerLimitExceeded {
        resource_id: ResourceId,
        max_consumers: u32,
    },
    #[error("resource {resource_id} is missing required capability {capability_id}")]
    MissingResourceCapability {
        resource_id: ResourceId,
        capability_id: CapabilityId,
    },
    #[error("derived resource {resource_id} is invalid: {reason}")]
    InvalidDerivedResource {
        resource_id: ResourceId,
        reason: String,
    },
    #[error("provider snapshot belongs to node {found}, not local node {expected}")]
    ProviderNodeMismatch { expected: NodeId, found: NodeId },
    #[error("resource {resource_id} does not belong to provider {provider_id}")]
    ProviderResourceMismatch {
        provider_id: ProviderId,
        resource_id: ResourceId,
    },
    #[error("provider {0} is not part of desired local state")]
    UnknownProvider(ProviderId),
    #[error("executor snapshot belongs to node {found}, not local node {expected}")]
    ExecutorNodeMismatch { expected: NodeId, found: NodeId },
    #[error("executor {0} is not part of desired local state")]
    UnknownExecutor(ExecutorId),
}
