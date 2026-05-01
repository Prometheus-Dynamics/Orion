use crate::{ResourceId, ResourceType, RuntimeType, WorkloadId};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum OrionError {
    #[error("invalid {type_name}: {value:?}")]
    InvalidValue {
        type_name: &'static str,
        value: String,
    },
    #[error("serialization failed: {0}")]
    Serialization(String),
    #[error("unassigned workload: {0}")]
    UnassignedWorkload(WorkloadId),
    #[error("unsupported runtime type: {0}")]
    UnsupportedRuntimeType(RuntimeType),
    #[error("unsupported resource type: {0}")]
    UnsupportedResourceType(ResourceType),
    #[error("resource not found: {0}")]
    ResourceNotFound(ResourceId),
}
