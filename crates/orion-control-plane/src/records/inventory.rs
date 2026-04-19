use crate::HealthState;
use orion_core::{
    ArtifactId, ExecutorId, NodeId, ProviderId, ResourceType, ResourceTypeDef, RuntimeType,
    RuntimeTypeDef,
};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};

#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct NodeRecord {
    pub node_id: NodeId,
    pub health: HealthState,
    pub labels: Vec<String>,
}

impl NodeRecord {
    pub fn builder(node_id: impl Into<NodeId>) -> NodeRecordBuilder {
        NodeRecordBuilder {
            node_id: node_id.into(),
            health: HealthState::Unknown,
            labels: Vec::new(),
        }
    }
}

#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct ArtifactRecord {
    pub artifact_id: ArtifactId,
    pub labels: Vec<String>,
    pub content_type: Option<String>,
    pub size_bytes: Option<u64>,
}

impl ArtifactRecord {
    pub fn builder(artifact_id: impl Into<ArtifactId>) -> ArtifactRecordBuilder {
        ArtifactRecordBuilder {
            artifact_id: artifact_id.into(),
            labels: Vec::new(),
            content_type: None,
            size_bytes: None,
        }
    }
}

#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct ProviderRecord {
    pub provider_id: ProviderId,
    pub node_id: NodeId,
    pub resource_types: Vec<ResourceType>,
}

impl ProviderRecord {
    pub fn builder(
        provider_id: impl Into<ProviderId>,
        node_id: impl Into<NodeId>,
    ) -> ProviderRecordBuilder {
        ProviderRecordBuilder {
            provider_id: provider_id.into(),
            node_id: node_id.into(),
            resource_types: Vec::new(),
        }
    }
}

#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct ExecutorRecord {
    pub executor_id: ExecutorId,
    pub node_id: NodeId,
    pub runtime_types: Vec<RuntimeType>,
}

impl ExecutorRecord {
    pub fn builder(
        executor_id: impl Into<ExecutorId>,
        node_id: impl Into<NodeId>,
    ) -> ExecutorRecordBuilder {
        ExecutorRecordBuilder {
            executor_id: executor_id.into(),
            node_id: node_id.into(),
            runtime_types: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeRecordBuilder {
    node_id: NodeId,
    health: HealthState,
    labels: Vec<String>,
}

impl NodeRecordBuilder {
    pub fn health(mut self, health: HealthState) -> Self {
        self.health = health;
        self
    }

    pub fn label(mut self, label: impl Into<String>) -> Self {
        self.labels.push(label.into());
        self
    }

    pub fn build(self) -> NodeRecord {
        NodeRecord {
            node_id: self.node_id,
            health: self.health,
            labels: self.labels,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ArtifactRecordBuilder {
    artifact_id: ArtifactId,
    labels: Vec<String>,
    content_type: Option<String>,
    size_bytes: Option<u64>,
}

impl ArtifactRecordBuilder {
    pub fn label(mut self, label: impl Into<String>) -> Self {
        self.labels.push(label.into());
        self
    }

    pub fn content_type(mut self, content_type: impl Into<String>) -> Self {
        self.content_type = Some(content_type.into());
        self
    }

    pub fn size_bytes(mut self, size_bytes: u64) -> Self {
        self.size_bytes = Some(size_bytes);
        self
    }

    pub fn build(self) -> ArtifactRecord {
        ArtifactRecord {
            artifact_id: self.artifact_id,
            labels: self.labels,
            content_type: self.content_type,
            size_bytes: self.size_bytes,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProviderRecordBuilder {
    provider_id: ProviderId,
    node_id: NodeId,
    resource_types: Vec<ResourceType>,
}

impl ProviderRecordBuilder {
    pub fn resource_type(mut self, resource_type: impl Into<ResourceType>) -> Self {
        self.resource_types.push(resource_type.into());
        self
    }

    pub fn supports<T: ResourceTypeDef>(mut self) -> Self {
        self.resource_types.push(ResourceType::of::<T>());
        self
    }

    pub fn build(self) -> ProviderRecord {
        ProviderRecord {
            provider_id: self.provider_id,
            node_id: self.node_id,
            resource_types: self.resource_types,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExecutorRecordBuilder {
    executor_id: ExecutorId,
    node_id: NodeId,
    runtime_types: Vec<RuntimeType>,
}

impl ExecutorRecordBuilder {
    pub fn runtime_type(mut self, runtime_type: impl Into<RuntimeType>) -> Self {
        self.runtime_types.push(runtime_type.into());
        self
    }

    pub fn supports<T: RuntimeTypeDef>(mut self) -> Self {
        self.runtime_types.push(RuntimeType::of::<T>());
        self
    }

    pub fn build(self) -> ExecutorRecord {
        ExecutorRecord {
            executor_id: self.executor_id,
            node_id: self.node_id,
            runtime_types: self.runtime_types,
        }
    }
}
