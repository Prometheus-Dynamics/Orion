use super::{
    ArtifactRecord, ExecutorRecord, LeaseRecord, NodeRecord, ProviderRecord, ResourceRecord,
    WorkloadRecord,
};
use orion_core::{ArtifactId, ExecutorId, NodeId, ProviderId, ResourceId, Revision, WorkloadId};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(
    Clone,
    Debug,
    Default,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub struct DesiredClusterState {
    pub revision: Revision,
    pub nodes: BTreeMap<NodeId, NodeRecord>,
    pub artifacts: BTreeMap<ArtifactId, ArtifactRecord>,
    pub workloads: BTreeMap<WorkloadId, WorkloadRecord>,
    pub workload_tombstones: BTreeMap<WorkloadId, Revision>,
    pub resources: BTreeMap<ResourceId, ResourceRecord>,
    pub providers: BTreeMap<ProviderId, ProviderRecord>,
    pub executors: BTreeMap<ExecutorId, ExecutorRecord>,
    pub leases: BTreeMap<ResourceId, LeaseRecord>,
}

impl DesiredClusterState {
    pub fn bump_revision(&mut self) {
        self.revision = self.revision.next();
    }

    pub fn put_node(&mut self, record: NodeRecord) {
        self.nodes.insert(record.node_id.clone(), record);
        self.bump_revision();
    }

    pub fn put_artifact(&mut self, record: ArtifactRecord) {
        self.artifacts.insert(record.artifact_id.clone(), record);
        self.bump_revision();
    }

    pub fn put_workload(&mut self, record: WorkloadRecord) {
        self.workload_tombstones.remove(&record.workload_id);
        self.workloads.insert(record.workload_id.clone(), record);
        self.bump_revision();
    }

    pub fn put_resource(&mut self, record: ResourceRecord) {
        self.resources.insert(record.resource_id.clone(), record);
        self.bump_revision();
    }

    pub fn put_provider(&mut self, record: ProviderRecord) {
        self.providers.insert(record.provider_id.clone(), record);
        self.bump_revision();
    }

    pub fn put_executor(&mut self, record: ExecutorRecord) {
        self.executors.insert(record.executor_id.clone(), record);
        self.bump_revision();
    }

    pub fn put_lease(&mut self, record: LeaseRecord) {
        self.leases.insert(record.resource_id.clone(), record);
        self.bump_revision();
    }

    pub fn remove_node(&mut self, node_id: &NodeId) -> Option<NodeRecord> {
        let removed = self.nodes.remove(node_id);
        if removed.is_some() {
            self.bump_revision();
        }
        removed
    }

    pub fn remove_artifact(&mut self, artifact_id: &ArtifactId) -> Option<ArtifactRecord> {
        let removed = self.artifacts.remove(artifact_id);
        if removed.is_some() {
            self.bump_revision();
        }
        removed
    }

    pub fn remove_workload(&mut self, workload_id: &WorkloadId) -> Option<WorkloadRecord> {
        let removed = self.workloads.remove(workload_id);
        if removed.is_some() || !self.workload_tombstones.contains_key(workload_id) {
            let next_revision = self.revision.next();
            self.revision = next_revision;
            self.workload_tombstones
                .insert(workload_id.clone(), next_revision);
        }
        removed
    }

    pub fn remove_resource(&mut self, resource_id: &ResourceId) -> Option<ResourceRecord> {
        let removed = self.resources.remove(resource_id);
        if removed.is_some() {
            self.bump_revision();
        }
        removed
    }

    pub fn remove_provider(&mut self, provider_id: &ProviderId) -> Option<ProviderRecord> {
        let removed = self.providers.remove(provider_id);
        if removed.is_some() {
            self.bump_revision();
        }
        removed
    }

    pub fn remove_executor(&mut self, executor_id: &ExecutorId) -> Option<ExecutorRecord> {
        let removed = self.executors.remove(executor_id);
        if removed.is_some() {
            self.bump_revision();
        }
        removed
    }

    pub fn remove_lease(&mut self, resource_id: &ResourceId) -> Option<LeaseRecord> {
        let removed = self.leases.remove(resource_id);
        if removed.is_some() {
            self.bump_revision();
        }
        removed
    }
}

#[derive(
    Clone,
    Debug,
    Default,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub struct ObservedClusterState {
    pub revision: Revision,
    pub nodes: BTreeMap<NodeId, NodeRecord>,
    pub workloads: BTreeMap<WorkloadId, WorkloadRecord>,
    pub resources: BTreeMap<ResourceId, ResourceRecord>,
    pub leases: BTreeMap<ResourceId, LeaseRecord>,
}

impl ObservedClusterState {
    pub fn set_revision(&mut self, revision: Revision) {
        self.revision = revision;
    }

    pub fn put_node(&mut self, record: NodeRecord) {
        self.nodes.insert(record.node_id.clone(), record);
    }

    pub fn put_workload(&mut self, record: WorkloadRecord) {
        self.workloads.insert(record.workload_id.clone(), record);
    }

    pub fn put_resource(&mut self, record: ResourceRecord) {
        self.resources.insert(record.resource_id.clone(), record);
    }

    pub fn put_lease(&mut self, record: LeaseRecord) {
        self.leases.insert(record.resource_id.clone(), record);
    }
}

#[derive(
    Clone,
    Debug,
    Default,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub struct AppliedClusterState {
    pub revision: Revision,
}

impl AppliedClusterState {
    pub fn mark_applied(&mut self, revision: Revision) {
        self.revision = revision;
    }
}

#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct ClusterStateEnvelope {
    pub desired: DesiredClusterState,
    pub observed: ObservedClusterState,
    pub applied: AppliedClusterState,
}

impl ClusterStateEnvelope {
    pub fn new(
        desired: DesiredClusterState,
        observed: ObservedClusterState,
        applied: AppliedClusterState,
    ) -> Self {
        Self {
            desired,
            observed,
            applied,
        }
    }
}
