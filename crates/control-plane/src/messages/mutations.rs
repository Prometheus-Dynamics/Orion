use crate::{
    ArtifactRecord, DesiredClusterState, ExecutorRecord, LeaseRecord, NodeRecord, ProviderRecord,
    ResourceRecord, WorkloadRecord,
};
use orion_core::{ArtifactId, ExecutorId, NodeId, ProviderId, ResourceId, Revision, WorkloadId};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum DesiredStateMutation {
    PutNode(NodeRecord),
    PutArtifact(ArtifactRecord),
    PutWorkload(WorkloadRecord),
    PutResource(ResourceRecord),
    PutProvider(ProviderRecord),
    PutExecutor(ExecutorRecord),
    PutLease(LeaseRecord),
    RemoveNode(NodeId),
    RemoveArtifact(ArtifactId),
    RemoveWorkload(WorkloadId),
    RemoveResource(ResourceId),
    RemoveProvider(ProviderId),
    RemoveExecutor(ExecutorId),
    RemoveLease(ResourceId),
}

#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct MutationBatch {
    pub base_revision: Revision,
    pub mutations: Vec<DesiredStateMutation>,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum MutationApplyError {
    #[error("revision mismatch: expected {expected}, found {found}")]
    RevisionMismatch { expected: Revision, found: Revision },
}

impl MutationBatch {
    pub fn full_state_replay(base_revision: Revision, desired: &DesiredClusterState) -> Self {
        let mut mutations = Vec::new();

        for record in desired.nodes.values().cloned() {
            mutations.push(DesiredStateMutation::PutNode(record));
        }
        for record in desired.artifacts.values().cloned() {
            mutations.push(DesiredStateMutation::PutArtifact(record));
        }
        for record in desired.workloads.values().cloned() {
            mutations.push(DesiredStateMutation::PutWorkload(record));
        }
        for workload_id in desired.workload_tombstones.keys().cloned() {
            mutations.push(DesiredStateMutation::RemoveWorkload(workload_id));
        }
        for record in desired.resources.values().cloned() {
            mutations.push(DesiredStateMutation::PutResource(record));
        }
        for record in desired.providers.values().cloned() {
            mutations.push(DesiredStateMutation::PutProvider(record));
        }
        for record in desired.executors.values().cloned() {
            mutations.push(DesiredStateMutation::PutExecutor(record));
        }
        for record in desired.leases.values().cloned() {
            mutations.push(DesiredStateMutation::PutLease(record));
        }

        Self {
            base_revision,
            mutations,
        }
    }

    pub fn apply_to(self, desired: &mut DesiredClusterState) {
        for mutation in self.mutations {
            match mutation {
                DesiredStateMutation::PutNode(record) => desired.put_node(record),
                DesiredStateMutation::PutArtifact(record) => desired.put_artifact(record),
                DesiredStateMutation::PutWorkload(record) => desired.put_workload(record),
                DesiredStateMutation::PutResource(record) => desired.put_resource(record),
                DesiredStateMutation::PutProvider(record) => desired.put_provider(record),
                DesiredStateMutation::PutExecutor(record) => desired.put_executor(record),
                DesiredStateMutation::PutLease(record) => desired.put_lease(record),
                DesiredStateMutation::RemoveNode(node_id) => {
                    desired.remove_node(&node_id);
                }
                DesiredStateMutation::RemoveArtifact(artifact_id) => {
                    desired.remove_artifact(&artifact_id);
                }
                DesiredStateMutation::RemoveWorkload(workload_id) => {
                    desired.remove_workload(&workload_id);
                }
                DesiredStateMutation::RemoveResource(resource_id) => {
                    desired.remove_resource(&resource_id);
                }
                DesiredStateMutation::RemoveProvider(provider_id) => {
                    desired.remove_provider(&provider_id);
                }
                DesiredStateMutation::RemoveExecutor(executor_id) => {
                    desired.remove_executor(&executor_id);
                }
                DesiredStateMutation::RemoveLease(resource_id) => {
                    desired.remove_lease(&resource_id);
                }
            }
        }
    }

    pub fn apply_to_checked(
        self,
        desired: &mut DesiredClusterState,
    ) -> Result<(), MutationApplyError> {
        if self.base_revision != desired.revision {
            return Err(MutationApplyError::RevisionMismatch {
                expected: self.base_revision,
                found: desired.revision,
            });
        }
        self.apply_to(desired);
        Ok(())
    }
}
