use crate::{
    ArtifactRecord, DesiredClusterState, ExecutorRecord, LeaseRecord, NodeRecord, ProviderRecord,
    ResourceRecord, WorkloadRecord,
};
use alloc::vec::Vec;
use orion_core::{
    ArtifactId, ExecutorId, HlcTimestamp, NodeId, ProviderId, ResourceId, Revision, WorkloadId,
};
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

/// An ordered list of desired-state writes.
///
/// `stamps` is either empty (an unstamped batch, for example from `orionctl`: the node checks
/// `base_revision` and stamps every mutation with its own clock when it commits the batch) or has
/// exactly one HLC timestamp per mutation (a batch replicated from a peer or replayed from the
/// mutation history, merged per object with [`DesiredClusterState::apply_stamped`]).
#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct MutationBatch {
    pub base_revision: Revision,
    pub mutations: Vec<DesiredStateMutation>,
    /// One timestamp per mutation, or empty for an unstamped batch.
    #[serde(default)]
    pub stamps: Vec<HlcTimestamp>,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum MutationApplyError {
    #[error("revision mismatch: expected {expected}, found {found}")]
    RevisionMismatch { expected: Revision, found: Revision },
    #[error("mutation batch has {stamps} stamps for {mutations} mutations")]
    StampCountMismatch { mutations: usize, stamps: usize },
}

impl MutationBatch {
    /// An unstamped batch.
    pub fn new(base_revision: Revision, mutations: Vec<DesiredStateMutation>) -> Self {
        Self {
            base_revision,
            mutations,
            stamps: Vec::new(),
        }
    }

    /// A batch with one timestamp per mutation.
    pub fn stamped(
        base_revision: Revision,
        versions: impl IntoIterator<Item = (DesiredStateMutation, HlcTimestamp)>,
    ) -> Self {
        let (mutations, stamps) = versions.into_iter().unzip();
        Self {
            base_revision,
            mutations,
            stamps,
        }
    }

    /// `true` when every mutation carries a timestamp (an empty batch counts as stamped).
    pub fn is_stamped(&self) -> bool {
        self.stamps.len() == self.mutations.len()
    }

    /// Checks that `stamps` is empty or matches `mutations` one to one.
    pub fn check_stamps(&self) -> Result<(), MutationApplyError> {
        if self.stamps.is_empty() || self.is_stamped() {
            Ok(())
        } else {
            Err(MutationApplyError::StampCountMismatch {
                mutations: self.mutations.len(),
                stamps: self.stamps.len(),
            })
        }
    }

    /// Pairs every mutation with its timestamp. Only meaningful for stamped batches.
    pub fn versions(&self) -> impl Iterator<Item = (&DesiredStateMutation, HlcTimestamp)> {
        self.mutations.iter().zip(self.stamps.iter().copied())
    }

    /// Every object and tombstone of `desired` as one batch (stamped with the state's versions).
    pub fn full_state_replay(base_revision: Revision, desired: &DesiredClusterState) -> Self {
        Self {
            base_revision,
            ..desired.stamped_batch()
        }
    }

    /// Applies the batch in order.
    ///
    /// A stamped batch is replayed exactly: every mutation is written with its timestamp and
    /// advances the revision by one (see [`DesiredClusterState::force_stamped`]). An unstamped
    /// batch uses the plain `put_*`/`remove_*` helpers and leaves versions untouched.
    pub fn apply_to(self, desired: &mut DesiredClusterState) {
        if !self.mutations.is_empty() && self.is_stamped() {
            for (mutation, stamp) in self.mutations.into_iter().zip(self.stamps) {
                desired.force_stamped(mutation, stamp);
            }
            return;
        }
        for mutation in self.mutations {
            apply_unstamped(desired, mutation);
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
        self.check_stamps()?;
        self.apply_to(desired);
        Ok(())
    }
}

fn apply_unstamped(desired: &mut DesiredClusterState, mutation: DesiredStateMutation) {
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
