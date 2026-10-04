use crate::{ClusterStateEnvelope, DesiredObjectStamps};
use alloc::collections::BTreeMap;
use alloc::vec::Vec;
use orion_core::{ArtifactId, ExecutorId, NodeId, ProviderId, ResourceId, Revision, WorkloadId};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};

#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct PeerHello {
    pub node_id: NodeId,
    pub desired_revision: Revision,
    pub desired_fingerprint: u64,
    pub desired_section_fingerprints: DesiredStateSectionFingerprints,
    pub observed_revision: Revision,
    pub applied_revision: Revision,
    pub transport_binding_version: Option<u16>,
    pub transport_binding_public_key: Option<Vec<u8>>,
    pub transport_tls_cert_pem: Option<Vec<u8>>,
    pub transport_binding_signature: Option<Vec<u8>>,
}

#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct StateSnapshot {
    pub state: ClusterStateEnvelope,
}

#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct SyncRequest {
    pub node_id: NodeId,
    pub desired_revision: Revision,
    pub desired_fingerprint: u64,
    pub desired_summary: Option<DesiredStateSummary>,
    pub sections: Vec<DesiredStateSection>,
    pub object_selectors: Vec<DesiredStateObjectSelector>,
}

#[derive(
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub enum DesiredStateSection {
    Nodes,
    Artifacts,
    Workloads,
    Resources,
    Providers,
    Executors,
    Leases,
}

#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum DesiredStateObjectSelector {
    Nodes(Vec<NodeId>),
    Artifacts(Vec<ArtifactId>),
    Workloads(Vec<WorkloadId>),
    Resources(Vec<ResourceId>),
    Providers(Vec<ProviderId>),
    Executors(Vec<ExecutorId>),
    Leases(Vec<ResourceId>),
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
pub struct DesiredStateSectionFingerprints {
    pub nodes: u64,
    pub artifacts: u64,
    pub workloads: u64,
    pub resources: u64,
    pub providers: u64,
    pub executors: u64,
    pub leases: u64,
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
pub struct DesiredStateSummary {
    pub revision: Revision,
    pub section_fingerprints: DesiredStateSectionFingerprints,
    pub nodes: BTreeMap<NodeId, u64>,
    pub artifacts: BTreeMap<ArtifactId, u64>,
    pub workloads: BTreeMap<WorkloadId, u64>,
    pub resources: BTreeMap<ResourceId, u64>,
    pub providers: BTreeMap<ProviderId, u64>,
    pub executors: BTreeMap<ExecutorId, u64>,
    pub leases: BTreeMap<ResourceId, u64>,
    /// HLC stamps of the summarized live objects.
    pub stamps: DesiredObjectStamps,
    /// HLC stamps of the summarized sections' tombstones.
    pub tombstones: DesiredObjectStamps,
}

#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct SyncSummaryRequest {
    pub node_id: NodeId,
    pub sections: Vec<DesiredStateSection>,
}

#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct SyncDiffRequest {
    pub node_id: NodeId,
    pub desired_revision: Revision,
    pub desired_summary: DesiredStateSummary,
    pub sections: Vec<DesiredStateSection>,
    pub object_selectors: Vec<DesiredStateObjectSelector>,
}
