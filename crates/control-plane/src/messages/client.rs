use super::sync::StateSnapshot;
use crate::{
    AppliedClusterState, ExecutorRecord, LeaseRecord, ObservedClusterState, ProviderRecord,
    ResourceRecord, WorkloadRecord,
};
use orion_core::{
    ClientName, ExecutorId, NodeId, PeerBaseUrl, ProviderId, PublicKeyHex, Revision, SessionId,
};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};

#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum ClientRole {
    ControlPlane,
    Provider,
    Executor,
}

#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct ClientHello {
    pub client_name: ClientName,
    pub role: ClientRole,
}

#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct ClientSession {
    pub session_id: SessionId,
    pub client_name: ClientName,
    pub role: ClientRole,
    pub node_id: NodeId,
    pub source: String,
}

#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct ProviderStateUpdate {
    pub provider: ProviderRecord,
    pub resources: Vec<ResourceRecord>,
}

#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct ExecutorStateUpdate {
    pub executor: ExecutorRecord,
    pub workloads: Vec<WorkloadRecord>,
    pub resources: Vec<ResourceRecord>,
}

#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct ExecutorWorkloadQuery {
    pub executor_id: ExecutorId,
}

#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct ProviderLeaseQuery {
    pub provider_id: ProviderId,
}

#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct PeerEnrollment {
    pub node_id: NodeId,
    pub base_url: PeerBaseUrl,
    pub trusted_public_key_hex: Option<PublicKeyHex>,
    pub trusted_tls_root_cert_pem: Option<Vec<u8>>,
}

#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct PeerIdentityUpdate {
    pub node_id: NodeId,
    pub public_key_hex: PublicKeyHex,
}

#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct StateWatch {
    pub desired_revision: Revision,
}

#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct ClientEventPoll {
    pub after_sequence: u64,
    pub max_events: u32,
}

#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum ClientEventKind {
    StateSnapshot(Box<StateSnapshot>),
    ExecutorWorkloads {
        executor_id: ExecutorId,
        workloads: Vec<WorkloadRecord>,
    },
    ProviderLeases {
        provider_id: ProviderId,
        leases: Vec<LeaseRecord>,
    },
}

#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct ClientEvent {
    pub sequence: u64,
    pub event: ClientEventKind,
}

#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct ObservedStateUpdate {
    pub observed: ObservedClusterState,
    pub applied: AppliedClusterState,
}
