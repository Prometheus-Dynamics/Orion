use super::client::{
    ClientEvent, ClientEventPoll, ClientHello, ClientSession, ExecutorStateUpdate,
    ExecutorWorkloadQuery, PeerEnrollment, PeerIdentityUpdate, ProviderLeaseQuery,
    ProviderStateUpdate, StateWatch,
};
use super::metrics::{NodeObservabilitySnapshot, PeerTrustSnapshot};
use super::mutations::MutationBatch;
use super::sync::{PeerHello, StateSnapshot, SyncDiffRequest, SyncRequest, SyncSummaryRequest};
use crate::{LeaseRecord, WorkloadRecord};
use orion_core::NodeId;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};

#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum ControlMessage {
    Hello(PeerHello),
    SyncRequest(SyncRequest),
    SyncSummaryRequest(SyncSummaryRequest),
    SyncDiffRequest(SyncDiffRequest),
    QueryStateSnapshot,
    Snapshot(StateSnapshot),
    Mutations(MutationBatch),
    ClientHello(ClientHello),
    ClientWelcome(ClientSession),
    ProviderState(ProviderStateUpdate),
    ExecutorState(ExecutorStateUpdate),
    QueryExecutorWorkloads(ExecutorWorkloadQuery),
    WatchExecutorWorkloads(ExecutorWorkloadQuery),
    ExecutorWorkloads(Vec<WorkloadRecord>),
    QueryProviderLeases(ProviderLeaseQuery),
    WatchProviderLeases(ProviderLeaseQuery),
    ProviderLeases(Vec<LeaseRecord>),
    EnrollPeer(PeerEnrollment),
    QueryPeerTrust,
    PeerTrust(PeerTrustSnapshot),
    RevokePeer(NodeId),
    ReplacePeerIdentity(PeerIdentityUpdate),
    RotateHttpTlsIdentity,
    QueryObservability,
    Observability(Box<NodeObservabilitySnapshot>),
    WatchState(StateWatch),
    PollClientEvents(ClientEventPoll),
    ClientEvents(Vec<ClientEvent>),
    Ping,
    Pong,
    Accepted,
    Rejected(String),
}
