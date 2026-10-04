use super::client::{
    ClientEvent, ClientEventPoll, ClientHello, ClientSession, ExecutorStateUpdate,
    ExecutorWorkloadQuery, PeerEnrollment, PeerIdentityUpdate, ProviderLeaseQuery,
    ProviderStateUpdate, StateWatch,
};
use super::maintenance::{MaintenanceCommand, MaintenanceStatus};
use super::metrics::{NodeObservabilitySnapshot, PeerTrustSnapshot};
use super::mutations::MutationBatch;
use super::status::{StatusEntry, StatusQuery};
use super::sync::{PeerHello, StateSnapshot, SyncDiffRequest, SyncRequest, SyncSummaryRequest};
use crate::{LeaseRecord, WorkloadRecord};
use alloc::{boxed::Box, string::String, vec::Vec};
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
    QueryMaintenance,
    UpdateMaintenance(MaintenanceCommand),
    MaintenanceStatus(MaintenanceStatus),
    QueryObservability,
    Observability(Box<NodeObservabilitySnapshot>),
    WatchState(StateWatch),
    PollClientEvents(ClientEventPoll),
    ClientEvents(Vec<ClientEvent>),
    /// Publishes a batch of volatile status entries (provider and executor clients only).
    PublishStatus(Vec<StatusEntry>),
    /// Queries the node's volatile status lane; answered with [`ControlMessage::Status`].
    QueryStatus(StatusQuery),
    /// Subscribes the client stream to coalesced status changes (`ClientEventKind::Status`).
    WatchStatus(StatusQuery),
    Status(Vec<StatusEntry>),
    Ping,
    Pong,
    Accepted,
    Rejected(String),
}
