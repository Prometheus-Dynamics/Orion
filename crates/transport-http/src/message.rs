use crate::route::{ControlRoute, HttpMethod};
use orion_auth::{AuthenticatedPeerRequest, PeerRequestPayload};
use orion_control_plane::{
    ControlMessage, DesiredStateSummary, MutationBatch, NodeHealthSnapshot,
    NodeObservabilitySnapshot, NodeReadinessSnapshot, ObservedStateUpdate, PeerHello,
    StateSnapshot,
};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum HttpRequestPayload {
    Control(Box<ControlMessage>),
    ObservedUpdate(ObservedStateUpdate),
    AuthenticatedPeer(AuthenticatedPeerRequest),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HttpRequest {
    pub method: HttpMethod,
    pub path: String,
    pub body: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HttpResponse {
    pub status: u16,
    pub body: Vec<u8>,
}

#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum HttpResponsePayload {
    Accepted,
    Hello(PeerHello),
    Summary(DesiredStateSummary),
    Snapshot(StateSnapshot),
    Mutations(MutationBatch),
    Observability(Box<NodeObservabilitySnapshot>),
    Health(NodeHealthSnapshot),
    Readiness(NodeReadinessSnapshot),
}

impl HttpRequestPayload {
    pub fn route(&self) -> Result<ControlRoute, crate::HttpTransportError> {
        match self {
            Self::AuthenticatedPeer(request) => match &request.payload {
                PeerRequestPayload::Control(message) => Self::Control(message.clone()).route(),
                PeerRequestPayload::ObservedUpdate(update) => {
                    Self::ObservedUpdate(update.clone()).route()
                }
            },
            Self::Control(message) => match message.as_ref() {
                ControlMessage::Hello(_) => Ok(ControlRoute::Hello),
                ControlMessage::SyncRequest(_)
                | ControlMessage::SyncSummaryRequest(_)
                | ControlMessage::SyncDiffRequest(_) => Ok(ControlRoute::Sync),
                ControlMessage::QueryStateSnapshot => Ok(ControlRoute::Snapshot),
                ControlMessage::Snapshot(_) => Ok(ControlRoute::Snapshot),
                ControlMessage::Mutations(_) => Ok(ControlRoute::Mutations),
                ControlMessage::QueryObservability => Ok(ControlRoute::Observability),
                ControlMessage::ClientHello(_)
                | ControlMessage::ClientWelcome(_)
                | ControlMessage::ProviderState(_)
                | ControlMessage::ExecutorState(_)
                | ControlMessage::QueryExecutorWorkloads(_)
                | ControlMessage::WatchExecutorWorkloads(_)
                | ControlMessage::ExecutorWorkloads(_)
                | ControlMessage::QueryProviderLeases(_)
                | ControlMessage::WatchProviderLeases(_)
                | ControlMessage::ProviderLeases(_)
                | ControlMessage::EnrollPeer(_)
                | ControlMessage::QueryPeerTrust
                | ControlMessage::PeerTrust(_)
                | ControlMessage::RevokePeer(_)
                | ControlMessage::ReplacePeerIdentity(_)
                | ControlMessage::RotateHttpTlsIdentity
                | ControlMessage::QueryMaintenance
                | ControlMessage::UpdateMaintenance(_)
                | ControlMessage::MaintenanceStatus(_)
                | ControlMessage::Observability(_)
                | ControlMessage::WatchState(_)
                | ControlMessage::PollClientEvents(_)
                | ControlMessage::ClientEvents(_)
                | ControlMessage::Ping
                | ControlMessage::Pong
                | ControlMessage::Accepted
                | ControlMessage::Rejected(_) => {
                    Err(crate::HttpTransportError::UnsupportedControlMessage)
                }
            },
            Self::ObservedUpdate(_) => Ok(ControlRoute::ObservedUpdate),
        }
    }
}
