use crate::{LinkType, TransportType};
use orion_core::{CompatibilityState, FeatureFlag, NodeId, ProtocolVersion};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct PeerCapabilities {
    pub node_id: NodeId,
    pub control_versions: Vec<ProtocolVersion>,
    pub data_versions: Vec<ProtocolVersion>,
    pub transports: Vec<TransportType>,
    pub link_types: Vec<LinkType>,
    pub features: Vec<FeatureFlag>,
}

#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct PeerLink {
    pub local_node_id: NodeId,
    pub remote_node_id: NodeId,
    pub control_version: ProtocolVersion,
    pub data_version: ProtocolVersion,
    pub control_transport: TransportType,
    pub data_transport: TransportType,
    pub link_type: LinkType,
    pub compatibility: CompatibilityState,
}

#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum NegotiationError {
    #[error("no compatible control protocol version")]
    NoControlVersion,
    #[error("no compatible data protocol version")]
    NoDataVersion,
    #[error("no compatible transport")]
    NoTransport,
    #[error("no compatible link type")]
    NoLinkType,
}

impl PeerCapabilities {
    pub fn negotiate_with(&self, remote: &Self) -> Result<PeerLink, NegotiationError> {
        let control_version =
            highest_common_version(&self.control_versions, &remote.control_versions)
                .ok_or(NegotiationError::NoControlVersion)?;
        let data_version = highest_common_version(&self.data_versions, &remote.data_versions)
            .ok_or(NegotiationError::NoDataVersion)?;
        let control_transport = preferred_common_transport(
            &self.transports,
            &remote.transports,
            &CONTROL_TRANSPORT_PREFERENCE,
        )
        .ok_or(NegotiationError::NoTransport)?;
        let data_transport = preferred_common_transport(
            &self.transports,
            &remote.transports,
            &DATA_TRANSPORT_PREFERENCE,
        )
        .ok_or(NegotiationError::NoTransport)?;
        let link_type = preferred_common_link_type(&self.link_types, &remote.link_types)
            .ok_or(NegotiationError::NoLinkType)?;

        let compatibility = if control_transport == CONTROL_TRANSPORT_PREFERENCE[0]
            && data_transport == DATA_TRANSPORT_PREFERENCE[0]
            && link_type == LINK_TYPE_PREFERENCE[0]
        {
            CompatibilityState::Preferred
        } else {
            CompatibilityState::Downgraded
        };

        Ok(PeerLink {
            local_node_id: self.node_id.clone(),
            remote_node_id: remote.node_id.clone(),
            control_version,
            data_version,
            control_transport,
            data_transport,
            link_type,
            compatibility,
        })
    }
}

const CONTROL_TRANSPORT_PREFERENCE: [TransportType; 4] = [
    TransportType::Ipc,
    TransportType::Http,
    TransportType::QuicStream,
    TransportType::TcpStream,
];

const DATA_TRANSPORT_PREFERENCE: [TransportType; 4] = [
    TransportType::Ipc,
    TransportType::QuicStream,
    TransportType::TcpStream,
    TransportType::Http,
];

const LINK_TYPE_PREFERENCE: [LinkType; 4] = [
    LinkType::LocalSharedMemory,
    LinkType::ReliableMultiplexed,
    LinkType::ReliableOrdered,
    LinkType::LossyLowLatency,
];

fn highest_common_version(
    left: &[ProtocolVersion],
    right: &[ProtocolVersion],
) -> Option<ProtocolVersion> {
    left.iter()
        .copied()
        .filter(|candidate| right.contains(candidate))
        .max()
}

fn preferred_common_transport(
    left: &[TransportType],
    right: &[TransportType],
    order: &[TransportType],
) -> Option<TransportType> {
    order
        .iter()
        .copied()
        .find(|candidate| left.contains(candidate) && right.contains(candidate))
}

fn preferred_common_link_type(left: &[LinkType], right: &[LinkType]) -> Option<LinkType> {
    LINK_TYPE_PREFERENCE
        .iter()
        .copied()
        .find(|candidate| left.contains(candidate) && right.contains(candidate))
}
