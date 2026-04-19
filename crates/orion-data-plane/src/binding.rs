use crate::{LinkType, TransportType};
use orion_core::{NodeId, ResourceId};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};

#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct ChannelBinding {
    pub remote_node_id: NodeId,
    pub resource_id: ResourceId,
    pub transport: TransportType,
    pub link_type: LinkType,
}

#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct ProxyBinding {
    pub remote_node_id: NodeId,
    pub resource_id: ResourceId,
}

#[derive(
    Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum RemoteBinding {
    Channel(ChannelBinding),
    Proxy(ProxyBinding),
}
