//! Transport-agnostic data-plane bindings, peer capabilities, and negotiation
//! contracts for Orion.

mod binding;
mod peer;
mod transport;

pub use binding::{ChannelBinding, ProxyBinding, RemoteBinding};
pub use peer::{NegotiationError, PeerCapabilities, PeerLink};
pub use transport::{LinkType, TransportType};

#[cfg(test)]
mod tests {
    use super::*;
    use orion_core::{CompatibilityState, FeatureFlag, NodeId, ProtocolVersion, ResourceId};

    #[test]
    fn proxy_and_channel_bindings_keep_remote_access_explicit() {
        let channel = ChannelBinding {
            remote_node_id: NodeId::new("node-b"),
            resource_id: ResourceId::new("node-b.camera-01"),
            transport: TransportType::TcpStream,
            link_type: LinkType::ReliableOrdered,
        };
        let proxy = ProxyBinding {
            remote_node_id: NodeId::new("node-c"),
            resource_id: ResourceId::new("node-c.imu-01"),
        };

        assert_eq!(channel.remote_node_id.as_str(), "node-b");
        assert_eq!(proxy.resource_id.as_str(), "node-c.imu-01");
    }

    #[test]
    fn negotiation_prefers_best_shared_versions_and_transports() {
        let left = PeerCapabilities {
            node_id: NodeId::new("node-a"),
            control_versions: vec![ProtocolVersion::new(1, 0), ProtocolVersion::new(2, 0)],
            data_versions: vec![ProtocolVersion::new(1, 0), ProtocolVersion::new(2, 0)],
            transports: vec![
                TransportType::Http,
                TransportType::TcpStream,
                TransportType::QuicStream,
            ],
            link_types: vec![LinkType::ReliableOrdered, LinkType::ReliableMultiplexed],
            features: vec![FeatureFlag::new("stream.resume")],
        };
        let right = PeerCapabilities {
            node_id: NodeId::new("node-b"),
            control_versions: vec![ProtocolVersion::new(2, 0)],
            data_versions: vec![ProtocolVersion::new(1, 0), ProtocolVersion::new(2, 0)],
            transports: vec![TransportType::Http, TransportType::QuicStream],
            link_types: vec![LinkType::ReliableMultiplexed],
            features: vec![FeatureFlag::new("stream.resume")],
        };

        let link = left
            .negotiate_with(&right)
            .expect("negotiation should succeed");

        assert_eq!(link.control_version, ProtocolVersion::new(2, 0));
        assert_eq!(link.data_version, ProtocolVersion::new(2, 0));
        assert_eq!(link.control_transport, TransportType::Http);
        assert_eq!(link.data_transport, TransportType::QuicStream);
        assert_eq!(link.link_type, LinkType::ReliableMultiplexed);
        assert_eq!(link.compatibility, CompatibilityState::Downgraded);
    }

    #[test]
    fn negotiation_rejects_missing_data_plane_compatibility() {
        let left = PeerCapabilities {
            node_id: NodeId::new("node-a"),
            control_versions: vec![ProtocolVersion::new(1, 0)],
            data_versions: vec![ProtocolVersion::new(1, 0)],
            transports: vec![TransportType::TcpStream],
            link_types: vec![LinkType::ReliableOrdered],
            features: Vec::new(),
        };
        let right = PeerCapabilities {
            node_id: NodeId::new("node-b"),
            control_versions: vec![ProtocolVersion::new(1, 0)],
            data_versions: vec![ProtocolVersion::new(2, 0)],
            transports: vec![TransportType::TcpStream],
            link_types: vec![LinkType::ReliableOrdered],
            features: Vec::new(),
        };

        let err = left
            .negotiate_with(&right)
            .expect_err("data version mismatch should fail");
        assert_eq!(err, NegotiationError::NoDataVersion);
    }

    #[test]
    fn preferred_link_can_be_fully_preferred_when_everything_matches() {
        let left = PeerCapabilities {
            node_id: NodeId::new("node-a"),
            control_versions: vec![ProtocolVersion::new(1, 0)],
            data_versions: vec![ProtocolVersion::new(1, 0)],
            transports: vec![TransportType::Ipc, TransportType::Http],
            link_types: vec![LinkType::LocalSharedMemory],
            features: Vec::new(),
        };
        let right = PeerCapabilities {
            node_id: NodeId::new("node-a.local"),
            control_versions: vec![ProtocolVersion::new(1, 0)],
            data_versions: vec![ProtocolVersion::new(1, 0)],
            transports: vec![TransportType::Ipc, TransportType::Http],
            link_types: vec![LinkType::LocalSharedMemory],
            features: Vec::new(),
        };

        let link = left
            .negotiate_with(&right)
            .expect("negotiation should succeed");
        assert_eq!(link.compatibility, CompatibilityState::Preferred);
        assert_eq!(link.control_transport, TransportType::Ipc);
        assert_eq!(link.data_transport, TransportType::Ipc);
        assert_eq!(link.link_type, LinkType::LocalSharedMemory);
    }

    #[test]
    fn negotiation_matrix_covers_preferred_and_downgraded_paths() {
        let cases = [
            (
                vec![TransportType::Ipc, TransportType::Http],
                vec![LinkType::LocalSharedMemory],
                CompatibilityState::Preferred,
                TransportType::Ipc,
                TransportType::Ipc,
            ),
            (
                vec![TransportType::Http, TransportType::TcpStream],
                vec![LinkType::ReliableOrdered],
                CompatibilityState::Downgraded,
                TransportType::Http,
                TransportType::TcpStream,
            ),
            (
                vec![TransportType::Http, TransportType::QuicStream],
                vec![LinkType::ReliableMultiplexed],
                CompatibilityState::Downgraded,
                TransportType::Http,
                TransportType::QuicStream,
            ),
        ];

        for (transports, link_types, compatibility, expected_control, expected_data) in cases {
            let left = PeerCapabilities {
                node_id: NodeId::new("node-a"),
                control_versions: vec![ProtocolVersion::new(1, 0)],
                data_versions: vec![ProtocolVersion::new(1, 0)],
                transports: transports.clone(),
                link_types: link_types.clone(),
                features: Vec::new(),
            };
            let right = PeerCapabilities {
                node_id: NodeId::new("node-b"),
                control_versions: vec![ProtocolVersion::new(1, 0)],
                data_versions: vec![ProtocolVersion::new(1, 0)],
                transports,
                link_types,
                features: Vec::new(),
            };

            let link = left
                .negotiate_with(&right)
                .expect("negotiation should succeed");
            assert_eq!(link.compatibility, compatibility);
            assert_eq!(link.control_transport, expected_control);
            assert_eq!(link.data_transport, expected_data);
        }
    }
}
