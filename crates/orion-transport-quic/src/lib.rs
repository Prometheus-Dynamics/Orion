mod codec;
mod endpoint;
mod error;
mod frame;
mod memory;
mod socket;
mod transport;

pub use codec::{QuicCodec, QuicCodecError};
pub use endpoint::QuicEndpoint;
pub use error::QuicTransportError;
pub use frame::{QuicChannel, QuicFrame};
pub use memory::QuicTransport;
pub use socket::{
    QuicClientTlsConfig, QuicFrameClient, QuicFrameHandler, QuicFrameServer, QuicServerClientAuth,
    QuicServerTlsConfig,
};
pub use transport::QuicTransportAdapter;

#[cfg(test)]
mod tests {
    use super::*;
    use orion_core::{CompatibilityState, NodeId, ProtocolVersion, ResourceId};
    use orion_data_plane::{
        ChannelBinding, LinkType, PeerCapabilities, PeerLink, RemoteBinding, TransportType,
    };

    fn peer_link() -> PeerLink {
        PeerCapabilities {
            node_id: NodeId::new("node-a"),
            control_versions: vec![ProtocolVersion::new(1, 0)],
            data_versions: vec![ProtocolVersion::new(1, 0)],
            transports: vec![TransportType::QuicStream],
            link_types: vec![LinkType::ReliableMultiplexed],
            features: Vec::new(),
        }
        .negotiate_with(&PeerCapabilities {
            node_id: NodeId::new("node-b"),
            control_versions: vec![ProtocolVersion::new(1, 0)],
            data_versions: vec![ProtocolVersion::new(1, 0)],
            transports: vec![TransportType::QuicStream],
            link_types: vec![LinkType::ReliableMultiplexed],
            features: Vec::new(),
        })
        .expect("peer capabilities should negotiate")
    }

    fn frame() -> QuicFrame {
        QuicFrame {
            source: QuicEndpoint::new("127.0.0.1", 5100).with_server_name("node-a.local"),
            destination: QuicEndpoint::new("127.0.0.1", 5200).with_server_name("node-b.local"),
            connection_id: 7,
            channel: QuicChannel::Stream(3),
            link: peer_link(),
            binding: RemoteBinding::Channel(ChannelBinding {
                remote_node_id: NodeId::new("node-b"),
                resource_id: ResourceId::new("resource.video"),
                transport: TransportType::QuicStream,
                link_type: LinkType::ReliableMultiplexed,
            }),
            payload: vec![1, 2, 3, 4],
        }
    }

    #[test]
    fn codec_roundtrips_frame_with_length_prefix() {
        let codec = QuicCodec;
        let encoded = codec.encode_frame(&frame()).expect("frame should encode");
        let decoded = codec.decode_frame(&encoded).expect("frame should decode");

        assert_eq!(decoded, frame());
        assert_eq!(decoded.link.compatibility, CompatibilityState::Downgraded);
    }

    #[test]
    fn codec_decodes_stream_and_preserves_remaining_bytes() {
        let codec = QuicCodec;
        let first = codec
            .encode_frame(&frame())
            .expect("first frame should encode");
        let mut second_frame = frame();
        second_frame.connection_id = 8;
        second_frame.channel = QuicChannel::Datagram;
        second_frame.payload = vec![9, 8];
        let second = codec
            .encode_frame(&second_frame)
            .expect("second frame should encode");

        let mut stream = first.clone();
        stream.extend_from_slice(&second);

        let (decoded_first, remaining) =
            codec.decode_stream(&stream).expect("stream should decode");
        let decoded_second = codec
            .decode_frame(remaining)
            .expect("remaining frame should decode");

        assert_eq!(decoded_first, frame());
        assert_eq!(decoded_second.channel, QuicChannel::Datagram);
        assert_eq!(decoded_second.payload, vec![9, 8]);
    }

    #[test]
    fn codec_rejects_incomplete_frames() {
        let codec = QuicCodec;
        let encoded = codec.encode_frame(&frame()).expect("frame should encode");

        let err = codec
            .decode_stream(&encoded[..2])
            .expect_err("partial header should fail");
        assert_eq!(err, QuicCodecError::IncompleteHeader);

        let err = codec
            .decode_stream(&encoded[..6])
            .expect_err("partial payload should fail");
        assert_eq!(err, QuicCodecError::IncompletePayload);

        let err = codec
            .decode_stream(&(u32::MAX.to_be_bytes()))
            .expect_err("oversized frame header should fail");
        assert_eq!(err, QuicCodecError::FrameTooLarge);
    }

    #[test]
    fn transport_delivers_multiplexed_frames_fifo() {
        let transport = QuicTransport::new();
        let source = QuicEndpoint::new("127.0.0.1", 5100).with_server_name("node-a.local");
        let destination = QuicEndpoint::new("127.0.0.1", 5200).with_server_name("node-b.local");

        assert!(transport.register_listener(source.clone()));
        assert!(transport.register_listener(destination.clone()));

        let mut sent = frame();
        sent.source = source;
        sent.destination = destination.clone();

        assert!(transport.send_frame(sent.clone()));

        let received = transport
            .recv_frame(&destination)
            .expect("frame should be delivered");
        assert_eq!(received, sent);
        assert!(transport.recv_frame(&destination).is_none());
    }

    #[test]
    fn transport_rejects_missing_listener_and_duplicate_registration() {
        let transport = QuicTransport::new();
        let endpoint = QuicEndpoint::new("127.0.0.1", 5200).with_server_name("node-b.local");

        assert!(transport.register_listener(endpoint.clone()));
        assert!(!transport.register_listener(endpoint.clone()));

        let mut sent = frame();
        sent.destination = QuicEndpoint::new("127.0.0.1", 5300).with_server_name("missing.local");
        assert!(!transport.send_frame(sent));
    }

    #[tokio::test]
    async fn real_quic_transport_roundtrips_frames() {
        use std::sync::Arc;

        struct EchoHandler;

        impl QuicFrameHandler for EchoHandler {
            fn handle_frame(&self, frame: QuicFrame) -> Result<QuicFrame, QuicTransportError> {
                Ok(QuicFrame {
                    source: frame.destination.clone(),
                    destination: frame.source.clone(),
                    ..frame
                })
            }
        }

        let rcgen::CertifiedKey { cert, signing_key } =
            rcgen::generate_simple_self_signed(vec!["node-b.local".to_owned()])
                .expect("test QUIC certificate should generate");
        let server_tls = QuicServerTlsConfig::new(
            cert.pem().into_bytes(),
            signing_key.serialize_pem().into_bytes(),
        );
        let (server, endpoint) = QuicFrameServer::bind_secure(
            QuicEndpoint::new("127.0.0.1", 0).with_server_name("node-b.local"),
            Arc::new(EchoHandler),
            server_tls,
        )
        .await
        .expect("QUIC server should bind");
        let task = tokio::spawn(server.serve());

        let client = QuicFrameClient::new(endpoint).expect("QUIC client should build");
        let response = client
            .send(frame())
            .await
            .expect("QUIC frame should roundtrip");

        assert_eq!(response.source.host, "127.0.0.1");
        assert_eq!(response.source.port, 5200);
        assert_eq!(response.payload, vec![1, 2, 3, 4]);

        task.abort();
    }

    #[tokio::test]
    async fn real_quic_transport_roundtrips_frames_over_verified_mtls() {
        use rcgen::generate_simple_self_signed;
        use std::sync::Arc;

        struct EchoHandler;

        impl QuicFrameHandler for EchoHandler {
            fn handle_frame(&self, frame: QuicFrame) -> Result<QuicFrame, QuicTransportError> {
                Ok(QuicFrame {
                    source: frame.destination.clone(),
                    destination: frame.source.clone(),
                    ..frame
                })
            }
        }

        let server_cert = generate_simple_self_signed(vec!["node-b.local".to_owned()])
            .expect("server cert should generate");
        let client_cert = generate_simple_self_signed(vec!["node-a.local".to_owned()])
            .expect("client cert should generate");
        let server_tls = QuicServerTlsConfig::new(
            server_cert.cert.pem().into_bytes(),
            server_cert.signing_key.serialize_pem().into_bytes(),
        )
        .with_client_auth(QuicServerClientAuth::Required {
            trusted_client_roots_pem: vec![client_cert.cert.pem().into_bytes()],
        });
        let client_tls = QuicClientTlsConfig::new(server_cert.cert.pem().into_bytes())
            .with_client_identity(
                client_cert.cert.pem().into_bytes(),
                client_cert.signing_key.serialize_pem().into_bytes(),
            );

        let (server, endpoint) = QuicFrameServer::bind_secure(
            QuicEndpoint::new("127.0.0.1", 0).with_server_name("node-b.local"),
            Arc::new(EchoHandler),
            server_tls,
        )
        .await
        .expect("QUIC server should bind");
        let task = tokio::spawn(server.serve());

        let client =
            QuicFrameClient::with_tls(endpoint, &client_tls).expect("QUIC client should build");
        let response = client
            .send(frame())
            .await
            .expect("verified mTLS QUIC frame should roundtrip");

        assert_eq!(response.payload, vec![1, 2, 3, 4]);
        assert_eq!(response.source.port, 5200);

        task.abort();
    }

    #[tokio::test]
    async fn real_quic_transport_rejects_untrusted_tls_clients_when_mtls_is_required() {
        use rcgen::generate_simple_self_signed;
        use std::sync::Arc;

        struct EchoHandler;

        impl QuicFrameHandler for EchoHandler {
            fn handle_frame(&self, frame: QuicFrame) -> Result<QuicFrame, QuicTransportError> {
                Ok(QuicFrame {
                    source: frame.destination.clone(),
                    destination: frame.source.clone(),
                    ..frame
                })
            }
        }

        let server_cert = generate_simple_self_signed(vec!["node-b.local".to_owned()])
            .expect("server cert should generate");
        let trusted_client = generate_simple_self_signed(vec!["node-a.local".to_owned()])
            .expect("trusted client cert should generate");
        let untrusted_client = generate_simple_self_signed(vec!["node-c.local".to_owned()])
            .expect("untrusted client cert should generate");
        let server_tls = QuicServerTlsConfig::new(
            server_cert.cert.pem().into_bytes(),
            server_cert.signing_key.serialize_pem().into_bytes(),
        )
        .with_client_auth(QuicServerClientAuth::Required {
            trusted_client_roots_pem: vec![trusted_client.cert.pem().into_bytes()],
        });
        let client_tls = QuicClientTlsConfig::new(server_cert.cert.pem().into_bytes())
            .with_client_identity(
                untrusted_client.cert.pem().into_bytes(),
                untrusted_client.signing_key.serialize_pem().into_bytes(),
            );

        let (server, endpoint) = QuicFrameServer::bind_secure(
            QuicEndpoint::new("127.0.0.1", 0).with_server_name("node-b.local"),
            Arc::new(EchoHandler),
            server_tls,
        )
        .await
        .expect("QUIC server should bind");
        let task = tokio::spawn(server.serve());

        let client =
            QuicFrameClient::with_tls(endpoint, &client_tls).expect("QUIC client should build");
        let error = client
            .send(frame())
            .await
            .expect_err("untrusted QUIC mTLS client should fail");
        assert!(
            matches!(
                error,
                QuicTransportError::ConnectFailed(_)
                    | QuicTransportError::OpenStreamFailed(_)
                    | QuicTransportError::ReadFailed(_)
            ),
            "unexpected QUIC TLS rejection shape: {error:?}"
        );

        task.abort();
    }
}
