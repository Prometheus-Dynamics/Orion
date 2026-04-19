mod codec;
mod endpoint;
mod error;
mod frame;
mod memory;
mod socket;
mod transport;

pub use codec::{TcpCodec, TcpCodecError};
pub use endpoint::TcpEndpoint;
pub use error::TcpTransportError;
pub use frame::TcpFrame;
pub use memory::TcpTransport;
pub use socket::{
    TcpClientTlsConfig, TcpFrameClient, TcpFrameHandler, TcpFrameServer, TcpServerClientAuth,
    TcpServerTlsConfig,
};
pub use transport::TcpStreamTransport;

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
            transports: vec![TransportType::TcpStream],
            link_types: vec![LinkType::ReliableOrdered],
            features: Vec::new(),
        }
        .negotiate_with(&PeerCapabilities {
            node_id: NodeId::new("node-b"),
            control_versions: vec![ProtocolVersion::new(1, 0)],
            data_versions: vec![ProtocolVersion::new(1, 0)],
            transports: vec![TransportType::TcpStream],
            link_types: vec![LinkType::ReliableOrdered],
            features: Vec::new(),
        })
        .expect("peer capabilities should negotiate")
    }

    fn frame() -> TcpFrame {
        TcpFrame {
            source: TcpEndpoint::new("127.0.0.1", 4100),
            destination: TcpEndpoint::new("127.0.0.1", 4200),
            link: peer_link(),
            binding: RemoteBinding::Channel(ChannelBinding {
                remote_node_id: NodeId::new("node-b"),
                resource_id: ResourceId::new("resource.camera"),
                transport: TransportType::TcpStream,
                link_type: LinkType::ReliableOrdered,
            }),
            payload: vec![1, 2, 3, 4],
        }
    }

    #[test]
    fn codec_roundtrips_frame_with_length_prefix() {
        let codec = TcpCodec;
        let encoded = codec.encode_frame(&frame()).expect("frame should encode");
        let decoded = codec.decode_frame(&encoded).expect("frame should decode");

        assert_eq!(decoded, frame());
        assert_eq!(decoded.link.compatibility, CompatibilityState::Downgraded);
    }

    #[test]
    fn codec_decodes_stream_and_preserves_remaining_bytes() {
        let codec = TcpCodec;
        let first = codec
            .encode_frame(&frame())
            .expect("first frame should encode");
        let mut second_frame = frame();
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
        assert_eq!(decoded_second.payload, vec![9, 8]);
    }

    #[test]
    fn codec_rejects_incomplete_frames() {
        let codec = TcpCodec;
        let encoded = codec.encode_frame(&frame()).expect("frame should encode");

        let err = codec
            .decode_stream(&encoded[..2])
            .expect_err("partial header should fail");
        assert_eq!(err, TcpCodecError::IncompleteHeader);

        let err = codec
            .decode_stream(&encoded[..6])
            .expect_err("partial payload should fail");
        assert_eq!(err, TcpCodecError::IncompletePayload);

        let err = codec
            .decode_stream(&(u32::MAX.to_be_bytes()))
            .expect_err("oversized frame header should fail");
        assert_eq!(err, TcpCodecError::FrameTooLarge);
    }

    #[test]
    fn transport_delivers_frames_fifo() {
        let transport = TcpTransport::new();
        let source = TcpEndpoint::new("127.0.0.1", 4100);
        let destination = TcpEndpoint::new("127.0.0.1", 4200);

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
        let transport = TcpTransport::new();
        let endpoint = TcpEndpoint::new("127.0.0.1", 4200);

        assert!(transport.register_listener(endpoint.clone()));
        assert!(!transport.register_listener(endpoint.clone()));

        let mut sent = frame();
        sent.destination = TcpEndpoint::new("127.0.0.1", 4300);
        assert!(!transport.send_frame(sent));
    }

    #[tokio::test]
    async fn real_tcp_transport_roundtrips_frames() {
        use std::sync::Arc;

        struct EchoHandler;

        impl TcpFrameHandler for EchoHandler {
            fn handle_frame(&self, frame: TcpFrame) -> Result<TcpFrame, TcpTransportError> {
                Ok(TcpFrame {
                    source: frame.destination.clone(),
                    destination: frame.source.clone(),
                    ..frame
                })
            }
        }

        let (addr, server, listener) = TcpFrameServer::bind("127.0.0.1:0", Arc::new(EchoHandler))
            .await
            .expect("TCP server should bind");
        let task = tokio::spawn(server.serve(listener));

        let client = TcpFrameClient::new(addr);
        let response = client
            .send(frame())
            .await
            .expect("TCP frame should roundtrip");

        assert_eq!(response.source, TcpEndpoint::new("127.0.0.1", 4200));
        assert_eq!(response.payload, vec![1, 2, 3, 4]);

        task.abort();
    }

    #[tokio::test]
    async fn real_tcp_transport_rejects_malformed_frames_without_poisoning_server() {
        use std::sync::Arc;
        use tokio::{
            io::{AsyncReadExt, AsyncWriteExt},
            net::TcpStream,
        };

        struct EchoHandler;

        impl TcpFrameHandler for EchoHandler {
            fn handle_frame(&self, frame: TcpFrame) -> Result<TcpFrame, TcpTransportError> {
                Ok(TcpFrame {
                    source: frame.destination.clone(),
                    destination: frame.source.clone(),
                    ..frame
                })
            }
        }

        let (addr, server, listener) = TcpFrameServer::bind("127.0.0.1:0", Arc::new(EchoHandler))
            .await
            .expect("TCP server should bind");
        let task = tokio::spawn(server.serve(listener));

        let mut raw = TcpStream::connect(addr)
            .await
            .expect("raw TCP client should connect");
        raw.write_all(&[0, 0, 0, 8, b'n', b'o', b't'])
            .await
            .expect("raw malformed frame should send");
        raw.shutdown()
            .await
            .expect("raw TCP client should shutdown write side");

        let mut response = Vec::new();
        raw.read_to_end(&mut response)
            .await
            .expect("raw TCP client should read server close");
        assert!(
            response.is_empty(),
            "malformed frames should not get a response payload"
        );

        let client = TcpFrameClient::new(addr);
        let roundtrip = client
            .send(frame())
            .await
            .expect("server should still accept a valid frame after malformed bytes");

        assert_eq!(roundtrip.payload, vec![1, 2, 3, 4]);
        assert_eq!(roundtrip.source, TcpEndpoint::new("127.0.0.1", 4200));

        task.abort();
    }

    #[tokio::test]
    async fn real_tcp_transport_graceful_shutdown_aborts_stalled_connections() {
        use std::sync::Arc;
        use tokio::{
            net::TcpStream,
            sync::oneshot,
            time::{Duration, timeout},
        };

        struct EchoHandler;

        impl TcpFrameHandler for EchoHandler {
            fn handle_frame(&self, frame: TcpFrame) -> Result<TcpFrame, TcpTransportError> {
                Ok(frame)
            }
        }

        let (addr, server, listener) = TcpFrameServer::bind("127.0.0.1:0", Arc::new(EchoHandler))
            .await
            .expect("TCP server should bind");
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let task = tokio::spawn(server.serve_with_shutdown(listener, async move {
            let _ = shutdown_rx.await;
        }));

        let stalled_client = TcpStream::connect(addr)
            .await
            .expect("stalled TCP client should connect");
        let _ = &stalled_client;

        shutdown_tx
            .send(())
            .expect("shutdown signal should be sent");
        timeout(Duration::from_millis(200), task)
            .await
            .expect("TCP server should shut down promptly")
            .expect("TCP server join should succeed")
            .expect("TCP server shutdown should succeed");
    }

    #[tokio::test]
    async fn real_tcp_transport_accepts_partial_writes_and_reconnects() {
        use std::sync::Arc;
        use tokio::{
            io::{AsyncReadExt, AsyncWriteExt},
            net::TcpStream,
        };

        struct EchoHandler;

        impl TcpFrameHandler for EchoHandler {
            fn handle_frame(&self, frame: TcpFrame) -> Result<TcpFrame, TcpTransportError> {
                Ok(TcpFrame {
                    source: frame.destination.clone(),
                    destination: frame.source.clone(),
                    ..frame
                })
            }
        }

        let (addr, server, listener) = TcpFrameServer::bind("127.0.0.1:0", Arc::new(EchoHandler))
            .await
            .expect("TCP server should bind");
        let task = tokio::spawn(server.serve(listener));

        let codec = TcpCodec;
        let payload = codec.encode_frame(&frame()).expect("frame should encode");
        let mut raw = TcpStream::connect(addr)
            .await
            .expect("raw TCP client should connect");
        let midpoint = payload.len() / 2;
        raw.write_all(&payload[..midpoint])
            .await
            .expect("first partial write should succeed");
        tokio::task::yield_now().await;
        raw.write_all(&payload[midpoint..])
            .await
            .expect("second partial write should succeed");
        raw.shutdown()
            .await
            .expect("raw TCP client should shutdown write side");

        let mut response = Vec::new();
        raw.read_to_end(&mut response)
            .await
            .expect("raw TCP client should read response");
        let response = codec
            .decode_frame(&response)
            .expect("partial-write response should decode");
        assert_eq!(response.payload, vec![1, 2, 3, 4]);

        let client = TcpFrameClient::new(addr);
        let second = client
            .send(frame())
            .await
            .expect("same client should reconnect across requests");
        assert_eq!(second.source, TcpEndpoint::new("127.0.0.1", 4200));

        task.abort();
    }

    #[tokio::test]
    async fn real_tcp_transport_survives_interrupted_clients_and_concurrent_pressure() {
        use std::sync::Arc;
        use tokio::io::AsyncWriteExt;

        struct EchoHandler;

        impl TcpFrameHandler for EchoHandler {
            fn handle_frame(&self, frame: TcpFrame) -> Result<TcpFrame, TcpTransportError> {
                Ok(TcpFrame {
                    source: frame.destination.clone(),
                    destination: frame.source.clone(),
                    ..frame
                })
            }
        }

        let (addr, server, listener) = TcpFrameServer::bind("127.0.0.1:0", Arc::new(EchoHandler))
            .await
            .expect("TCP server should bind");
        let task = tokio::spawn(server.serve(listener));

        let codec = TcpCodec;
        let interrupted_payload = codec
            .encode_frame(&frame())
            .expect("interrupted frame should encode");
        let mut interrupted = tokio::net::TcpStream::connect(addr)
            .await
            .expect("interrupted TCP client should connect");
        interrupted
            .write_all(&interrupted_payload)
            .await
            .expect("interrupted frame should send");
        drop(interrupted);

        let mut joins = Vec::new();
        for index in 0..16 {
            let client = TcpFrameClient::new(addr);
            joins.push(tokio::spawn(async move {
                let mut next = frame();
                next.payload = vec![index as u8; 32];
                client.send(next).await
            }));
        }

        for join in joins {
            let response = join
                .await
                .expect("concurrent TCP task should join")
                .expect("concurrent TCP request should succeed");
            assert_eq!(response.payload.len(), 32);
        }

        task.abort();
    }

    #[tokio::test]
    async fn real_tcp_transport_roundtrips_frames_over_verified_mtls() {
        use rcgen::generate_simple_self_signed;
        use std::sync::Arc;

        struct EchoHandler;

        impl TcpFrameHandler for EchoHandler {
            fn handle_frame(&self, frame: TcpFrame) -> Result<TcpFrame, TcpTransportError> {
                Ok(TcpFrame {
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
        let server_tls = TcpServerTlsConfig::new(
            server_cert.cert.pem().into_bytes(),
            server_cert.signing_key.serialize_pem().into_bytes(),
        )
        .with_client_auth(TcpServerClientAuth::Required {
            trusted_client_roots_pem: vec![client_cert.cert.pem().into_bytes()],
        });
        let client_tls =
            TcpClientTlsConfig::new(server_cert.cert.pem().into_bytes(), "node-b.local")
                .with_client_identity(
                    client_cert.cert.pem().into_bytes(),
                    client_cert.signing_key.serialize_pem().into_bytes(),
                );

        let (addr, server, listener) = TcpFrameServer::bind("127.0.0.1:0", Arc::new(EchoHandler))
            .await
            .expect("TCP server should bind");
        let task = tokio::spawn(server.serve_tls(listener, server_tls));

        let client = TcpFrameClient::with_tls(addr, client_tls);
        let response = client
            .send(frame())
            .await
            .expect("verified mTLS TCP frame should roundtrip");

        assert_eq!(response.payload, vec![1, 2, 3, 4]);
        assert_eq!(response.source, TcpEndpoint::new("127.0.0.1", 4200));

        task.abort();
    }

    #[tokio::test]
    async fn real_tcp_transport_rejects_untrusted_tls_clients_when_mtls_is_required() {
        use rcgen::generate_simple_self_signed;
        use std::sync::Arc;

        struct EchoHandler;

        impl TcpFrameHandler for EchoHandler {
            fn handle_frame(&self, frame: TcpFrame) -> Result<TcpFrame, TcpTransportError> {
                Ok(TcpFrame {
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
        let server_tls = TcpServerTlsConfig::new(
            server_cert.cert.pem().into_bytes(),
            server_cert.signing_key.serialize_pem().into_bytes(),
        )
        .with_client_auth(TcpServerClientAuth::Required {
            trusted_client_roots_pem: vec![trusted_client.cert.pem().into_bytes()],
        });
        let client_tls =
            TcpClientTlsConfig::new(server_cert.cert.pem().into_bytes(), "node-b.local")
                .with_client_identity(
                    untrusted_client.cert.pem().into_bytes(),
                    untrusted_client.signing_key.serialize_pem().into_bytes(),
                );

        let (addr, server, listener) = TcpFrameServer::bind("127.0.0.1:0", Arc::new(EchoHandler))
            .await
            .expect("TCP server should bind");
        let task = tokio::spawn(server.serve_tls(listener, server_tls));

        let client = TcpFrameClient::with_tls(addr, client_tls);
        let error = client
            .send(frame())
            .await
            .expect_err("untrusted mTLS client should fail");
        assert!(
            matches!(
                error,
                TcpTransportError::ConnectFailed(_) | TcpTransportError::ReadFailed(_)
            ),
            "unexpected TLS rejection shape: {error:?}"
        );

        task.abort();
    }
}
