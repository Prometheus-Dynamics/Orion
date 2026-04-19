use super::*;
use crate::{QuicChannel, QuicEndpoint, QuicFrame};
use orion_core::{NodeId, ProtocolVersion, ResourceId};
use orion_data_plane::{ChannelBinding, LinkType, PeerCapabilities, RemoteBinding, TransportType};

fn frame() -> QuicFrame {
    let link = PeerCapabilities {
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
    .expect("peer capabilities should negotiate");

    QuicFrame {
        source: QuicEndpoint::new("127.0.0.1", 5100).with_server_name("node-a.local"),
        destination: QuicEndpoint::new("127.0.0.1", 5200).with_server_name("node-b.local"),
        connection_id: 7,
        channel: QuicChannel::Stream(3),
        link,
        binding: RemoteBinding::Channel(ChannelBinding {
            remote_node_id: NodeId::new("node-b"),
            resource_id: ResourceId::new("resource.video"),
            transport: TransportType::QuicStream,
            link_type: LinkType::ReliableMultiplexed,
        }),
        payload: vec![1, 2, 3, 4],
    }
}

#[tokio::test]
async fn real_quic_transport_rejects_malformed_frames_without_poisoning_server() {
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

    let (server, endpoint) = QuicFrameServer::bind(
        QuicEndpoint::new("127.0.0.1", 0).with_server_name("node-b.local"),
        Arc::new(EchoHandler),
    )
    .await
    .expect("QUIC server should bind");
    let task = tokio::spawn(server.serve());

    install_rustls_crypto_provider();
    let bind_addr: SocketAddr = "127.0.0.1:0"
        .parse()
        .expect("test client bind addr should parse");
    let mut raw_endpoint = Endpoint::client(bind_addr).expect("raw client endpoint should bind");
    raw_endpoint.set_default_client_config(
        build_client_config(&QuicClientTlsConfig {
            root_cert_pem: Vec::new(),
            client_cert_pem: None,
            client_key_pem: None,
        })
        .expect("client config"),
    );
    let remote_addr: SocketAddr = format!("{}:{}", endpoint.host, endpoint.port)
        .parse()
        .expect("remote addr should parse");
    let connection = raw_endpoint
        .connect(
            remote_addr,
            endpoint.server_name.as_deref().unwrap_or("localhost"),
        )
        .expect("raw QUIC connect should start")
        .await
        .expect("raw QUIC connect should succeed");

    let (mut send, mut recv) = connection
        .open_bi()
        .await
        .expect("raw QUIC stream should open");
    send.write_all(&[0, 0, 0, 8, b'n', b'o', b't'])
        .await
        .expect("raw malformed QUIC frame should send");
    send.finish().expect("raw QUIC send should finish");
    let raw_response = recv.read_to_end(usize::MAX).await;
    assert!(
        raw_response
            .as_ref()
            .map(|bytes| bytes.is_empty())
            .unwrap_or(true),
        "malformed QUIC frames should not get a response payload"
    );

    let client = QuicFrameClient::new(endpoint).expect("QUIC client should build");
    let roundtrip = client
        .send(frame())
        .await
        .expect("server should still accept a valid frame after malformed bytes");

    assert_eq!(roundtrip.payload, vec![1, 2, 3, 4]);
    assert_eq!(roundtrip.source.port, 5200);

    task.abort();
}

#[tokio::test]
async fn real_quic_transport_accepts_partial_writes_and_reconnects() {
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

    let (server, endpoint) = QuicFrameServer::bind(
        QuicEndpoint::new("127.0.0.1", 0).with_server_name("node-b.local"),
        Arc::new(EchoHandler),
    )
    .await
    .expect("QUIC server should bind");
    let task = tokio::spawn(server.serve());

    install_rustls_crypto_provider();
    let bind_addr: SocketAddr = "127.0.0.1:0"
        .parse()
        .expect("test client bind addr should parse");
    let mut raw_endpoint = Endpoint::client(bind_addr).expect("raw client endpoint should bind");
    raw_endpoint.set_default_client_config(
        build_client_config(&QuicClientTlsConfig {
            root_cert_pem: Vec::new(),
            client_cert_pem: None,
            client_key_pem: None,
        })
        .expect("client config"),
    );
    let remote_addr: SocketAddr = format!("{}:{}", endpoint.host, endpoint.port)
        .parse()
        .expect("remote addr should parse");
    let connection = raw_endpoint
        .connect(
            remote_addr,
            endpoint.server_name.as_deref().unwrap_or("localhost"),
        )
        .expect("raw QUIC connect should start")
        .await
        .expect("raw QUIC connect should succeed");

    let codec = QuicCodec;
    let encoded = codec
        .encode_frame(&frame())
        .expect("QUIC frame should encode");
    let midpoint = encoded.len() / 2;
    let (mut send, mut recv) = connection
        .open_bi()
        .await
        .expect("raw QUIC stream should open");
    send.write_all(&encoded[..midpoint])
        .await
        .expect("first partial QUIC write should succeed");
    tokio::task::yield_now().await;
    send.write_all(&encoded[midpoint..])
        .await
        .expect("second partial QUIC write should succeed");
    send.finish().expect("raw QUIC send should finish");
    let response = recv
        .read_to_end(usize::MAX)
        .await
        .expect("raw QUIC response should read");
    let response = codec
        .decode_frame(&response)
        .expect("partial-write QUIC response should decode");
    assert_eq!(response.payload, vec![1, 2, 3, 4]);

    let client = QuicFrameClient::new(endpoint).expect("QUIC client should build");
    let second = client
        .send(frame())
        .await
        .expect("same QUIC client should reconnect across requests");
    assert_eq!(second.source.port, 5200);

    task.abort();
}

#[tokio::test]
async fn real_quic_transport_graceful_shutdown_aborts_stalled_connections() {
    struct EchoHandler;

    impl QuicFrameHandler for EchoHandler {
        fn handle_frame(&self, frame: QuicFrame) -> Result<QuicFrame, QuicTransportError> {
            Ok(frame)
        }
    }

    let (server, endpoint) = QuicFrameServer::bind(
        QuicEndpoint::new("127.0.0.1", 0).with_server_name("node-b.local"),
        Arc::new(EchoHandler),
    )
    .await
    .expect("QUIC server should bind");
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(server.serve_with_shutdown(async move {
        let _ = shutdown_rx.await;
    }));

    install_rustls_crypto_provider();
    let bind_addr: SocketAddr = "127.0.0.1:0"
        .parse()
        .expect("test client bind addr should parse");
    let mut raw_endpoint = Endpoint::client(bind_addr).expect("raw client endpoint should bind");
    raw_endpoint.set_default_client_config(
        build_client_config(&QuicClientTlsConfig {
            root_cert_pem: Vec::new(),
            client_cert_pem: None,
            client_key_pem: None,
        })
        .expect("client config"),
    );
    let remote_addr: SocketAddr = format!("{}:{}", endpoint.host, endpoint.port)
        .parse()
        .expect("remote addr should parse");
    let stalled_connection = raw_endpoint
        .connect(
            remote_addr,
            endpoint.server_name.as_deref().unwrap_or("localhost"),
        )
        .expect("raw QUIC connect should start")
        .await
        .expect("raw QUIC connect should succeed");
    let _ = &stalled_connection;

    shutdown_tx
        .send(())
        .expect("shutdown signal should be sent");
    tokio::time::timeout(std::time::Duration::from_millis(200), task)
        .await
        .expect("QUIC server should shut down promptly")
        .expect("QUIC server join should succeed")
        .expect("QUIC server shutdown should succeed");
}

#[tokio::test]
async fn real_quic_transport_survives_interrupted_clients_and_concurrent_pressure() {
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

    let (server, endpoint) = QuicFrameServer::bind(
        QuicEndpoint::new("127.0.0.1", 0).with_server_name("node-b.local"),
        Arc::new(EchoHandler),
    )
    .await
    .expect("QUIC server should bind");
    let task = tokio::spawn(server.serve());

    let client = QuicFrameClient::new(endpoint.clone()).expect("QUIC client should build");

    install_rustls_crypto_provider();
    let bind_addr: SocketAddr = "127.0.0.1:0"
        .parse()
        .expect("test client bind addr should parse");
    let mut raw_endpoint = Endpoint::client(bind_addr).expect("raw client endpoint should bind");
    raw_endpoint.set_default_client_config(
        build_client_config(&QuicClientTlsConfig {
            root_cert_pem: Vec::new(),
            client_cert_pem: None,
            client_key_pem: None,
        })
        .expect("client config"),
    );
    let remote_addr: SocketAddr = format!("{}:{}", endpoint.host, endpoint.port)
        .parse()
        .expect("remote addr should parse");
    let connection = raw_endpoint
        .connect(
            remote_addr,
            endpoint.server_name.as_deref().unwrap_or("localhost"),
        )
        .expect("raw QUIC connect should start")
        .await
        .expect("raw QUIC connect should succeed");
    let codec = QuicCodec;
    let interrupted_payload = codec
        .encode_frame(&frame())
        .expect("interrupted QUIC frame should encode");
    let (mut send, _recv) = connection
        .open_bi()
        .await
        .expect("interrupted QUIC stream should open");
    send.write_all(&interrupted_payload)
        .await
        .expect("interrupted QUIC payload should send");
    send.finish().expect("interrupted QUIC send should finish");
    drop(connection);

    let mut joins = Vec::new();
    for index in 0..16 {
        let client = client.clone();
        joins.push(tokio::spawn(async move {
            let mut next = frame();
            next.connection_id = 100 + index;
            next.payload = vec![index as u8; 64];
            client.send(next).await
        }));
    }

    for join in joins {
        let response = join
            .await
            .expect("concurrent QUIC task should join")
            .expect("concurrent QUIC request should succeed");
        assert_eq!(response.payload.len(), 64);
    }

    task.abort();
}
