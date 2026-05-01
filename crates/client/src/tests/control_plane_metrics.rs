use super::*;
use orion_control_plane::{ControlMessage, PeerTrustSnapshot};
use orion_core::{decode_from_slice, encode_to_vec};
use orion_transport_ipc::{ControlEnvelope, LocalAddress, read_control_frame, write_control_frame};
use std::{
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::net::UnixListener;

fn unique_socket_path(label: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time should advance")
        .as_nanos();
    std::env::temp_dir().join(format!("orion-client-{label}-{nanos}.sock"))
}

#[tokio::test]
async fn local_control_plane_client_reports_its_request_metrics() {
    let socket_path = unique_socket_path("control-plane-client-metrics");
    let _ = tokio::fs::remove_file(&socket_path).await;
    let listener = UnixListener::bind(&socket_path).expect("listener should bind");

    let server_task = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("hello accept should work");
        let hello = read_unary_envelope(&mut stream).await;
        assert!(matches!(
            hello.message,
            ControlMessage::ClientHello(ref request)
                if request.role == ClientRole::ControlPlane
                    && request.client_name.as_str() == "orionctl.metrics"
        ));
        write_unary_envelope(
            &mut stream,
            &ControlEnvelope {
                source: LocalAddress::new("orion"),
                destination: hello.source,
                message: ControlMessage::ClientWelcome(orion_control_plane::ClientSession {
                    session_id: "node-a:orionctl.metrics".into(),
                    role: ClientRole::ControlPlane,
                    node_id: NodeId::new("node-a"),
                    source: "orionctl.metrics".into(),
                    client_name: "orionctl.metrics".into(),
                }),
            },
        )
        .await;

        let (mut stream, _) = listener.accept().await.expect("request accept should work");
        let request = read_unary_envelope(&mut stream).await;
        assert!(matches!(request.message, ControlMessage::QueryPeerTrust));
        write_unary_envelope(
            &mut stream,
            &ControlEnvelope {
                source: LocalAddress::new("orion"),
                destination: request.source,
                message: ControlMessage::PeerTrust(PeerTrustSnapshot {
                    http_mutual_tls_mode: orion_control_plane::HttpMutualTlsMode::Disabled,
                    peers: Vec::new(),
                }),
            },
        )
        .await;
    });

    let client = LocalControlPlaneClient::connect_at(&socket_path, "orionctl.metrics")
        .expect("local control-plane client should connect");
    assert_eq!(
        client
            .local_communication_metrics()
            .metrics
            .latency
            .samples_total,
        0
    );

    let trust = client
        .query_peer_trust()
        .await
        .expect("peer trust query should succeed");
    assert!(trust.peers.is_empty());

    let metrics = client.local_communication_metrics();
    assert_eq!(
        metrics.id,
        "ipc/client-local-unary/orion-client.control-plane.orionctl.metrics"
    );
    assert_eq!(metrics.transport, "ipc");
    assert_eq!(metrics.scope, "client_local_unary");
    assert_eq!(metrics.metrics.messages_sent_total, 1);
    assert_eq!(metrics.metrics.messages_received_total, 1);
    assert!(metrics.metrics.bytes_sent_total > 0);
    assert!(metrics.metrics.bytes_received_total > 0);
    assert_eq!(metrics.metrics.latency.samples_total, 1);
    assert_eq!(
        metrics.metrics.latency.bucket_le_1_ms
            + metrics.metrics.latency.bucket_le_5_ms
            + metrics.metrics.latency.bucket_le_10_ms
            + metrics.metrics.latency.bucket_le_50_ms
            + metrics.metrics.latency.bucket_le_100_ms
            + metrics.metrics.latency.bucket_le_500_ms
            + metrics.metrics.latency.bucket_gt_500_ms,
        metrics.metrics.latency.samples_total
    );

    server_task.await.expect("server task should complete");
    let _ = tokio::fs::remove_file(&socket_path).await;
}

#[tokio::test]
async fn local_control_plane_stream_client_reuses_session_across_requests() {
    let socket_path = unique_socket_path("control-plane-client-stream");
    let _ = tokio::fs::remove_file(&socket_path).await;
    let listener = UnixListener::bind(&socket_path).expect("listener should bind");

    let server_task = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("stream accept should work");
        let hello = read_control_frame(&mut stream)
            .await
            .expect("hello should read")
            .expect("hello should exist");
        assert!(matches!(
            hello.message,
            ControlMessage::ClientHello(ref request)
                if request.role == ClientRole::ControlPlane
                    && request.client_name.as_str() == "orionctl.stream"
        ));
        write_control_frame(
            &mut stream,
            &ControlEnvelope {
                source: LocalAddress::new("orion"),
                destination: hello.source,
                message: ControlMessage::ClientWelcome(orion_control_plane::ClientSession {
                    session_id: "node-a:orionctl.stream".into(),
                    role: ClientRole::ControlPlane,
                    node_id: NodeId::new("node-a"),
                    source: "orionctl.stream".into(),
                    client_name: "orionctl.stream".into(),
                }),
            },
        )
        .await
        .expect("welcome should write");

        for _ in 0..2 {
            let request = read_control_frame(&mut stream)
                .await
                .expect("request should read")
                .expect("request should exist");
            assert!(matches!(request.message, ControlMessage::QueryPeerTrust));
            write_control_frame(
                &mut stream,
                &ControlEnvelope {
                    source: LocalAddress::new("orion"),
                    destination: request.source,
                    message: ControlMessage::PeerTrust(PeerTrustSnapshot {
                        http_mutual_tls_mode: orion_control_plane::HttpMutualTlsMode::Disabled,
                        peers: Vec::new(),
                    }),
                },
            )
            .await
            .expect("response should write");
        }
    });

    let identity =
        crate::session::local_identity_for_role("orionctl.stream", ClientRole::ControlPlane);
    let client = LocalControlPlaneClient::connect_stream(
        &socket_path,
        identity.clone(),
        crate::session::local_session_config_for_role(&identity),
    )
    .expect("local control-plane stream client should build");

    client
        .query_peer_trust()
        .await
        .expect("first peer trust query should succeed");
    client
        .query_peer_trust()
        .await
        .expect("second peer trust query should reuse the stream");

    let metrics = client.local_communication_metrics();
    assert_eq!(
        metrics.id,
        "ipc/client-local-stream/orion-client.control-plane.orionctl.stream"
    );
    assert_eq!(metrics.scope, "client_local_stream");
    assert!(metrics.connected);
    assert_eq!(metrics.metrics.reconnects_total, 1);
    assert_eq!(metrics.metrics.messages_sent_total, 2);
    assert_eq!(metrics.metrics.messages_received_total, 2);

    server_task.await.expect("server task should complete");
    let _ = tokio::fs::remove_file(&socket_path).await;
}

async fn read_unary_envelope(stream: &mut tokio::net::UnixStream) -> ControlEnvelope {
    use tokio::io::AsyncReadExt;

    let mut payload = Vec::new();
    stream
        .read_to_end(&mut payload)
        .await
        .expect("unary payload should read");
    decode_from_slice(&payload).expect("unary payload should decode")
}

async fn write_unary_envelope(stream: &mut tokio::net::UnixStream, envelope: &ControlEnvelope) {
    use tokio::io::AsyncWriteExt;

    let payload = encode_to_vec(envelope).expect("unary response should encode");
    stream
        .write_all(&payload)
        .await
        .expect("unary payload should write");
    stream
        .shutdown()
        .await
        .expect("unary stream should shutdown");
}
