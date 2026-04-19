use super::*;
use orion_auth::{
    AuthenticatedPeerRequest, PEER_REQUEST_AUTH_VERSION, PeerRequestAuth, PeerRequestPayload,
};
use orion_control_plane::{
    AppliedClusterState, ClusterStateEnvelope, ControlMessage, DesiredClusterState,
    NodeHealthSnapshot, NodeHealthStatus, NodeReadinessSnapshot, NodeReadinessStatus,
    ObservedClusterState, ObservedStateUpdate, PeerHello, StateSnapshot,
};
use orion_core::{NodeId, Revision, encode_to_vec};
use rcgen::generate_simple_self_signed;
use std::{fs, path::PathBuf, time::SystemTime};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

#[derive(Default)]
struct LoopbackService;

impl HttpService for LoopbackService {
    fn handle(&self, request: HttpRequest) -> HttpResponse {
        let codec = HttpCodec;
        let payload = codec
            .decode_request(&request)
            .expect("request should decode");

        match payload {
            HttpRequestPayload::Control(message) => match *message {
                ControlMessage::Snapshot(snapshot) => codec
                    .encode_response(&HttpResponsePayload::Snapshot(snapshot))
                    .expect("snapshot response should encode"),
                _ => codec
                    .encode_response(&HttpResponsePayload::Accepted)
                    .expect("accepted response should encode"),
            },
            HttpRequestPayload::AuthenticatedPeer(request) => match request.payload {
                PeerRequestPayload::Control(message) => match *message {
                    ControlMessage::Snapshot(snapshot) => codec
                        .encode_response(&HttpResponsePayload::Snapshot(snapshot))
                        .expect("snapshot response should encode"),
                    _ => codec
                        .encode_response(&HttpResponsePayload::Accepted)
                        .expect("accepted response should encode"),
                },
                PeerRequestPayload::ObservedUpdate(_) => codec
                    .encode_response(&HttpResponsePayload::Accepted)
                    .expect("accepted response should encode"),
            },
            HttpRequestPayload::ObservedUpdate(_) => codec
                .encode_response(&HttpResponsePayload::Accepted)
                .expect("accepted response should encode"),
        }
    }
}

fn hello_message() -> ControlMessage {
    ControlMessage::Hello(PeerHello {
        node_id: NodeId::new("node-a"),
        desired_revision: Revision::new(1),
        desired_fingerprint: 11,
        desired_section_fingerprints: orion_control_plane::DesiredStateSectionFingerprints {
            nodes: 1,
            artifacts: 2,
            workloads: 3,
            resources: 4,
            providers: 5,
            executors: 6,
            leases: 7,
        },
        observed_revision: Revision::new(1),
        applied_revision: Revision::new(1),
        transport_binding_version: None,
        transport_binding_public_key: None,
        transport_tls_cert_pem: None,
        transport_binding_signature: None,
    })
}

fn authenticated_hello_request() -> AuthenticatedPeerRequest {
    AuthenticatedPeerRequest {
        auth: PeerRequestAuth {
            version: PEER_REQUEST_AUTH_VERSION,
            node_id: NodeId::new("node-a"),
            public_key: vec![1; 32],
            nonce: 7,
            signature: vec![2; 64],
        },
        payload: PeerRequestPayload::Control(Box::new(hello_message())),
    }
}

fn temp_tls_dir(label: &str) -> PathBuf {
    let unique = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .expect("system time should be after unix epoch")
        .as_nanos();
    let path = std::env::temp_dir().join(format!("orion-http-{label}-{unique}"));
    fs::create_dir_all(&path).expect("temporary TLS dir should be created");
    path
}

#[test]
fn codec_maps_hello_to_hello_route() {
    let codec = HttpCodec;
    let request = codec
        .encode_request(&HttpRequestPayload::Control(Box::new(hello_message())))
        .expect("hello request should encode");

    assert_eq!(request.method, HttpMethod::Post);
    assert_eq!(request.path, "/v1/control/hello");

    let decoded = codec
        .decode_request(&request)
        .expect("hello request should decode");
    assert_eq!(
        decoded,
        HttpRequestPayload::Control(Box::new(hello_message()))
    );
}

#[test]
fn codec_maps_observed_updates_to_observed_route() {
    let codec = HttpCodec;
    let update = ObservedStateUpdate {
        observed: ObservedClusterState::default(),
        applied: AppliedClusterState::default(),
    };
    let request = codec
        .encode_request(&HttpRequestPayload::ObservedUpdate(update.clone()))
        .expect("observed update request should encode");

    assert_eq!(request.path, "/v1/control/observed");

    let decoded = codec
        .decode_request(&request)
        .expect("observed update should decode");
    assert_eq!(decoded, HttpRequestPayload::ObservedUpdate(update));
}

#[test]
fn codec_rejects_control_payload_sent_to_observed_route() {
    let codec = HttpCodec;
    let body = encode_to_vec(&hello_message()).expect("hello request should encode");

    let err = codec
        .decode_request(&HttpRequest {
            method: HttpMethod::Post,
            path: "/v1/control/observed".to_owned(),
            body,
        })
        .expect_err("control payload on observed route should be rejected");

    assert!(matches!(err, HttpTransportError::DecodeRequest(_)));
}

#[test]
fn codec_roundtrips_authenticated_peer_requests() {
    let codec = HttpCodec;
    let authenticated = authenticated_hello_request();
    let request = codec
        .encode_request(&HttpRequestPayload::AuthenticatedPeer(
            authenticated.clone(),
        ))
        .expect("authenticated request should encode");

    assert_eq!(request.path, "/v1/control/hello");

    let decoded = codec
        .decode_request(&request)
        .expect("authenticated request should decode");
    assert_eq!(
        decoded,
        HttpRequestPayload::AuthenticatedPeer(authenticated)
    );
}

#[test]
fn codec_rejects_unknown_paths_and_methods() {
    let codec = HttpCodec;

    let err = codec
        .decode_request(&HttpRequest {
            method: HttpMethod::Post,
            path: "/v1/control/unknown".to_owned(),
            body: vec![1],
        })
        .expect_err("unknown path should be rejected");
    assert!(matches!(err, HttpTransportError::UnsupportedPath(_)));

    let err = codec
        .decode_request(&HttpRequest {
            method: HttpMethod::Get,
            path: "/v1/control/hello".to_owned(),
            body: vec![1],
        })
        .expect_err("wrong method should be rejected");
    assert!(matches!(err, HttpTransportError::UnsupportedMethod(_)));
}

#[test]
fn transport_roundtrips_snapshot_responses() {
    let transport = HttpTransport::new();
    let service = LoopbackService;
    let snapshot = StateSnapshot {
        state: ClusterStateEnvelope::new(
            DesiredClusterState::default(),
            ObservedClusterState::default(),
            AppliedClusterState::default(),
        ),
    };

    let response = transport
        .send(
            &service,
            &HttpRequestPayload::Control(Box::new(ControlMessage::Snapshot(snapshot.clone()))),
        )
        .expect("snapshot request should succeed");

    assert_eq!(response, HttpResponsePayload::Snapshot(snapshot));
}

#[test]
fn transport_accepts_non_snapshot_control_messages() {
    let transport = HttpTransport::new();
    let service = LoopbackService;

    let response = transport
        .send(
            &service,
            &HttpRequestPayload::Control(Box::new(hello_message())),
        )
        .expect("hello request should succeed");

    assert_eq!(response, HttpResponsePayload::Accepted);
}

struct NetworkLoopbackService;

impl HttpControlHandler for NetworkLoopbackService {
    fn handle_payload(
        &self,
        payload: HttpRequestPayload,
    ) -> Result<HttpResponsePayload, HttpTransportError> {
        Ok(match payload {
            HttpRequestPayload::Control(message) => match *message {
                ControlMessage::Hello(hello) => HttpResponsePayload::Hello(hello),
                ControlMessage::Snapshot(snapshot) => HttpResponsePayload::Snapshot(snapshot),
                _ => HttpResponsePayload::Accepted,
            },
            HttpRequestPayload::AuthenticatedPeer(request) => match request.payload {
                PeerRequestPayload::Control(message) => match *message {
                    ControlMessage::Hello(hello) => HttpResponsePayload::Hello(hello),
                    ControlMessage::Snapshot(snapshot) => HttpResponsePayload::Snapshot(snapshot),
                    _ => HttpResponsePayload::Accepted,
                },
                PeerRequestPayload::ObservedUpdate(_) => HttpResponsePayload::Accepted,
            },
            HttpRequestPayload::ObservedUpdate(_) => HttpResponsePayload::Accepted,
        })
    }

    fn handle_health(&self) -> Result<HttpResponsePayload, HttpTransportError> {
        Ok(HttpResponsePayload::Health(NodeHealthSnapshot {
            node_id: NodeId::new("node-a"),
            alive: true,
            replay_completed: true,
            replay_successful: true,
            http_bound: true,
            ipc_bound: true,
            ipc_stream_bound: true,
            degraded_peer_count: 0,
            status: NodeHealthStatus::Healthy,
            reasons: Vec::new(),
        }))
    }

    fn handle_readiness(&self) -> Result<HttpResponsePayload, HttpTransportError> {
        Ok(HttpResponsePayload::Readiness(NodeReadinessSnapshot {
            node_id: NodeId::new("node-a"),
            ready: true,
            replay_completed: true,
            replay_successful: true,
            http_bound: true,
            ipc_bound: true,
            ipc_stream_bound: true,
            initial_sync_complete: true,
            ready_peer_count: 0,
            pending_peer_count: 0,
            degraded_peer_count: 0,
            status: NodeReadinessStatus::Ready,
            reasons: Vec::new(),
        }))
    }
}

#[tokio::test]
async fn client_and_server_roundtrip_over_real_http() {
    let service = std::sync::Arc::new(NetworkLoopbackService);
    let (addr, server, listener) = HttpServer::bind(
        "127.0.0.1:0".parse().expect("address should parse"),
        service,
    )
    .await
    .expect("listener should bind");
    let server_task = tokio::spawn(server.serve(listener));

    let client = HttpClient::try_new(format!("http://{}", addr)).expect("HTTP client should build");
    let response = client
        .send(&HttpRequestPayload::Control(Box::new(hello_message())))
        .await
        .expect("hello request should succeed");

    assert!(matches!(response, HttpResponsePayload::Hello(_)));

    server_task.abort();
}

#[tokio::test]
async fn client_and_server_roundtrip_over_real_https() {
    let service = std::sync::Arc::new(NetworkLoopbackService);
    let (addr, server, listener) = HttpServer::bind(
        "127.0.0.1:0".parse().expect("address should parse"),
        service,
    )
    .await
    .expect("listener should bind");
    let rcgen::CertifiedKey { cert, signing_key } =
        generate_simple_self_signed(vec!["localhost".to_owned()])
            .expect("TLS test certificate should generate");
    let tls = HttpServerTlsConfig {
        cert_pem: cert.pem().into_bytes(),
        key_pem: signing_key.serialize_pem().into_bytes(),
        cert_path: None,
        key_path: None,
        client_auth: HttpServerClientAuth::Disabled,
    };
    let server_task = tokio::spawn(server.serve_tls(listener, tls));

    let client = HttpClient::with_tls(
        format!("https://localhost:{}", addr.port()),
        HttpClientTlsConfig {
            root_cert_pem: cert.pem().into_bytes(),
            client_cert_pem: None,
            client_key_pem: None,
        },
    )
    .expect("HTTPS client should build");
    let response = client
        .send(&HttpRequestPayload::Control(Box::new(hello_message())))
        .await
        .expect("hello request should succeed over HTTPS");

    assert!(matches!(response, HttpResponsePayload::Hello(_)));

    server_task.abort();
}

#[tokio::test]
async fn client_and_server_roundtrip_over_real_https_with_required_mtls() {
    let service = std::sync::Arc::new(NetworkLoopbackService);
    let (addr, server, listener) = HttpServer::bind(
        "127.0.0.1:0".parse().expect("address should parse"),
        service,
    )
    .await
    .expect("listener should bind");
    let rcgen::CertifiedKey {
        cert: server_cert,
        signing_key: server_key,
    } = generate_simple_self_signed(vec!["localhost".to_owned()])
        .expect("server TLS test certificate should generate");
    let rcgen::CertifiedKey {
        cert: client_cert,
        signing_key: client_key,
    } = generate_simple_self_signed(vec!["client.local".to_owned()])
        .expect("client TLS test certificate should generate");
    let tls = HttpServerTlsConfig {
        cert_pem: server_cert.pem().into_bytes(),
        key_pem: server_key.serialize_pem().into_bytes(),
        cert_path: None,
        key_path: None,
        client_auth: HttpServerClientAuth::RequiredStatic {
            trusted_client_roots_pem: vec![client_cert.pem().into_bytes()],
        },
    };
    let server_task = tokio::spawn(server.serve_tls(listener, tls));

    let missing_client_identity = HttpClient::with_tls(
        format!("https://localhost:{}", addr.port()),
        HttpClientTlsConfig {
            root_cert_pem: server_cert.pem().into_bytes(),
            client_cert_pem: None,
            client_key_pem: None,
        },
    )
    .expect("HTTPS client should build without client identity");
    let missing_client_result = missing_client_identity
        .send(&HttpRequestPayload::Control(Box::new(hello_message())))
        .await;
    assert!(missing_client_result.is_err());

    let mtls_client = HttpClient::with_tls(
        format!("https://localhost:{}", addr.port()),
        HttpClientTlsConfig {
            root_cert_pem: server_cert.pem().into_bytes(),
            client_cert_pem: Some(client_cert.pem().into_bytes()),
            client_key_pem: Some(client_key.serialize_pem().into_bytes()),
        },
    )
    .expect("HTTPS client with client identity should build");
    let response = mtls_client
        .send(&HttpRequestPayload::Control(Box::new(hello_message())))
        .await
        .expect("hello request should succeed over mTLS");

    assert!(matches!(response, HttpResponsePayload::Hello(_)));

    server_task.abort();
}

#[tokio::test]
async fn https_server_reloads_path_backed_tls_identity_after_rotation() {
    let service = std::sync::Arc::new(NetworkLoopbackService);
    let (addr, server, listener) = HttpServer::bind(
        "127.0.0.1:0".parse().expect("address should parse"),
        service,
    )
    .await
    .expect("listener should bind");
    let tls_dir = temp_tls_dir("reloads-path-identity");
    let cert_path = tls_dir.join("cert.pem");
    let key_path = tls_dir.join("key.pem");

    let rcgen::CertifiedKey {
        cert: initial_cert,
        signing_key: initial_key,
    } = generate_simple_self_signed(vec!["localhost".to_owned()])
        .expect("initial TLS test certificate should generate");
    fs::write(&cert_path, initial_cert.pem()).expect("initial cert should write");
    fs::write(&key_path, initial_key.serialize_pem()).expect("initial key should write");

    let tls = HttpServerTlsConfig {
        cert_pem: Vec::new(),
        key_pem: Vec::new(),
        cert_path: Some(cert_path.clone()),
        key_path: Some(key_path.clone()),
        client_auth: HttpServerClientAuth::Disabled,
    };
    let server_task = tokio::spawn(server.serve_tls(listener, tls));

    let initial_client = HttpClient::with_tls(
        format!("https://localhost:{}", addr.port()),
        HttpClientTlsConfig {
            root_cert_pem: initial_cert.pem().into_bytes(),
            client_cert_pem: None,
            client_key_pem: None,
        },
    )
    .expect("initial HTTPS client should build");
    initial_client
        .send(&HttpRequestPayload::Control(Box::new(hello_message())))
        .await
        .expect("initial HTTPS request should succeed");

    let rcgen::CertifiedKey {
        cert: rotated_cert,
        signing_key: rotated_key,
    } = generate_simple_self_signed(vec!["localhost".to_owned()])
        .expect("rotated TLS test certificate should generate");
    let cert_tmp = tls_dir.join("cert.pem.tmp");
    let key_tmp = tls_dir.join("key.pem.tmp");
    fs::write(&cert_tmp, rotated_cert.pem()).expect("rotated cert should write");
    fs::write(&key_tmp, rotated_key.serialize_pem()).expect("rotated key should write");
    fs::rename(&cert_tmp, &cert_path).expect("rotated cert should replace original");
    fs::rename(&key_tmp, &key_path).expect("rotated key should replace original");

    tokio::time::sleep(std::time::Duration::from_millis(5)).await;

    let stale_client = HttpClient::with_tls(
        format!("https://localhost:{}", addr.port()),
        HttpClientTlsConfig {
            root_cert_pem: initial_cert.pem().into_bytes(),
            client_cert_pem: None,
            client_key_pem: None,
        },
    )
    .expect("stale HTTPS client should build");
    assert!(
        stale_client
            .send(&HttpRequestPayload::Control(Box::new(hello_message())))
            .await
            .is_err()
    );

    let rotated_client = HttpClient::with_tls(
        format!("https://localhost:{}", addr.port()),
        HttpClientTlsConfig {
            root_cert_pem: rotated_cert.pem().into_bytes(),
            client_cert_pem: None,
            client_key_pem: None,
        },
    )
    .expect("rotated HTTPS client should build");
    rotated_client
        .send(&HttpRequestPayload::Control(Box::new(hello_message())))
        .await
        .expect("rotated HTTPS request should succeed");

    server_task.abort();
    let _ = fs::remove_dir_all(tls_dir);
}

#[tokio::test]
async fn client_and_server_roundtrip_health_and_readiness_over_real_http() {
    let service = std::sync::Arc::new(NetworkLoopbackService);
    let (addr, server, listener) = HttpServer::bind(
        "127.0.0.1:0".parse().expect("address should parse"),
        service,
    )
    .await
    .expect("listener should bind");
    let server_task = tokio::spawn(server.serve(listener));

    let client = HttpClient::try_new(format!("http://{}", addr)).expect("HTTP client should build");
    let health = client
        .get_route(ControlRoute::Health)
        .await
        .expect("health request should succeed");
    let readiness = client
        .get_route(ControlRoute::Readiness)
        .await
        .expect("readiness request should succeed");

    assert!(matches!(health, HttpResponsePayload::Health(_)));
    assert!(matches!(readiness, HttpResponsePayload::Readiness(_)));

    server_task.abort();
}

#[tokio::test]
async fn http_client_reconnects_across_multiple_requests() {
    let service = std::sync::Arc::new(NetworkLoopbackService);
    let (addr, server, listener) = HttpServer::bind(
        "127.0.0.1:0".parse().expect("address should parse"),
        service,
    )
    .await
    .expect("listener should bind");
    let server_task = tokio::spawn(server.serve(listener));

    let client = HttpClient::try_new(format!("http://{}", addr)).expect("HTTP client should build");
    let first = client
        .send(&HttpRequestPayload::Control(Box::new(hello_message())))
        .await
        .expect("first HTTP request should succeed");
    let second = client
        .send(&HttpRequestPayload::Control(Box::new(hello_message())))
        .await
        .expect("second HTTP request should succeed");

    assert!(matches!(first, HttpResponsePayload::Hello(_)));
    assert!(matches!(second, HttpResponsePayload::Hello(_)));

    server_task.abort();
}

#[tokio::test]
async fn http_server_accepts_partial_writes_and_survives_interrupted_clients() {
    let service = std::sync::Arc::new(NetworkLoopbackService);
    let (addr, server, listener) = HttpServer::bind(
        "127.0.0.1:0".parse().expect("address should parse"),
        service,
    )
    .await
    .expect("listener should bind");
    let server_task = tokio::spawn(server.serve(listener));

    let codec = HttpCodec;
    let request = codec
        .encode_request(&HttpRequestPayload::Control(Box::new(hello_message())))
        .expect("hello request should encode");
    let raw = format!(
        "POST {} HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n",
        request.path,
        request.body.len()
    );

    let mut partial = TcpStream::connect(addr)
        .await
        .expect("partial HTTP client should connect");
    let midpoint = request.body.len() / 2;
    partial
        .write_all(raw.as_bytes())
        .await
        .expect("HTTP headers should send");
    partial
        .write_all(&request.body[..midpoint])
        .await
        .expect("first partial body write should succeed");
    tokio::task::yield_now().await;
    partial
        .write_all(&request.body[midpoint..])
        .await
        .expect("second partial body write should succeed");
    partial
        .shutdown()
        .await
        .expect("partial HTTP client should shutdown write side");

    let mut partial_response = Vec::new();
    partial
        .read_to_end(&mut partial_response)
        .await
        .expect("partial HTTP client should read response");
    assert!(
        partial_response.starts_with(b"HTTP/1.1 200 OK"),
        "expected 200 response for hello roundtrip, got {:?}",
        &partial_response[..partial_response.len().min(32)]
    );

    let mut interrupted = TcpStream::connect(addr)
        .await
        .expect("interrupted HTTP client should connect");
    interrupted
        .write_all(raw.as_bytes())
        .await
        .expect("interrupted HTTP headers should send");
    interrupted
        .write_all(&request.body)
        .await
        .expect("interrupted HTTP body should send");
    drop(interrupted);

    let client = HttpClient::try_new(format!("http://{}", addr)).expect("HTTP client should build");
    let response = client
        .send(&HttpRequestPayload::Control(Box::new(hello_message())))
        .await
        .expect("HTTP server should remain healthy after interrupted client");
    assert!(matches!(response, HttpResponsePayload::Hello(_)));

    server_task.abort();
}

#[tokio::test]
async fn http_server_handles_concurrent_client_pressure() {
    let service = std::sync::Arc::new(NetworkLoopbackService);
    let (addr, server, listener) = HttpServer::bind(
        "127.0.0.1:0".parse().expect("address should parse"),
        service,
    )
    .await
    .expect("listener should bind");
    let server_task = tokio::spawn(server.serve(listener));

    let mut joins = Vec::new();
    for _ in 0..32 {
        let client =
            HttpClient::try_new(format!("http://{}", addr)).expect("HTTP client should build");
        joins.push(tokio::spawn(async move {
            client
                .send(&HttpRequestPayload::Control(Box::new(hello_message())))
                .await
        }));
    }

    for join in joins {
        let response = join
            .await
            .expect("concurrent HTTP task should join")
            .expect("concurrent HTTP request should succeed");
        assert!(matches!(response, HttpResponsePayload::Hello(_)));
    }

    server_task.abort();
}
