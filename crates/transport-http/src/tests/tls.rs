//! HTTPS client/server tests: TLS handshakes, mTLS, handshake timeouts and identity reload.

use super::*;
use rcgen::generate_simple_self_signed;
use std::{fs, path::PathBuf, time::SystemTime};

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
fn plain_http_client_rejects_tls_configuration() {
    let error = HttpClient::with_tls(
        "http://127.0.0.1:9100",
        HttpClientTlsConfig {
            root_cert_pem: b"pem".to_vec(),
            client_cert_pem: None,
            client_key_pem: None,
        },
    )
    .expect_err("plain HTTP should reject TLS configuration");
    assert!(
        error
            .to_string()
            .contains("plain HTTP targets do not use TLS configuration"),
        "unexpected error: {error}"
    );
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
async fn https_server_times_out_stalled_tls_handshakes_and_releases_capacity() {
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
    let server_task = tokio::spawn(
        server
            .with_io_timeout(std::time::Duration::from_millis(20))
            .with_max_connections(1)
            .serve_tls(listener, tls),
    );

    let stalled = TcpStream::connect(addr)
        .await
        .expect("stalled TCP client should connect");
    tokio::time::sleep(std::time::Duration::from_millis(60)).await;

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
        .expect("valid HTTPS client should acquire capacity after stalled handshake times out");

    assert!(matches!(response, HttpResponsePayload::Hello(_)));

    drop(stalled);
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
