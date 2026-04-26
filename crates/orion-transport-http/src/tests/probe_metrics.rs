use super::*;

#[tokio::test]
async fn probe_server_exposes_metrics_but_control_server_does_not() {
    let probe_service = std::sync::Arc::new(NetworkLoopbackService);
    let (probe_addr, probe_server, probe_listener) = HttpServer::bind_probe(
        "127.0.0.1:0".parse().expect("address should parse"),
        probe_service,
    )
    .await
    .expect("probe listener should bind");
    let probe_task = tokio::spawn(probe_server.serve(probe_listener));

    let control_service = std::sync::Arc::new(NetworkLoopbackService);
    let (control_addr, control_server, control_listener) = HttpServer::bind(
        "127.0.0.1:0".parse().expect("address should parse"),
        control_service,
    )
    .await
    .expect("control listener should bind");
    let control_task = tokio::spawn(control_server.serve(control_listener));

    let probe_response = reqwest::get(format!("http://{probe_addr}{METRICS_PATH}"))
        .await
        .expect("probe metrics request should send");
    assert_eq!(probe_response.status(), reqwest::StatusCode::OK);
    assert_eq!(
        probe_response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok()),
        Some("text/plain; version=0.0.4; charset=utf-8")
    );
    assert_eq!(
        probe_response
            .text()
            .await
            .expect("probe metrics body should read"),
        "# TYPE test_metric counter\ntest_metric 1\n"
    );

    let control_response = reqwest::get(format!("http://{control_addr}{METRICS_PATH}"))
        .await
        .expect("control metrics request should send");
    assert_eq!(control_response.status(), reqwest::StatusCode::NOT_FOUND);

    probe_task.abort();
    control_task.abort();
}

#[tokio::test]
async fn tls_probe_server_exposes_metrics_with_configured_tls() {
    let service = std::sync::Arc::new(NetworkLoopbackService);
    let (addr, server, listener) = HttpServer::bind_probe(
        "127.0.0.1:0".parse().expect("address should parse"),
        service,
    )
    .await
    .expect("probe listener should bind");
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

    let client = reqwest::Client::builder()
        .add_root_certificate(
            reqwest::Certificate::from_pem(cert.pem().as_bytes())
                .expect("test root cert should parse"),
        )
        .build()
        .expect("HTTPS reqwest client should build");
    let response = client
        .get(format!("https://localhost:{}{METRICS_PATH}", addr.port()))
        .send()
        .await
        .expect("TLS probe metrics request should succeed");

    assert_eq!(response.status(), reqwest::StatusCode::OK);
    assert_eq!(
        response.text().await.expect("metrics body should read"),
        "# TYPE test_metric counter\ntest_metric 1\n"
    );

    server_task.abort();
}
