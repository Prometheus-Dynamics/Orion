use super::*;
use orion_core::{CONTROL_PROTOCOL_HTTP_HEADER, CONTROL_PROTOCOL_VERSION};

#[tokio::test]
async fn server_rejects_skewed_client_with_conflict_before_decoding() {
    let (addr, server, listener) = HttpServer::bind(
        "127.0.0.1:0".parse().expect("address should parse"),
        std::sync::Arc::new(NetworkLoopbackService),
    )
    .await
    .expect("control listener should bind");
    let task = tokio::spawn(server.serve(listener));

    let request = HttpCodec
        .encode_request(&HttpRequestPayload::Control(Box::new(hello_message())))
        .expect("hello should encode");
    let response = reqwest::Client::new()
        .post(format!("http://{addr}{}", request.path))
        .header(CONTROL_PROTOCOL_HTTP_HEADER, CONTROL_PROTOCOL_VERSION + 1)
        .body(request.body.clone())
        .send()
        .await
        .expect("skewed request should return");
    assert_eq!(response.status(), reqwest::StatusCode::CONFLICT);
    assert_eq!(
        response
            .headers()
            .get(CONTROL_PROTOCOL_HTTP_HEADER)
            .and_then(|value| value.to_str().ok()),
        Some(CONTROL_PROTOCOL_VERSION.to_string().as_str())
    );
    let body = response.text().await.expect("body should read");
    assert!(body.contains("upgrade orionctl"), "{body}");

    let response = reqwest::Client::new()
        .post(format!("http://{addr}{}", request.path))
        .body(request.body)
        .send()
        .await
        .expect("headerless request should return");
    assert_eq!(response.status(), reqwest::StatusCode::BAD_REQUEST);

    // A same-version client still works.
    HttpClient::try_new(format!("http://{addr}"))
        .expect("client should build")
        .send(&HttpRequestPayload::Control(Box::new(hello_message())))
        .await
        .expect("same-version request should succeed");
    task.abort();
}

#[tokio::test]
async fn client_reports_mismatch_from_skewed_server() {
    use axum::{Router, http::StatusCode, routing::post};

    // Pretend to be an orion-node from another release: its archive is not decodable here.
    let router = Router::new().route(
        ControlRoute::Hello.path(),
        post(|| async {
            (
                StatusCode::OK,
                [(
                    CONTROL_PROTOCOL_HTTP_HEADER,
                    (CONTROL_PROTOCOL_VERSION + 1).to_string(),
                )],
                vec![0xde_u8, 0xad, 0xbe, 0xef],
            )
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener.local_addr().expect("local addr");
    let task = tokio::spawn(async move { axum::serve(listener, router).await });

    let err = HttpClient::try_new(format!("http://{addr}"))
        .expect("client should build")
        .send(&HttpRequestPayload::Control(Box::new(hello_message())))
        .await
        .expect_err("skewed server should be rejected");
    assert_eq!(
        err,
        HttpTransportError::ProtocolMismatch {
            local: CONTROL_PROTOCOL_VERSION,
            remote: CONTROL_PROTOCOL_VERSION + 1,
        }
    );
    task.abort();
}
