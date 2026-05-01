use super::*;

#[tokio::test]
async fn managed_surface_launcher_starts_named_http_control_surface() {
    struct AcceptHandler;

    impl HttpControlHandler for AcceptHandler {
        fn handle_payload(
            &self,
            _payload: HttpRequestPayload,
        ) -> Result<HttpResponsePayload, HttpTransportError> {
            Ok(HttpResponsePayload::Accepted)
        }
    }

    let state_dir = temp_state_dir("managed-surface-launcher-http");
    let node_id = NodeId::new("node.surface.http");
    let app = NodeApp::builder()
        .config(NodeConfig {
            node_id: node_id.clone(),
            http_bind_addr: "127.0.0.1:0".parse().expect("socket address should parse"),
            ipc_socket_path: NodeConfig::default_ipc_socket_path_for("node.surface.http"),
            reconcile_interval: Duration::from_millis(10),
            state_dir: Some(state_dir.clone()),
            peers: Vec::new(),
            peer_authentication: PeerAuthenticationMode::Optional,
            peer_sync_execution: NodeConfig::try_peer_sync_execution_from_env()
                .expect("peer sync execution defaults should parse"),
            ipc_stream_heartbeat_interval: NodeConfig::default_ipc_stream_heartbeat_interval(),
            ipc_stream_heartbeat_timeout: NodeConfig::default_ipc_stream_heartbeat_timeout(),
            runtime_tuning: NodeConfig::try_runtime_tuning_from_env()
                .expect("runtime tuning defaults should parse"),
        })
        .with_auto_http_tls(true)
        .with_http_mutual_tls_mode(crate::app::HttpMutualTlsMode::Required)
        .try_build()
        .expect("node app should build");

    let local_cert_pem = fs::read(
        app.http_tls_cert_path()
            .expect("local auto TLS cert path should exist"),
    )
    .expect("local TLS cert should be readable");
    app.trust_peer_tls_root_cert_pem(&node_id, local_cert_pem)
        .expect("local TLS cert should be trusted for loopback mtls");

    let (binding, handle) = app
        .start_managed_surface(ManagedSurfaceLaunchRequest::PeerHttpControl {
            addr: "127.0.0.1:0".parse().expect("socket address should parse"),
            handler: Arc::new(AcceptHandler),
        })
        .await
        .expect("managed surface launcher should start HTTP control surface");

    #[cfg(not(feature = "transport-quic"))]
    let ManagedTransportBinding::Socket(addr) = binding;
    #[cfg(feature = "transport-quic")]
    let addr = match binding {
        ManagedTransportBinding::Socket(addr) => addr,
        ManagedTransportBinding::Quic(_) => {
            panic!("expected socket binding for HTTP control surface")
        }
    };

    #[cfg(not(any(feature = "transport-tcp", feature = "transport-quic")))]
    let ManagedClientTransportSecurity::Http(tls) = app
        .managed_surface_client_transport_security(
            ManagedNodeTransportSurface::PeerHttpControl,
            &node_id,
            None,
            None,
            Some(&format!("https://{addr}")),
        )
        .expect("managed HTTP client config should resolve")
        .expect("managed HTTP client TLS config should exist");
    #[cfg(any(feature = "transport-tcp", feature = "transport-quic"))]
    let tls = match app
        .managed_surface_client_transport_security(
            ManagedNodeTransportSurface::PeerHttpControl,
            &node_id,
            None,
            None,
            Some(&format!("https://{addr}")),
        )
        .expect("managed HTTP client config should resolve")
        .expect("managed HTTP client TLS config should exist")
    {
        ManagedClientTransportSecurity::Http(config) => config,
        _ => panic!("expected managed HTTP client config"),
    };

    let client =
        HttpClient::with_tls(format!("https://{addr}"), tls).expect("HTTP client should build");
    let response = client
        .send(&HttpRequestPayload::Control(Box::new(
            ControlMessage::QueryObservability,
        )))
        .await
        .expect("managed HTTP control request should succeed");
    assert_eq!(response, HttpResponsePayload::Accepted);

    handle.abort();
    let _ = fs::remove_dir_all(state_dir);
}

#[tokio::test]
async fn managed_surface_launcher_starts_named_http_probe_surface() {
    #[derive(Clone)]
    struct ProbeHandler {
        health: orion::control_plane::NodeHealthSnapshot,
        readiness: orion::control_plane::NodeReadinessSnapshot,
    }

    impl HttpControlHandler for ProbeHandler {
        fn handle_payload(
            &self,
            _payload: HttpRequestPayload,
        ) -> Result<HttpResponsePayload, HttpTransportError> {
            Ok(HttpResponsePayload::Accepted)
        }

        fn handle_health(&self) -> Result<HttpResponsePayload, HttpTransportError> {
            Ok(HttpResponsePayload::Health(self.health.clone()))
        }

        fn handle_readiness(&self) -> Result<HttpResponsePayload, HttpTransportError> {
            Ok(HttpResponsePayload::Readiness(self.readiness.clone()))
        }
    }

    let app = NodeApp::builder()
        .config(NodeConfig {
            node_id: NodeId::new("node.surface.probe"),
            http_bind_addr: "127.0.0.1:0".parse().expect("socket address should parse"),
            ipc_socket_path: NodeConfig::default_ipc_socket_path_for("node.surface.probe"),
            reconcile_interval: Duration::from_millis(10),
            state_dir: None,
            peers: Vec::new(),
            peer_authentication: PeerAuthenticationMode::Optional,
            peer_sync_execution: NodeConfig::try_peer_sync_execution_from_env()
                .expect("peer sync execution defaults should parse"),
            ipc_stream_heartbeat_interval: NodeConfig::default_ipc_stream_heartbeat_interval(),
            ipc_stream_heartbeat_timeout: NodeConfig::default_ipc_stream_heartbeat_timeout(),
            runtime_tuning: NodeConfig::try_runtime_tuning_from_env()
                .expect("runtime tuning defaults should parse"),
        })
        .with_http_mutual_tls_mode(crate::app::HttpMutualTlsMode::Required)
        .try_build()
        .expect("node app should build");

    let handler = ProbeHandler {
        health: app.health_snapshot(),
        readiness: app.readiness_snapshot(),
    };

    let (binding, handle) = app
        .start_managed_surface(ManagedSurfaceLaunchRequest::HttpProbe {
            addr: "127.0.0.1:0".parse().expect("socket address should parse"),
            handler: Arc::new(handler),
        })
        .await
        .expect("managed surface launcher should start HTTP probe surface");

    #[cfg(not(feature = "transport-quic"))]
    let ManagedTransportBinding::Socket(addr) = binding;
    #[cfg(feature = "transport-quic")]
    let addr = match binding {
        ManagedTransportBinding::Socket(addr) => addr,
        ManagedTransportBinding::Quic(_) => panic!("expected socket binding for probe surface"),
    };

    let client = HttpClient::try_new(format!("http://{addr}")).expect("HTTP client should build");
    let health = client
        .get_route(ControlRoute::Health)
        .await
        .expect("probe health route should succeed");
    let readiness = client
        .get_route(ControlRoute::Readiness)
        .await
        .expect("probe readiness route should succeed");

    assert!(matches!(health, HttpResponsePayload::Health(_)));
    assert!(matches!(readiness, HttpResponsePayload::Readiness(_)));

    handle.abort();
}

#[tokio::test]
async fn managed_http_probe_surface_exposes_metrics_without_control_routes() {
    let app = NodeApp::builder()
        .config(NodeConfig {
            node_id: NodeId::new("node.surface.metrics"),
            http_bind_addr: "127.0.0.1:0".parse().expect("socket address should parse"),
            ipc_socket_path: NodeConfig::default_ipc_socket_path_for("node.surface.metrics"),
            reconcile_interval: Duration::from_millis(10),
            state_dir: None,
            peers: Vec::new(),
            peer_authentication: PeerAuthenticationMode::Optional,
            peer_sync_execution: NodeConfig::try_peer_sync_execution_from_env()
                .expect("peer sync execution defaults should parse"),
            ipc_stream_heartbeat_interval: NodeConfig::default_ipc_stream_heartbeat_interval(),
            ipc_stream_heartbeat_timeout: NodeConfig::default_ipc_stream_heartbeat_timeout(),
            runtime_tuning: NodeConfig::try_runtime_tuning_from_env()
                .expect("runtime tuning defaults should parse"),
        })
        .with_http_mutual_tls_mode(crate::app::HttpMutualTlsMode::Required)
        .try_build()
        .expect("node app should build");

    let (binding, handle) = app
        .start_managed_surface(ManagedSurfaceLaunchRequest::HttpProbe {
            addr: "127.0.0.1:0".parse().expect("socket address should parse"),
            handler: Arc::new(app.http_probe_handler()),
        })
        .await
        .expect("managed probe metrics surface should start");

    #[cfg(not(feature = "transport-quic"))]
    let ManagedTransportBinding::Socket(addr) = binding;
    #[cfg(feature = "transport-quic")]
    let addr = match binding {
        ManagedTransportBinding::Socket(addr) => addr,
        ManagedTransportBinding::Quic(_) => panic!("expected socket binding for probe surface"),
    };

    let metrics = reqwest::get(format!("http://{addr}/metrics"))
        .await
        .expect("probe metrics request should send");
    assert_eq!(metrics.status(), reqwest::StatusCode::OK);
    assert_eq!(
        metrics
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok()),
        Some("text/plain; version=0.0.4; charset=utf-8")
    );
    let body = metrics.text().await.expect("metrics body should read");
    assert!(body.contains("orion_peer_count"));
    assert!(body.contains("orion_communication_messages_sent_total"));

    let control = reqwest::Client::new()
        .post(format!(
            "http://{addr}{}",
            ControlRoute::Observability.path()
        ))
        .body(Vec::new())
        .send()
        .await
        .expect("probe control request should send");
    assert_eq!(control.status(), reqwest::StatusCode::NOT_FOUND);

    let snapshot = app.observability_snapshot();
    let endpoint = snapshot
        .communication
        .iter()
        .find(|endpoint| endpoint.id == "http/metrics")
        .expect("metrics endpoint communication should be recorded");
    assert_eq!(endpoint.transport, "http");
    assert_eq!(endpoint.scope, "metrics");
    assert_eq!(endpoint.metrics.messages_sent_total, 1);
    assert_eq!(endpoint.metrics.messages_received_total, 1);
    assert!(endpoint.metrics.bytes_sent_total > 0);

    handle.abort();
}

#[cfg(feature = "transport-tcp")]
#[tokio::test]
async fn managed_surface_launcher_starts_named_tcp_surface() {
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

    let state_dir = temp_state_dir("managed-surface-launcher-tcp");
    let node_id = NodeId::new("node.surface.launcher");
    let app = NodeApp::builder()
        .config(NodeConfig {
            node_id: node_id.clone(),
            http_bind_addr: "127.0.0.1:0".parse().expect("socket address should parse"),
            ipc_socket_path: NodeConfig::default_ipc_socket_path_for("node.surface.launcher"),
            reconcile_interval: Duration::from_millis(10),
            state_dir: Some(state_dir.clone()),
            peers: Vec::new(),
            peer_authentication: PeerAuthenticationMode::Optional,
            peer_sync_execution: NodeConfig::try_peer_sync_execution_from_env()
                .expect("peer sync execution defaults should parse"),
            ipc_stream_heartbeat_interval: NodeConfig::default_ipc_stream_heartbeat_interval(),
            ipc_stream_heartbeat_timeout: NodeConfig::default_ipc_stream_heartbeat_timeout(),
            runtime_tuning: NodeConfig::try_runtime_tuning_from_env()
                .expect("runtime tuning defaults should parse"),
        })
        .with_auto_http_tls(true)
        .with_http_mutual_tls_mode(crate::app::HttpMutualTlsMode::Required)
        .try_build()
        .expect("node app should build");

    let local_cert_pem = fs::read(
        app.http_tls_cert_path()
            .expect("local auto TLS cert path should exist"),
    )
    .expect("local TLS cert should be readable");
    app.trust_peer_tls_root_cert_pem(&node_id, local_cert_pem)
        .expect("local TLS cert should be trusted for loopback mtls");

    let (binding, handle) = app
        .start_managed_surface(ManagedSurfaceLaunchRequest::PeerTcpData {
            addr: "127.0.0.1:0".parse().expect("socket address should parse"),
            handler: Arc::new(EchoHandler),
        })
        .await
        .expect("managed surface launcher should start TCP surface");

    #[cfg(not(feature = "transport-quic"))]
    let ManagedTransportBinding::Socket(addr) = binding;
    #[cfg(feature = "transport-quic")]
    let addr = match binding {
        ManagedTransportBinding::Socket(addr) => addr,
        ManagedTransportBinding::Quic(_) => panic!("expected socket binding for TCP surface"),
    };

    let tls = match app
        .managed_surface_client_transport_security(
            ManagedNodeTransportSurface::PeerTcpData,
            &node_id,
            None,
            Some("localhost"),
            None,
        )
        .expect("managed TCP client config should resolve")
        .expect("managed TCP client TLS config should exist")
    {
        ManagedClientTransportSecurity::Tcp(config) => config,
        _ => panic!("expected managed TCP client config"),
    };

    let client = TcpFrameClient::with_tls(addr, tls);
    let response = client
        .send(tcp_test_frame())
        .await
        .expect("managed TCP frame should roundtrip via generic launcher");

    assert_eq!(response.payload, vec![1, 2, 3, 4]);

    handle.abort();
    let _ = fs::remove_dir_all(state_dir);
}

#[cfg(feature = "transport-quic")]
#[tokio::test]
async fn managed_surface_launcher_starts_named_quic_surface() {
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

    let state_dir = temp_state_dir("managed-surface-launcher-quic");
    let node_id = NodeId::new("node.surface.quic");
    let app = NodeApp::builder()
        .config(NodeConfig {
            node_id: node_id.clone(),
            http_bind_addr: "127.0.0.1:0".parse().expect("socket address should parse"),
            ipc_socket_path: NodeConfig::default_ipc_socket_path_for("node.surface.quic"),
            reconcile_interval: Duration::from_millis(10),
            state_dir: Some(state_dir.clone()),
            peers: Vec::new(),
            peer_authentication: PeerAuthenticationMode::Optional,
            peer_sync_execution: NodeConfig::try_peer_sync_execution_from_env()
                .expect("peer sync execution defaults should parse"),
            ipc_stream_heartbeat_interval: NodeConfig::default_ipc_stream_heartbeat_interval(),
            ipc_stream_heartbeat_timeout: NodeConfig::default_ipc_stream_heartbeat_timeout(),
            runtime_tuning: NodeConfig::try_runtime_tuning_from_env()
                .expect("runtime tuning defaults should parse"),
        })
        .with_auto_http_tls(true)
        .with_http_mutual_tls_mode(crate::app::HttpMutualTlsMode::Required)
        .try_build()
        .expect("node app should build");

    let local_cert_pem = fs::read(
        app.http_tls_cert_path()
            .expect("local auto TLS cert path should exist"),
    )
    .expect("local TLS cert should be readable");
    app.trust_peer_tls_root_cert_pem(&node_id, local_cert_pem)
        .expect("local TLS cert should be trusted for loopback mtls");

    let (binding, handle) = app
        .start_managed_surface(ManagedSurfaceLaunchRequest::PeerQuicData {
            addr: "127.0.0.1:0".parse().expect("socket address should parse"),
            handler: Arc::new(EchoHandler),
            server_name: Some("localhost".into()),
        })
        .await
        .expect("managed surface launcher should start QUIC surface");

    let endpoint = match binding {
        ManagedTransportBinding::Quic(endpoint) => endpoint,
        ManagedTransportBinding::Socket(_) => panic!("expected QUIC binding for QUIC surface"),
    };

    let tls = match app
        .managed_surface_client_transport_security(
            ManagedNodeTransportSurface::PeerQuicData,
            &node_id,
            None,
            None,
            None,
        )
        .expect("managed QUIC client config should resolve")
        .expect("managed QUIC client TLS config should exist")
    {
        ManagedClientTransportSecurity::Quic(config) => config,
        _ => panic!("expected managed QUIC client config"),
    };

    let client =
        QuicFrameClient::with_tls(endpoint, &tls).expect("managed QUIC client should build");
    let response = client
        .send(quic_test_frame())
        .await
        .expect("managed QUIC frame should roundtrip via generic launcher");

    assert_eq!(response.payload, vec![1, 2, 3, 4]);

    handle.abort();
    let _ = fs::remove_dir_all(state_dir);
}

#[cfg(feature = "transport-tcp")]
#[tokio::test]
async fn managed_tcp_transport_adapter_starts_secure_data_surface() {
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

    let state_dir = temp_state_dir("managed-tcp-surface");
    let node_id = NodeId::new("node.tcp.surface");
    let app = NodeApp::builder()
        .config(NodeConfig {
            node_id: node_id.clone(),
            http_bind_addr: "127.0.0.1:0".parse().expect("socket address should parse"),
            ipc_socket_path: NodeConfig::default_ipc_socket_path_for("node.tcp.surface"),
            reconcile_interval: Duration::from_millis(10),
            state_dir: Some(state_dir.clone()),
            peers: Vec::new(),
            peer_authentication: PeerAuthenticationMode::Optional,
            peer_sync_execution: NodeConfig::try_peer_sync_execution_from_env()
                .expect("peer sync execution defaults should parse"),
            ipc_stream_heartbeat_interval: NodeConfig::default_ipc_stream_heartbeat_interval(),
            ipc_stream_heartbeat_timeout: NodeConfig::default_ipc_stream_heartbeat_timeout(),
            runtime_tuning: NodeConfig::try_runtime_tuning_from_env()
                .expect("runtime tuning defaults should parse"),
        })
        .with_auto_http_tls(true)
        .with_http_mutual_tls_mode(crate::app::HttpMutualTlsMode::Required)
        .try_build()
        .expect("node app should build");

    let local_cert_pem = fs::read(
        app.http_tls_cert_path()
            .expect("local auto TLS cert path should exist"),
    )
    .expect("local TLS cert should be readable");
    app.trust_peer_tls_root_cert_pem(&node_id, local_cert_pem)
        .expect("local TLS cert should be trusted for loopback mtls");

    let (addr, handle) = app
        .start_tcp_data_server(
            "127.0.0.1:0".parse().expect("socket address should parse"),
            Arc::new(EchoHandler),
        )
        .await
        .expect("managed TCP data surface should start");

    let tls = match app
        .managed_surface_client_transport_security(
            ManagedNodeTransportSurface::PeerTcpData,
            &node_id,
            None,
            Some("localhost"),
            None,
        )
        .expect("managed TCP client config should resolve")
        .expect("managed TCP client TLS config should exist")
    {
        ManagedClientTransportSecurity::Tcp(config) => config,
        _ => panic!("expected managed TCP client config"),
    };

    let client = TcpFrameClient::with_tls(addr, tls);
    let response = app
        .send_tcp_data_frame_metered(&client, tcp_test_frame())
        .await
        .expect("managed TCP frame should roundtrip");

    assert_eq!(response.payload, vec![1, 2, 3, 4]);
    assert_eq!(response.source, TcpEndpoint::new("127.0.0.1", 4200));

    let snapshot = app.observability_snapshot();
    let endpoint = snapshot
        .communication
        .iter()
        .find(|endpoint| endpoint.id == "tcp/data-plane/node-b/resource.camera")
        .expect("managed TCP data-plane communication endpoint should be reported");
    assert_eq!(endpoint.transport, "tcp");
    assert_eq!(endpoint.scope, "data_plane");
    assert_eq!(
        endpoint.labels.get("peer_node_id").map(String::as_str),
        Some("node-b")
    );
    assert_eq!(
        endpoint.labels.get("resource_id").map(String::as_str),
        Some("resource.camera")
    );
    assert!(endpoint.metrics.messages_received_total >= 2);
    assert!(endpoint.metrics.messages_sent_total >= 2);
    assert!(endpoint.metrics.bytes_received_total > 0);
    assert!(endpoint.metrics.bytes_sent_total > 0);

    handle.abort();
    let _ = fs::remove_dir_all(state_dir);
}

#[cfg(feature = "transport-quic")]
#[tokio::test]
async fn managed_quic_transport_adapter_starts_secure_data_surface() {
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

    let state_dir = temp_state_dir("managed-quic-surface");
    let node_id = NodeId::new("node.quic.surface");
    let app = NodeApp::builder()
        .config(NodeConfig {
            node_id: node_id.clone(),
            http_bind_addr: "127.0.0.1:0".parse().expect("socket address should parse"),
            ipc_socket_path: NodeConfig::default_ipc_socket_path_for("node.quic.surface"),
            reconcile_interval: Duration::from_millis(10),
            state_dir: Some(state_dir.clone()),
            peers: Vec::new(),
            peer_authentication: PeerAuthenticationMode::Optional,
            peer_sync_execution: NodeConfig::try_peer_sync_execution_from_env()
                .expect("peer sync execution defaults should parse"),
            ipc_stream_heartbeat_interval: NodeConfig::default_ipc_stream_heartbeat_interval(),
            ipc_stream_heartbeat_timeout: NodeConfig::default_ipc_stream_heartbeat_timeout(),
            runtime_tuning: NodeConfig::try_runtime_tuning_from_env()
                .expect("runtime tuning defaults should parse"),
        })
        .with_auto_http_tls(true)
        .with_http_mutual_tls_mode(crate::app::HttpMutualTlsMode::Required)
        .try_build()
        .expect("node app should build");

    let local_cert_pem = fs::read(
        app.http_tls_cert_path()
            .expect("local auto TLS cert path should exist"),
    )
    .expect("local TLS cert should be readable");
    app.trust_peer_tls_root_cert_pem(&node_id, local_cert_pem)
        .expect("local TLS cert should be trusted for loopback mtls");

    let (endpoint, handle) = app
        .start_quic_data_server(
            "127.0.0.1:0".parse().expect("socket address should parse"),
            Some("localhost".into()),
            Arc::new(EchoHandler),
        )
        .await
        .expect("managed QUIC data surface should start");

    let tls = match app
        .managed_surface_client_transport_security(
            ManagedNodeTransportSurface::PeerQuicData,
            &node_id,
            None,
            None,
            None,
        )
        .expect("managed QUIC client config should resolve")
        .expect("managed QUIC client TLS config should exist")
    {
        ManagedClientTransportSecurity::Quic(config) => config,
        _ => panic!("expected managed QUIC client config"),
    };

    let client =
        QuicFrameClient::with_tls(endpoint, &tls).expect("managed QUIC client should build");
    let response = app
        .send_quic_data_frame_metered(&client, quic_test_frame())
        .await
        .expect("managed QUIC frame should roundtrip");

    assert_eq!(response.payload, vec![1, 2, 3, 4]);
    assert_eq!(response.source.host, "127.0.0.1");
    assert_eq!(response.source.port, 5200);

    let snapshot = app.observability_snapshot();
    let endpoint = snapshot
        .communication
        .iter()
        .find(|endpoint| endpoint.id == "quic/data-plane/node-b/resource.video")
        .expect("managed QUIC data-plane communication endpoint should be reported");
    assert_eq!(endpoint.transport, "quic");
    assert_eq!(endpoint.scope, "data_plane");
    assert_eq!(
        endpoint.labels.get("peer_node_id").map(String::as_str),
        Some("node-b")
    );
    assert_eq!(
        endpoint.labels.get("resource_id").map(String::as_str),
        Some("resource.video")
    );
    assert!(endpoint.metrics.messages_received_total >= 2);
    assert!(endpoint.metrics.messages_sent_total >= 2);
    assert!(endpoint.metrics.bytes_received_total > 0);
    assert!(endpoint.metrics.bytes_sent_total > 0);

    handle.abort();
    let _ = fs::remove_dir_all(state_dir);
}
