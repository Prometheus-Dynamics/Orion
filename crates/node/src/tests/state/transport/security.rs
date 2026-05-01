use super::*;

#[test]
fn transport_security_manager_centralizes_http_tcp_and_quic_trust_material() {
    let state_dir = temp_state_dir("transport-security");
    let app = NodeApp::builder()
        .config(NodeConfig {
            node_id: NodeId::new("node.transport"),
            http_bind_addr: "127.0.0.1:0".parse().expect("socket address should parse"),
            ipc_socket_path: NodeConfig::default_ipc_socket_path_for("node.transport"),
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
        .with_http_mutual_tls_mode(crate::app::HttpMutualTlsMode::Optional)
        .try_build()
        .expect("node app should build");

    let peer_cert = generate_simple_self_signed(vec!["node-peer.local".to_owned()])
        .expect("peer cert should generate");
    let peer_cert_pem = peer_cert.cert.pem().into_bytes();
    app.trust_peer_tls_root_cert_pem(&NodeId::new("node-peer"), peer_cert_pem.clone())
        .expect("peer TLS root should be trusted");

    let local_cert_pem = fs::read(
        app.http_tls_cert_path()
            .expect("local auto TLS cert path should exist"),
    )
    .expect("local TLS cert should be readable");

    #[cfg(not(any(feature = "transport-tcp", feature = "transport-quic")))]
    let ManagedClientTransportSecurity::Http(http_client) = app
        .managed_surface_client_transport_security(
            ManagedNodeTransportSurface::PeerHttpControl,
            &NodeId::new("node-peer"),
            None,
            None,
            Some("https://node-peer:9443"),
        )
        .expect("http client config should resolve")
        .expect("http client TLS config should exist");
    #[cfg(any(feature = "transport-tcp", feature = "transport-quic"))]
    let http_client = match app
        .managed_surface_client_transport_security(
            ManagedNodeTransportSurface::PeerHttpControl,
            &NodeId::new("node-peer"),
            None,
            None,
            Some("https://node-peer:9443"),
        )
        .expect("http client config should resolve")
        .expect("http client TLS config should exist")
    {
        ManagedClientTransportSecurity::Http(config) => config,
        _ => panic!("expected managed HTTP client config"),
    };
    #[cfg(feature = "transport-tcp")]
    let tcp_client = match app
        .managed_surface_client_transport_security(
            ManagedNodeTransportSurface::PeerTcpData,
            &NodeId::new("node-peer"),
            None,
            Some("node-peer.local"),
            None,
        )
        .expect("tcp client config should resolve")
        .expect("tcp client TLS config should exist")
    {
        ManagedClientTransportSecurity::Tcp(config) => config,
        _ => panic!("expected managed TCP client config"),
    };
    #[cfg(feature = "transport-quic")]
    let quic_client = match app
        .managed_surface_client_transport_security(
            ManagedNodeTransportSurface::PeerQuicData,
            &NodeId::new("node-peer"),
            None,
            None,
            None,
        )
        .expect("quic client config should resolve")
        .expect("quic client TLS config should exist")
    {
        ManagedClientTransportSecurity::Quic(config) => config,
        _ => panic!("expected managed QUIC client config"),
    };

    assert_eq!(http_client.root_cert_pem, peer_cert_pem);
    #[cfg(feature = "transport-tcp")]
    assert_eq!(tcp_client.root_cert_pem, peer_cert_pem);
    #[cfg(feature = "transport-quic")]
    assert_eq!(quic_client.root_cert_pem, peer_cert_pem);
    assert_eq!(http_client.client_cert_pem, Some(local_cert_pem.clone()));
    #[cfg(feature = "transport-tcp")]
    assert_eq!(tcp_client.client_cert_pem, Some(local_cert_pem.clone()));
    #[cfg(feature = "transport-quic")]
    assert_eq!(quic_client.client_cert_pem, Some(local_cert_pem));

    #[cfg(not(any(feature = "transport-tcp", feature = "transport-quic")))]
    let ManagedServerTransportSecurity::Http(http_server) = app
        .managed_surface_server_transport_security(ManagedNodeTransportSurface::PeerHttpControl)
        .expect("http server config should resolve")
        .expect("http server TLS config should exist");
    #[cfg(any(feature = "transport-tcp", feature = "transport-quic"))]
    let http_server = match app
        .managed_surface_server_transport_security(ManagedNodeTransportSurface::PeerHttpControl)
        .expect("http server config should resolve")
        .expect("http server TLS config should exist")
    {
        ManagedServerTransportSecurity::Http(config) => config,
        _ => panic!("expected managed HTTP server config"),
    };
    #[cfg(feature = "transport-tcp")]
    let tcp_server = match app
        .managed_surface_server_transport_security(ManagedNodeTransportSurface::PeerTcpData)
        .expect("tcp server config should resolve")
        .expect("tcp server TLS config should exist")
    {
        ManagedServerTransportSecurity::Tcp(config) => config,
        _ => panic!("expected managed TCP server config"),
    };
    #[cfg(feature = "transport-quic")]
    let quic_server = match app
        .managed_surface_server_transport_security(ManagedNodeTransportSurface::PeerQuicData)
        .expect("quic server config should resolve")
        .expect("quic server TLS config should exist")
    {
        ManagedServerTransportSecurity::Quic(config) => config,
        _ => panic!("expected managed QUIC server config"),
    };

    assert!(matches!(
        http_server.client_auth,
        orion::transport::http::HttpServerClientAuth::OptionalDynamic { .. }
    ));
    #[cfg(feature = "transport-tcp")]
    assert!(matches!(
        tcp_server.client_auth,
        orion::transport::tcp::TcpServerClientAuth::Optional { .. }
    ));
    #[cfg(feature = "transport-quic")]
    assert!(matches!(
        quic_server.client_auth,
        orion::transport::quic::QuicServerClientAuth::Optional { .. }
    ));

    let _ = fs::remove_dir_all(state_dir);
}

#[test]
fn managed_http_server_surface_uses_generic_transport_security_api() {
    let state_dir = temp_state_dir("managed-http-surface");
    let app = NodeApp::builder()
        .config(NodeConfig {
            node_id: NodeId::new("node.http.surface"),
            http_bind_addr: "127.0.0.1:0".parse().expect("socket address should parse"),
            ipc_socket_path: NodeConfig::default_ipc_socket_path_for("node.http.surface"),
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

    match app
        .managed_surface_server_transport_security(ManagedNodeTransportSurface::PeerHttpControl)
        .expect("managed HTTP server config should resolve")
        .expect("managed HTTP server config should exist")
    {
        ManagedServerTransportSecurity::Http(config) => {
            assert!(matches!(
                config.client_auth,
                orion::transport::http::HttpServerClientAuth::RequiredDynamic { .. }
            ));
        }
        #[cfg(any(feature = "transport-tcp", feature = "transport-quic"))]
        _ => panic!("expected managed HTTP server config"),
    }

    let _ = fs::remove_dir_all(state_dir);
}

#[test]
fn managed_probe_surface_explicitly_skips_transport_security() {
    let state_dir = temp_state_dir("managed-probe-surface");
    let app = NodeApp::builder()
        .config(NodeConfig {
            node_id: NodeId::new("node.probe.surface"),
            http_bind_addr: "127.0.0.1:0".parse().expect("socket address should parse"),
            ipc_socket_path: NodeConfig::default_ipc_socket_path_for("node.probe.surface"),
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

    let probe = app
        .managed_surface_server_transport_security(ManagedNodeTransportSurface::HttpProbe)
        .expect("managed probe surface should resolve");
    assert!(
        probe.is_none(),
        "probe surface should not attach transport security"
    );

    let _ = fs::remove_dir_all(state_dir);
}
