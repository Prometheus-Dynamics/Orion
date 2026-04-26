use super::*;
use orion::transport::ipc::UnixControlHandler;

#[test]
fn observability_reports_host_and_local_communication_metrics() {
    let app = NodeApp::builder()
        .config(NodeConfig {
            node_id: NodeId::new("node-obs-communication"),
            http_bind_addr: "127.0.0.1:0".parse().expect("socket address should parse"),
            ipc_socket_path: NodeConfig::default_ipc_socket_path_for("node-obs-communication"),
            reconcile_interval: Duration::from_millis(10),
            state_dir: None,
            peers: Vec::new(),
            peer_authentication: crate::PeerAuthenticationMode::Disabled,
            peer_sync_execution: NodeConfig::try_peer_sync_execution_from_env()
                .expect("peer sync execution defaults should parse"),
            ipc_stream_heartbeat_interval: NodeConfig::default_ipc_stream_heartbeat_interval(),
            ipc_stream_heartbeat_timeout: NodeConfig::default_ipc_stream_heartbeat_timeout(),
            runtime_tuning: NodeConfig::try_runtime_tuning_from_env()
                .expect("runtime tuning defaults should parse"),
        })
        .try_build()
        .expect("node app should build");

    app.http_control_handler()
        .handle_health()
        .expect("health should be served");
    app.unix_control_handler()
        .handle_control(ControlEnvelope {
            source: LocalAddress::new("orionctl.metrics"),
            destination: LocalAddress::new("orion"),
            message: ControlMessage::ClientHello(ClientHello {
                client_name: "orionctl.metrics".into(),
                role: ClientRole::ControlPlane,
            }),
        })
        .expect("local client hello should be served");

    let snapshot = app.observability_snapshot();
    assert_eq!(snapshot.host.process_id, std::process::id());
    assert!(!snapshot.host.os_name.is_empty());
    assert!(!snapshot.host.architecture.is_empty());

    let http_health = snapshot
        .communication
        .iter()
        .find(|endpoint| endpoint.id == "http/health")
        .expect("HTTP health communication endpoint should be reported");
    assert_eq!(http_health.transport, "http");
    assert_eq!(http_health.scope, "health");
    assert_eq!(http_health.metrics.messages_sent_total, 1);
    assert_eq!(http_health.metrics.messages_received_total, 1);
    assert!(http_health.metrics.bytes_sent_total > 0);
    assert!(
        http_health.metrics.estimated_wire_bytes_sent_total > http_health.metrics.bytes_sent_total
    );
    assert_eq!(http_health.metrics.recent.successes, 1);
    assert_eq!(http_health.metrics.recent.failures, 0);
    assert_eq!(http_health.metrics.latency.samples_total, 1);
    assert_eq!(http_health.metrics.stages.socket_write.samples_total, 1);
    assert_eq!(
        latency_bucket_total(&http_health.metrics.latency),
        http_health.metrics.latency.samples_total
    );

    let ipc_unary = snapshot
        .communication
        .iter()
        .find(|endpoint| endpoint.id == "ipc/local-unary/orionctl.metrics")
        .expect("local unary communication endpoint should be reported");
    assert_eq!(ipc_unary.transport, "ipc");
    assert_eq!(ipc_unary.scope, "local_unary");
    assert_eq!(ipc_unary.metrics.messages_sent_total, 1);
    assert_eq!(ipc_unary.metrics.messages_received_total, 1);
    assert!(ipc_unary.metrics.bytes_sent_total > 0);
    assert!(ipc_unary.metrics.bytes_received_total > 0);
    assert!(ipc_unary.metrics.estimated_wire_bytes_sent_total > ipc_unary.metrics.bytes_sent_total);
    assert_eq!(ipc_unary.metrics.recent.successes, 1);
    assert_eq!(ipc_unary.metrics.stages.encode.samples_total, 1);
    assert_eq!(ipc_unary.metrics.stages.decode.samples_total, 1);
    assert_eq!(ipc_unary.metrics.stages.socket_read.samples_total, 1);
    assert_eq!(ipc_unary.metrics.stages.socket_write.samples_total, 1);
    assert_eq!(
        latency_bucket_total(&ipc_unary.metrics.latency),
        ipc_unary.metrics.latency.samples_total
    );
}

#[test]
fn communication_metrics_classify_failures_and_degrade_health() {
    let app = NodeApp::builder()
        .config(NodeConfig {
            node_id: NodeId::new("node-obs-communication-failure"),
            http_bind_addr: "127.0.0.1:0".parse().expect("socket address should parse"),
            ipc_socket_path: NodeConfig::default_ipc_socket_path_for(
                "node-obs-communication-failure",
            ),
            reconcile_interval: Duration::from_millis(10),
            state_dir: None,
            peers: Vec::new(),
            peer_authentication: crate::PeerAuthenticationMode::Disabled,
            peer_sync_execution: NodeConfig::try_peer_sync_execution_from_env()
                .expect("peer sync execution defaults should parse"),
            ipc_stream_heartbeat_interval: NodeConfig::default_ipc_stream_heartbeat_interval(),
            ipc_stream_heartbeat_timeout: NodeConfig::default_ipc_stream_heartbeat_timeout(),
            runtime_tuning: NodeConfig::try_runtime_tuning_from_env()
                .expect("runtime tuning defaults should parse"),
        })
        .try_build()
        .expect("node app should build");
    let mut endpoint =
        crate::app::CommunicationEndpointRuntime::new("http/probe/test", "http", "probe");
    endpoint.connected = false;

    app.record_communication_endpoint_failure_kind(
        endpoint,
        Some(Duration::from_millis(17)),
        orion::control_plane::CommunicationFailureKind::Timeout,
        "transport wording can change without changing metrics",
    );

    let snapshot = app.observability_snapshot();
    let endpoint = snapshot
        .communication
        .iter()
        .find(|endpoint| endpoint.id == "http/probe/test")
        .expect("failed endpoint should be reported");
    assert_eq!(endpoint.metrics.recent.failures, 1);
    assert_eq!(endpoint.metrics.recent.avg_latency_ms, Some(17));
    assert_eq!(
        endpoint
            .metrics
            .failures_by_kind
            .iter()
            .find(|failure| failure.kind == orion::control_plane::CommunicationFailureKind::Timeout)
            .map(|failure| failure.count),
        Some(1)
    );

    let health = app.health_snapshot();
    assert!(matches!(
        health.status,
        orion::control_plane::NodeHealthStatus::Degraded
    ));
    assert!(
        health
            .reasons
            .iter()
            .any(|reason| reason.contains("communication endpoints degraded"))
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn observability_reports_ipc_stream_communication_metrics() {
    let state_dir = temp_state_dir("observability-ipc-stream");
    let app = NodeApp::builder()
        .config(test_node_config_with_state_dir_and_auth(
            "node-obs-ipc-stream",
            "node-obs-ipc-stream",
            state_dir.clone(),
            crate::PeerAuthenticationMode::Disabled,
        ))
        .try_build()
        .expect("node app should build");
    let socket_path = state_dir.join("control-stream.sock");
    let (_path, handle) = app
        .start_ipc_stream_server(&socket_path)
        .await
        .expect("IPC stream server should start");

    let mut client = UnixControlStreamClient::connect(&socket_path)
        .await
        .expect("IPC stream client should connect");
    client
        .send(&ControlEnvelope {
            source: LocalAddress::new("executor.metrics"),
            destination: LocalAddress::new("orion"),
            message: ControlMessage::ClientHello(ClientHello {
                client_name: "executor.metrics".into(),
                role: ClientRole::Executor,
            }),
        })
        .await
        .expect("client hello should send");
    let welcome = client
        .recv()
        .await
        .expect("welcome read should succeed")
        .expect("welcome frame should be returned");
    assert!(matches!(welcome.message, ControlMessage::ClientWelcome(_)));

    let snapshot = app.observability_snapshot();
    let ipc_stream = snapshot
        .communication
        .iter()
        .find(|endpoint| endpoint.id == "ipc/local-stream/executor.metrics")
        .expect("local stream communication endpoint should be reported");
    assert_eq!(ipc_stream.transport, "ipc");
    assert_eq!(ipc_stream.scope, "local_stream");
    assert!(ipc_stream.connected);
    assert_eq!(ipc_stream.metrics.messages_sent_total, 1);
    assert_eq!(ipc_stream.metrics.messages_received_total, 1);
    assert!(ipc_stream.metrics.bytes_sent_total > 0);
    assert!(ipc_stream.metrics.bytes_received_total > 0);
    assert_eq!(
        ipc_stream.labels.get("client_name").map(String::as_str),
        Some("executor.metrics")
    );
    assert!(ipc_stream.labels.contains_key("uid"));
    assert!(ipc_stream.labels.contains_key("gid"));

    handle.abort();
    let _ = fs::remove_dir_all(state_dir);
}

fn latency_bucket_total(latency: &orion::control_plane::LatencyMetricsSnapshot) -> u64 {
    latency.bucket_le_1_ms
        + latency.bucket_le_5_ms
        + latency.bucket_le_10_ms
        + latency.bucket_le_50_ms
        + latency.bucket_le_100_ms
        + latency.bucket_le_500_ms
        + latency.bucket_gt_500_ms
}
