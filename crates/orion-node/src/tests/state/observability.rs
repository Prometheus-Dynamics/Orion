use super::*;
use crate::config::AuditLogOverloadPolicy;
use orion::control_plane::DesiredStateMutation;
use orion::transport::http::HttpResponsePayload;
use orion::transport::ipc::UnixControlHandler;

#[test]
fn observability_tracks_mutation_failures_and_replay_timing() {
    let state_dir = temp_state_dir("observability");
    let app = NodeApp::builder()
        .config(NodeConfig {
            node_id: NodeId::new("node-obs"),
            http_bind_addr: "127.0.0.1:0".parse().expect("socket address should parse"),
            ipc_socket_path: NodeConfig::default_ipc_socket_path_for("node-test"),
            reconcile_interval: Duration::from_millis(10),
            state_dir: Some(state_dir.clone()),
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

    let err = app
        .http_control_handler()
        .handle_payload(
            app.security
                .wrap_http_payload(orion::transport::http::HttpRequestPayload::Control(
                    Box::new(ControlMessage::Mutations(MutationBatch {
                        base_revision: Revision::new(42),
                        mutations: Vec::new(),
                    })),
                ))
                .expect("mutation request should be signed"),
        )
        .expect_err("stale batch should be rejected");
    assert!(
        err.to_string()
            .contains("configured peer identity is required")
            || err.to_string().contains("revision mismatch")
    );

    let success = app
        .http_control_handler()
        .handle_payload(
            app.security
                .wrap_http_payload(orion::transport::http::HttpRequestPayload::Control(
                    Box::new(ControlMessage::Mutations(MutationBatch {
                        base_revision: Revision::ZERO,
                        mutations: vec![DesiredStateMutation::PutArtifact(
                            ArtifactRecord::builder("artifact.obs").build(),
                        )],
                    })),
                ))
                .expect("mutation request should be signed"),
        )
        .expect("valid mutation should be accepted");
    assert_eq!(success, HttpResponsePayload::Accepted);

    app.persist_state().expect("state should persist");

    let replayed = NodeApp::builder()
        .config(NodeConfig {
            node_id: NodeId::new("node-obs"),
            http_bind_addr: "127.0.0.1:0".parse().expect("socket address should parse"),
            ipc_socket_path: NodeConfig::default_ipc_socket_path_for("node-test"),
            reconcile_interval: Duration::from_millis(10),
            state_dir: Some(state_dir.clone()),
            peers: Vec::new(),
            peer_authentication: crate::PeerAuthenticationMode::Disabled,
            peer_sync_execution: NodeConfig::try_peer_sync_execution_from_env()
                .expect("peer sync execution defaults should parse"),
            ipc_stream_heartbeat_interval: NodeConfig::default_ipc_stream_heartbeat_interval(),
            ipc_stream_heartbeat_timeout: NodeConfig::default_ipc_stream_heartbeat_timeout(),
            runtime_tuning: NodeConfig::try_runtime_tuning_from_env()
                .expect("runtime tuning defaults should parse"),
        })
        .with_startup_replay(false)
        .try_build()
        .expect("node app should build");
    let _ = replayed.replay_state().expect("replay should succeed");

    let snapshot = app.observability_snapshot();
    assert_eq!(snapshot.mutation_apply.failure_count, 1);
    assert_eq!(snapshot.mutation_apply.success_count, 1);
    assert_eq!(snapshot.mutation_apply.last_error_category, None);
    assert!(snapshot.recent_events.iter().any(|event| {
        matches!(
            event.kind,
            orion::control_plane::ObservabilityEventKind::MutationApply
        ) && !event.success
    }));
    assert!(snapshot.recent_events.iter().any(|event| {
        matches!(
            event.kind,
            orion::control_plane::ObservabilityEventKind::MutationApply
        ) && event.success
    }));

    let replay_snapshot = replayed.observability_snapshot();
    assert_eq!(replay_snapshot.replay.success_count, 1);
    assert!(replay_snapshot.replay.last_duration_ms.is_some());
    assert_eq!(replay_snapshot.replay.last_error_category, None);
    assert!(replay_snapshot.recent_events.iter().any(|event| {
        matches!(
            event.kind,
            orion::control_plane::ObservabilityEventKind::Replay
        ) && event.success
    }));

    let _ = fs::remove_dir_all(state_dir);
}

#[test]
fn observability_tracks_http_tls_failures() {
    let app = NodeApp::builder()
        .config(NodeConfig {
            node_id: NodeId::new("node-obs-tls"),
            http_bind_addr: "127.0.0.1:0".parse().expect("socket address should parse"),
            ipc_socket_path: NodeConfig::default_ipc_socket_path_for("node-obs-tls"),
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

    app.record_http_transport_error(&HttpTransportError::Tls(
        "tls handshake failed: peer sent no certificates".into(),
    ));

    let snapshot = app.observability_snapshot();
    assert_eq!(snapshot.transport.http_tls_failures, 1);
    assert_eq!(snapshot.transport.http_tls_client_auth_failures, 1);
    assert!(snapshot.recent_events.iter().any(|event| {
        matches!(
            event.kind,
            orion::control_plane::ObservabilityEventKind::TransportSecurity
        ) && !event.success
            && event.detail.contains("http tls failure")
    }));
}

#[test]
fn observability_tracks_non_tls_http_and_ipc_transport_failures() {
    let app = NodeApp::builder()
        .config(NodeConfig {
            node_id: NodeId::new("node-obs-transport"),
            http_bind_addr: "127.0.0.1:0".parse().expect("socket address should parse"),
            ipc_socket_path: NodeConfig::default_ipc_socket_path_for("node-obs-transport"),
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

    app.record_http_transport_error(&HttpTransportError::request_failed(
        "peer connection refused",
    ));
    app.record_http_transport_error(&HttpTransportError::DecodeRequest(
        "invalid request body".into(),
    ));
    app.record_ipc_transport_error(&orion::transport::ipc::IpcTransportError::DecodeFailed(
        "invalid frame payload".into(),
    ));

    let snapshot = app.observability_snapshot();
    assert_eq!(snapshot.transport.http_request_failures, 1);
    assert_eq!(snapshot.transport.http_malformed_input_count, 1);
    assert_eq!(snapshot.transport.ipc_frame_failures, 1);
    assert_eq!(snapshot.transport.ipc_malformed_input_count, 1);
    assert_eq!(snapshot.audit_log_queue_capacity, 1024);
    assert_eq!(snapshot.audit_log_dropped_records, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn observability_tracks_persistence_latency_and_worker_backpressure() {
    let state_dir = temp_state_dir("observability-persistence");
    let app = NodeApp::builder()
        .config(test_node_config_with_state_dir_and_auth(
            "node-obs-persist",
            "node-obs-persist",
            state_dir.clone(),
            crate::PeerAuthenticationMode::Disabled,
        ))
        .with_persistence_worker_queue_capacity(1)
        .try_build()
        .expect("node app should build");

    let artifact_a = ArtifactRecord::builder("artifact.obs.a")
        .content_type("application/octet-stream")
        .size_bytes(1)
        .build();
    let artifact_b = ArtifactRecord::builder("artifact.obs.b")
        .content_type("application/octet-stream")
        .size_bytes(1)
        .build();

    set_test_persist_delay(Duration::from_millis(40));

    let app_a = app.clone();
    let write_a =
        tokio::spawn(async move { app_a.put_artifact_content_async(&artifact_a, &[1]).await });
    let app_b = app.clone();
    let write_b =
        tokio::spawn(async move { app_b.put_artifact_content_async(&artifact_b, &[2]).await });

    write_a
        .await
        .expect("first artifact write task should join")
        .expect("first artifact write should succeed");
    write_b
        .await
        .expect("second artifact write task should join")
        .expect("second artifact write should succeed");

    clear_test_persist_delay();

    let snapshot = app.observability_snapshot();
    assert!(snapshot.persistence.state_persist.success_count >= 4);
    assert_eq!(snapshot.persistence.artifact_write.success_count, 2);
    assert!(
        snapshot
            .persistence
            .state_persist
            .last_duration_ms
            .is_some()
    );
    assert!(
        snapshot
            .persistence
            .artifact_write
            .last_duration_ms
            .is_some()
    );
    assert_eq!(snapshot.persistence.worker_queue_capacity, 1);
    assert!(snapshot.persistence.worker_enqueue_count >= 6);
    assert!(snapshot.persistence.worker_enqueue_wait_count > 0);
    assert!(snapshot.persistence.worker_enqueue_wait_ms_total > 0);
    assert!(snapshot.persistence.worker_enqueue_wait_ms_max > 0);
    assert!(snapshot.recent_events.iter().any(|event| {
        matches!(
            event.kind,
            orion::control_plane::ObservabilityEventKind::Persistence
        )
    }));

    let _ = fs::remove_dir_all(state_dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn control_queries_remain_fast_under_persistence_and_audit_pressure() {
    let state_dir = temp_state_dir("control-pressure");
    let audit_log_path = state_dir.join("audit").join("events.jsonl");
    let mut config = test_node_config_with_state_dir_and_auth(
        "node-control-pressure",
        "node-control-pressure",
        state_dir.clone(),
        crate::PeerAuthenticationMode::Disabled,
    );
    config.runtime_tuning = config
        .runtime_tuning
        .with_persistence_worker_queue_capacity(1)
        .with_audit_log_queue_capacity(1)
        .with_audit_log_overload_policy(AuditLogOverloadPolicy::DropNewest);
    let app = NodeApp::builder()
        .config(config)
        .with_audit_log_path(&audit_log_path)
        .try_build()
        .expect("node app should build");

    let artifact_a = ArtifactRecord::builder("artifact.pressure.a")
        .content_type("application/octet-stream")
        .size_bytes(1)
        .build();
    let artifact_b = ArtifactRecord::builder("artifact.pressure.b")
        .content_type("application/octet-stream")
        .size_bytes(1)
        .build();

    set_test_persist_delay(Duration::from_millis(40));
    set_test_audit_append_delay(Duration::from_millis(40));

    let persist_a = {
        let app = app.clone();
        tokio::spawn(async move { app.put_artifact_content_async(&artifact_a, &[1]).await })
    };
    let persist_b = {
        let app = app.clone();
        tokio::spawn(async move { app.put_artifact_content_async(&artifact_b, &[2]).await })
    };
    let audit_spam = {
        let app = app.clone();
        tokio::spawn(async move {
            for idx in 0..8 {
                app.enroll_peer(PeerConfig::new(
                    format!("peer-pressure-{idx}"),
                    format!("http://127.0.0.1:{}", 4100 + idx),
                ))
                .expect("peer enrollment should succeed");
            }
        })
    };

    tokio::time::sleep(Duration::from_millis(5)).await;

    let http_started = std::time::Instant::now();
    let http_response = app
        .http_control_handler()
        .handle_payload(HttpRequestPayload::Control(Box::new(
            ControlMessage::QueryObservability,
        )))
        .expect("observability query should succeed");
    assert!(
        http_started.elapsed() < Duration::from_millis(30),
        "HTTP observability query should stay fast under worker pressure"
    );
    assert!(matches!(
        http_response,
        HttpResponsePayload::Observability(_)
    ));

    let ipc_started = std::time::Instant::now();
    let ipc_response = app
        .unix_control_handler()
        .handle_control(ControlEnvelope {
            source: LocalAddress::new("client"),
            destination: LocalAddress::new("orion"),
            message: ControlMessage::Ping,
        })
        .expect("local ping should succeed");
    assert!(
        ipc_started.elapsed() < Duration::from_millis(30),
        "local IPC ping should stay fast under worker pressure"
    );
    assert_eq!(ipc_response.message, ControlMessage::Pong);

    persist_a
        .await
        .expect("first persistence task should join")
        .expect("first persistence task should succeed");
    persist_b
        .await
        .expect("second persistence task should join")
        .expect("second persistence task should succeed");
    audit_spam.await.expect("audit pressure task should join");

    clear_test_persist_delay();
    clear_test_audit_append_delay();

    let snapshot = app.observability_snapshot();
    assert!(snapshot.persistence.worker_enqueue_wait_count > 0);
    assert!(snapshot.audit_log_dropped_records > 0);

    let _ = fs::remove_dir_all(state_dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_peer_sync_persistence_and_local_control_activity_remains_responsive() {
    let state_dir = temp_state_dir("peer-sync-persistence-control");
    let app = NodeApp::builder()
        .config(test_node_config_with_state_dir_and_auth(
            "node-concurrency-obs",
            "node-concurrency-obs",
            state_dir.clone(),
            crate::PeerAuthenticationMode::Disabled,
        ))
        .with_persistence_worker_queue_capacity(1)
        .try_build()
        .expect("node app should build");
    app.enroll_peer(PeerConfig::new("peer-unreachable", "http://127.0.0.1:9"))
        .expect("peer enrollment should succeed");

    let artifact_a = ArtifactRecord::builder("artifact.concurrent.a")
        .content_type("application/octet-stream")
        .size_bytes(1)
        .build();
    let artifact_b = ArtifactRecord::builder("artifact.concurrent.b")
        .content_type("application/octet-stream")
        .size_bytes(1)
        .build();

    set_test_persist_delay(Duration::from_millis(40));

    let persist_a = {
        let app = app.clone();
        tokio::spawn(async move { app.put_artifact_content_async(&artifact_a, &[1]).await })
    };
    let persist_b = {
        let app = app.clone();
        tokio::spawn(async move { app.put_artifact_content_async(&artifact_b, &[2]).await })
    };
    let sync_task = {
        let app = app.clone();
        tokio::spawn(async move { app.sync_peer(&NodeId::new("peer-unreachable")).await })
    };

    tokio::time::sleep(Duration::from_millis(5)).await;

    for _ in 0..4 {
        let http_started = std::time::Instant::now();
        let http_response = app
            .http_control_handler()
            .handle_payload(HttpRequestPayload::Control(Box::new(
                ControlMessage::QueryObservability,
            )))
            .expect("observability query should succeed");
        assert!(
            http_started.elapsed() < Duration::from_millis(30),
            "HTTP observability query should stay fast while peer sync and persistence are active"
        );
        assert!(matches!(
            http_response,
            HttpResponsePayload::Observability(_)
        ));

        let ipc_started = std::time::Instant::now();
        let ipc_response = app
            .unix_control_handler()
            .handle_control(ControlEnvelope {
                source: LocalAddress::new("client"),
                destination: LocalAddress::new("orion"),
                message: ControlMessage::Ping,
            })
            .expect("local ping should succeed");
        assert!(
            ipc_started.elapsed() < Duration::from_millis(30),
            "local IPC ping should stay fast while peer sync and persistence are active"
        );
        assert_eq!(ipc_response.message, ControlMessage::Pong);
    }

    persist_a
        .await
        .expect("first persistence task should join")
        .expect("first persistence task should succeed");
    persist_b
        .await
        .expect("second persistence task should join")
        .expect("second persistence task should succeed");
    let sync_result = sync_task.await.expect("peer sync task should join");

    clear_test_persist_delay();

    assert!(
        sync_result.is_err(),
        "synthetic peer sync should fail against the unreachable endpoint"
    );

    let snapshot = app.observability_snapshot();
    assert!(snapshot.peer_sync.failure_count >= 1);
    assert_eq!(snapshot.persistence.worker_queue_capacity, 1);
    assert!(snapshot.persistence.worker_enqueue_wait_count > 0);
    assert!(snapshot.persistence.worker_enqueue_wait_ms_total > 0);
    assert!(snapshot.persistence.state_persist.success_count >= 4);

    let _ = fs::remove_dir_all(state_dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn saturated_persistence_queue_exposes_backpressure_metrics_with_bounded_completion() {
    let state_dir = temp_state_dir("persistence-saturation");
    let app = NodeApp::builder()
        .config(test_node_config_with_state_dir_and_auth(
            "node-persist-saturation",
            "node-persist-saturation",
            state_dir.clone(),
            crate::PeerAuthenticationMode::Disabled,
        ))
        .with_persistence_worker_queue_capacity(1)
        .try_build()
        .expect("node app should build");

    let artifacts = (0..8)
        .map(|idx| {
            ArtifactRecord::builder(format!("artifact.saturation.{idx}"))
                .content_type("application/octet-stream")
                .size_bytes(1)
                .build()
        })
        .collect::<Vec<_>>();

    set_test_persist_delay(Duration::from_millis(25));
    let started = std::time::Instant::now();

    let writes = artifacts
        .into_iter()
        .enumerate()
        .map(|(idx, artifact)| {
            let app = app.clone();
            tokio::spawn(async move {
                app.put_artifact_content_async(&artifact, &[idx as u8])
                    .await
            })
        })
        .collect::<Vec<_>>();

    for write in writes {
        write
            .await
            .expect("persistence task should join")
            .expect("persistence task should succeed");
    }

    let elapsed = started.elapsed();
    clear_test_persist_delay();

    let snapshot = app.observability_snapshot();
    assert_eq!(snapshot.persistence.worker_queue_capacity, 1);
    assert_eq!(snapshot.persistence.artifact_write.success_count, 8);
    assert!(snapshot.persistence.state_persist.success_count >= 8);
    assert!(snapshot.persistence.worker_enqueue_count >= 16);
    assert!(snapshot.persistence.worker_enqueue_wait_count >= 7);
    assert!(snapshot.persistence.worker_enqueue_wait_ms_total > 0);
    assert!(snapshot.persistence.worker_enqueue_wait_ms_max > 0);
    assert!(
        snapshot.persistence.worker_enqueue_wait_ms_max < 1_000,
        "synthetic saturated-queue wait should stay below the release regression threshold"
    );
    assert!(
        elapsed < Duration::from_millis(1_500),
        "synthetic saturated persistence run should complete within the release regression bound"
    );

    let _ = fs::remove_dir_all(state_dir);
}

#[tokio::test]
async fn health_and_readiness_reflect_graceful_transport_shutdown() {
    let state_dir = temp_state_dir("observability-shutdown");
    let ipc_path = state_dir.join("control.sock");
    let stream_path = state_dir.join("control-stream.sock");
    let app = NodeApp::builder()
        .config(NodeConfig {
            node_id: NodeId::new("node-obs-shutdown"),
            http_bind_addr: "127.0.0.1:0".parse().expect("socket address should parse"),
            ipc_socket_path: ipc_path.clone(),
            reconcile_interval: Duration::from_millis(10),
            state_dir: Some(state_dir.clone()),
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

    let (_http_addr, http_server) = app
        .start_http_server_graceful("127.0.0.1:0".parse().expect("socket address should parse"))
        .await
        .expect("http server should start");
    let (_ipc_bound_path, ipc_server) = app
        .start_ipc_server_graceful(&ipc_path)
        .await
        .expect("ipc server should start");
    let (_stream_bound_path, stream_server) = app
        .start_ipc_stream_server_graceful(&stream_path)
        .await
        .expect("ipc stream server should start");
    app.replay_state().expect("startup replay should complete");

    let ready = app.readiness_snapshot();
    let healthy = app.health_snapshot();
    assert!(ready.ready);
    assert_eq!(
        healthy.status,
        orion::control_plane::NodeHealthStatus::Healthy
    );

    ipc_server
        .shutdown()
        .await
        .expect("ipc server shutdown should succeed");

    let readiness_after_ipc_shutdown = app.readiness_snapshot();
    let health_after_ipc_shutdown = app.health_snapshot();
    assert!(!readiness_after_ipc_shutdown.ready);
    assert!(
        readiness_after_ipc_shutdown
            .reasons
            .iter()
            .any(|reason| reason == "ipc control transport not bound")
    );
    assert_eq!(
        health_after_ipc_shutdown.status,
        orion::control_plane::NodeHealthStatus::Degraded
    );
    assert!(
        health_after_ipc_shutdown
            .reasons
            .iter()
            .any(|reason| reason == "ipc control transport not bound")
    );

    stream_server
        .shutdown()
        .await
        .expect("ipc stream server shutdown should succeed");
    http_server
        .shutdown()
        .await
        .expect("http server shutdown should succeed");

    let readiness_after_full_shutdown = app.readiness_snapshot();
    let health_after_full_shutdown = app.health_snapshot();
    assert!(
        readiness_after_full_shutdown
            .reasons
            .iter()
            .any(|reason| reason == "http transport not bound")
    );
    assert!(
        readiness_after_full_shutdown
            .reasons
            .iter()
            .any(|reason| reason == "ipc stream transport not bound")
    );
    assert!(
        health_after_full_shutdown
            .reasons
            .iter()
            .any(|reason| reason == "http transport not bound")
    );
    assert!(
        health_after_full_shutdown
            .reasons
            .iter()
            .any(|reason| reason == "ipc stream transport not bound")
    );

    let _ = fs::remove_dir_all(state_dir);
}

#[test]
fn observability_reports_peer_sync_counts_and_effective_parallel_cap() {
    let app = NodeApp::builder()
        .config(NodeConfig {
            node_id: NodeId::new("node-obs-peers"),
            http_bind_addr: "127.0.0.1:0".parse().expect("socket address should parse"),
            ipc_socket_path: NodeConfig::default_ipc_socket_path_for("node-obs-peers"),
            reconcile_interval: Duration::from_millis(10),
            state_dir: None,
            peers: vec![
                PeerConfig::new("node-a", "http://127.0.0.1:9101"),
                PeerConfig::new("node-b", "http://127.0.0.1:9102"),
                PeerConfig::new("node-c", "http://127.0.0.1:9103"),
                PeerConfig::new("node-d", "http://127.0.0.1:9104"),
                PeerConfig::new("node-e", "http://127.0.0.1:9105"),
                PeerConfig::new("node-f", "http://127.0.0.1:9106"),
            ],
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

    app.set_peer_sync_status(&NodeId::new("node-a"), PeerSyncStatus::Synced)
        .expect("peer A sync status should update");
    app.set_peer_sync_status(&NodeId::new("node-b"), PeerSyncStatus::Ready)
        .expect("peer B sync status should update");
    app.set_peer_sync_status(&NodeId::new("node-c"), PeerSyncStatus::BackingOff)
        .expect("peer C sync status should update");
    app.set_peer_sync_status(&NodeId::new("node-d"), PeerSyncStatus::Error)
        .expect("peer D sync status should update");
    app.set_peer_sync_status(&NodeId::new("node-e"), PeerSyncStatus::Syncing)
        .expect("peer E sync status should update");

    let snapshot = app.observability_snapshot();
    assert_eq!(snapshot.configured_peer_count, 6);
    assert_eq!(snapshot.ready_peer_count, 2);
    assert_eq!(snapshot.pending_peer_count, 2);
    assert_eq!(snapshot.degraded_peer_count, 2);
    assert_eq!(snapshot.peer_sync_parallel_in_flight_cap, 3);
}
