mod support;

use std::time::Duration;

use tokio::{io::AsyncWriteExt, net::UnixStream};

use orion::{
    ArtifactId, NodeId, RuntimeType, WorkloadId,
    control_plane::{
        ArtifactRecord, ClientEventKind, ClientHello, ClientRole, ControlMessage, DesiredState,
        DesiredStateMutation, MutationBatch, NodeHealthStatus, NodeReadinessStatus, WorkloadRecord,
    },
    transport::{
        http::{ControlRoute, HttpClient, HttpRequestPayload, HttpResponsePayload},
        ipc::{ControlEnvelope, LocalAddress, UnixControlStreamClient},
    },
};
use orion_node::NodeApp;

use support::{
    integration_node_config, recv_non_ping, snapshot, spawn_node, temp_state_dir, test_hello,
    wait_for_workload,
};

#[tokio::test]
async fn orion_node_binary_serves_http_control_plane() {
    let process = spawn_node("node.proc", None, &[]);

    let response = process
        .client()
        .send(&HttpRequestPayload::Control(Box::new(
            ControlMessage::Hello(test_hello("node.peer")),
        )))
        .await
        .expect("hello request should succeed");

    match response {
        HttpResponsePayload::Hello(hello) => {
            assert_eq!(hello.desired_revision, orion::Revision::ZERO);
        }
        other => panic!("expected hello response, got {other:?}"),
    }
}

#[tokio::test]
async fn orion_node_binary_reports_observability_snapshot() {
    let process = spawn_node("node.obs", None, &[]);
    let client = process.client();

    let snapshot = snapshot(&client).await;
    let response = client
        .send(&HttpRequestPayload::Control(Box::new(
            ControlMessage::Mutations(MutationBatch {
                base_revision: snapshot.state.desired.revision,
                mutations: vec![DesiredStateMutation::PutArtifact(
                    ArtifactRecord::builder("artifact.obs").build(),
                )],
            }),
        )))
        .await
        .expect("mutation request should succeed");
    assert_eq!(response, HttpResponsePayload::Accepted);

    let response = client
        .send(&HttpRequestPayload::Control(Box::new(
            ControlMessage::QueryObservability,
        )))
        .await
        .expect("observability request should succeed");

    match response {
        HttpResponsePayload::Observability(snapshot) => {
            assert_eq!(snapshot.node_id.as_str(), "node.obs");
            assert!(snapshot.reconcile.success_count >= 1);
            assert!(snapshot.mutation_apply.success_count >= 1);
            assert!(snapshot.recent_events.iter().any(|event| {
                matches!(
                    event.kind,
                    orion::control_plane::ObservabilityEventKind::MutationApply
                ) && event.success
            }));
        }
        other => panic!("expected observability response, got {other:?}"),
    }
}

#[tokio::test]
async fn orion_node_binary_observability_tracks_malformed_http_and_ipc_inputs() {
    let process = spawn_node("node.transport.obs", None, &[]);

    let response = reqwest::Client::new()
        .post(format!("{}/v1/control/hello", process.http_endpoint))
        .body("not-rkyv")
        .send()
        .await
        .expect("malformed http request should return");
    assert_eq!(response.status().as_u16(), 400);

    let mut stream = UnixStream::connect(&process.ipc_socket)
        .await
        .expect("ipc socket should connect");
    stream
        .write_all(b"garbage")
        .await
        .expect("garbage ipc bytes should send");
    stream.shutdown().await.expect("ipc socket should shutdown");
    tokio::time::sleep(Duration::from_millis(50)).await;

    let response = process
        .client()
        .send(&HttpRequestPayload::Control(Box::new(
            ControlMessage::QueryObservability,
        )))
        .await
        .expect("observability request should succeed");

    match response {
        HttpResponsePayload::Observability(snapshot) => {
            assert!(snapshot.transport.http_malformed_input_count >= 1);
            assert!(snapshot.transport.ipc_frame_failures >= 1);
            assert!(snapshot.transport.ipc_malformed_input_count >= 1);
        }
        other => panic!("expected observability response, got {other:?}"),
    }
}

#[tokio::test]
async fn orion_node_binary_observability_tracks_ipc_reconnects() {
    let process = spawn_node("node.reconnect.obs", None, &[]);

    for _ in 0..2 {
        let mut client = UnixControlStreamClient::connect(&process.ipc_stream_socket)
            .await
            .expect("stream client should connect");
        client
            .send(&ControlEnvelope {
                source: LocalAddress::new("reconnect-client"),
                destination: LocalAddress::new("orion"),
                message: ControlMessage::ClientHello(ClientHello {
                    client_name: "reconnect-client".into(),
                    role: ClientRole::ControlPlane,
                }),
            })
            .await
            .expect("stream hello should send");
        let _ = recv_non_ping(&mut client, "reconnect-client").await;
        drop(client);
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    let response = process
        .client()
        .send(&HttpRequestPayload::Control(Box::new(
            ControlMessage::QueryObservability,
        )))
        .await
        .expect("observability request should succeed");

    match response {
        HttpResponsePayload::Observability(snapshot) => {
            assert!(snapshot.transport.reconnect_count >= 1);
        }
        other => panic!("expected observability response, got {other:?}"),
    }
}

#[tokio::test]
async fn orion_node_binary_reports_health_snapshot() {
    let process = spawn_node("node.health", None, &[]);
    let response = process
        .client()
        .get_route(ControlRoute::Health)
        .await
        .expect("health request should succeed");

    match response {
        HttpResponsePayload::Health(snapshot) => {
            assert_eq!(snapshot.node_id.as_str(), "node.health");
            assert!(snapshot.alive);
            assert!(snapshot.replay_completed);
            assert!(snapshot.replay_successful);
            assert!(snapshot.http_bound);
            assert!(snapshot.ipc_bound);
            assert!(snapshot.ipc_stream_bound);
            assert_eq!(snapshot.status, NodeHealthStatus::Healthy);
            assert!(snapshot.reasons.is_empty());
        }
        other => panic!("expected health response, got {other:?}"),
    }
}

#[tokio::test]
async fn orion_node_binary_reports_readiness_snapshot() {
    let process = spawn_node("node.ready", None, &[]);
    let response = process
        .client()
        .get_route(ControlRoute::Readiness)
        .await
        .expect("readiness request should succeed");

    match response {
        HttpResponsePayload::Readiness(snapshot) => {
            assert_eq!(snapshot.node_id.as_str(), "node.ready");
            assert!(snapshot.ready);
            assert!(snapshot.replay_completed);
            assert!(snapshot.replay_successful);
            assert!(snapshot.http_bound);
            assert!(snapshot.ipc_bound);
            assert!(snapshot.ipc_stream_bound);
            assert!(snapshot.initial_sync_complete);
            assert_eq!(snapshot.status, NodeReadinessStatus::Ready);
            assert!(snapshot.reasons.is_empty());
        }
        other => panic!("expected readiness response, got {other:?}"),
    }
}

#[tokio::test]
async fn orion_node_binary_can_expose_plain_probe_http_while_control_requires_mtls() {
    let process = spawn_node(
        "node.probe.strict",
        None,
        &[
            ("ORION_NODE_HTTP_TLS_AUTO", "1"),
            ("ORION_NODE_HTTP_MTLS", "required"),
            ("ORION_NODE_HTTP_PROBE_ADDR", "127.0.0.1:0"),
        ],
    );

    let response = process
        .probe_client()
        .get_route(ControlRoute::Health)
        .await
        .expect("probe health request should succeed");
    assert!(matches!(response, HttpResponsePayload::Health(_)));

    let control_client = HttpClient::with_dangerous_tls(process.http_endpoint.clone())
        .expect("dangerous TLS client should build");
    let error = control_client
        .send(&HttpRequestPayload::Control(Box::new(
            ControlMessage::Hello(test_hello("node.peer")),
        )))
        .await
        .expect_err("control request without client cert should fail");
    assert!(
        matches!(
            error,
            orion::transport::http::HttpTransportError::RequestFailed { .. }
        ),
        "expected request failure from required mTLS, got {error:?}"
    );

    let response = reqwest::Client::new()
        .post(format!(
            "http://{}/v1/control/hello",
            process
                .probe_addr
                .as_ref()
                .expect("probe listener should be available")
        ))
        .body(Vec::<u8>::new())
        .send()
        .await
        .expect("probe hello request should return");
    assert_eq!(response.status().as_u16(), 404);
}

#[tokio::test]
async fn orion_node_binary_survives_repeated_boot_shutdown_cycles() {
    for cycle in 0..5 {
        let process = spawn_node(&format!("node.boot.{cycle}"), None, &[]);
        let response = process
            .client()
            .send(&HttpRequestPayload::Control(Box::new(
                ControlMessage::Hello(test_hello("node.peer")),
            )))
            .await
            .expect("hello request should succeed");
        assert!(matches!(response, HttpResponsePayload::Hello(_)));
    }
}

#[tokio::test]
async fn orion_node_binary_boots_with_persisted_state_directory() {
    let state_dir = temp_state_dir("persisted-boot");
    let seed = NodeApp::builder()
        .config(integration_node_config("node.persisted", state_dir.clone()))
        .try_build()
        .expect("node app should build");

    seed.put_artifact_content(
        &ArtifactRecord::builder("artifact.persisted")
            .content_type("application/octet-stream")
            .size_bytes(3)
            .build(),
        &[1, 2, 3],
    )
    .expect("artifact should persist");

    let mut desired = seed.state_snapshot().state.desired;
    desired.put_workload(
        WorkloadRecord::builder(
            WorkloadId::new("workload.persisted"),
            RuntimeType::new("graph.exec.v1"),
            ArtifactId::new("artifact.persisted"),
        )
        .desired_state(DesiredState::Stopped)
        .assigned_to(NodeId::new("node.persisted"))
        .build(),
    );
    seed.replace_desired(desired);
    seed.persist_state().expect("state should persist");
    drop(seed);

    let mut process = spawn_node("node.persisted", Some(state_dir.clone()), &[]);
    wait_for_workload(
        &process.client(),
        "workload.persisted",
        Duration::from_secs(5),
    )
    .await;

    process.cleanup();
    let _ = std::fs::remove_dir_all(state_dir);
}

#[tokio::test]
async fn orion_node_binary_boots_from_history_when_snapshot_is_missing() {
    let state_dir = temp_state_dir("history-only-boot");
    let seed = NodeApp::builder()
        .config(integration_node_config(
            "node.history.only",
            state_dir.clone(),
        ))
        .try_build()
        .expect("node app should build");

    seed.put_artifact_content(
        &ArtifactRecord::builder("artifact.history.only")
            .content_type("application/octet-stream")
            .size_bytes(3)
            .build(),
        &[3, 2, 1],
    )
    .expect("artifact should persist");

    let mut desired = seed.state_snapshot().state.desired;
    desired.put_workload(
        WorkloadRecord::builder(
            WorkloadId::new("workload.history.only"),
            RuntimeType::new("graph.exec.v1"),
            ArtifactId::new("artifact.history.only"),
        )
        .desired_state(DesiredState::Stopped)
        .assigned_to(NodeId::new("node.history.only"))
        .build(),
    );
    seed.replace_desired(desired);
    seed.persist_state().expect("state should persist");
    drop(seed);

    std::fs::remove_file(state_dir.join("snapshot-manifest.rkyv"))
        .expect("snapshot manifest should be removed");

    let mut process = spawn_node("node.history.only", Some(state_dir.clone()), &[]);
    wait_for_workload(
        &process.client(),
        "workload.history.only",
        Duration::from_secs(5),
    )
    .await;

    process.cleanup();
    let _ = std::fs::remove_dir_all(state_dir);
}

#[tokio::test]
async fn orion_node_binary_recovers_from_corrupt_persistence_via_peer_sync() {
    let leader_state_dir = temp_state_dir("peer-recovery-leader");
    let follower_state_dir = temp_state_dir("peer-recovery-follower");

    let leader_seed = NodeApp::builder()
        .config(integration_node_config(
            "node.leader",
            leader_state_dir.clone(),
        ))
        .try_build()
        .expect("node app should build");

    leader_seed
        .put_artifact_content(
            &ArtifactRecord::builder("artifact.peer").build(),
            &[7, 7, 7],
        )
        .expect("artifact should persist");
    let mut desired = leader_seed.state_snapshot().state.desired;
    desired.put_workload(
        WorkloadRecord::builder(
            WorkloadId::new("workload.peer-recovered"),
            RuntimeType::new("graph.exec.v1"),
            ArtifactId::new("artifact.peer"),
        )
        .desired_state(DesiredState::Stopped)
        .assigned_to(NodeId::new("node.leader"))
        .build(),
    );
    leader_seed.replace_desired(desired);
    leader_seed
        .persist_state()
        .expect("leader state should persist");
    drop(leader_seed);

    let mut leader = spawn_node("node.leader", Some(leader_state_dir.clone()), &[]);

    std::fs::create_dir_all(&follower_state_dir).expect("follower state dir should exist");
    std::fs::write(
        follower_state_dir.join("snapshot-manifest.rkyv"),
        b"not-rkyv",
    )
    .expect("corrupt snapshot should be written");

    let peer_spec = format!("node.leader={}", leader.http_endpoint.trim_end_matches('/'));
    let mut follower = spawn_node(
        "node.follower",
        Some(follower_state_dir.clone()),
        &[("ORION_NODE_PEERS", &peer_spec)],
    );

    wait_for_workload(
        &follower.client(),
        "workload.peer-recovered",
        Duration::from_secs(5),
    )
    .await;

    leader.cleanup();
    follower.cleanup();
    let _ = std::fs::remove_dir_all(leader_state_dir);
    let _ = std::fs::remove_dir_all(follower_state_dir);
}

#[tokio::test]
async fn orion_node_binary_recovers_with_mixed_healthy_and_unhealthy_peers() {
    let leader_state_dir = temp_state_dir("mixed-peer-recovery-leader");
    let follower_state_dir = temp_state_dir("mixed-peer-recovery-follower");

    let leader_seed = NodeApp::builder()
        .config(integration_node_config(
            "node.leader.mixed",
            leader_state_dir.clone(),
        ))
        .try_build()
        .expect("node app should build");

    leader_seed
        .put_artifact_content(
            &ArtifactRecord::builder("artifact.mixed.peer").build(),
            &[8, 8, 8],
        )
        .expect("artifact should persist");
    let mut desired = leader_seed.state_snapshot().state.desired;
    desired.put_workload(
        WorkloadRecord::builder(
            WorkloadId::new("workload.mixed-peer-recovered"),
            RuntimeType::new("graph.exec.v1"),
            ArtifactId::new("artifact.mixed.peer"),
        )
        .desired_state(DesiredState::Stopped)
        .assigned_to(NodeId::new("node.leader.mixed"))
        .build(),
    );
    leader_seed.replace_desired(desired);
    leader_seed
        .persist_state()
        .expect("leader state should persist");
    drop(leader_seed);

    let mut leader = spawn_node("node.leader.mixed", Some(leader_state_dir.clone()), &[]);

    std::fs::create_dir_all(&follower_state_dir).expect("follower state dir should exist");
    std::fs::write(
        follower_state_dir.join("snapshot-manifest.rkyv"),
        b"not-rkyv",
    )
    .expect("corrupt snapshot should be written");

    let peer_spec = format!(
        "node.leader.mixed=http://{},node.dead=http://127.0.0.1:9",
        leader.http_endpoint.trim_start_matches("http://")
    );
    let mut follower = spawn_node(
        "node.follower.mixed",
        Some(follower_state_dir.clone()),
        &[("ORION_NODE_PEERS", &peer_spec)],
    );

    wait_for_workload(
        &follower.client(),
        "workload.mixed-peer-recovered",
        Duration::from_secs(5),
    )
    .await;

    leader.cleanup();
    follower.cleanup();
    let _ = std::fs::remove_dir_all(leader_state_dir);
    let _ = std::fs::remove_dir_all(follower_state_dir);
}

#[tokio::test]
async fn orion_node_binary_handles_repeated_control_plane_mutations() {
    let process = spawn_node("node.mutations", None, &[]);
    let client = process.client();

    for index in 0..25 {
        let workload_id = format!("workload.mutation.{index}");
        let artifact_id = format!("artifact.mutation.{index}");
        let snapshot = snapshot(&client).await;
        let batch = MutationBatch {
            base_revision: snapshot.state.desired.revision,
            mutations: vec![
                DesiredStateMutation::PutArtifact(
                    ArtifactRecord::builder(orion::ArtifactId::new(artifact_id.clone())).build(),
                ),
                DesiredStateMutation::PutWorkload(
                    WorkloadRecord::builder(
                        WorkloadId::new(workload_id.as_str()),
                        RuntimeType::new("graph.exec.v1"),
                        ArtifactId::new(artifact_id.as_str()),
                    )
                    .desired_state(DesiredState::Stopped)
                    .assigned_to(NodeId::new("node.mutations"))
                    .build(),
                ),
            ],
        };

        let response = client
            .send(&HttpRequestPayload::Control(Box::new(
                ControlMessage::Mutations(batch),
            )))
            .await
            .expect("mutation request should succeed");
        assert_eq!(response, HttpResponsePayload::Accepted);
    }

    wait_for_workload(&client, "workload.mutation.24", Duration::from_secs(5)).await;
}

#[tokio::test]
async fn orion_node_binary_survives_concurrent_http_ipc_and_peer_sync_activity() {
    let process = spawn_node(
        "node.concurrent",
        None,
        &[("ORION_NODE_PEERS", "node-peer=http://127.0.0.1:9")],
    );
    let client = process.client();

    let mutation_task = {
        let client = client.clone();
        tokio::spawn(async move {
            for index in 0..12 {
                let workload_id = format!("workload.concurrent.{index}");
                let artifact_id = format!("artifact.concurrent.{index}");
                let snapshot = snapshot(&client).await;
                let batch = MutationBatch {
                    base_revision: snapshot.state.desired.revision,
                    mutations: vec![
                        DesiredStateMutation::PutArtifact(
                            ArtifactRecord::builder(orion::ArtifactId::new(artifact_id.clone()))
                                .build(),
                        ),
                        DesiredStateMutation::PutWorkload(
                            WorkloadRecord::builder(
                                WorkloadId::new(workload_id.as_str()),
                                RuntimeType::new("graph.exec.v1"),
                                ArtifactId::new(artifact_id.as_str()),
                            )
                            .desired_state(DesiredState::Stopped)
                            .assigned_to(NodeId::new("node.concurrent"))
                            .build(),
                        ),
                    ],
                };
                let response = client
                    .send(&HttpRequestPayload::Control(Box::new(
                        ControlMessage::Mutations(batch),
                    )))
                    .await
                    .expect("concurrent mutation request should succeed");
                assert_eq!(response, HttpResponsePayload::Accepted);
            }
        })
    };

    let probe_task = {
        let client = client.clone();
        tokio::spawn(async move {
            for _ in 0..12 {
                let health = client
                    .get_route(ControlRoute::Health)
                    .await
                    .expect("health request should succeed");
                assert!(matches!(health, HttpResponsePayload::Health(_)));

                let readiness = client
                    .get_route(ControlRoute::Readiness)
                    .await
                    .expect("readiness request should succeed");
                assert!(matches!(readiness, HttpResponsePayload::Readiness(_)));

                let observability = client
                    .send(&HttpRequestPayload::Control(Box::new(
                        ControlMessage::QueryObservability,
                    )))
                    .await
                    .expect("observability request should succeed");
                assert!(matches!(
                    observability,
                    HttpResponsePayload::Observability(_)
                ));
            }
        })
    };

    let stream_task = {
        let ipc_stream_socket = process.ipc_stream_socket.clone();
        tokio::spawn(async move {
            let mut client = UnixControlStreamClient::connect(&ipc_stream_socket)
                .await
                .expect("stream client should connect");

            client
                .send(&ControlEnvelope {
                    source: LocalAddress::new("concurrent-stream"),
                    destination: LocalAddress::new("orion"),
                    message: ControlMessage::ClientHello(ClientHello {
                        client_name: "concurrent-stream".into(),
                        role: ClientRole::ControlPlane,
                    }),
                })
                .await
                .expect("stream hello should send");
            assert!(matches!(
                recv_non_ping(&mut client, "concurrent-stream")
                    .await
                    .message,
                ControlMessage::ClientWelcome(_)
            ));

            client
                .send(&ControlEnvelope {
                    source: LocalAddress::new("concurrent-stream"),
                    destination: LocalAddress::new("orion"),
                    message: ControlMessage::WatchState(orion::control_plane::StateWatch {
                        desired_revision: orion::Revision::ZERO,
                    }),
                })
                .await
                .expect("watch registration should send");
            assert!(matches!(
                recv_non_ping(&mut client, "concurrent-stream")
                    .await
                    .message,
                ControlMessage::Accepted
            ));

            let mut state_event_count = 0usize;
            while state_event_count < 3 {
                let message = recv_non_ping(&mut client, "concurrent-stream")
                    .await
                    .message;
                if let ControlMessage::ClientEvents(events) = message {
                    state_event_count += events
                        .iter()
                        .filter(|event| matches!(event.event, ClientEventKind::StateSnapshot(_)))
                        .count();
                }
            }
        })
    };

    tokio::time::timeout(Duration::from_secs(15), async {
        mutation_task.await.expect("mutation task should complete");
        probe_task.await.expect("probe task should complete");
        stream_task.await.expect("stream task should complete");
    })
    .await
    .expect("concurrent mixed-load tasks should finish without stalling");

    wait_for_workload(&client, "workload.concurrent.11", Duration::from_secs(5)).await;

    let response = client
        .send(&HttpRequestPayload::Control(Box::new(
            ControlMessage::QueryObservability,
        )))
        .await
        .expect("final observability request should succeed");

    match response {
        HttpResponsePayload::Observability(snapshot) => {
            assert!(snapshot.mutation_apply.success_count >= 12);
            assert!(snapshot.peer_sync.failure_count >= 1);
            assert_eq!(snapshot.configured_peer_count, 1);
            assert!(snapshot.degraded_peer_count >= 1);
        }
        other => panic!("expected observability response, got {other:?}"),
    }
}
