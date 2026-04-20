mod support;

use std::time::Duration;

use orion::{
    ArtifactId, Revision, RuntimeType, WorkloadId,
    control_plane::{
        ArtifactRecord, ClientEventKind, ClientHello, ClientRole, ControlMessage, DesiredState,
        DesiredStateMutation, ExecutorWorkloadQuery, MutationBatch, ProviderLeaseQuery,
        WorkloadRecord,
    },
    transport::{
        http::{
            HttpRequestFailureKind, HttpRequestPayload, HttpResponsePayload, HttpTransportError,
        },
        ipc::{ControlEnvelope, LocalAddress, UnixControlClient, UnixControlStreamClient},
    },
};

use support::{recv_non_ping, snapshot, spawn_node};

async fn send_control_with_timeout_retry(
    client: &orion::transport::http::HttpClient,
    payload: ControlMessage,
) -> HttpResponsePayload {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);

    loop {
        match client
            .send(&HttpRequestPayload::Control(Box::new(payload.clone())))
            .await
        {
            Ok(response) => return response,
            Err(HttpTransportError::RequestFailed {
                kind: HttpRequestFailureKind::Timeout,
                ..
            }) if tokio::time::Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(error) => panic!("control request should succeed: {error:?}"),
        }
    }
}

#[tokio::test]
async fn orion_node_binary_ipc_stream_pushes_control_plane_state_events() {
    let process = spawn_node("node.ipc.control", None, &[]);
    let mut client = UnixControlStreamClient::connect(&process.ipc_stream_socket)
        .await
        .expect("stream client should connect");

    client
        .send(&ControlEnvelope {
            source: LocalAddress::new("cli-stream"),
            destination: LocalAddress::new("orion"),
            message: ControlMessage::ClientHello(ClientHello {
                client_name: "cli-stream".into(),
                role: ClientRole::ControlPlane,
            }),
        })
        .await
        .expect("stream hello should send");
    assert!(matches!(
        recv_non_ping(&mut client, "cli-stream").await.message,
        ControlMessage::ClientWelcome(_)
    ));

    client
        .send(&ControlEnvelope {
            source: LocalAddress::new("cli-stream"),
            destination: LocalAddress::new("orion"),
            message: ControlMessage::WatchState(orion::control_plane::StateWatch {
                desired_revision: Revision::ZERO,
            }),
        })
        .await
        .expect("watch registration should send");
    assert!(matches!(
        recv_non_ping(&mut client, "cli-stream").await.message,
        ControlMessage::Accepted
    ));

    let snapshot = snapshot(&process.client()).await;
    let batch = MutationBatch {
        base_revision: snapshot.state.desired.revision,
        mutations: vec![DesiredStateMutation::PutArtifact(
            ArtifactRecord::builder("artifact.ipc.control").build(),
        )],
    };
    let response = process
        .client()
        .send(&HttpRequestPayload::Control(Box::new(
            ControlMessage::Mutations(batch),
        )))
        .await
        .expect("mutation request should succeed");
    assert_eq!(response, HttpResponsePayload::Accepted);

    let pushed = recv_non_ping(&mut client, "cli-stream").await;
    match pushed.message {
        ControlMessage::ClientEvents(events) => {
            assert_eq!(events.len(), 1);
            match &events[0].event {
                ClientEventKind::StateSnapshot(snapshot) => {
                    assert!(
                        snapshot
                            .state
                            .desired
                            .artifacts
                            .contains_key(&ArtifactId::new("artifact.ipc.control"))
                    );
                }
                other => panic!("unexpected event kind: {other:?}"),
            }
        }
        other => panic!("unexpected pushed message: {other:?}"),
    }
}

#[tokio::test]
async fn orion_node_binary_ipc_stream_pushes_executor_assignment_events() {
    let process = spawn_node("node.ipc.executor", None, &[]);
    let mut client = UnixControlStreamClient::connect(&process.ipc_stream_socket)
        .await
        .expect("stream client should connect");

    client
        .send(&ControlEnvelope {
            source: LocalAddress::new("executor-stream"),
            destination: LocalAddress::new("orion"),
            message: ControlMessage::ClientHello(ClientHello {
                client_name: "executor-stream".into(),
                role: ClientRole::Executor,
            }),
        })
        .await
        .expect("stream hello should send");
    let _ = recv_non_ping(&mut client, "executor-stream").await;

    let snapshot = snapshot(&process.client()).await;
    let batch = MutationBatch {
        base_revision: snapshot.state.desired.revision,
        mutations: vec![
            DesiredStateMutation::PutExecutor(
                orion::control_plane::ExecutorRecord::builder(
                    "executor.stream.proc",
                    "node.ipc.executor",
                )
                .runtime_type(RuntimeType::new("graph.exec.v1"))
                .build(),
            ),
            DesiredStateMutation::PutArtifact(
                ArtifactRecord::builder("artifact.ipc.executor").build(),
            ),
            DesiredStateMutation::PutWorkload(
                WorkloadRecord::builder(
                    WorkloadId::new("workload.ipc.executor"),
                    RuntimeType::new("graph.exec.v1"),
                    ArtifactId::new("artifact.ipc.executor"),
                )
                .desired_state(DesiredState::Running)
                .assigned_to(orion::NodeId::new("node.ipc.executor"))
                .build(),
            ),
        ],
    };
    let response = process
        .client()
        .send(&HttpRequestPayload::Control(Box::new(
            ControlMessage::Mutations(batch),
        )))
        .await
        .expect("mutation request should succeed");
    assert_eq!(response, HttpResponsePayload::Accepted);

    client
        .send(&ControlEnvelope {
            source: LocalAddress::new("executor-stream"),
            destination: LocalAddress::new("orion"),
            message: ControlMessage::WatchExecutorWorkloads(ExecutorWorkloadQuery {
                executor_id: orion::ExecutorId::new("executor.stream.proc"),
            }),
        })
        .await
        .expect("executor watch should send");
    assert!(matches!(
        recv_non_ping(&mut client, "executor-stream").await.message,
        ControlMessage::Accepted
    ));

    let pushed = recv_non_ping(&mut client, "executor-stream").await;
    match pushed.message {
        ControlMessage::ClientEvents(events) => {
            assert_eq!(events.len(), 1);
            match &events[0].event {
                ClientEventKind::ExecutorWorkloads {
                    executor_id,
                    workloads,
                } => {
                    assert_eq!(executor_id.as_str(), "executor.stream.proc");
                    assert_eq!(workloads.len(), 1);
                    assert_eq!(workloads[0].workload_id.as_str(), "workload.ipc.executor");
                }
                other => panic!("unexpected event kind: {other:?}"),
            }
        }
        other => panic!("unexpected pushed message: {other:?}"),
    }
}

#[tokio::test]
async fn orion_node_binary_ipc_stream_pushes_provider_lease_events() {
    let process = spawn_node("node.ipc.provider", None, &[]);
    let mut client = UnixControlStreamClient::connect(&process.ipc_stream_socket)
        .await
        .expect("stream client should connect");

    client
        .send(&ControlEnvelope {
            source: LocalAddress::new("provider-stream"),
            destination: LocalAddress::new("orion"),
            message: ControlMessage::ClientHello(ClientHello {
                client_name: "provider-stream".into(),
                role: ClientRole::Provider,
            }),
        })
        .await
        .expect("stream hello should send");
    let _ = recv_non_ping(&mut client, "provider-stream").await;

    let snapshot = snapshot(&process.client()).await;
    let batch = MutationBatch {
        base_revision: snapshot.state.desired.revision,
        mutations: vec![
            DesiredStateMutation::PutProvider(
                orion::control_plane::ProviderRecord::builder(
                    "provider.stream.proc",
                    "node.ipc.provider",
                )
                .resource_type(orion::ResourceType::new("imu.sample"))
                .build(),
            ),
            DesiredStateMutation::PutResource(
                orion::control_plane::ResourceRecord::builder(
                    "resource.stream.proc",
                    "imu.sample",
                    "provider.stream.proc",
                )
                .health(orion::control_plane::HealthState::Healthy)
                .availability(orion::control_plane::AvailabilityState::Available)
                .lease_state(orion::control_plane::LeaseState::Leased)
                .build(),
            ),
            DesiredStateMutation::PutLease(
                orion::control_plane::LeaseRecord::builder("resource.stream.proc")
                    .lease_state(orion::control_plane::LeaseState::Leased)
                    .holder_node("node.ipc.provider")
                    .holder_workload("workload.stream.proc")
                    .build(),
            ),
        ],
    };
    let response = process
        .client()
        .send(&HttpRequestPayload::Control(Box::new(
            ControlMessage::Mutations(batch),
        )))
        .await
        .expect("mutation request should succeed");
    assert_eq!(response, HttpResponsePayload::Accepted);

    client
        .send(&ControlEnvelope {
            source: LocalAddress::new("provider-stream"),
            destination: LocalAddress::new("orion"),
            message: ControlMessage::WatchProviderLeases(ProviderLeaseQuery {
                provider_id: orion::ProviderId::new("provider.stream.proc"),
            }),
        })
        .await
        .expect("provider watch should send");
    assert!(matches!(
        recv_non_ping(&mut client, "provider-stream").await.message,
        ControlMessage::Accepted
    ));

    let pushed = recv_non_ping(&mut client, "provider-stream").await;
    match pushed.message {
        ControlMessage::ClientEvents(events) => {
            assert_eq!(events.len(), 1);
            match &events[0].event {
                ClientEventKind::ProviderLeases {
                    provider_id,
                    leases,
                } => {
                    assert_eq!(provider_id.as_str(), "provider.stream.proc");
                    assert_eq!(leases.len(), 1);
                    assert_eq!(leases[0].resource_id.as_str(), "resource.stream.proc");
                }
                other => panic!("unexpected event kind: {other:?}"),
            }
        }
        other => panic!("unexpected pushed message: {other:?}"),
    }
}

#[tokio::test]
async fn orion_node_binary_ipc_stream_rejects_non_hello_first_message() {
    let process = spawn_node("node.ipc.invalid-first", None, &[]);
    let mut client = UnixControlStreamClient::connect(&process.ipc_stream_socket)
        .await
        .expect("stream client should connect");

    client
        .send(&ControlEnvelope {
            source: LocalAddress::new("bad-stream"),
            destination: LocalAddress::new("orion"),
            message: ControlMessage::Ping,
        })
        .await
        .expect("invalid first frame should still send");

    let response = client
        .recv()
        .await
        .expect("rejection should read")
        .expect("rejection");
    match response.message {
        ControlMessage::Rejected(message) => {
            assert!(message.contains("ClientHello as first message"));
        }
        other => panic!("expected rejection, got {other:?}"),
    }
}

#[tokio::test]
async fn orion_node_binary_ipc_unary_rejects_role_invalid_requests() {
    let process = spawn_node("node.ipc.role-invalid", None, &[]);
    let client = UnixControlClient::new(&process.ipc_socket);

    let _ = client
        .send(ControlEnvelope {
            source: LocalAddress::new("cli-invalid"),
            destination: LocalAddress::new("orion"),
            message: ControlMessage::ClientHello(ClientHello {
                client_name: "cli-invalid".into(),
                role: ClientRole::ControlPlane,
            }),
        })
        .await
        .expect("client hello should succeed");

    let response = client
        .send(ControlEnvelope {
            source: LocalAddress::new("cli-invalid"),
            destination: LocalAddress::new("orion"),
            message: ControlMessage::QueryExecutorWorkloads(ExecutorWorkloadQuery {
                executor_id: orion::ExecutorId::new("executor.invalid"),
            }),
        })
        .await;
    match response.expect("role-invalid unary request should roundtrip") {
        ControlEnvelope {
            message: ControlMessage::Rejected(reason),
            ..
        } => {
            assert!(reason.contains("role mismatch") || reason.contains("not allowed"));
        }
        other => panic!("expected unary rejection, got {other:?}"),
    }
}

#[tokio::test]
async fn orion_node_binary_ipc_unary_accepts_control_plane_mutations() {
    let process = spawn_node("node.ipc.local-mutate", None, &[]);
    let client = UnixControlClient::new(&process.ipc_socket);

    let hello = client
        .send(ControlEnvelope {
            source: LocalAddress::new("cli-local-mutate"),
            destination: LocalAddress::new("orion"),
            message: ControlMessage::ClientHello(ClientHello {
                client_name: "cli-local-mutate".into(),
                role: ClientRole::ControlPlane,
            }),
        })
        .await
        .expect("client hello should roundtrip");
    assert!(matches!(hello.message, ControlMessage::ClientWelcome(_)));

    let initial_snapshot = snapshot(&process.client()).await;
    let response = client
        .send(ControlEnvelope {
            source: LocalAddress::new("cli-local-mutate"),
            destination: LocalAddress::new("orion"),
            message: ControlMessage::Mutations(MutationBatch {
                base_revision: initial_snapshot.state.desired.revision,
                mutations: vec![DesiredStateMutation::PutArtifact(
                    ArtifactRecord::builder("artifact.ipc.local-mutate").build(),
                )],
            }),
        })
        .await
        .expect("local mutation should roundtrip");
    assert!(matches!(response.message, ControlMessage::Accepted));

    let updated = snapshot(&process.client()).await;
    assert!(
        updated
            .state
            .desired
            .artifacts
            .contains_key(&ArtifactId::new("artifact.ipc.local-mutate"))
    );
}

#[tokio::test]
async fn orion_node_binary_rate_limits_unary_ipc_clients() {
    let process = spawn_node(
        "node.ipc.unary-rate",
        None,
        &[
            ("ORION_NODE_LOCAL_RATE_LIMIT_WINDOW_MS", "1000"),
            ("ORION_NODE_LOCAL_RATE_LIMIT_MAX_MESSAGES", "1"),
        ],
    );
    let client = UnixControlClient::new(&process.ipc_socket);

    let hello = client
        .send(ControlEnvelope {
            source: LocalAddress::new("cli-rate"),
            destination: LocalAddress::new("orion"),
            message: ControlMessage::ClientHello(ClientHello {
                client_name: "cli-rate".into(),
                role: ClientRole::ControlPlane,
            }),
        })
        .await
        .expect("client hello should roundtrip");
    assert!(matches!(hello.message, ControlMessage::ClientWelcome(_)));

    let first = client
        .send(ControlEnvelope {
            source: LocalAddress::new("cli-rate"),
            destination: LocalAddress::new("orion"),
            message: ControlMessage::WatchState(orion::control_plane::StateWatch {
                desired_revision: Revision::ZERO,
            }),
        })
        .await
        .expect("first request should roundtrip");
    assert!(matches!(first.message, ControlMessage::Accepted));

    let second = client
        .send(ControlEnvelope {
            source: LocalAddress::new("cli-rate"),
            destination: LocalAddress::new("orion"),
            message: ControlMessage::WatchState(orion::control_plane::StateWatch {
                desired_revision: Revision::ZERO,
            }),
        })
        .await
        .expect("rate-limited request should roundtrip");
    match second.message {
        ControlMessage::Rejected(reason) => {
            assert!(reason.contains("rate limit"));
        }
        other => panic!("expected rate-limited rejection, got {other:?}"),
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
            assert!(snapshot.client_sessions.rate_limited_total >= 1);
            assert!(snapshot.recent_events.iter().any(|event| {
                matches!(
                    event.kind,
                    orion::control_plane::ObservabilityEventKind::ClientRateLimited
                )
            }));
        }
        other => panic!("expected observability response, got {other:?}"),
    }
}

#[tokio::test]
async fn orion_node_binary_rate_limits_stream_ipc_clients() {
    let process = spawn_node(
        "node.ipc.stream-rate",
        None,
        &[
            ("ORION_NODE_LOCAL_RATE_LIMIT_WINDOW_MS", "1000"),
            ("ORION_NODE_LOCAL_RATE_LIMIT_MAX_MESSAGES", "1"),
        ],
    );
    let mut client = UnixControlStreamClient::connect(&process.ipc_stream_socket)
        .await
        .expect("stream client should connect");

    client
        .send(&ControlEnvelope {
            source: LocalAddress::new("cli-stream-rate"),
            destination: LocalAddress::new("orion"),
            message: ControlMessage::ClientHello(ClientHello {
                client_name: "cli-stream-rate".into(),
                role: ClientRole::ControlPlane,
            }),
        })
        .await
        .expect("stream hello should send");
    assert!(matches!(
        recv_non_ping(&mut client, "cli-stream-rate").await.message,
        ControlMessage::ClientWelcome(_)
    ));

    client
        .send(&ControlEnvelope {
            source: LocalAddress::new("cli-stream-rate"),
            destination: LocalAddress::new("orion"),
            message: ControlMessage::WatchState(orion::control_plane::StateWatch {
                desired_revision: Revision::ZERO,
            }),
        })
        .await
        .expect("first stream request should send");
    assert!(matches!(
        recv_non_ping(&mut client, "cli-stream-rate").await.message,
        ControlMessage::Accepted
    ));

    client
        .send(&ControlEnvelope {
            source: LocalAddress::new("cli-stream-rate"),
            destination: LocalAddress::new("orion"),
            message: ControlMessage::WatchState(orion::control_plane::StateWatch {
                desired_revision: Revision::ZERO,
            }),
        })
        .await
        .expect("rate-limited stream request should send");
    match recv_non_ping(&mut client, "cli-stream-rate").await.message {
        ControlMessage::Rejected(reason) => {
            assert!(reason.contains("rate limit"));
        }
        other => panic!("expected stream rate-limited rejection, got {other:?}"),
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
            assert!(snapshot.client_sessions.rate_limited_total >= 1);
        }
        other => panic!("expected observability response, got {other:?}"),
    }
}

#[tokio::test]
async fn orion_node_binary_stream_reconnect_resumes_queued_events_before_ttl() {
    let process = spawn_node(
        "node.ipc.resume",
        None,
        &[("ORION_NODE_LOCAL_SESSION_TTL_MS", "1000")],
    );
    let source = LocalAddress::new("cli-stream-resume");

    {
        let mut client = UnixControlStreamClient::connect(&process.ipc_stream_socket)
            .await
            .expect("stream client should connect");
        client
            .send(&ControlEnvelope {
                source: source.clone(),
                destination: LocalAddress::new("orion"),
                message: ControlMessage::ClientHello(ClientHello {
                    client_name: "cli-stream-resume".into(),
                    role: ClientRole::ControlPlane,
                }),
            })
            .await
            .expect("stream hello should send");
        assert!(matches!(
            recv_non_ping(&mut client, "cli-stream-resume")
                .await
                .message,
            ControlMessage::ClientWelcome(_)
        ));
        client
            .send(&ControlEnvelope {
                source: source.clone(),
                destination: LocalAddress::new("orion"),
                message: ControlMessage::WatchState(orion::control_plane::StateWatch {
                    desired_revision: Revision::ZERO,
                }),
            })
            .await
            .expect("watch registration should send");
        assert!(matches!(
            recv_non_ping(&mut client, "cli-stream-resume")
                .await
                .message,
            ControlMessage::Accepted
        ));
    }

    let snapshot = snapshot(&process.client()).await;
    let response = process
        .client()
        .send(&HttpRequestPayload::Control(Box::new(
            ControlMessage::Mutations(MutationBatch {
                base_revision: snapshot.state.desired.revision,
                mutations: vec![DesiredStateMutation::PutArtifact(
                    ArtifactRecord::builder("artifact.resume").build(),
                )],
            }),
        )))
        .await
        .expect("mutation request should succeed");
    assert_eq!(response, HttpResponsePayload::Accepted);

    let mut client = UnixControlStreamClient::connect(&process.ipc_stream_socket)
        .await
        .expect("stream client should reconnect");
    client
        .send(&ControlEnvelope {
            source,
            destination: LocalAddress::new("orion"),
            message: ControlMessage::ClientHello(ClientHello {
                client_name: "cli-stream-resume".into(),
                role: ClientRole::ControlPlane,
            }),
        })
        .await
        .expect("reconnect hello should send");
    assert!(matches!(
        recv_non_ping(&mut client, "cli-stream-resume")
            .await
            .message,
        ControlMessage::ClientWelcome(_)
    ));

    match recv_non_ping(&mut client, "cli-stream-resume")
        .await
        .message
    {
        ControlMessage::ClientEvents(events) => {
            assert_eq!(events.len(), 1);
            match &events[0].event {
                ClientEventKind::StateSnapshot(snapshot) => {
                    assert!(
                        snapshot
                            .state
                            .desired
                            .artifacts
                            .contains_key(&ArtifactId::new("artifact.resume"))
                    );
                }
                other => panic!("unexpected event kind: {other:?}"),
            }
        }
        other => panic!("expected resumed client events, got {other:?}"),
    }
}

#[tokio::test]
async fn orion_node_binary_stream_reconnect_after_ttl_drops_resume_state() {
    let process = spawn_node(
        "node.ipc.expire",
        None,
        &[("ORION_NODE_LOCAL_SESSION_TTL_MS", "50")],
    );
    let source = LocalAddress::new("cli-stream-expire");

    {
        let mut client = UnixControlStreamClient::connect(&process.ipc_stream_socket)
            .await
            .expect("stream client should connect");
        client
            .send(&ControlEnvelope {
                source: source.clone(),
                destination: LocalAddress::new("orion"),
                message: ControlMessage::ClientHello(ClientHello {
                    client_name: "cli-stream-expire".into(),
                    role: ClientRole::ControlPlane,
                }),
            })
            .await
            .expect("stream hello should send");
        assert!(matches!(
            recv_non_ping(&mut client, "cli-stream-expire")
                .await
                .message,
            ControlMessage::ClientWelcome(_)
        ));
        client
            .send(&ControlEnvelope {
                source: source.clone(),
                destination: LocalAddress::new("orion"),
                message: ControlMessage::WatchState(orion::control_plane::StateWatch {
                    desired_revision: Revision::ZERO,
                }),
            })
            .await
            .expect("watch registration should send");
        assert!(matches!(
            recv_non_ping(&mut client, "cli-stream-expire")
                .await
                .message,
            ControlMessage::Accepted
        ));
    }

    tokio::time::sleep(Duration::from_millis(80)).await;

    let snapshot = snapshot(&process.client()).await;
    let response = send_control_with_timeout_retry(
        &process.client(),
        ControlMessage::Mutations(MutationBatch {
            base_revision: snapshot.state.desired.revision,
            mutations: vec![DesiredStateMutation::PutArtifact(
                ArtifactRecord::builder("artifact.expire").build(),
            )],
        }),
    )
    .await;
    assert_eq!(response, HttpResponsePayload::Accepted);

    let mut client = UnixControlStreamClient::connect(&process.ipc_stream_socket)
        .await
        .expect("stream client should reconnect");
    client
        .send(&ControlEnvelope {
            source: source.clone(),
            destination: LocalAddress::new("orion"),
            message: ControlMessage::ClientHello(ClientHello {
                client_name: "cli-stream-expire".into(),
                role: ClientRole::ControlPlane,
            }),
        })
        .await
        .expect("reconnect hello should send");
    assert!(matches!(
        recv_non_ping(&mut client, "cli-stream-expire")
            .await
            .message,
        ControlMessage::ClientWelcome(_)
    ));

    client
        .send(&ControlEnvelope {
            source,
            destination: LocalAddress::new("orion"),
            message: ControlMessage::PollClientEvents(orion::control_plane::ClientEventPoll {
                after_sequence: 0,
                max_events: 8,
            }),
        })
        .await
        .expect("poll request should send");
    assert_eq!(
        recv_non_ping(&mut client, "cli-stream-expire")
            .await
            .message,
        ControlMessage::ClientEvents(Vec::new())
    );
}
