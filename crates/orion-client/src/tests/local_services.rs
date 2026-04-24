use super::*;
use orion_control_plane::{
    ClientEvent, ClientEventKind, ClusterStateEnvelope, ControlMessage, DesiredClusterState,
    ExecutorWorkloadQuery, LeaseRecord, ProviderLeaseQuery, StateSnapshot, StateWatch,
};
use orion_core::{decode_from_slice, encode_to_vec};
use orion_transport_ipc::{read_control_frame, write_control_frame};
use std::{
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::net::UnixListener;
use tokio::time::{Duration, sleep};

fn unique_socket_path(label: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time should advance")
        .as_nanos();
    std::env::temp_dir().join(format!("orion-client-{label}-{nanos}.sock"))
}

#[tokio::test]
async fn local_executor_service_bootstraps_and_watches_workloads() {
    let unary_socket_path = unique_socket_path("executor-service-unary");
    let stream_socket_path = unique_socket_path("executor-service-stream");
    let _ = tokio::fs::remove_file(&unary_socket_path).await;
    let _ = tokio::fs::remove_file(&stream_socket_path).await;
    let unary_listener =
        UnixListener::bind(&unary_socket_path).expect("unary listener should bind");
    let stream_listener =
        UnixListener::bind(&stream_socket_path).expect("stream listener should bind");

    let executor = ExecutorRecord {
        executor_id: ExecutorId::new("executor.local"),
        node_id: NodeId::new("node-a"),
        runtime_types: vec![RuntimeType::new("graph.exec.v1")],
    };
    let bootstrap_workload = WorkloadRecord::builder(
        WorkloadId::new("workload.bootstrap"),
        RuntimeType::new("graph.exec.v1"),
        ArtifactId::new("artifact.bootstrap"),
    )
    .desired_state(DesiredState::Running)
    .assigned_to(NodeId::new("node-a"))
    .build();
    let watched_workload = WorkloadRecord::builder(
        WorkloadId::new("workload.changed"),
        RuntimeType::new("graph.exec.v1"),
        ArtifactId::new("artifact.changed"),
    )
    .desired_state(DesiredState::Running)
    .assigned_to(NodeId::new("node-a"))
    .build();

    let unary_task = tokio::spawn(serve_executor_bootstrap_query(
        unary_listener,
        executor.clone(),
        vec![bootstrap_workload.clone()],
    ));
    let stream_task = tokio::spawn(serve_executor_watch_stream(
        stream_listener,
        executor.clone(),
        vec![watched_workload.clone()],
        7,
    ));

    let runtime = LocalNodeRuntime::new(&unary_socket_path, &stream_socket_path);
    let service = LocalExecutorService::new(runtime, "engine", executor);
    let mut subscription = service
        .subscribe_workloads()
        .await
        .expect("executor subscription should connect");

    assert_eq!(
        subscription
            .next_event()
            .await
            .expect("bootstrap event should arrive"),
        LocalExecutorEvent::Bootstrap(vec![bootstrap_workload])
    );
    assert_eq!(
        subscription
            .next_event()
            .await
            .expect("watch event should arrive"),
        LocalExecutorEvent::WorkloadsChanged {
            sequence: 7,
            workloads: vec![watched_workload],
        }
    );

    unary_task.await.expect("unary task should complete");
    stream_task.await.expect("stream task should complete");
    let _ = tokio::fs::remove_file(&unary_socket_path).await;
    let _ = tokio::fs::remove_file(&stream_socket_path).await;
}

#[tokio::test]
async fn local_executor_service_retries_until_orion_is_available() {
    let unary_socket_path = unique_socket_path("executor-service-retry-unary");
    let stream_socket_path = unique_socket_path("executor-service-retry-stream");
    let _ = tokio::fs::remove_file(&unary_socket_path).await;
    let _ = tokio::fs::remove_file(&stream_socket_path).await;

    let executor = ExecutorRecord {
        executor_id: ExecutorId::new("executor.retry"),
        node_id: NodeId::new("node-a"),
        runtime_types: vec![RuntimeType::new("graph.exec.v1")],
    };
    let watched_workload = WorkloadRecord::builder(
        WorkloadId::new("workload.retry"),
        RuntimeType::new("graph.exec.v1"),
        ArtifactId::new("artifact.retry"),
    )
    .desired_state(DesiredState::Running)
    .assigned_to(NodeId::new("node-a"))
    .build();

    let unary_path = unary_socket_path.clone();
    let stream_path = stream_socket_path.clone();
    let delayed_unary_executor = executor.clone();
    let delayed_stream_executor = executor.clone();
    let delayed_unary_task = tokio::spawn(async move {
        sleep(Duration::from_millis(40)).await;
        let unary_listener = UnixListener::bind(&unary_path).expect("delayed unary bind");
        serve_executor_bootstrap_query(unary_listener, delayed_unary_executor, Vec::new()).await;
    });
    let delayed_stream_task = tokio::spawn(async move {
        sleep(Duration::from_millis(40)).await;
        let stream_listener = UnixListener::bind(&stream_path).expect("delayed stream bind");
        serve_executor_watch_stream(
            stream_listener,
            delayed_stream_executor,
            vec![watched_workload],
            3,
        )
        .await;
    });

    let runtime = LocalNodeRuntime::new(&unary_socket_path, &stream_socket_path);
    let service = LocalExecutorService::new(runtime, "engine-retry", executor).with_retry_policy(
        LocalServiceRetryPolicy::fixed_delay(Duration::from_millis(10)),
    );
    let mut subscription = service
        .subscribe_workloads()
        .await
        .expect("executor subscription should retry until sockets appear");

    assert_eq!(
        subscription
            .next_event()
            .await
            .expect("bootstrap event should arrive"),
        LocalExecutorEvent::Bootstrap(Vec::new())
    );
    assert_eq!(
        subscription
            .next_event()
            .await
            .expect("watch event should arrive"),
        LocalExecutorEvent::WorkloadsChanged {
            sequence: 3,
            workloads: vec![
                WorkloadRecord::builder(
                    WorkloadId::new("workload.retry"),
                    RuntimeType::new("graph.exec.v1"),
                    ArtifactId::new("artifact.retry"),
                )
                .desired_state(DesiredState::Running)
                .assigned_to(NodeId::new("node-a"))
                .build()
            ],
        }
    );

    delayed_unary_task
        .await
        .expect("delayed unary task should complete");
    delayed_stream_task
        .await
        .expect("delayed stream task should complete");
    let _ = tokio::fs::remove_file(&unary_socket_path).await;
    let _ = tokio::fs::remove_file(&stream_socket_path).await;
}

#[tokio::test]
async fn local_provider_service_bootstraps_leases_and_state_then_watches_both() {
    let unary_socket_path = unique_socket_path("provider-service-unary");
    let stream_socket_path = unique_socket_path("provider-service-stream");
    let _ = tokio::fs::remove_file(&unary_socket_path).await;
    let _ = tokio::fs::remove_file(&stream_socket_path).await;
    let unary_listener =
        UnixListener::bind(&unary_socket_path).expect("unary listener should bind");
    let stream_listener =
        UnixListener::bind(&stream_socket_path).expect("combined stream listener should bind");

    let provider = ProviderRecord::builder("provider.local", "node-a")
        .resource_type("camera.device")
        .build();
    let bootstrap_lease = LeaseRecord::builder(ResourceId::new("resource.bootstrap"))
        .holder_node(NodeId::new("node-a"))
        .build();
    let watched_lease = LeaseRecord::builder(ResourceId::new("resource.changed"))
        .holder_node(NodeId::new("node-a"))
        .build();
    let bootstrap_snapshot = StateSnapshot {
        state: orion_control_plane::ClusterStateEnvelope::new(
            orion_control_plane::DesiredClusterState::default(),
            orion_control_plane::ObservedClusterState::default(),
            orion_control_plane::AppliedClusterState::default(),
        ),
    };
    let watched_snapshot = StateSnapshot {
        state: ClusterStateEnvelope::new(
            DesiredClusterState {
                revision: Revision::new(2),
                ..Default::default()
            },
            orion_control_plane::ObservedClusterState::default(),
            orion_control_plane::AppliedClusterState::default(),
        ),
    };

    let unary_task = tokio::spawn(serve_provider_bootstrap_queries(
        unary_listener,
        provider.clone(),
        vec![bootstrap_lease.clone()],
        bootstrap_snapshot.clone(),
    ));
    let stream_task = tokio::spawn(serve_provider_and_control_watch_streams(
        stream_listener,
        provider.clone(),
        vec![watched_lease.clone()],
        watched_snapshot.clone(),
        5,
        9,
    ));

    let runtime = LocalNodeRuntime::new(&unary_socket_path, &stream_socket_path);
    let service = LocalProviderService::new(runtime, "peripherals", provider);
    let mut subscription = service
        .subscribe(Revision::ZERO)
        .await
        .expect("provider subscription should connect");

    assert_eq!(
        subscription
            .next_event()
            .await
            .expect("bootstrap leases should arrive"),
        LocalProviderEvent::BootstrapLeases(vec![bootstrap_lease])
    );
    assert_eq!(
        subscription
            .next_event()
            .await
            .expect("bootstrap snapshot should arrive"),
        LocalProviderEvent::BootstrapStateSnapshot(bootstrap_snapshot)
    );

    let next_a = subscription
        .next_event()
        .await
        .expect("watch event should arrive");
    let next_b = subscription
        .next_event()
        .await
        .expect("watch event should arrive");
    let saw_leases = matches!(
        &next_a,
        LocalProviderEvent::LeasesChanged {
            sequence: 5,
            leases
        } if *leases == vec![watched_lease.clone()]
    ) || matches!(
        &next_b,
        LocalProviderEvent::LeasesChanged {
            sequence: 5,
            leases
        } if *leases == vec![watched_lease.clone()]
    );
    let saw_state = matches!(
        &next_a,
        LocalProviderEvent::StateSnapshot {
            sequence: 9,
            snapshot
        } if *snapshot == watched_snapshot
    ) || matches!(
        &next_b,
        LocalProviderEvent::StateSnapshot {
            sequence: 9,
            snapshot
        } if *snapshot == watched_snapshot
    );
    assert!(saw_leases, "expected a provider lease change event");
    assert!(saw_state, "expected a provider state snapshot event");

    unary_task.await.expect("unary task should complete");
    stream_task.await.expect("stream task should complete");
    let _ = tokio::fs::remove_file(&unary_socket_path).await;
    let _ = tokio::fs::remove_file(&stream_socket_path).await;
}

#[tokio::test]
async fn local_runtime_publisher_registers_provider_and_executor() {
    let socket_path = unique_socket_path("runtime-publisher-register");
    let _ = tokio::fs::remove_file(&socket_path).await;
    let listener = UnixListener::bind(&socket_path).expect("listener should bind");

    let provider = ProviderRecord {
        provider_id: ProviderId::new("provider.local"),
        node_id: NodeId::new("node-a"),
        resource_types: vec![ResourceType::new("camera.device")],
    };
    let executor = ExecutorRecord {
        executor_id: ExecutorId::new("executor.local"),
        node_id: NodeId::new("node-a"),
        runtime_types: vec![RuntimeType::new("graph.exec.v1")],
    };

    let server_task = tokio::spawn(async move {
        for expected_role in [ClientRole::Provider, ClientRole::Executor] {
            let role = expected_role.clone();
            let (mut stream, _) = listener.accept().await.expect("accept should work");
            let hello = read_unary_envelope(&mut stream).await;
            assert!(matches!(
                hello.message,
                ControlMessage::ClientHello(ref request) if request.role == role
            ));
            write_unary_envelope(
                &mut stream,
                &ControlEnvelope {
                    source: LocalAddress::new("orion"),
                    destination: hello.source,
                    message: ControlMessage::ClientWelcome(orion_control_plane::ClientSession {
                        session_id: format!("node-a:{role:?}").into(),
                        role: role.clone(),
                        node_id: NodeId::new("node-a"),
                        source: "publisher".into(),
                        client_name: "publisher".into(),
                    }),
                },
            )
            .await;

            let (mut stream, _) = listener.accept().await.expect("accept should work");
            let request = read_unary_envelope(&mut stream).await;
            match request.message {
                ControlMessage::ProviderState(_) if role == ClientRole::Provider => {}
                ControlMessage::ExecutorState(_) if role == ClientRole::Executor => {}
                other => panic!("unexpected register message: {other:?}"),
            }
            write_unary_envelope(
                &mut stream,
                &ControlEnvelope {
                    source: LocalAddress::new("orion"),
                    destination: request.source,
                    message: ControlMessage::Accepted,
                },
            )
            .await;
        }
    });

    let publisher = LocalRuntimePublisher::builder(
        LocalNodeRuntime::new(&socket_path, &socket_path),
        "publisher",
    )
    .provider(provider)
    .executor(executor)
    .build();
    publisher
        .register_all()
        .await
        .expect("registration should succeed");

    server_task.await.expect("server should complete");
    let _ = tokio::fs::remove_file(&socket_path).await;
}

#[tokio::test]
async fn local_runtime_publisher_publishes_provider_resources_and_executor_snapshot() {
    let socket_path = unique_socket_path("runtime-publisher-publish");
    let _ = tokio::fs::remove_file(&socket_path).await;
    let listener = UnixListener::bind(&socket_path).expect("listener should bind");

    let provider = ProviderRecord {
        provider_id: ProviderId::new("provider.local"),
        node_id: NodeId::new("node-a"),
        resource_types: vec![ResourceType::new("camera.device")],
    };
    let executor = ExecutorRecord {
        executor_id: ExecutorId::new("executor.local"),
        node_id: NodeId::new("node-a"),
        runtime_types: vec![RuntimeType::new("graph.exec.v1")],
    };
    let provider_resource =
        ResourceRecord::builder("resource.camera.raw", "camera.device", "provider.local")
            .availability(orion_control_plane::AvailabilityState::Available)
            .build();
    let workload = WorkloadRecord::builder("workload.camera", "graph.exec.v1", "artifact.camera")
        .desired_state(DesiredState::Running)
        .assigned_to("node-a")
        .build();
    let derived_resource = ResourceRecord::builder(
        "resource.camera.stream",
        "camera.frame_stream",
        "provider.local",
    )
    .realized_by_executor("executor.local")
    .build();
    let expected_provider = provider.clone();
    let expected_executor = executor.clone();
    let expected_provider_resource = provider_resource.clone();
    let expected_workload = workload.clone();
    let expected_derived_resource = derived_resource.clone();

    let server_task = tokio::spawn(async move {
        for expected_message in ["provider", "executor"] {
            let (mut stream, _) = listener.accept().await.expect("accept should work");
            let hello = read_unary_envelope(&mut stream).await;
            write_unary_envelope(
                &mut stream,
                &ControlEnvelope {
                    source: LocalAddress::new("orion"),
                    destination: hello.source,
                    message: ControlMessage::ClientWelcome(orion_control_plane::ClientSession {
                        session_id: format!("node-a:{expected_message}").into(),
                        role: if expected_message == "provider" {
                            ClientRole::Provider
                        } else {
                            ClientRole::Executor
                        },
                        node_id: NodeId::new("node-a"),
                        source: expected_message.into(),
                        client_name: expected_message.into(),
                    }),
                },
            )
            .await;

            let (mut stream, _) = listener.accept().await.expect("accept should work");
            let request = read_unary_envelope(&mut stream).await;
            match request.message {
                ControlMessage::ProviderState(update) if expected_message == "provider" => {
                    assert_eq!(update.provider, expected_provider);
                    assert_eq!(update.resources, vec![expected_provider_resource.clone()]);
                }
                ControlMessage::ExecutorState(update) if expected_message == "executor" => {
                    assert_eq!(update.executor, expected_executor);
                    assert_eq!(update.workloads, vec![expected_workload.clone()]);
                    assert_eq!(update.resources, vec![expected_derived_resource.clone()]);
                }
                other => panic!("unexpected publish message: {other:?}"),
            }
            write_unary_envelope(
                &mut stream,
                &ControlEnvelope {
                    source: LocalAddress::new("orion"),
                    destination: request.source,
                    message: ControlMessage::Accepted,
                },
            )
            .await;
        }
    });

    let publisher = LocalRuntimePublisher::builder(
        LocalNodeRuntime::new(&socket_path, &socket_path),
        "publisher",
    )
    .provider(provider.clone())
    .executor(executor.clone())
    .build();
    publisher
        .publish_all(
            vec![provider_resource.clone()],
            orion_runtime::ExecutorSnapshot {
                executor,
                workloads: vec![workload],
                resources: vec![derived_resource],
            },
        )
        .await
        .expect("publish should succeed");

    server_task.await.expect("server should complete");
    let _ = tokio::fs::remove_file(&socket_path).await;
}

async fn serve_executor_bootstrap_query(
    listener: UnixListener,
    executor: ExecutorRecord,
    workloads: Vec<WorkloadRecord>,
) {
    let (mut stream, _) = listener.accept().await.expect("unary accept should work");
    let hello = read_unary_envelope(&mut stream).await;
    write_unary_envelope(
        &mut stream,
        &ControlEnvelope {
            source: LocalAddress::new("orion"),
            destination: hello.source,
            message: ControlMessage::ClientWelcome(orion_control_plane::ClientSession {
                session_id: "node-a:engine".into(),
                role: ClientRole::Executor,
                node_id: NodeId::new("node-a"),
                source: "engine".into(),
                client_name: "engine".into(),
            }),
        },
    )
    .await;

    let (mut stream, _) = listener.accept().await.expect("query accept should work");
    let request = read_unary_envelope(&mut stream).await;
    assert!(matches!(
        request.message,
        ControlMessage::QueryExecutorWorkloads(ExecutorWorkloadQuery { executor_id }) if executor_id == executor.executor_id
    ));
    write_unary_envelope(
        &mut stream,
        &ControlEnvelope {
            source: LocalAddress::new("orion"),
            destination: request.source,
            message: ControlMessage::ExecutorWorkloads(workloads),
        },
    )
    .await;
}

async fn serve_executor_watch_stream(
    listener: UnixListener,
    executor: ExecutorRecord,
    workloads: Vec<WorkloadRecord>,
    sequence: u64,
) {
    let (mut stream, _) = listener.accept().await.expect("watch accept should work");
    let hello = read_control_frame(&mut stream)
        .await
        .expect("hello frame should decode")
        .expect("hello frame should exist");
    write_welcome(
        &mut stream,
        hello.source,
        ClientRole::Executor,
        "engine-watch",
    )
    .await;

    let request = read_control_frame(&mut stream)
        .await
        .expect("watch request should decode")
        .expect("watch request should exist");
    assert!(matches!(
        request.message,
        ControlMessage::WatchExecutorWorkloads(ExecutorWorkloadQuery { executor_id }) if executor_id == executor.executor_id
    ));
    write_control_frame(
        &mut stream,
        &ControlEnvelope {
            source: LocalAddress::new("orion"),
            destination: request.source.clone(),
            message: ControlMessage::Accepted,
        },
    )
    .await
    .expect("accepted frame should send");
    write_control_frame(
        &mut stream,
        &ControlEnvelope {
            source: LocalAddress::new("orion"),
            destination: request.source,
            message: ControlMessage::ClientEvents(vec![ClientEvent {
                sequence,
                event: ClientEventKind::ExecutorWorkloads {
                    executor_id: executor.executor_id,
                    workloads,
                },
            }]),
        },
    )
    .await
    .expect("client events should send");
}

async fn serve_provider_bootstrap_queries(
    listener: UnixListener,
    provider: ProviderRecord,
    leases: Vec<LeaseRecord>,
    snapshot: StateSnapshot,
) {
    for _ in 0..4 {
        let (mut stream, _) = listener.accept().await.expect("unary accept should work");
        let request = read_unary_envelope(&mut stream).await;
        let message = match request.message {
            ControlMessage::ClientHello(hello) if hello.role == ClientRole::Provider => {
                ControlMessage::ClientWelcome(orion_control_plane::ClientSession {
                    session_id: "node-a:peripherals".into(),
                    role: ClientRole::Provider,
                    node_id: NodeId::new("node-a"),
                    source: "peripherals".into(),
                    client_name: hello.client_name,
                })
            }
            ControlMessage::ClientHello(hello) if hello.role == ClientRole::ControlPlane => {
                ControlMessage::ClientWelcome(orion_control_plane::ClientSession {
                    session_id: "node-a:peripherals-control".into(),
                    role: ClientRole::ControlPlane,
                    node_id: NodeId::new("node-a"),
                    source: "peripherals-control".into(),
                    client_name: hello.client_name,
                })
            }
            ControlMessage::QueryProviderLeases(ProviderLeaseQuery { provider_id }) => {
                assert_eq!(provider_id, provider.provider_id);
                ControlMessage::ProviderLeases(leases.clone())
            }
            ControlMessage::QueryStateSnapshot => ControlMessage::Snapshot(snapshot.clone()),
            other => panic!("unexpected bootstrap query: {other:?}"),
        };
        write_unary_envelope(
            &mut stream,
            &ControlEnvelope {
                source: LocalAddress::new("orion"),
                destination: request.source,
                message,
            },
        )
        .await;
    }
}

async fn serve_provider_and_control_watch_streams(
    listener: UnixListener,
    provider: ProviderRecord,
    leases: Vec<LeaseRecord>,
    snapshot: StateSnapshot,
    lease_sequence: u64,
    snapshot_sequence: u64,
) {
    let mut held_streams = Vec::new();
    for stream_index in 0..2 {
        let (mut stream, _) = listener.accept().await.expect("stream accept should work");
        let hello = read_control_frame(&mut stream)
            .await
            .expect("hello frame should decode")
            .expect("hello frame should exist");
        let role = if stream_index == 0 {
            ClientRole::Provider
        } else {
            ClientRole::ControlPlane
        };
        let client_name = if stream_index == 0 {
            "peripherals-leases"
        } else {
            "peripherals-control"
        };
        write_welcome(&mut stream, hello.source, role, client_name).await;

        let request = read_control_frame(&mut stream)
            .await
            .expect("watch request should decode")
            .expect("watch request should exist");
        let response = match request.message {
            ControlMessage::WatchProviderLeases(ProviderLeaseQuery { provider_id }) => {
                assert_eq!(provider_id, provider.provider_id);
                ControlMessage::ClientEvents(vec![ClientEvent {
                    sequence: lease_sequence,
                    event: ClientEventKind::ProviderLeases {
                        provider_id,
                        leases: leases.clone(),
                    },
                }])
            }
            ControlMessage::WatchState(StateWatch { .. }) => {
                ControlMessage::ClientEvents(vec![ClientEvent {
                    sequence: snapshot_sequence,
                    event: ClientEventKind::StateSnapshot(Box::new(snapshot.clone())),
                }])
            }
            other => panic!("unexpected watch request: {other:?}"),
        };
        write_control_frame(
            &mut stream,
            &ControlEnvelope {
                source: LocalAddress::new("orion"),
                destination: request.source.clone(),
                message: ControlMessage::Accepted,
            },
        )
        .await
        .expect("accepted frame should send");
        write_control_frame(
            &mut stream,
            &ControlEnvelope {
                source: LocalAddress::new("orion"),
                destination: request.source,
                message: response,
            },
        )
        .await
        .expect("watch event should send");
        held_streams.push(stream);
    }
    sleep(Duration::from_millis(50)).await;
}

async fn write_welcome(
    stream: &mut tokio::net::UnixStream,
    destination: LocalAddress,
    role: ClientRole,
    client_name: &str,
) {
    write_control_frame(
        stream,
        &ControlEnvelope {
            source: LocalAddress::new("orion"),
            destination,
            message: ControlMessage::ClientWelcome(orion_control_plane::ClientSession {
                session_id: format!("node-a:{client_name}").into(),
                role,
                node_id: NodeId::new("node-a"),
                source: client_name.into(),
                client_name: client_name.into(),
            }),
        },
    )
    .await
    .expect("welcome frame should send");
}

async fn read_unary_envelope(stream: &mut tokio::net::UnixStream) -> ControlEnvelope {
    use tokio::io::AsyncReadExt;

    let mut payload = Vec::new();
    stream
        .read_to_end(&mut payload)
        .await
        .expect("unary payload should read");
    decode_from_slice(&payload).expect("unary payload should decode")
}

async fn write_unary_envelope(stream: &mut tokio::net::UnixStream, envelope: &ControlEnvelope) {
    use tokio::io::AsyncWriteExt;

    let payload = encode_to_vec(envelope).expect("unary response should encode");
    stream
        .write_all(&payload)
        .await
        .expect("unary payload should write");
    stream
        .shutdown()
        .await
        .expect("unary stream should shutdown");
}
