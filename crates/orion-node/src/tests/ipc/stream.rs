use super::*;

#[tokio::test]
async fn node_ipc_stream_server_pushes_state_events_to_live_stream_clients() {
    let (_socket_path, app) = build_stream_ipc_app("stream-control");
    let stream_socket_path = temp_stream_socket_path("stream-events");

    let (_path, server) = app
        .start_ipc_stream_server(&stream_socket_path)
        .await
        .expect("ipc stream server should start");
    let mut client = UnixControlStreamClient::connect(&stream_socket_path)
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
        .expect("stream client hello should send");
    let welcome = client
        .recv()
        .await
        .expect("stream welcome frame should arrive")
        .expect("stream welcome should exist");
    assert!(matches!(welcome.message, ControlMessage::ClientWelcome(_)));

    client
        .send(&ControlEnvelope {
            source: LocalAddress::new("cli-stream"),
            destination: LocalAddress::new("orion"),
            message: ControlMessage::WatchState(orion::control_plane::StateWatch {
                desired_revision: Revision::ZERO,
            }),
        })
        .await
        .expect("stream watch registration should send");
    let accepted = client
        .recv()
        .await
        .expect("stream watch ack should arrive")
        .expect("stream watch ack should exist");
    assert!(matches!(accepted.message, ControlMessage::Accepted));

    let mut desired = app.state_snapshot().state.desired;
    desired.put_node(orion::control_plane::NodeRecord::builder("node-stream-extra").build());
    app.replace_desired(desired);

    let pushed = client
        .recv()
        .await
        .expect("pushed client events should arrive")
        .expect("pushed client events should exist");
    match pushed.message {
        ControlMessage::ClientEvents(events) => {
            assert_eq!(events.len(), 1);
            match &events[0].event {
                ClientEventKind::StateSnapshot(snapshot) => {
                    assert!(
                        snapshot
                            .state
                            .desired
                            .nodes
                            .contains_key(&NodeId::new("node-stream-extra"))
                    );
                }
                ClientEventKind::ExecutorWorkloads { .. }
                | ClientEventKind::ProviderLeases { .. } => {
                    panic!("unexpected non-state event");
                }
            }
        }
        other => panic!("unexpected pushed stream response: {other:?}"),
    }

    server.abort();
    let _ = fs::remove_file(stream_socket_path);
}

#[tokio::test]
async fn node_ipc_stream_server_evicts_stale_clients_that_do_not_pong() {
    let (_socket_path, app) = build_stream_ipc_app("stream-stale-control");
    let stream_socket_path = temp_stream_socket_path("stream-stale-events");

    let (_path, server) = app
        .start_ipc_stream_server(&stream_socket_path)
        .await
        .expect("ipc stream server should start");
    let mut client = UnixControlStreamClient::connect(&stream_socket_path)
        .await
        .expect("stream client should connect");

    client
        .send(&ControlEnvelope {
            source: LocalAddress::new("cli-stale"),
            destination: LocalAddress::new("orion"),
            message: ControlMessage::ClientHello(ClientHello {
                client_name: "cli-stale".into(),
                role: ClientRole::ControlPlane,
            }),
        })
        .await
        .expect("stream client hello should send");
    let welcome = client
        .recv()
        .await
        .expect("stream welcome frame should arrive")
        .expect("stream welcome should exist");
    assert!(matches!(welcome.message, ControlMessage::ClientWelcome(_)));

    let ping = client
        .recv()
        .await
        .expect("stream heartbeat ping should arrive")
        .expect("stream heartbeat ping should exist");
    assert!(matches!(ping.message, ControlMessage::Ping));

    let closed = client
        .recv()
        .await
        .expect("stale stream close should not error");
    assert!(closed.is_none(), "stale stream should be closed by server");

    server.abort();
    let _ = fs::remove_file(stream_socket_path);
}

#[tokio::test]
async fn node_ipc_stream_server_pushes_executor_workload_events() {
    let (_socket_path, app) = build_stream_ipc_app("stream-executor-control");
    let stream_socket_path = temp_stream_socket_path("stream-executor-events");

    let mut desired = app.state_snapshot().state.desired;
    desired.put_executor(
        ExecutorRecord::builder("executor.stream", "node-a")
            .runtime_type(RuntimeType::new("graph.exec.v1"))
            .build(),
    );
    desired.put_workload(
        WorkloadRecord::builder(
            WorkloadId::new("workload.stream"),
            RuntimeType::new("graph.exec.v1"),
            ArtifactId::new("artifact.stream"),
        )
        .desired_state(DesiredState::Running)
        .assigned_to(NodeId::new("node-a"))
        .build(),
    );
    app.replace_desired(desired);

    let (_path, server) = app
        .start_ipc_stream_server(&stream_socket_path)
        .await
        .expect("ipc stream server should start");
    let mut client = UnixControlStreamClient::connect(&stream_socket_path)
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
        .expect("stream client hello should send");
    let _ = client.recv().await.expect("welcome read").expect("welcome");

    client
        .send(&ControlEnvelope {
            source: LocalAddress::new("executor-stream"),
            destination: LocalAddress::new("orion"),
            message: ControlMessage::WatchExecutorWorkloads(
                orion::control_plane::ExecutorWorkloadQuery {
                    executor_id: ExecutorId::new("executor.stream"),
                },
            ),
        })
        .await
        .expect("executor watch send should succeed");
    let accepted = client
        .recv()
        .await
        .expect("accepted read")
        .expect("accepted");
    assert!(matches!(accepted.message, ControlMessage::Accepted));

    let pushed = client.recv().await.expect("event read").expect("event");
    match pushed.message {
        ControlMessage::ClientEvents(events) => {
            assert_eq!(events.len(), 1);
            match &events[0].event {
                ClientEventKind::ExecutorWorkloads {
                    executor_id,
                    workloads,
                } => {
                    assert_eq!(executor_id, &ExecutorId::new("executor.stream"));
                    assert_eq!(workloads.len(), 1);
                    assert_eq!(workloads[0].workload_id, WorkloadId::new("workload.stream"));
                }
                other => panic!("unexpected executor event kind: {other:?}"),
            }
        }
        other => panic!("unexpected pushed executor response: {other:?}"),
    }

    server.abort();
    let _ = fs::remove_file(stream_socket_path);
}

#[tokio::test]
async fn node_ipc_stream_server_pushes_provider_lease_events() {
    let (_socket_path, app) = build_stream_ipc_app("stream-provider-control");
    let stream_socket_path = temp_stream_socket_path("stream-provider-events");

    let mut desired = app.state_snapshot().state.desired;
    desired.put_provider(
        ProviderRecord::builder("provider.stream", "node-a")
            .resource_type(ResourceType::new("imu.sample"))
            .build(),
    );
    desired.put_resource(
        ResourceRecord::builder("resource.stream", "imu.sample", "provider.stream")
            .health(HealthState::Healthy)
            .availability(AvailabilityState::Available)
            .lease_state(LeaseState::Leased)
            .build(),
    );
    desired.put_lease(
        orion::control_plane::LeaseRecord::builder("resource.stream")
            .lease_state(LeaseState::Leased)
            .holder_node("node-a")
            .holder_workload("workload.stream")
            .build(),
    );
    app.replace_desired(desired);

    let (_path, server) = app
        .start_ipc_stream_server(&stream_socket_path)
        .await
        .expect("ipc stream server should start");
    let mut client = UnixControlStreamClient::connect(&stream_socket_path)
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
        .expect("stream client hello should send");
    let _ = client.recv().await.expect("welcome read").expect("welcome");

    client
        .send(&ControlEnvelope {
            source: LocalAddress::new("provider-stream"),
            destination: LocalAddress::new("orion"),
            message: ControlMessage::WatchProviderLeases(
                orion::control_plane::ProviderLeaseQuery {
                    provider_id: ProviderId::new("provider.stream"),
                },
            ),
        })
        .await
        .expect("provider watch send should succeed");
    let accepted = client
        .recv()
        .await
        .expect("accepted read")
        .expect("accepted");
    assert!(matches!(accepted.message, ControlMessage::Accepted));

    let pushed = client.recv().await.expect("event read").expect("event");
    match pushed.message {
        ControlMessage::ClientEvents(events) => {
            assert_eq!(events.len(), 1);
            match &events[0].event {
                ClientEventKind::ProviderLeases {
                    provider_id,
                    leases,
                } => {
                    assert_eq!(provider_id, &ProviderId::new("provider.stream"));
                    assert_eq!(leases.len(), 1);
                    assert_eq!(
                        leases[0].resource_id,
                        orion::ResourceId::new("resource.stream")
                    );
                }
                other => panic!("unexpected provider event kind: {other:?}"),
            }
        }
        other => panic!("unexpected pushed provider response: {other:?}"),
    }

    server.abort();
    let _ = fs::remove_file(stream_socket_path);
}
