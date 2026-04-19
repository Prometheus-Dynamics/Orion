use super::*;

#[tokio::test]
async fn node_ipc_server_registers_local_client_session() {
    let (socket_path, app) = build_ipc_app("client-hello");

    let (_path, server) = app
        .start_ipc_server(&socket_path)
        .await
        .expect("ipc server should start");

    let client = UnixControlClient::new(&socket_path);
    let response = client
        .send(ControlEnvelope {
            source: LocalAddress::new("cli"),
            destination: LocalAddress::new("orion"),
            message: ControlMessage::ClientHello(ClientHello {
                client_name: "cli".into(),
                role: ClientRole::ControlPlane,
            }),
        })
        .await
        .expect("client hello should roundtrip");

    match response.message {
        ControlMessage::ClientWelcome(session) => {
            assert_eq!(session.client_name, "cli");
            assert_eq!(session.role, ClientRole::ControlPlane);
            assert_eq!(session.node_id, NodeId::new("node-a"));
        }
        other => panic!("unexpected IPC response: {other:?}"),
    }

    server.abort();
    let _ = fs::remove_file(socket_path);
}

#[tokio::test]
async fn node_ipc_server_accepts_provider_state_updates() {
    let (socket_path, app) = build_ipc_app("provider-state");

    let (_path, server) = app
        .start_ipc_server(&socket_path)
        .await
        .expect("ipc server should start");
    let client = UnixControlClient::new(&socket_path);

    let welcome = client
        .send(ControlEnvelope {
            source: LocalAddress::new("provider-a"),
            destination: LocalAddress::new("orion"),
            message: ControlMessage::ClientHello(ClientHello {
                client_name: "provider-a".into(),
                role: ClientRole::Provider,
            }),
        })
        .await
        .expect("provider hello should roundtrip");
    assert!(matches!(welcome.message, ControlMessage::ClientWelcome(_)));

    let response = client
        .send(ControlEnvelope {
            source: LocalAddress::new("provider-a"),
            destination: LocalAddress::new("orion"),
            message: ControlMessage::ProviderState(ProviderStateUpdate {
                provider: ProviderRecord::builder("provider.socket", "node-a")
                    .resource_type(ResourceType::new("imu.sample"))
                    .build(),
                resources: vec![
                    ResourceRecord::builder("resource.imu-socket", "imu.sample", "provider.socket")
                        .health(HealthState::Healthy)
                        .availability(AvailabilityState::Available)
                        .lease_state(LeaseState::Unleased)
                        .build(),
                ],
            }),
        })
        .await
        .expect("provider state should roundtrip");

    assert!(matches!(response.message, ControlMessage::Accepted));
    let snapshot = app.state_snapshot();
    assert!(
        snapshot
            .state
            .desired
            .providers
            .contains_key(&ProviderId::new("provider.socket"))
    );
    assert!(
        snapshot
            .state
            .observed
            .resources
            .contains_key(&orion::ResourceId::new("resource.imu-socket"))
    );

    server.abort();
    let _ = fs::remove_file(socket_path);
}

#[tokio::test]
async fn node_ipc_provider_state_noops_when_provider_record_is_unchanged() {
    let (socket_path, app) = build_ipc_app("provider-state-dedup");

    let (_path, server) = app
        .start_ipc_server(&socket_path)
        .await
        .expect("ipc server should start");
    let client = UnixControlClient::new(&socket_path);

    let welcome = client
        .send(ControlEnvelope {
            source: LocalAddress::new("provider-dedup"),
            destination: LocalAddress::new("orion"),
            message: ControlMessage::ClientHello(ClientHello {
                client_name: "provider-dedup".into(),
                role: ClientRole::Provider,
            }),
        })
        .await
        .expect("provider hello should roundtrip");
    assert!(matches!(welcome.message, ControlMessage::ClientWelcome(_)));

    let update = ProviderStateUpdate {
        provider: ProviderRecord::builder("provider.socket", "node-a")
            .resource_type(ResourceType::new("imu.sample"))
            .build(),
        resources: vec![
            ResourceRecord::builder("resource.imu-socket", "imu.sample", "provider.socket")
                .health(HealthState::Healthy)
                .availability(AvailabilityState::Available)
                .lease_state(LeaseState::Unleased)
                .build(),
        ],
    };

    let response = client
        .send(ControlEnvelope {
            source: LocalAddress::new("provider-dedup"),
            destination: LocalAddress::new("orion"),
            message: ControlMessage::ProviderState(update.clone()),
        })
        .await
        .expect("initial provider state should roundtrip");
    assert!(matches!(response.message, ControlMessage::Accepted));
    let revision_after_first_update = app.state_snapshot().state.desired.revision;

    let response = client
        .send(ControlEnvelope {
            source: LocalAddress::new("provider-dedup"),
            destination: LocalAddress::new("orion"),
            message: ControlMessage::ProviderState(update),
        })
        .await
        .expect("duplicate provider state should roundtrip");
    assert!(matches!(response.message, ControlMessage::Accepted));
    assert_eq!(
        app.state_snapshot().state.desired.revision,
        revision_after_first_update
    );

    server.abort();
    let _ = fs::remove_file(socket_path);
}

#[tokio::test]
async fn node_ipc_server_accepts_executor_state_updates() {
    let (socket_path, app) = build_ipc_app("executor-state");

    let mut desired = app.state_snapshot().state.desired;
    desired.put_workload(
        WorkloadRecord::builder(
            WorkloadId::new("workload.socket"),
            RuntimeType::new("graph.exec.v1"),
            ArtifactId::new("artifact.socket"),
        )
        .desired_state(DesiredState::Running)
        .assigned_to(NodeId::new("node-a"))
        .build(),
    );
    app.replace_desired(desired);

    let (_path, server) = app
        .start_ipc_server(&socket_path)
        .await
        .expect("ipc server should start");
    let client = UnixControlClient::new(&socket_path);

    let welcome = client
        .send(ControlEnvelope {
            source: LocalAddress::new("executor-a"),
            destination: LocalAddress::new("orion"),
            message: ControlMessage::ClientHello(ClientHello {
                client_name: "executor-a".into(),
                role: ClientRole::Executor,
            }),
        })
        .await
        .expect("executor hello should roundtrip");
    assert!(matches!(welcome.message, ControlMessage::ClientWelcome(_)));

    let response = client
        .send(ControlEnvelope {
            source: LocalAddress::new("executor-a"),
            destination: LocalAddress::new("orion"),
            message: ControlMessage::ExecutorState(ExecutorStateUpdate {
                executor: ExecutorRecord::builder("executor.socket", "node-a")
                    .runtime_type(RuntimeType::new("graph.exec.v1"))
                    .build(),
                workloads: vec![
                    WorkloadRecord::builder(
                        WorkloadId::new("workload.socket"),
                        RuntimeType::new("graph.exec.v1"),
                        ArtifactId::new("artifact.socket"),
                    )
                    .desired_state(DesiredState::Running)
                    .assigned_to(NodeId::new("node-a"))
                    .observed_state(WorkloadObservedState::Running)
                    .build(),
                ],
                resources: Vec::new(),
            }),
        })
        .await
        .expect("executor state should roundtrip");

    assert!(matches!(response.message, ControlMessage::Accepted));
    let snapshot = app.state_snapshot();
    assert!(
        snapshot
            .state
            .desired
            .executors
            .contains_key(&ExecutorId::new("executor.socket"))
    );
    assert_eq!(
        snapshot
            .state
            .observed
            .workloads
            .get(&WorkloadId::new("workload.socket"))
            .expect("observed workload should exist")
            .observed_state,
        WorkloadObservedState::Running
    );

    server.abort();
    let _ = fs::remove_file(socket_path);
}

#[tokio::test]
async fn node_ipc_executor_state_noops_when_executor_record_is_unchanged() {
    let (socket_path, app) = build_ipc_app("executor-state-dedup");

    let mut desired = app.state_snapshot().state.desired;
    desired.put_workload(
        WorkloadRecord::builder(
            WorkloadId::new("workload.socket"),
            RuntimeType::new("graph.exec.v1"),
            ArtifactId::new("artifact.socket"),
        )
        .desired_state(DesiredState::Running)
        .assigned_to(NodeId::new("node-a"))
        .build(),
    );
    app.replace_desired(desired);

    let (_path, server) = app
        .start_ipc_server(&socket_path)
        .await
        .expect("ipc server should start");
    let client = UnixControlClient::new(&socket_path);

    let welcome = client
        .send(ControlEnvelope {
            source: LocalAddress::new("executor-dedup"),
            destination: LocalAddress::new("orion"),
            message: ControlMessage::ClientHello(ClientHello {
                client_name: "executor-dedup".into(),
                role: ClientRole::Executor,
            }),
        })
        .await
        .expect("executor hello should roundtrip");
    assert!(matches!(welcome.message, ControlMessage::ClientWelcome(_)));

    let update = ExecutorStateUpdate {
        executor: ExecutorRecord::builder("executor.socket", "node-a")
            .runtime_type(RuntimeType::new("graph.exec.v1"))
            .build(),
        workloads: vec![
            WorkloadRecord::builder(
                WorkloadId::new("workload.socket"),
                RuntimeType::new("graph.exec.v1"),
                ArtifactId::new("artifact.socket"),
            )
            .desired_state(DesiredState::Running)
            .assigned_to(NodeId::new("node-a"))
            .observed_state(WorkloadObservedState::Running)
            .build(),
        ],
        resources: Vec::new(),
    };

    let response = client
        .send(ControlEnvelope {
            source: LocalAddress::new("executor-dedup"),
            destination: LocalAddress::new("orion"),
            message: ControlMessage::ExecutorState(update.clone()),
        })
        .await
        .expect("initial executor state should roundtrip");
    assert!(matches!(response.message, ControlMessage::Accepted));
    let revision_after_first_update = app.state_snapshot().state.desired.revision;

    let response = client
        .send(ControlEnvelope {
            source: LocalAddress::new("executor-dedup"),
            destination: LocalAddress::new("orion"),
            message: ControlMessage::ExecutorState(update),
        })
        .await
        .expect("duplicate executor state should roundtrip");
    assert!(matches!(response.message, ControlMessage::Accepted));
    assert_eq!(
        app.state_snapshot().state.desired.revision,
        revision_after_first_update
    );

    server.abort();
    let _ = fs::remove_file(socket_path);
}

#[tokio::test]
async fn node_ipc_server_returns_assigned_workloads_for_executor() {
    let (socket_path, app) = build_ipc_app("executor-query");

    let mut desired = app.state_snapshot().state.desired;
    desired.put_executor(
        ExecutorRecord::builder("executor.query", "node-a")
            .runtime_type(RuntimeType::new("graph.exec.v1"))
            .build(),
    );
    desired.put_workload(
        WorkloadRecord::builder(
            WorkloadId::new("workload.query"),
            RuntimeType::new("graph.exec.v1"),
            ArtifactId::new("artifact.query"),
        )
        .desired_state(DesiredState::Running)
        .assigned_to(NodeId::new("node-a"))
        .build(),
    );
    app.replace_desired(desired);

    let (_path, server) = app
        .start_ipc_server(&socket_path)
        .await
        .expect("ipc server should start");
    let client = UnixControlClient::new(&socket_path);

    let _ = client
        .send(ControlEnvelope {
            source: LocalAddress::new("executor-query"),
            destination: LocalAddress::new("orion"),
            message: ControlMessage::ClientHello(ClientHello {
                client_name: "executor-query".into(),
                role: ClientRole::Executor,
            }),
        })
        .await
        .expect("executor hello should roundtrip");

    let response = client
        .send(ControlEnvelope {
            source: LocalAddress::new("executor-query"),
            destination: LocalAddress::new("orion"),
            message: ControlMessage::QueryExecutorWorkloads(
                orion::control_plane::ExecutorWorkloadQuery {
                    executor_id: ExecutorId::new("executor.query"),
                },
            ),
        })
        .await
        .expect("executor workload query should roundtrip");

    match response.message {
        ControlMessage::ExecutorWorkloads(workloads) => {
            assert_eq!(workloads.len(), 1);
            assert_eq!(workloads[0].workload_id, WorkloadId::new("workload.query"));
        }
        other => panic!("unexpected executor query response: {other:?}"),
    }

    server.abort();
    let _ = fs::remove_file(socket_path);
}

#[tokio::test]
async fn node_ipc_server_returns_provider_leases() {
    let (socket_path, app) = build_ipc_app("provider-leases");

    let mut desired = app.state_snapshot().state.desired;
    desired.put_provider(
        ProviderRecord::builder("provider.query", "node-a")
            .resource_type(ResourceType::new("imu.sample"))
            .build(),
    );
    desired.put_resource(
        ResourceRecord::builder("resource.query", "imu.sample", "provider.query")
            .health(HealthState::Healthy)
            .availability(AvailabilityState::Available)
            .lease_state(LeaseState::Leased)
            .build(),
    );
    desired.put_lease(
        orion::control_plane::LeaseRecord::builder("resource.query")
            .lease_state(LeaseState::Leased)
            .holder_node("node-a")
            .holder_workload("workload.query")
            .build(),
    );
    app.replace_desired(desired);

    let (_path, server) = app
        .start_ipc_server(&socket_path)
        .await
        .expect("ipc server should start");
    let client = UnixControlClient::new(&socket_path);

    let _ = client
        .send(ControlEnvelope {
            source: LocalAddress::new("provider-query"),
            destination: LocalAddress::new("orion"),
            message: ControlMessage::ClientHello(ClientHello {
                client_name: "provider-query".into(),
                role: ClientRole::Provider,
            }),
        })
        .await
        .expect("provider hello should roundtrip");

    let response = client
        .send(ControlEnvelope {
            source: LocalAddress::new("provider-query"),
            destination: LocalAddress::new("orion"),
            message: ControlMessage::QueryProviderLeases(
                orion::control_plane::ProviderLeaseQuery {
                    provider_id: ProviderId::new("provider.query"),
                },
            ),
        })
        .await
        .expect("provider lease query should roundtrip");

    match response.message {
        ControlMessage::ProviderLeases(leases) => {
            assert_eq!(leases.len(), 1);
            assert_eq!(
                leases[0].resource_id,
                orion::ResourceId::new("resource.query")
            );
        }
        other => panic!("unexpected provider lease response: {other:?}"),
    }

    server.abort();
    let _ = fs::remove_file(socket_path);
}

#[tokio::test]
async fn node_ipc_server_queues_state_events_for_registered_watchers() {
    let (socket_path, app) = build_ipc_app("watch-state");

    let (_path, server) = app
        .start_ipc_server(&socket_path)
        .await
        .expect("ipc server should start");
    let client = UnixControlClient::new(&socket_path);

    let _ = client
        .send(ControlEnvelope {
            source: LocalAddress::new("cli-watch"),
            destination: LocalAddress::new("orion"),
            message: ControlMessage::ClientHello(ClientHello {
                client_name: "cli-watch".into(),
                role: ClientRole::ControlPlane,
            }),
        })
        .await
        .expect("client hello should roundtrip");

    let mut desired = app.state_snapshot().state.desired;
    desired.put_node(orion::control_plane::NodeRecord::builder("node-extra").build());
    app.replace_desired(desired);

    let response = client
        .send(ControlEnvelope {
            source: LocalAddress::new("cli-watch"),
            destination: LocalAddress::new("orion"),
            message: ControlMessage::WatchState(orion::control_plane::StateWatch {
                desired_revision: Revision::ZERO,
            }),
        })
        .await
        .expect("watch state should roundtrip");
    assert!(matches!(response.message, ControlMessage::Accepted));

    let response = client
        .send(ControlEnvelope {
            source: LocalAddress::new("cli-watch"),
            destination: LocalAddress::new("orion"),
            message: ControlMessage::PollClientEvents(ClientEventPoll {
                after_sequence: 0,
                max_events: 8,
            }),
        })
        .await
        .expect("client events should roundtrip");

    match response.message {
        ControlMessage::ClientEvents(events) => {
            assert_eq!(events.len(), 1);
            assert_eq!(events[0].sequence, 1);
            match &events[0].event {
                ClientEventKind::StateSnapshot(snapshot) => {
                    assert!(snapshot.state.desired.revision > Revision::ZERO);
                }
                ClientEventKind::ExecutorWorkloads { .. }
                | ClientEventKind::ProviderLeases { .. } => {
                    panic!("unexpected non-state event");
                }
            }
        }
        other => panic!("unexpected state watch response: {other:?}"),
    }

    let response = client
        .send(ControlEnvelope {
            source: LocalAddress::new("cli-watch"),
            destination: LocalAddress::new("orion"),
            message: ControlMessage::PollClientEvents(ClientEventPoll {
                after_sequence: 1,
                max_events: 8,
            }),
        })
        .await
        .expect("drained client events should roundtrip");
    assert_eq!(response.message, ControlMessage::ClientEvents(Vec::new()));

    server.abort();
    let _ = fs::remove_file(socket_path);
}

#[tokio::test]
async fn node_ipc_server_does_not_queue_duplicate_state_events_without_new_revision() {
    let (socket_path, app) = build_ipc_app("watch-state-dedup");

    let (_path, server) = app
        .start_ipc_server(&socket_path)
        .await
        .expect("ipc server should start");
    let client = UnixControlClient::new(&socket_path);

    let _ = client
        .send(ControlEnvelope {
            source: LocalAddress::new("cli-watch-dedup"),
            destination: LocalAddress::new("orion"),
            message: ControlMessage::ClientHello(ClientHello {
                client_name: "cli-watch-dedup".into(),
                role: ClientRole::ControlPlane,
            }),
        })
        .await
        .expect("client hello should roundtrip");

    let response = client
        .send(ControlEnvelope {
            source: LocalAddress::new("cli-watch-dedup"),
            destination: LocalAddress::new("orion"),
            message: ControlMessage::WatchState(orion::control_plane::StateWatch {
                desired_revision: Revision::ZERO,
            }),
        })
        .await
        .expect("watch registration should roundtrip");
    assert!(matches!(response.message, ControlMessage::Accepted));

    let mut desired = app.state_snapshot().state.desired;
    desired.put_node(orion::control_plane::NodeRecord::builder("node-extra").build());
    app.replace_desired(desired);

    let response = client
        .send(ControlEnvelope {
            source: LocalAddress::new("cli-watch-dedup"),
            destination: LocalAddress::new("orion"),
            message: ControlMessage::PollClientEvents(ClientEventPoll {
                after_sequence: 0,
                max_events: 8,
            }),
        })
        .await
        .expect("first event poll should roundtrip");
    let ControlMessage::ClientEvents(events) = response.message else {
        panic!("expected queued client events");
    };
    assert_eq!(events.len(), 1);

    let response = client
        .send(ControlEnvelope {
            source: LocalAddress::new("cli-watch-dedup"),
            destination: LocalAddress::new("orion"),
            message: ControlMessage::PollClientEvents(ClientEventPoll {
                after_sequence: 1,
                max_events: 8,
            }),
        })
        .await
        .expect("second event poll should roundtrip");
    assert_eq!(response.message, ControlMessage::ClientEvents(Vec::new()));

    server.abort();
    let _ = fs::remove_file(socket_path);
}
