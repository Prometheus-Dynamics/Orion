//! Volatile status lane over local IPC: ownership, caps, TTL expiry, and coalesced watches.

use super::*;
use crate::config::NodeRuntimeTuning;
use orion::control_plane::{
    ClientEventKind, ClientEventPoll, ClientHello, ClientRole, ExecutorStateUpdate,
    ProviderStateUpdate, StatusEntry, StatusQuery, StatusSubject,
};
use orion::transport::ipc::UnixControlClient;
use orion_core::ClientName;

const PROVIDER: &str = "provider.status";

fn status_app(
    name: &str,
    configure: impl FnOnce(NodeRuntimeTuning) -> NodeRuntimeTuning,
) -> NodeApp {
    NodeApp::builder()
        .config(test_node_config(NodeId::new(name), name).with_runtime_tuning_mut(configure))
        .try_build()
        .expect("node app should build")
}

fn hello(app: &NodeApp, source: &str, role: ClientRole) -> LocalAddress {
    let source = LocalAddress::new(source);
    app.apply_local_control_message(
        &source,
        ControlMessage::ClientHello(ClientHello {
            client_name: ClientName::new(source.as_str()),
            role,
        }),
    )
    .expect("hello should be accepted");
    source
}

fn publish_provider(app: &NodeApp, source: &LocalAddress, provider: &str) {
    let response = app
        .apply_local_control_message(
            source,
            ControlMessage::ProviderState(ProviderStateUpdate {
                provider: ProviderRecord::builder(
                    ProviderId::new(provider),
                    app.config.node_id.clone(),
                )
                .resource_type(ResourceType::new("camera.frame"))
                .build(),
                resources: vec![
                    ResourceRecord::builder(
                        ResourceId::new(format!("{provider}.camera")),
                        "camera.frame",
                        ProviderId::new(provider),
                    )
                    .health(HealthState::Healthy)
                    .availability(AvailabilityState::Available)
                    .build(),
                ],
            }),
        )
        .expect("provider state should apply");
    assert_eq!(response, ControlMessage::Accepted);
}

fn provider_subject(provider: &str) -> StatusSubject {
    StatusSubject::Provider(ProviderId::new(provider))
}

fn uint(subject: StatusSubject, key: &str, value: u64) -> StatusEntry {
    StatusEntry::new(subject, key, TypedConfigValue::UInt(value))
}

fn query(app: &NodeApp, source: &LocalAddress, query: StatusQuery) -> Vec<StatusEntry> {
    match app
        .apply_local_control_message(source, ControlMessage::QueryStatus(query))
        .expect("query should succeed")
    {
        ControlMessage::Status(entries) => entries,
        other => panic!("unexpected status response: {other:?}"),
    }
}

fn status_events(app: &NodeApp, source: &LocalAddress) -> Vec<orion::control_plane::StatusChange> {
    match app
        .apply_local_control_message(
            source,
            ControlMessage::PollClientEvents(ClientEventPoll {
                after_sequence: 0,
                max_events: 64,
            }),
        )
        .expect("poll should succeed")
    {
        ControlMessage::ClientEvents(events) => events
            .into_iter()
            .filter_map(|event| match event.event {
                ClientEventKind::Status(change) => Some(change),
                _ => None,
            })
            .collect(),
        other => panic!("unexpected poll response: {other:?}"),
    }
}

#[test]
fn providers_publish_and_anyone_queries_their_status() {
    let app = status_app("node.status.basic", |tuning| tuning);
    let provider = hello(&app, "status-provider", ClientRole::Provider);
    publish_provider(&app, &provider, PROVIDER);
    let reader = hello(&app, "status-reader", ClientRole::ControlPlane);

    let resource = StatusSubject::Resource(ResourceId::new(format!("{PROVIDER}.camera")));
    app.apply_local_control_message(
        &provider,
        ControlMessage::PublishStatus(vec![
            uint(provider_subject(PROVIDER), "fps", 30).with_ttl_ms(10_000),
            uint(resource.clone(), "exposure_us", 800),
        ]),
    )
    .expect("owned subjects should be accepted");

    let all = query(&app, &reader, StatusQuery::all());
    assert_eq!(all.len(), 2);
    let fps = all
        .iter()
        .find(|entry| entry.key == "fps")
        .expect("fps entry");
    assert_eq!(fps.ttl_ms, 10_000);
    assert!(fps.published_at_ms > 0);
    let exposure = all
        .iter()
        .find(|entry| entry.key == "exposure_us")
        .expect("exposure entry");
    assert_eq!(
        exposure.ttl_ms,
        app.config.runtime_tuning.status_max_ttl.as_millis() as u64,
        "ttl 0 means the node maximum"
    );
    assert_eq!(
        query(&app, &reader, StatusQuery::subject(resource)).len(),
        1
    );
    assert_eq!(
        query(&app, &reader, StatusQuery::all().with_key_prefix("fp")).len(),
        1
    );

    // Latest value wins.
    app.apply_local_control_message(
        &provider,
        ControlMessage::PublishStatus(vec![uint(provider_subject(PROVIDER), "fps", 25)]),
    )
    .expect("update should be accepted");
    let fps = query(
        &app,
        &reader,
        StatusQuery::subject(provider_subject(PROVIDER)),
    );
    assert_eq!(fps.len(), 1);
    assert_eq!(fps[0].value, TypedConfigValue::UInt(25));

    let usage = app.observability_snapshot().resource_usage.status_lane;
    assert_eq!(usage.entries, 2);
    assert_eq!(usage.publishers, 1);
    assert_eq!(usage.published_total, 3);
}

#[test]
fn clients_can_only_publish_for_subjects_they_own() {
    let app = status_app("node.status.owner", |tuning| tuning);
    let owner = hello(&app, "status-owner", ClientRole::Provider);
    publish_provider(&app, &owner, PROVIDER);
    let intruder = hello(&app, "status-intruder", ClientRole::Provider);
    publish_provider(&app, &intruder, "provider.other");

    for subject in [
        provider_subject(PROVIDER),
        StatusSubject::Resource(ResourceId::new(format!("{PROVIDER}.camera"))),
        StatusSubject::Executor(ExecutorId::new("executor.nobody")),
        StatusSubject::Workload(WorkloadId::new("workload.nobody")),
    ] {
        let error = app
            .apply_local_control_message(
                &intruder,
                ControlMessage::PublishStatus(vec![
                    uint(provider_subject("provider.other"), "ok", 1),
                    uint(subject.clone(), "stolen", 1),
                ]),
            )
            .expect_err("foreign subjects are refused");
        assert!(
            error.to_string().contains("does not own status subject"),
            "{subject}: {error}"
        );
    }
    let reader = hello(&app, "status-reader", ClientRole::ControlPlane);
    assert!(
        query(&app, &reader, StatusQuery::all()).is_empty(),
        "a refused batch stores nothing"
    );
    assert_eq!(
        app.observability_snapshot()
            .resource_usage
            .status_lane
            .unauthorized_total,
        4
    );
}

#[test]
fn executors_publish_for_their_executor_and_assigned_workloads() {
    let app = status_app("node.status.executor", |tuning| tuning);
    let executor_source = hello(&app, "status-executor", ClientRole::Executor);
    let executor = ExecutorRecord::builder("executor.status", app.config.node_id.clone())
        .runtime_type(RuntimeType::new("graph.exec.v1"))
        .build();
    app.apply_local_control_message(
        &executor_source,
        ControlMessage::ExecutorState(ExecutorStateUpdate {
            executor: executor.clone(),
            workloads: Vec::new(),
            resources: Vec::new(),
        }),
    )
    .expect("executor state should apply");
    let mut desired = app.state_snapshot().state.desired;
    desired.put_workload(
        WorkloadRecord::builder(
            WorkloadId::new("workload.status"),
            RuntimeType::new("graph.exec.v1"),
            ArtifactId::new("artifact.status"),
        )
        .assigned_to(app.config.node_id.clone())
        .build(),
    );
    app.replace_desired(desired);

    app.apply_local_control_message(
        &executor_source,
        ControlMessage::PublishStatus(vec![
            uint(
                StatusSubject::Executor(executor.executor_id.clone()),
                "slots_free",
                2,
            ),
            uint(
                StatusSubject::Workload(WorkloadId::new("workload.status")),
                "latency_ms",
                12,
            ),
        ]),
    )
    .expect("executor and assigned workload are owned");
    assert_eq!(query(&app, &executor_source, StatusQuery::all()).len(), 2);
}

#[test]
fn caps_and_invalid_entries_reject_whole_batches() {
    let app = status_app("node.status.caps", |tuning: NodeRuntimeTuning| {
        tuning.with_status_limits(3, 2, Duration::from_secs(60))
    });
    let provider = hello(&app, "status-provider", ClientRole::Provider);
    publish_provider(&app, &provider, PROVIDER);

    let error = app
        .apply_local_control_message(
            &provider,
            ControlMessage::PublishStatus(vec![
                uint(provider_subject(PROVIDER), "a", 1),
                uint(provider_subject(PROVIDER), "b", 1),
                uint(provider_subject(PROVIDER), "c", 1),
            ]),
        )
        .expect_err("publisher cap is 2");
    assert!(error.to_string().contains("cap"), "{error}");
    let error = app
        .apply_local_control_message(
            &provider,
            ControlMessage::PublishStatus(vec![uint(provider_subject(PROVIDER), "", 1)]),
        )
        .expect_err("empty keys are invalid");
    assert!(error.to_string().contains("empty key"), "{error}");
    app.apply_local_control_message(
        &provider,
        ControlMessage::PublishStatus(vec![
            uint(provider_subject(PROVIDER), "a", 1),
            uint(provider_subject(PROVIDER), "b", 1),
        ]),
    )
    .expect("two entries fit");
    let usage = app.observability_snapshot().resource_usage.status_lane;
    assert_eq!(usage.entries, 2);
    assert_eq!(usage.dropped_total, 4);
    assert_eq!(usage.max_entries, 3);
    assert_eq!(usage.max_entries_per_publisher, 2);
}

#[test]
fn watchers_get_a_bootstrap_then_coalesced_changes_and_expirations() {
    let app = status_app("node.status.watch", |tuning| tuning);
    let provider = hello(&app, "status-provider", ClientRole::Provider);
    publish_provider(&app, &provider, PROVIDER);
    app.apply_local_control_message(
        &provider,
        ControlMessage::PublishStatus(vec![uint(provider_subject(PROVIDER), "fps", 30)]),
    )
    .expect("publish should succeed");

    let watcher = hello(&app, "status-watcher", ClientRole::ControlPlane);
    app.apply_local_control_message(
        &watcher,
        ControlMessage::WatchStatus(StatusQuery::subject(provider_subject(PROVIDER))),
    )
    .expect("watch should succeed");
    // A burst of updates while the watcher does not read: the queued status event is merged in
    // place, newest value per key, so the backlog stays one event.
    for value in 0..50 {
        app.apply_local_control_message(
            &provider,
            ControlMessage::PublishStatus(vec![
                uint(provider_subject(PROVIDER), "fps", value),
                uint(provider_subject(PROVIDER), "short", value).with_ttl_ms(1),
            ]),
        )
        .expect("publish should succeed");
    }
    let changes = status_events(&app, &watcher);
    assert_eq!(changes.len(), 1, "status events coalesce: {changes:?}");
    let change = &changes[0];
    assert!(change.bootstrap);
    let fps = change
        .updated
        .iter()
        .find(|entry| entry.key == "fps")
        .expect("fps");
    assert_eq!(fps.value, TypedConfigValue::UInt(49));

    std::thread::sleep(Duration::from_millis(5));
    app.sweep_expired_status();
    let changes = status_events(&app, &watcher);
    assert_eq!(changes.len(), 1);
    assert!(!changes[0].bootstrap);
    assert!(changes[0].updated.is_empty());
    assert_eq!(changes[0].expired.len(), 1);
    assert_eq!(changes[0].expired[0].key, "short");
    assert!(
        app.observability_snapshot()
            .resource_usage
            .status_lane
            .expired_total
            >= 1
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reconcile_loop_sweeps_expired_entries_without_queries() {
    let app = status_app("node.status.sweep", |tuning| tuning);
    let provider = hello(&app, "status-provider", ClientRole::Provider);
    publish_provider(&app, &provider, PROVIDER);
    let watcher = hello(&app, "status-watcher", ClientRole::ControlPlane);
    app.apply_local_control_message(&watcher, ControlMessage::WatchStatus(StatusQuery::all()))
        .expect("watch should succeed");
    let handle = app.spawn_reconcile_loop(Duration::from_millis(10));
    app.apply_local_control_message(
        &provider,
        ControlMessage::PublishStatus(vec![
            uint(provider_subject(PROVIDER), "blink", 1).with_ttl_ms(50),
        ]),
    )
    .expect("publish should succeed");
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    loop {
        let usage = app.observability_snapshot().resource_usage.status_lane;
        if usage.expired_total == 1 && usage.entries == 0 {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "entry should expire: {usage:?}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    handle.shutdown().await;
    let expired: Vec<_> = status_events(&app, &watcher)
        .into_iter()
        .flat_map(|change| change.expired)
        .collect();
    assert_eq!(expired.len(), 1);
}

#[tokio::test]
async fn status_messages_are_authorized_by_role_over_ipc() {
    let (socket_path, app) = build_ipc_app("status-roles");
    let (_path, server) = app
        .start_ipc_server(&socket_path)
        .await
        .expect("ipc server should start");
    let client = UnixControlClient::new(&socket_path);
    let send = |source: &'static str, message: ControlMessage| {
        let client = client.clone();
        async move {
            client
                .send(ControlEnvelope {
                    source: LocalAddress::new(source),
                    destination: LocalAddress::new("orion"),
                    message,
                })
                .await
                .expect("ipc roundtrip")
                .message
        }
    };
    send(
        "status-cli",
        ControlMessage::ClientHello(ClientHello {
            client_name: "status-cli".into(),
            role: ClientRole::ControlPlane,
        }),
    )
    .await;
    let response = send(
        "status-cli",
        ControlMessage::PublishStatus(vec![uint(provider_subject(PROVIDER), "fps", 1)]),
    )
    .await;
    assert!(
        matches!(response, ControlMessage::Rejected(_)),
        "control-plane clients cannot publish status: {response:?}"
    );
    let response = send(
        "status-cli",
        ControlMessage::QueryStatus(StatusQuery::all()),
    )
    .await;
    assert_eq!(response, ControlMessage::Status(Vec::new()));

    server.abort();
    let _ = fs::remove_file(socket_path);
}
