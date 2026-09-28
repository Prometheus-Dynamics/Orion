use super::*;
use orion::transport::ipc::UnixControlHandler;

fn resource_usage_app(state_dir: PathBuf) -> NodeApp {
    NodeApp::builder()
        .config(test_node_config_with_state_dir_and_auth(
            "node-resource-usage",
            "node-resource-usage",
            state_dir.clone(),
            crate::PeerAuthenticationMode::Disabled,
        ))
        .with_max_mutation_history_batches(4)
        .with_local_stream_send_queue_capacity(16)
        .with_local_client_event_queue_limit(8)
        .with_audit_log_path(state_dir.join("audit.log"))
        .try_build()
        .expect("node app should build")
}

fn put_test_workload(app: &NodeApp, index: usize) {
    let mut desired = app.state_snapshot().state.desired;
    desired.put_artifact(
        ArtifactRecord::builder(ArtifactId::new(format!("artifact.usage.{index}"))).build(),
    );
    desired.put_workload(
        WorkloadRecord::builder(
            WorkloadId::new(format!("workload.usage.{index}")),
            RuntimeType::new("graph.exec.v1"),
            ArtifactId::new(format!("artifact.usage.{index}")),
        )
        .desired_state(DesiredState::Stopped)
        .assigned_to(NodeId::new("node-resource-usage"))
        .build(),
    );
    app.replace_desired(desired);
}

#[test]
fn observability_reports_state_history_and_queue_usage() {
    let state_dir = temp_state_dir("resource-usage");
    let app = resource_usage_app(state_dir);

    for index in 0..6 {
        put_test_workload(&app, index);
    }
    app.persist_state().expect("state should persist");
    app.unix_control_handler()
        .handle_control(ControlEnvelope {
            source: LocalAddress::new("orionctl.usage"),
            destination: LocalAddress::new("orion"),
            message: ControlMessage::ClientHello(ClientHello {
                client_name: "orionctl.usage".into(),
                role: ClientRole::ControlPlane,
            }),
        })
        .expect("local client hello should be served");

    let usage = app.observability_snapshot().resource_usage;

    assert_eq!(usage.state.desired.artifacts, 6);
    assert_eq!(usage.state.desired.workloads, 6);
    assert!(usage.state.desired.total() >= 12);
    assert!(usage.state.persisted_snapshot_bytes.unwrap_or(0) > 0);
    assert!(usage.state.persisted_mutation_history_bytes.is_some());

    let history = &usage.mutation_history;
    assert!(history.batches >= 1 && history.batches <= 4);
    assert_eq!(history.max_batches, 4);
    assert!(history.mutations >= history.batches);
    assert_eq!(
        history.max_bytes,
        app.config.runtime_tuning.max_mutation_history_bytes as u64
    );
    let encoded_bytes = history
        .encoded_bytes
        .expect("history size should be measured");
    assert!(encoded_bytes > 0 && encoded_bytes <= history.max_bytes);

    assert_eq!(usage.local_streams.registered_clients, 1);
    assert_eq!(usage.local_streams.attached_streams, 0);
    assert_eq!(usage.local_streams.send_queue_capacity, 16);
    assert_eq!(usage.local_streams.client_event_queue_limit, 8);
    assert_eq!(usage.local_streams.dropped_client_events_total, 0);
    assert_eq!(usage.registries.local_clients, 1);
    assert_eq!(usage.registries.peers, 0);
    assert!(usage.registries.communication_endpoint_limit > 0);
    assert!(usage.registries.recent_event_limit > 0);

    let queue = |name: &str| {
        usage
            .worker_queues
            .iter()
            .find(|queue| queue.name == name)
            .unwrap_or_else(|| panic!("{name} worker queue should be reported"))
    };
    assert_eq!(
        queue("persistence").capacity,
        app.config.runtime_tuning.persistence_worker_queue_capacity as u64
    );
    assert!(queue("persistence").depth <= queue("persistence").capacity);
    assert_eq!(queue("audit_log").dropped_total, Some(0));
    assert_eq!(
        queue("auth_state").capacity,
        app.config.runtime_tuning.auth_state_worker_queue_capacity as u64
    );

    if cfg!(target_os = "linux") {
        assert!(usage.process.vm_rss_bytes.unwrap_or(0) > 0);
        assert!(usage.process.vm_hwm_bytes >= usage.process.vm_rss_bytes);
        assert!(usage.process.threads.unwrap_or(0) >= 1);
    }
}

#[test]
fn mutation_history_size_is_recomputed_only_after_history_changes() {
    let state_dir = temp_state_dir("resource-usage-cache");
    let app = resource_usage_app(state_dir);
    put_test_workload(&app, 0);

    let first = app
        .observability_snapshot()
        .resource_usage
        .mutation_history
        .encoded_bytes
        .expect("history size should be measured");
    let repeated = app
        .observability_snapshot()
        .resource_usage
        .mutation_history
        .encoded_bytes;
    assert_eq!(repeated, Some(first));

    put_test_workload(&app, 1);
    let grown = app
        .observability_snapshot()
        .resource_usage
        .mutation_history
        .encoded_bytes
        .expect("history size should be measured");
    assert!(grown > first, "history grew from {first} to {grown}");
}

#[test]
fn observability_json_without_resource_usage_still_decodes() {
    let app = NodeApp::builder()
        .config(test_node_config(
            "node-resource-usage-json",
            "node-usage-json",
        ))
        .try_build()
        .expect("node app should build");
    let mut value =
        serde_json::to_value(app.observability_snapshot()).expect("snapshot should serialize");
    value
        .as_object_mut()
        .expect("snapshot should be a JSON object")
        .remove("resource_usage");

    let decoded: orion::control_plane::NodeObservabilitySnapshot =
        serde_json::from_value(value).expect("older snapshot JSON should decode");
    assert_eq!(decoded.resource_usage, Default::default());
    assert!(decoded.resource_usage.worker_queues.is_empty());
}
