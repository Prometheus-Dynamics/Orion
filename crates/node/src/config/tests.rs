use super::{
    AuditLogOverloadPolicy, NodeRuntimeTuning, parse_config_value, runtime_tuning_doc_defaults,
};

#[test]
fn runtime_tuning_normalizes_worker_queue_capacities() {
    let mut tuning = NodeRuntimeTuning {
        persistence_worker_queue_capacity: 0,
        auth_state_worker_queue_capacity: 0,
        audit_log_queue_capacity: 0,
        audit_log_overload_policy: AuditLogOverloadPolicy::DropNewest,
        ..NodeRuntimeTuning::default()
    };

    tuning.normalize();

    assert_eq!(tuning.persistence_worker_queue_capacity, 1);
    assert_eq!(tuning.auth_state_worker_queue_capacity, 1);
    assert_eq!(tuning.audit_log_queue_capacity, 1);
    assert_eq!(
        tuning.audit_log_overload_policy,
        AuditLogOverloadPolicy::DropNewest
    );
}

#[test]
fn runtime_tuning_fluent_setters_normalize_values() {
    let tuning = NodeRuntimeTuning::default()
        .with_max_mutation_history_batches(0)
        .with_snapshot_rewrite_cadence(0)
        .with_peer_sync_parallel_small_cluster_cap(0)
        .with_transport_max_payload_bytes(0)
        .with_persistence_worker_queue_capacity(0);

    assert_eq!(tuning.max_mutation_history_batches, 1);
    assert_eq!(tuning.snapshot_rewrite_cadence, 1);
    assert_eq!(tuning.peer_sync_parallel_small_cluster_cap, 1);
    assert_eq!(tuning.transport_max_payload_bytes, 1);
    assert_eq!(tuning.persistence_worker_queue_capacity, 1);
}

#[test]
fn parse_config_value_rejects_invalid_values() {
    let err = parse_config_value::<usize>("ORION_NODE_MAX_MUTATION_HISTORY", "not-a-number")
        .expect_err("invalid value should fail");

    assert!(
        matches!(err, crate::NodeError::Config(message) if message.contains("ORION_NODE_MAX_MUTATION_HISTORY"))
    );
}

#[test]
fn parse_config_value_rejects_invalid_boolean_values() {
    let err = parse_config_value::<bool>("ORION_NODE_HTTP_TLS_AUTO", "not-bool")
        .expect_err("invalid bool should fail");

    assert!(
        matches!(err, crate::NodeError::Config(message) if message.contains("ORION_NODE_HTTP_TLS_AUTO"))
    );
}

#[test]
fn node_env_docs_include_runtime_tuning_defaults() {
    let docs_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../docs/node-env.md");
    let Ok(docs) = std::fs::read_to_string(docs_path) else {
        return;
    };

    for (key, default) in runtime_tuning_doc_defaults() {
        let expected = format!("| `{key}` | `{default}` |");
        assert!(
            docs.contains(&expected),
            "node-env.md is missing runtime tuning default `{expected}`"
        );
    }
}
