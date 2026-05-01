use super::{
    AuditLogOverloadPolicy, NodeRuntimeTuning, parse_config_value, runtime_tuning_doc_defaults,
};
use std::sync::{Mutex, OnceLock};

fn env_lock() -> &'static Mutex<()> {
    static ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    ENV_LOCK.get_or_init(|| Mutex::new(()))
}

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
fn runtime_tuning_parses_transport_payload_limit_from_env() {
    let _guard = env_lock().lock().expect("env lock should not be poisoned");
    let prior = std::env::var_os("ORION_NODE_TRANSPORT_MAX_PAYLOAD_BYTES");

    unsafe { std::env::set_var("ORION_NODE_TRANSPORT_MAX_PAYLOAD_BYTES", "4096") };

    let tuning = NodeRuntimeTuning::try_from_env().expect("runtime tuning should parse");

    match prior {
        Some(value) => unsafe {
            std::env::set_var("ORION_NODE_TRANSPORT_MAX_PAYLOAD_BYTES", value)
        },
        None => unsafe { std::env::remove_var("ORION_NODE_TRANSPORT_MAX_PAYLOAD_BYTES") },
    }

    assert_eq!(tuning.transport_max_payload_bytes, 4096);
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

#[test]
fn node_config_try_from_env_rejects_invalid_ipc_stream_heartbeat_interval() {
    let _guard = env_lock().lock().expect("env lock should not be poisoned");
    let prior = std::env::var_os("ORION_NODE_IPC_STREAM_HEARTBEAT_INTERVAL_MS");

    unsafe { std::env::set_var("ORION_NODE_IPC_STREAM_HEARTBEAT_INTERVAL_MS", "invalid") };

    let err = crate::NodeConfig::try_from_env()
        .expect_err("invalid heartbeat interval should fail typed config loading");

    match prior {
        Some(value) => unsafe {
            std::env::set_var("ORION_NODE_IPC_STREAM_HEARTBEAT_INTERVAL_MS", value)
        },
        None => unsafe { std::env::remove_var("ORION_NODE_IPC_STREAM_HEARTBEAT_INTERVAL_MS") },
    }

    assert!(
        matches!(err, crate::NodeError::Config(message) if message.contains("ORION_NODE_IPC_STREAM_HEARTBEAT_INTERVAL_MS"))
    );
}

#[test]
fn node_config_try_from_env_rejects_empty_node_id() {
    let _guard = env_lock().lock().expect("env lock should not be poisoned");
    let prior = std::env::var_os("ORION_NODE_ID");

    unsafe { std::env::set_var("ORION_NODE_ID", "   ") };

    let err = crate::NodeConfig::try_from_env()
        .expect_err("empty node id should fail typed config loading");

    match prior {
        Some(value) => unsafe { std::env::set_var("ORION_NODE_ID", value) },
        None => unsafe { std::env::remove_var("ORION_NODE_ID") },
    }

    assert!(matches!(err, crate::NodeError::Config(message) if message.contains("ORION_NODE_ID")));
}

#[test]
fn node_config_try_from_env_rejects_empty_peer_node_id() {
    let _guard = env_lock().lock().expect("env lock should not be poisoned");
    let prior = std::env::var_os("ORION_NODE_PEERS");

    unsafe { std::env::set_var("ORION_NODE_PEERS", " =http://127.0.0.1:9101") };

    let err = crate::NodeConfig::try_from_env()
        .expect_err("empty peer node id should fail typed config loading");

    match prior {
        Some(value) => unsafe { std::env::set_var("ORION_NODE_PEERS", value) },
        None => unsafe { std::env::remove_var("ORION_NODE_PEERS") },
    }

    assert!(
        matches!(err, crate::NodeError::Config(message) if message.contains("ORION_NODE_PEERS"))
    );
}

#[test]
fn node_process_config_try_from_env_rejects_invalid_shutdown_after_init() {
    let _guard = env_lock().lock().expect("env lock should not be poisoned");
    let prior = std::env::var_os("ORION_NODE_SHUTDOWN_AFTER_INIT_MS");

    unsafe { std::env::set_var("ORION_NODE_SHUTDOWN_AFTER_INIT_MS", "invalid") };

    let err = crate::NodeProcessConfig::try_from_env()
        .expect_err("invalid shutdown delay should fail typed process config loading");

    match prior {
        Some(value) => unsafe { std::env::set_var("ORION_NODE_SHUTDOWN_AFTER_INIT_MS", value) },
        None => unsafe { std::env::remove_var("ORION_NODE_SHUTDOWN_AFTER_INIT_MS") },
    }

    assert!(
        matches!(err, crate::NodeError::Config(message) if message.contains("ORION_NODE_SHUTDOWN_AFTER_INIT_MS"))
    );
}
