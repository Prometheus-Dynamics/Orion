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

fn with_env_vars<T>(vars: &[(&str, Option<&str>)], run: impl FnOnce() -> T) -> T {
    let _guard = env_lock().lock().expect("env lock should not be poisoned");
    let prior: Vec<_> = vars
        .iter()
        .map(|(key, _)| (*key, std::env::var_os(key)))
        .collect();
    for (key, value) in vars {
        match value {
            Some(value) => unsafe { std::env::set_var(key, value) },
            None => unsafe { std::env::remove_var(key) },
        }
    }
    let result = run();
    for (key, value) in prior {
        match value {
            Some(value) => unsafe { std::env::set_var(key, value) },
            None => unsafe { std::env::remove_var(key) },
        }
    }
    result
}

#[test]
fn node_process_config_disables_http_listener_with_off() {
    let process = with_env_vars(
        &[
            ("ORION_NODE_HTTP_ADDR", Some("off")),
            ("ORION_NODE_PEERS", None),
            ("ORION_NODE_HTTP_TLS_AUTO", None),
        ],
        crate::NodeProcessConfig::try_from_env,
    )
    .expect("http off should load");

    assert!(!process.http_enabled);
    assert_eq!(
        process.node.http_bind_addr,
        crate::NodeConfig::default_http_bind_addr()
    );
}

#[cfg(feature = "transport-http")]
#[test]
fn node_process_config_keeps_http_listener_enabled_by_default() {
    let process = with_env_vars(
        &[("ORION_NODE_HTTP_ADDR", None), ("ORION_NODE_PEERS", None)],
        crate::NodeProcessConfig::try_from_env,
    )
    .expect("default config should load");

    assert!(process.http_enabled);
    assert_eq!(
        process.runtime_threads,
        crate::NodeRuntimeThreads::default()
    );
}

#[test]
fn node_process_config_rejects_http_off_with_peers() {
    let err = with_env_vars(
        &[
            ("ORION_NODE_HTTP_ADDR", Some("disabled")),
            ("ORION_NODE_PEERS", Some("node-b=http://127.0.0.1:9101")),
        ],
        crate::NodeProcessConfig::try_from_env,
    )
    .expect_err("http off with peers should fail");

    assert!(
        matches!(err, crate::NodeError::Config(message) if message.contains("ORION_NODE_PEERS"))
    );
}

#[test]
fn node_process_config_parses_runtime_threads() {
    let process = with_env_vars(
        &[
            ("ORION_NODE_RUNTIME_WORKER_THREADS", Some("2")),
            ("ORION_NODE_RUNTIME_MAX_BLOCKING_THREADS", Some("4")),
        ],
        crate::NodeProcessConfig::try_from_env,
    )
    .expect("runtime threads should load");

    assert_eq!(process.runtime_threads.worker_threads, Some(2));
    assert_eq!(process.runtime_threads.max_blocking_threads, Some(4));
    let runtime = process
        .runtime_threads
        .build_runtime()
        .expect("runtime should build");
    assert_eq!(runtime.metrics().num_workers(), 2);
}

#[test]
fn node_process_config_rejects_zero_runtime_worker_threads() {
    let err = with_env_vars(
        &[("ORION_NODE_RUNTIME_WORKER_THREADS", Some("0"))],
        crate::NodeProcessConfig::try_from_env,
    )
    .expect_err("zero workers should fail");

    assert!(
        matches!(err, crate::NodeError::Config(message) if message.contains("ORION_NODE_RUNTIME_WORKER_THREADS"))
    );
}

#[cfg(not(feature = "transport-http"))]
mod without_transport_http {
    use super::*;
    use crate::{NodeConfig, NodeProcessConfig};

    fn config_error(vars: &[(&str, Option<&str>)]) -> String {
        match with_env_vars(vars, NodeProcessConfig::try_from_env) {
            Err(crate::NodeError::Config(message)) => message,
            other => panic!("expected config error, got {other:?}"),
        }
    }

    #[test]
    fn http_listener_is_off_by_default() {
        let process = with_env_vars(
            &[("ORION_NODE_HTTP_ADDR", None), ("ORION_NODE_PEERS", None)],
            NodeProcessConfig::try_from_env,
        )
        .expect("IPC-only process config should parse");
        assert!(!process.http_enabled);
        assert!(process.http_probe_addr.is_none());
    }

    #[test]
    fn explicit_off_is_accepted() {
        let process = with_env_vars(
            &[
                ("ORION_NODE_HTTP_ADDR", Some("off")),
                ("ORION_NODE_PEERS", None),
            ],
            NodeProcessConfig::try_from_env,
        )
        .expect("ORION_NODE_HTTP_ADDR=off should parse");
        assert!(!process.http_enabled);
    }

    #[test]
    fn http_address_is_rejected() {
        let message = config_error(&[
            ("ORION_NODE_HTTP_ADDR", Some("127.0.0.1:9100")),
            ("ORION_NODE_PEERS", None),
        ]);
        assert!(message.contains("ORION_NODE_HTTP_ADDR"), "{message}");
        assert!(message.contains("transport-http"), "{message}");
    }

    #[test]
    fn peers_are_rejected() {
        let message = config_error(&[
            ("ORION_NODE_HTTP_ADDR", None),
            ("ORION_NODE_PEERS", Some("node-b=http://127.0.0.1:9101")),
        ]);
        assert!(message.contains("ORION_NODE_PEERS"), "{message}");
        assert!(message.contains("transport-http"), "{message}");
    }

    #[test]
    fn http_tls_settings_are_rejected() {
        for var in [
            ("ORION_NODE_HTTP_TLS_AUTO", Some("true")),
            ("ORION_NODE_HTTP_TLS_CERT", Some("/tmp/cert.pem")),
        ] {
            let message = config_error(&[
                ("ORION_NODE_HTTP_ADDR", None),
                ("ORION_NODE_PEERS", None),
                var,
            ]);
            assert!(message.contains("ORION_NODE_HTTP_TLS"), "{message}");
            assert!(message.contains("transport-http"), "{message}");
        }
    }

    #[test]
    fn http_probe_address_is_rejected() {
        let message = config_error(&[
            ("ORION_NODE_HTTP_ADDR", None),
            ("ORION_NODE_PEERS", None),
            ("ORION_NODE_HTTP_PROBE_ADDR", Some("127.0.0.1:9180")),
        ]);
        assert!(message.contains("ORION_NODE_HTTP_PROBE_ADDR"), "{message}");
        assert!(message.contains("transport-http"), "{message}");
    }

    #[test]
    fn builder_rejects_peers_and_http_tls() {
        let peers = NodeConfig::for_local_node("node-a").with_peers(vec![crate::PeerConfig::new(
            "node-b",
            "http://127.0.0.1:9101",
        )]);
        let err = crate::NodeApp::builder()
            .config(peers)
            .try_build()
            .err()
            .expect("peers should be rejected");
        assert!(err.to_string().contains("transport-http"), "{err}");

        let tls = crate::NodeApp::builder()
            .config(NodeConfig::for_local_node("node-a"))
            .with_auto_http_tls(true)
            .try_build()
            .err()
            .expect("HTTP TLS should be rejected");
        assert!(tls.to_string().contains("transport-http"), "{tls}");
    }
}
