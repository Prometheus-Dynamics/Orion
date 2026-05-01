use orion_control_plane::{
    CommunicationEndpointScope, CommunicationEndpointSnapshot, CommunicationFailureCountSnapshot,
    CommunicationFailureKind, CommunicationMetricsSnapshot, CommunicationRecentMetricsSnapshot,
    CommunicationStageMetricsSnapshot, CommunicationTransportKind, LatencyMetricsSnapshot,
    MetricsExportConfig, render_communication_metrics, render_communication_metrics_with_config,
};
use orion_core::NodeId;
use std::collections::BTreeMap;
use std::sync::{Mutex, OnceLock};

fn env_lock() -> &'static Mutex<()> {
    static ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    ENV_LOCK.get_or_init(|| Mutex::new(()))
}

#[test]
fn communication_metrics_render_prometheus_histogram_and_escape_labels() {
    let endpoint = CommunicationEndpointSnapshot {
        id: "ipc/local-unary/client\"a".to_owned(),
        transport: CommunicationTransportKind::Ipc,
        scope: CommunicationEndpointScope::LocalUnary,
        local: None,
        remote: None,
        labels: BTreeMap::from([
            ("client_name".to_owned(), "client\\a\nnext".to_owned()),
            ("unpromoted".to_owned(), "ignored".to_owned()),
        ]),
        connected: false,
        queued: Some(4),
        metrics: communication_metrics_with_latency(16, Some(5)),
    };

    let rendered = render_communication_metrics(&NodeId::new("node-a"), &[endpoint]);

    assert!(rendered.contains(
        "orion_communication_messages_sent_total{node_id=\"node-a\",id=\"ipc/local-unary/client\\\"a\",transport=\"ipc\",scope=\"local_unary\",client_name=\"client\\\\a\\nnext\"} 2"
    ));
    assert!(rendered.contains(
        "orion_communication_latency_ms_bucket{node_id=\"node-a\",id=\"ipc/local-unary/client\\\"a\",transport=\"ipc\",scope=\"local_unary\",client_name=\"client\\\\a\\nnext\",le=\"+Inf\"} 3"
    ));
    assert!(rendered.contains(
        "orion_communication_failures_by_kind_total{node_id=\"node-a\",id=\"ipc/local-unary/client\\\"a\",transport=\"ipc\",scope=\"local_unary\",client_name=\"client\\\\a\\nnext\",kind=\"timeout\"} 1"
    ));
    assert!(rendered.contains(
        "orion_communication_recent_avg_latency_ms{node_id=\"node-a\",id=\"ipc/local-unary/client\\\"a\",transport=\"ipc\",scope=\"local_unary\",client_name=\"client\\\\a\\nnext\"} 5"
    ));
    assert!(!rendered.contains("unpromoted"));
}

#[test]
fn communication_metrics_match_golden_fixture() {
    let endpoint = CommunicationEndpointSnapshot {
        id: "ipc/local-unary/client-a".to_owned(),
        transport: CommunicationTransportKind::Ipc,
        scope: CommunicationEndpointScope::LocalUnary,
        local: None,
        remote: None,
        labels: BTreeMap::from([("client_name".to_owned(), "client-a".to_owned())]),
        connected: true,
        queued: Some(2),
        metrics: communication_metrics_with_latency(507, Some(169)),
    };

    assert_eq!(
        render_communication_metrics(&NodeId::new("node-a"), &[endpoint]),
        include_str!("fixtures/communication_metrics.prom")
    );
}

#[test]
fn communication_metrics_bound_export_cardinality_and_label_length() {
    let long_id = "x".repeat(256);
    let endpoints = (0..520)
        .map(|index| CommunicationEndpointSnapshot {
            id: if index == 0 {
                long_id.clone()
            } else {
                format!("endpoint-{index}")
            },
            transport: CommunicationTransportKind::Ipc,
            scope: CommunicationEndpointScope::LocalUnary,
            local: None,
            remote: None,
            labels: BTreeMap::new(),
            connected: true,
            queued: None,
            metrics: empty_communication_metrics(),
        })
        .collect::<Vec<_>>();

    let rendered = render_communication_metrics_with_config(
        &NodeId::new("node-a"),
        &endpoints,
        &MetricsExportConfig {
            max_communication_endpoints: 10,
            max_label_value_len: 48,
        },
    );

    assert!(
        rendered.contains("orion_communication_export_dropped_endpoints{node_id=\"node-a\"} 510")
    );
    assert!(!rendered.contains(&long_id));
    assert!(!rendered.contains("endpoint-10"));
}

#[test]
fn metrics_export_config_rejects_invalid_env_values() {
    let _guard = env_lock().lock().expect("env lock should not be poisoned");
    let prior = std::env::var_os("ORION_METRICS_MAX_COMMUNICATION_ENDPOINTS");

    unsafe { std::env::set_var("ORION_METRICS_MAX_COMMUNICATION_ENDPOINTS", "many") };

    let err = MetricsExportConfig::try_from_env()
        .expect_err("invalid metrics endpoint limit should fail");

    match prior {
        Some(value) => unsafe {
            std::env::set_var("ORION_METRICS_MAX_COMMUNICATION_ENDPOINTS", value)
        },
        None => unsafe { std::env::remove_var("ORION_METRICS_MAX_COMMUNICATION_ENDPOINTS") },
    }

    assert!(err.contains("ORION_METRICS_MAX_COMMUNICATION_ENDPOINTS"));
}

fn communication_metrics_with_latency(
    total_duration_ms: u64,
    recent_avg_latency_ms: Option<u64>,
) -> CommunicationMetricsSnapshot {
    CommunicationMetricsSnapshot {
        messages_sent_total: 2,
        messages_received_total: 3,
        bytes_sent_total: 20,
        bytes_received_total: 30,
        estimated_wire_bytes_sent_total: 84,
        estimated_wire_bytes_received_total: 94,
        failures_total: 1,
        failures_by_kind: vec![CommunicationFailureCountSnapshot {
            kind: CommunicationFailureKind::Timeout,
            count: 1,
        }],
        reconnects_total: 0,
        last_success_at_ms: Some(7),
        last_failure_at_ms: Some(8),
        last_error: Some("failed".to_owned()),
        latency: LatencyMetricsSnapshot {
            samples_total: 3,
            total_duration_ms,
            last_duration_ms: Some(501),
            max_duration_ms: 501,
            bucket_le_1_ms: 1,
            bucket_le_5_ms: 1,
            bucket_le_10_ms: u64::from(total_duration_ms < 500),
            bucket_le_50_ms: 0,
            bucket_le_100_ms: 0,
            bucket_le_500_ms: 0,
            bucket_gt_500_ms: u64::from(total_duration_ms >= 500),
        },
        stages: CommunicationStageMetricsSnapshot::default(),
        recent: CommunicationRecentMetricsSnapshot {
            window_ms: 300_000,
            successes: 2,
            failures: 1,
            avg_latency_ms: recent_avg_latency_ms,
            max_latency_ms: 501,
            bytes_sent: 20,
            bytes_received: 30,
        },
    }
}

fn empty_communication_metrics() -> CommunicationMetricsSnapshot {
    CommunicationMetricsSnapshot {
        messages_sent_total: 0,
        messages_received_total: 0,
        bytes_sent_total: 0,
        bytes_received_total: 0,
        estimated_wire_bytes_sent_total: 0,
        estimated_wire_bytes_received_total: 0,
        failures_total: 0,
        failures_by_kind: Vec::new(),
        reconnects_total: 0,
        last_success_at_ms: None,
        last_failure_at_ms: None,
        last_error: None,
        latency: LatencyMetricsSnapshot::default(),
        stages: CommunicationStageMetricsSnapshot::default(),
        recent: CommunicationRecentMetricsSnapshot::default(),
    }
}
