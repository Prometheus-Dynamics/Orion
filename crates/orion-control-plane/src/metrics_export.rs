use crate::{
    CommunicationEndpointSnapshot, HostMetricsSnapshot, LatencyMetricsSnapshot,
    NodeObservabilitySnapshot, OperationMetricsSnapshot,
};
use orion_core::NodeId;

const PROMOTED_ENDPOINT_LABELS: &[&str] = &[
    "peer_node_id",
    "resource_id",
    "client_name",
    "role",
    "binding",
    "sync_status",
];
const DEFAULT_MAX_EXPORTED_COMMUNICATION_ENDPOINTS: usize = 512;
const DEFAULT_MAX_LABEL_VALUE_LEN: usize = 120;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MetricsExportConfig {
    pub max_communication_endpoints: usize,
    pub max_label_value_len: usize,
}

impl Default for MetricsExportConfig {
    fn default() -> Self {
        Self {
            max_communication_endpoints: DEFAULT_MAX_EXPORTED_COMMUNICATION_ENDPOINTS,
            max_label_value_len: DEFAULT_MAX_LABEL_VALUE_LEN,
        }
    }
}

impl MetricsExportConfig {
    pub fn try_from_env() -> Result<Self, String> {
        let mut config = Self::default();
        if let Some(value) = env_usize("ORION_METRICS_MAX_COMMUNICATION_ENDPOINTS")? {
            config.max_communication_endpoints = value;
        }
        if let Some(value) = env_usize("ORION_METRICS_MAX_LABEL_VALUE_LEN")? {
            config.max_label_value_len = value.max(24);
        }
        Ok(config)
    }

    pub fn from_env() -> Self {
        Self::try_from_env().unwrap_or_default()
    }
}

pub fn render_observability_metrics(snapshot: &NodeObservabilitySnapshot) -> String {
    render_observability_metrics_with_config(snapshot, &MetricsExportConfig::from_env())
}

pub fn render_observability_metrics_with_config(
    snapshot: &NodeObservabilitySnapshot,
    config: &MetricsExportConfig,
) -> String {
    let mut out = String::new();
    append_host_metrics(&mut out, &snapshot.node_id, &snapshot.host);
    append_cluster_gauges(&mut out, snapshot);
    append_operation_metric_headers(&mut out);
    append_operation_metrics(&mut out, &snapshot.node_id, "replay", &snapshot.replay);
    append_operation_metrics(
        &mut out,
        &snapshot.node_id,
        "peer_sync",
        &snapshot.peer_sync,
    );
    append_operation_metrics(
        &mut out,
        &snapshot.node_id,
        "reconcile",
        &snapshot.reconcile,
    );
    append_operation_metrics(
        &mut out,
        &snapshot.node_id,
        "mutation_apply",
        &snapshot.mutation_apply,
    );
    append_communication_metrics(&mut out, &snapshot.node_id, &snapshot.communication, config);
    out
}

fn append_operation_metric_headers(out: &mut String) {
    metric_help(
        out,
        "orion_operation_success_total",
        "Successful node operations by operation type.",
    );
    metric_type(out, "orion_operation_success_total", "counter");
    metric_help(
        out,
        "orion_operation_failure_total",
        "Failed node operations by operation type.",
    );
    metric_type(out, "orion_operation_failure_total", "counter");
    metric_help(
        out,
        "orion_operation_duration_ms_total",
        "Total operation duration in milliseconds by operation type.",
    );
    metric_type(out, "orion_operation_duration_ms_total", "counter");
    metric_help(
        out,
        "orion_operation_duration_ms_max",
        "Maximum observed operation duration in milliseconds by operation type.",
    );
    metric_type(out, "orion_operation_duration_ms_max", "gauge");
    metric_help(
        out,
        "orion_operation_duration_ms",
        "Operation duration in milliseconds by operation type.",
    );
    metric_type(out, "orion_operation_duration_ms", "histogram");
}

pub fn render_host_metrics(node_id: &NodeId, host: &HostMetricsSnapshot) -> String {
    let mut out = String::new();
    append_host_metrics(&mut out, node_id, host);
    out
}

pub fn render_communication_metrics(
    node_id: &NodeId,
    endpoints: &[CommunicationEndpointSnapshot],
) -> String {
    render_communication_metrics_with_config(node_id, endpoints, &MetricsExportConfig::from_env())
}

pub fn render_communication_metrics_with_config(
    node_id: &NodeId,
    endpoints: &[CommunicationEndpointSnapshot],
    config: &MetricsExportConfig,
) -> String {
    let mut out = String::new();
    append_communication_metrics(&mut out, node_id, endpoints, config);
    out
}

fn append_cluster_gauges(out: &mut String, snapshot: &NodeObservabilitySnapshot) {
    metric_help(
        out,
        "orion_peer_count",
        "Number of peers by runtime peer status.",
    );
    metric_type(out, "orion_peer_count", "gauge");
    sample(
        out,
        "orion_peer_count",
        &[
            ("node_id", snapshot.node_id.as_str()),
            ("status", "configured"),
        ],
        snapshot.configured_peer_count,
    );
    sample(
        out,
        "orion_peer_count",
        &[("node_id", snapshot.node_id.as_str()), ("status", "ready")],
        snapshot.ready_peer_count,
    );
    sample(
        out,
        "orion_peer_count",
        &[
            ("node_id", snapshot.node_id.as_str()),
            ("status", "pending"),
        ],
        snapshot.pending_peer_count,
    );
    sample(
        out,
        "orion_peer_count",
        &[
            ("node_id", snapshot.node_id.as_str()),
            ("status", "degraded"),
        ],
        snapshot.degraded_peer_count,
    );
    gauge(
        out,
        "orion_peer_sync_parallel_in_flight_cap",
        "Effective peer-sync parallel in-flight cap.",
        &[("node_id", snapshot.node_id.as_str())],
        snapshot.peer_sync_parallel_in_flight_cap,
    );
    gauge(
        out,
        "orion_audit_log_dropped_records_total",
        "Audit-log records dropped by overload handling.",
        &[("node_id", snapshot.node_id.as_str())],
        snapshot.audit_log_dropped_records,
    );
    gauge(
        out,
        "orion_client_sessions",
        "Client session counts by state.",
        &[
            ("node_id", snapshot.node_id.as_str()),
            ("state", "registered"),
        ],
        snapshot.client_sessions.registered_clients,
    );
    sample(
        out,
        "orion_client_sessions",
        &[
            ("node_id", snapshot.node_id.as_str()),
            ("state", "live_stream"),
        ],
        snapshot.client_sessions.live_stream_clients,
    );
}

fn append_host_metrics(out: &mut String, node_id: &NodeId, host: &HostMetricsSnapshot) {
    let node = node_id.as_str();
    optional_gauge(
        out,
        "orion_host_uptime_seconds",
        "Host uptime in seconds.",
        &[("node_id", node)],
        host.uptime_seconds,
    );
    optional_gauge(
        out,
        "orion_host_load1",
        "Host one-minute load average.",
        &[("node_id", node)],
        host.load_1_milli.map(milli_to_f64),
    );
    optional_gauge(
        out,
        "orion_host_load5",
        "Host five-minute load average.",
        &[("node_id", node)],
        host.load_5_milli.map(milli_to_f64),
    );
    optional_gauge(
        out,
        "orion_host_load15",
        "Host fifteen-minute load average.",
        &[("node_id", node)],
        host.load_15_milli.map(milli_to_f64),
    );
    optional_gauge(
        out,
        "orion_host_memory_total_bytes",
        "Host total memory in bytes.",
        &[("node_id", node)],
        host.memory_total_bytes,
    );
    optional_gauge(
        out,
        "orion_host_memory_available_bytes",
        "Host available memory in bytes.",
        &[("node_id", node)],
        host.memory_available_bytes,
    );
    optional_gauge(
        out,
        "orion_host_swap_total_bytes",
        "Host total swap in bytes.",
        &[("node_id", node)],
        host.swap_total_bytes,
    );
    optional_gauge(
        out,
        "orion_host_swap_free_bytes",
        "Host free swap in bytes.",
        &[("node_id", node)],
        host.swap_free_bytes,
    );
    gauge(
        out,
        "orion_process_id",
        "Orion process id.",
        &[("node_id", node)],
        host.process_id,
    );
    optional_gauge(
        out,
        "orion_process_rss_bytes",
        "Orion process resident set size in bytes.",
        &[("node_id", node)],
        host.process_rss_bytes,
    );
}

fn append_operation_metrics(
    out: &mut String,
    node_id: &NodeId,
    operation: &str,
    metrics: &OperationMetricsSnapshot,
) {
    let labels = &[("node_id", node_id.as_str()), ("operation", operation)];
    sample(
        out,
        "orion_operation_success_total",
        labels,
        metrics.success_count,
    );
    sample(
        out,
        "orion_operation_failure_total",
        labels,
        metrics.failure_count,
    );
    sample(
        out,
        "orion_operation_duration_ms_total",
        labels,
        metrics.total_duration_ms,
    );
    sample(
        out,
        "orion_operation_duration_ms_max",
        labels,
        metrics.max_duration_ms,
    );
    append_histogram_samples(
        out,
        "orion_operation_duration_ms",
        &[
            ("node_id".to_owned(), node_id.to_string()),
            ("operation".to_owned(), operation.to_owned()),
        ],
        &metrics.latency,
    );
}

fn append_communication_metrics(
    out: &mut String,
    node_id: &NodeId,
    endpoints: &[CommunicationEndpointSnapshot],
    config: &MetricsExportConfig,
) {
    metric_help(
        out,
        "orion_communication_messages_sent_total",
        "Messages sent by communication endpoint.",
    );
    metric_type(out, "orion_communication_messages_sent_total", "counter");
    metric_help(
        out,
        "orion_communication_messages_received_total",
        "Messages received by communication endpoint.",
    );
    metric_type(
        out,
        "orion_communication_messages_received_total",
        "counter",
    );
    metric_help(
        out,
        "orion_communication_bytes_sent_total",
        "Serialized Orion payload bytes sent by communication endpoint.",
    );
    metric_type(out, "orion_communication_bytes_sent_total", "counter");
    metric_help(
        out,
        "orion_communication_bytes_received_total",
        "Serialized Orion payload bytes received by communication endpoint.",
    );
    metric_type(out, "orion_communication_bytes_received_total", "counter");
    metric_help(
        out,
        "orion_communication_estimated_wire_bytes_sent_total",
        "Estimated transport bytes sent by communication endpoint, including Orion payload plus local framing estimate.",
    );
    metric_type(
        out,
        "orion_communication_estimated_wire_bytes_sent_total",
        "counter",
    );
    metric_help(
        out,
        "orion_communication_estimated_wire_bytes_received_total",
        "Estimated transport bytes received by communication endpoint, including Orion payload plus local framing estimate.",
    );
    metric_type(
        out,
        "orion_communication_estimated_wire_bytes_received_total",
        "counter",
    );
    metric_help(
        out,
        "orion_communication_failures_total",
        "Failures observed by communication endpoint.",
    );
    metric_type(out, "orion_communication_failures_total", "counter");
    metric_help(
        out,
        "orion_communication_failures_by_kind_total",
        "Failures observed by communication endpoint and normalized failure kind.",
    );
    metric_type(out, "orion_communication_failures_by_kind_total", "counter");
    metric_help(
        out,
        "orion_communication_reconnects_total",
        "Reconnects observed by communication endpoint.",
    );
    metric_type(out, "orion_communication_reconnects_total", "counter");
    metric_help(
        out,
        "orion_communication_connected",
        "Communication endpoint connected state, where 1 means connected.",
    );
    metric_type(out, "orion_communication_connected", "gauge");
    metric_help(
        out,
        "orion_communication_queued",
        "Queued items reported by communication endpoint when available.",
    );
    metric_type(out, "orion_communication_queued", "gauge");
    metric_help(
        out,
        "orion_communication_latency_ms",
        "Communication endpoint latency in milliseconds.",
    );
    metric_type(out, "orion_communication_latency_ms", "histogram");
    metric_help(
        out,
        "orion_communication_stage_latency_ms",
        "Communication endpoint stage latency in milliseconds.",
    );
    metric_type(out, "orion_communication_stage_latency_ms", "histogram");
    metric_help(
        out,
        "orion_communication_recent_failures_total",
        "Failures observed by communication endpoint in the recent rolling window.",
    );
    metric_type(out, "orion_communication_recent_failures_total", "counter");
    metric_help(
        out,
        "orion_communication_recent_successes_total",
        "Successes observed by communication endpoint in the recent rolling window.",
    );
    metric_type(out, "orion_communication_recent_successes_total", "counter");
    metric_help(
        out,
        "orion_communication_recent_avg_latency_ms",
        "Average communication latency in the recent rolling window.",
    );
    metric_type(out, "orion_communication_recent_avg_latency_ms", "gauge");
    metric_help(
        out,
        "orion_communication_export_dropped_endpoints",
        "Communication endpoints omitted from metrics export because of exporter cardinality limits.",
    );
    metric_type(out, "orion_communication_export_dropped_endpoints", "gauge");

    let dropped = endpoints
        .len()
        .saturating_sub(config.max_communication_endpoints);
    sample(
        out,
        "orion_communication_export_dropped_endpoints",
        &[("node_id", node_id.as_str())],
        dropped,
    );

    for endpoint in endpoints.iter().take(config.max_communication_endpoints) {
        let labels = endpoint_labels(node_id, endpoint, config);
        sample_owned(
            out,
            "orion_communication_messages_sent_total",
            &labels,
            endpoint.metrics.messages_sent_total,
        );
        sample_owned(
            out,
            "orion_communication_messages_received_total",
            &labels,
            endpoint.metrics.messages_received_total,
        );
        sample_owned(
            out,
            "orion_communication_bytes_sent_total",
            &labels,
            endpoint.metrics.bytes_sent_total,
        );
        sample_owned(
            out,
            "orion_communication_bytes_received_total",
            &labels,
            endpoint.metrics.bytes_received_total,
        );
        sample_owned(
            out,
            "orion_communication_estimated_wire_bytes_sent_total",
            &labels,
            endpoint.metrics.estimated_wire_bytes_sent_total,
        );
        sample_owned(
            out,
            "orion_communication_estimated_wire_bytes_received_total",
            &labels,
            endpoint.metrics.estimated_wire_bytes_received_total,
        );
        sample_owned(
            out,
            "orion_communication_failures_total",
            &labels,
            endpoint.metrics.failures_total,
        );
        for failure in &endpoint.metrics.failures_by_kind {
            let mut failure_labels = labels.clone();
            failure_labels.push(("kind".to_owned(), failure.kind.to_string()));
            sample_owned(
                out,
                "orion_communication_failures_by_kind_total",
                &failure_labels,
                failure.count,
            );
        }
        sample_owned(
            out,
            "orion_communication_reconnects_total",
            &labels,
            endpoint.metrics.reconnects_total,
        );
        sample_owned(
            out,
            "orion_communication_connected",
            &labels,
            u64::from(endpoint.connected),
        );
        if let Some(queued) = endpoint.queued {
            sample_owned(out, "orion_communication_queued", &labels, queued);
        }
        append_histogram_samples(
            out,
            "orion_communication_latency_ms",
            &labels,
            &endpoint.metrics.latency,
        );
        for (stage, latency) in [
            ("queue_wait", &endpoint.metrics.stages.queue_wait),
            ("encode", &endpoint.metrics.stages.encode),
            ("decode", &endpoint.metrics.stages.decode),
            ("socket_read", &endpoint.metrics.stages.socket_read),
            ("socket_write", &endpoint.metrics.stages.socket_write),
            ("retry_delay", &endpoint.metrics.stages.retry_delay),
            ("backpressure", &endpoint.metrics.stages.backpressure),
        ] {
            if latency.samples_total == 0 {
                continue;
            }
            let mut stage_labels = labels.clone();
            stage_labels.push(("stage".to_owned(), stage.to_owned()));
            append_histogram_samples(
                out,
                "orion_communication_stage_latency_ms",
                &stage_labels,
                latency,
            );
        }
        sample_owned(
            out,
            "orion_communication_recent_successes_total",
            &labels,
            endpoint.metrics.recent.successes,
        );
        sample_owned(
            out,
            "orion_communication_recent_failures_total",
            &labels,
            endpoint.metrics.recent.failures,
        );
        if let Some(avg_latency) = endpoint.metrics.recent.avg_latency_ms {
            sample_owned(
                out,
                "orion_communication_recent_avg_latency_ms",
                &labels,
                avg_latency,
            );
        }
    }
}

fn append_histogram_samples(
    out: &mut String,
    name: &str,
    labels: &[(String, String)],
    latency: &LatencyMetricsSnapshot,
) {
    let mut cumulative = 0_u64;
    for (le, value) in [
        ("1", latency.bucket_le_1_ms),
        ("5", latency.bucket_le_5_ms),
        ("10", latency.bucket_le_10_ms),
        ("50", latency.bucket_le_50_ms),
        ("100", latency.bucket_le_100_ms),
        ("500", latency.bucket_le_500_ms),
        ("+Inf", latency.bucket_gt_500_ms),
    ] {
        cumulative = cumulative.saturating_add(value);
        let mut bucket_labels = labels.to_vec();
        bucket_labels.push(("le".to_owned(), le.to_owned()));
        sample_owned(out, &format!("{name}_bucket"), &bucket_labels, cumulative);
    }
    sample_owned(out, &format!("{name}_count"), labels, latency.samples_total);
    sample_owned(
        out,
        &format!("{name}_sum"),
        labels,
        latency.total_duration_ms,
    );
}

fn endpoint_labels(
    node_id: &NodeId,
    endpoint: &CommunicationEndpointSnapshot,
    config: &MetricsExportConfig,
) -> Vec<(String, String)> {
    let mut labels = vec![
        ("node_id".to_owned(), node_id.to_string()),
        (
            "id".to_owned(),
            bounded_label_value(&endpoint.id, config.max_label_value_len),
        ),
        ("transport".to_owned(), endpoint.transport.to_string()),
        ("scope".to_owned(), endpoint.scope.to_string()),
    ];
    for key in PROMOTED_ENDPOINT_LABELS {
        if let Some(value) = endpoint.labels.get(*key) {
            labels.push((
                (*key).to_owned(),
                bounded_label_value(value, config.max_label_value_len),
            ));
        }
    }
    labels
}

fn bounded_label_value(value: &str, max_len: usize) -> String {
    let max_len = max_len.max(24);
    if value.len() <= max_len {
        return value.to_owned();
    }
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in value.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    let mut end = max_len.saturating_sub(18);
    while !value.is_char_boundary(end) {
        end = end.saturating_sub(1);
    }
    format!("{}..{:016x}", &value[..end], hash)
}

fn env_usize(name: &str) -> Result<Option<usize>, String> {
    match std::env::var(name) {
        Ok(value) => value
            .parse()
            .map(Some)
            .map_err(|err| format!("{name} must be a non-negative integer: {value} ({err})")),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(std::env::VarError::NotUnicode(_)) => Err(format!("{name} must be valid unicode")),
    }
}

fn gauge<T>(out: &mut String, name: &str, help: &str, labels: &[(&str, &str)], value: T)
where
    T: std::fmt::Display,
{
    metric_help(out, name, help);
    metric_type(out, name, "gauge");
    sample(out, name, labels, value);
}

fn optional_gauge<T>(
    out: &mut String,
    name: &str,
    help: &str,
    labels: &[(&str, &str)],
    value: Option<T>,
) where
    T: std::fmt::Display,
{
    if let Some(value) = value {
        gauge(out, name, help, labels, value);
    }
}

fn metric_help(out: &mut String, name: &str, help: &str) {
    out.push_str("# HELP ");
    out.push_str(name);
    out.push(' ');
    out.push_str(help);
    out.push('\n');
}

fn metric_type(out: &mut String, name: &str, metric_type: &str) {
    out.push_str("# TYPE ");
    out.push_str(name);
    out.push(' ');
    out.push_str(metric_type);
    out.push('\n');
}

fn sample<T>(out: &mut String, name: &str, labels: &[(&str, &str)], value: T)
where
    T: std::fmt::Display,
{
    out.push_str(name);
    append_labels(out, labels.iter().copied());
    out.push(' ');
    out.push_str(&value.to_string());
    out.push('\n');
}

fn sample_owned<T>(out: &mut String, name: &str, labels: &[(String, String)], value: T)
where
    T: std::fmt::Display,
{
    out.push_str(name);
    append_labels(
        out,
        labels
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str())),
    );
    out.push(' ');
    out.push_str(&value.to_string());
    out.push('\n');
}

fn append_labels<'a>(out: &mut String, labels: impl Iterator<Item = (&'a str, &'a str)>) {
    let labels = labels.collect::<Vec<_>>();
    if labels.is_empty() {
        return;
    }
    out.push('{');
    for (index, (key, value)) in labels.iter().enumerate() {
        if index > 0 {
            out.push(',');
        }
        out.push_str(key);
        out.push_str("=\"");
        append_escaped_label_value(out, value);
        out.push('"');
    }
    out.push('}');
}

fn append_escaped_label_value(out: &mut String, value: &str) {
    for ch in value.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            _ => out.push(ch),
        }
    }
}

fn milli_to_f64(value: u64) -> f64 {
    value as f64 / 1000.0
}
