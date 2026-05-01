mod audit;
mod classification;
mod lifecycle;
mod metrics;

use orion::{
    NodeId,
    control_plane::{
        CommunicationEndpointScope, CommunicationEndpointSnapshot, CommunicationFailureKind,
        CommunicationTransportKind, HttpMutualTlsMode as PublicHttpMutualTlsMode,
        ObservabilityEvent, ObservabilityEventKind,
    },
};
use std::{
    collections::{BTreeMap, VecDeque},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

const COMMUNICATION_ENDPOINT_RUNTIME_LIMIT: usize = 512;

#[cfg(test)]
use audit::should_emit_audit_drop_warning;
pub(super) use audit::{AuditEventKind, AuditLogSink, write_audit_record};
#[cfg(test)]
pub(crate) use audit::{clear_test_audit_append_delay, set_test_audit_append_delay};
#[cfg(feature = "transport-quic")]
pub(crate) use classification::classify_quic_communication_failure;
#[cfg(feature = "transport-tcp")]
pub(crate) use classification::classify_tcp_communication_failure;
pub(crate) use classification::{
    classify_http_communication_failure, classify_node_error, classify_peer_sync_error,
    classify_peer_sync_error_kind, is_client_auth_tls_error, peer_sync_troubleshooting_hint,
};
pub(super) use lifecycle::{LifecycleSnapshot, LifecycleState};
use metrics::OperationMetrics;
pub(crate) use metrics::{CommunicationMetrics, CommunicationStageDurations};

#[derive(Clone, Debug, Default)]
pub(super) struct ObservabilityState {
    pub(super) replay: OperationMetrics,
    pub(super) peer_sync: OperationMetrics,
    pub(super) reconcile: OperationMetrics,
    pub(super) mutation_apply: OperationMetrics,
    pub(super) persistence: OperationMetrics,
    pub(super) artifact_write: OperationMetrics,
    pub(super) event_limit: usize,
    next_event_sequence: u64,
    pub(super) recent_events: VecDeque<ObservabilityEvent>,
    pub(super) client_registrations_total: u64,
    pub(super) client_stream_attaches_total: u64,
    pub(super) client_stream_detaches_total: u64,
    pub(super) stale_client_evictions_total: u64,
    pub(super) client_rate_limited_total: u64,
    pub(super) http_request_failures: u64,
    pub(super) http_malformed_input_count: u64,
    pub(super) http_tls_failures: u64,
    pub(super) http_tls_client_auth_failures: u64,
    pub(super) ipc_frame_failures: u64,
    pub(super) ipc_malformed_input_count: u64,
    pub(super) reconnect_count: u64,
    pub(super) peer_http_communication: BTreeMap<NodeId, CommunicationMetrics>,
    pub(super) communication_endpoints: BTreeMap<String, CommunicationEndpointRuntime>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CommunicationEndpointRuntime {
    pub(crate) id: String,
    pub(crate) transport: CommunicationTransportKind,
    pub(crate) scope: CommunicationEndpointScope,
    pub(crate) local: Option<String>,
    pub(crate) remote: Option<String>,
    pub(crate) labels: BTreeMap<String, String>,
    pub(crate) connected: bool,
    pub(crate) queued: Option<u64>,
    pub(crate) metrics: CommunicationMetrics,
}

impl CommunicationEndpointRuntime {
    pub(crate) fn new(
        id: impl Into<String>,
        transport: impl Into<String>,
        scope: impl Into<String>,
    ) -> Self {
        Self {
            id: id.into(),
            transport: CommunicationTransportKind::from_label(transport),
            scope: CommunicationEndpointScope::from_label(scope),
            local: None,
            remote: None,
            labels: BTreeMap::new(),
            connected: true,
            queued: None,
            metrics: CommunicationMetrics::default(),
        }
    }

    pub(crate) fn snapshot(&self) -> CommunicationEndpointSnapshot {
        CommunicationEndpointSnapshot {
            id: self.id.clone(),
            transport: self.transport.clone(),
            scope: self.scope.clone(),
            local: self.local.clone(),
            remote: self.remote.clone(),
            labels: self.labels.clone(),
            connected: self.connected,
            queued: self.queued,
            metrics: self.metrics.snapshot(),
        }
    }
}

pub(super) struct ObservabilityTxn<'a> {
    state: &'a mut ObservabilityState,
}

impl<'a> ObservabilityTxn<'a> {
    pub(super) fn new(state: &'a mut ObservabilityState) -> Self {
        Self { state }
    }

    pub(super) fn state_mut(&mut self) -> &mut ObservabilityState {
        self.state
    }

    pub(super) fn push_event(
        &mut self,
        kind: ObservabilityEventKind,
        success: bool,
        duration: Option<Duration>,
        detail: String,
    ) {
        push_observability_event(self.state, kind, success, duration, detail);
    }

    pub(super) fn push_event_with_context(
        &mut self,
        kind: ObservabilityEventKind,
        success: bool,
        duration: Option<Duration>,
        correlation_id: Option<String>,
        subject: Option<String>,
        detail: String,
    ) {
        push_observability_event_with_context(
            self.state,
            kind,
            success,
            duration,
            correlation_id,
            subject,
            detail,
        );
    }
}

impl ObservabilityState {
    pub(super) fn with_event_limit(event_limit: usize) -> Self {
        Self {
            event_limit: event_limit.max(1),
            ..Self::default()
        }
    }
}

impl crate::app::NodeApp {
    pub(crate) fn record_communication_endpoint_exchange_with_stages(
        &self,
        mut endpoint: CommunicationEndpointRuntime,
        bytes_received: u64,
        bytes_sent: u64,
        duration: Duration,
        stages: CommunicationStageDurations,
    ) {
        let now_ms = Self::current_time_ms();
        self.with_observability_txn(|txn| {
            prune_communication_endpoints_for_insert(txn.state_mut(), &endpoint.id);
            let stored = txn
                .state_mut()
                .communication_endpoints
                .entry(endpoint.id.clone())
                .or_insert_with(|| endpoint.clone());
            endpoint.metrics = stored.metrics.clone();
            *stored = endpoint;
            stored.metrics.record_received(bytes_received);
            stored.metrics.record_sent(bytes_sent);
            stored.metrics.record_success_exchange_with_stages(
                now_ms,
                duration,
                bytes_sent,
                bytes_received,
                &stages,
            );
        });
    }

    pub(crate) fn record_communication_endpoint_failure_kind(
        &self,
        mut endpoint: CommunicationEndpointRuntime,
        duration: Option<Duration>,
        kind: CommunicationFailureKind,
        error: impl Into<String>,
    ) {
        let now_ms = Self::current_time_ms();
        self.with_observability_txn(|txn| {
            prune_communication_endpoints_for_insert(txn.state_mut(), &endpoint.id);
            let stored = txn
                .state_mut()
                .communication_endpoints
                .entry(endpoint.id.clone())
                .or_insert_with(|| endpoint.clone());
            endpoint.metrics = stored.metrics.clone();
            *stored = endpoint;
            stored
                .metrics
                .record_failure_kind(now_ms, duration, kind, error);
        });
    }
}

fn prune_communication_endpoints_for_insert(observability: &mut ObservabilityState, id: &str) {
    if observability.communication_endpoints.contains_key(id) {
        return;
    }
    while observability.communication_endpoints.len() >= COMMUNICATION_ENDPOINT_RUNTIME_LIMIT {
        let Some(oldest_key) = observability.communication_endpoints.keys().next().cloned() else {
            break;
        };
        observability.communication_endpoints.remove(&oldest_key);
    }
}

pub(super) fn public_http_mutual_tls_mode(
    mode: crate::app::HttpMutualTlsMode,
) -> PublicHttpMutualTlsMode {
    match mode {
        crate::app::HttpMutualTlsMode::Disabled => PublicHttpMutualTlsMode::Disabled,
        crate::app::HttpMutualTlsMode::Optional => PublicHttpMutualTlsMode::Optional,
        crate::app::HttpMutualTlsMode::Required => PublicHttpMutualTlsMode::Required,
    }
}

pub(super) fn push_observability_event(
    observability: &mut ObservabilityState,
    kind: ObservabilityEventKind,
    success: bool,
    duration: Option<Duration>,
    detail: String,
) {
    push_observability_event_with_context(
        observability,
        kind,
        success,
        duration,
        None,
        None,
        detail,
    );
}

pub(super) fn push_observability_event_with_context(
    observability: &mut ObservabilityState,
    kind: ObservabilityEventKind,
    success: bool,
    duration: Option<Duration>,
    correlation_id: Option<String>,
    subject: Option<String>,
    detail: String,
) {
    let duration_ms = duration.map(|value| value.as_millis().min(u128::from(u64::MAX)) as u64);
    let timestamp_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|value| value.as_millis().min(u128::from(u64::MAX)) as u64)
        .unwrap_or(0);
    let sequence = observability.next_event_sequence;
    observability.next_event_sequence = observability.next_event_sequence.saturating_add(1);
    observability.recent_events.push_back(ObservabilityEvent {
        sequence,
        timestamp_ms,
        kind,
        success,
        duration_ms,
        correlation_id,
        subject,
        detail,
    });
    while observability.recent_events.len() > observability.event_limit.max(1) {
        observability.recent_events.pop_front();
    }
}

#[cfg(test)]
mod tests {
    use super::{
        AuditEventKind, AuditLogSink, COMMUNICATION_ENDPOINT_RUNTIME_LIMIT,
        CommunicationEndpointRuntime, ObservabilityState, classify_peer_sync_error,
        clear_test_audit_append_delay, prune_communication_endpoints_for_insert,
        set_test_audit_append_delay, should_emit_audit_drop_warning,
    };
    use crate::NodeError;
    use crate::config::AuditLogOverloadPolicy;
    use orion::control_plane::PeerSyncErrorKind;
    use orion_transport_http::{HttpRequestFailureKind, HttpTransportError};
    use std::{
        env, fs,
        time::{Duration, Instant, SystemTime, UNIX_EPOCH},
    };

    #[test]
    fn audit_drop_warning_is_sampled() {
        assert!(should_emit_audit_drop_warning(1));
        assert!(should_emit_audit_drop_warning(2));
        assert!(!should_emit_audit_drop_warning(3));
        assert!(should_emit_audit_drop_warning(4));
        assert!(!should_emit_audit_drop_warning(6));
        assert!(should_emit_audit_drop_warning(8));
    }

    #[test]
    fn communication_endpoint_runtime_map_is_bounded() {
        let mut observability = ObservabilityState::with_event_limit(128);
        for index in 0..(COMMUNICATION_ENDPOINT_RUNTIME_LIMIT + 32) {
            let id = format!("tcp/data-plane/node-a/resource-{index}");
            prune_communication_endpoints_for_insert(&mut observability, &id);
            observability.communication_endpoints.insert(
                id.clone(),
                CommunicationEndpointRuntime::new(id, "tcp", "data_plane"),
            );
        }

        assert_eq!(
            observability.communication_endpoints.len(),
            COMMUNICATION_ENDPOINT_RUNTIME_LIMIT
        );
    }

    #[test]
    fn audit_log_drops_newest_when_queue_is_full() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after epoch")
            .as_nanos();
        let root = env::temp_dir().join(format!("orion-audit-drop-{unique}"));
        let path = root.join("audit").join("events.jsonl");
        let sink = AuditLogSink::new(path.clone(), 1, AuditLogOverloadPolicy::DropNewest)
            .expect("audit log sink should build");
        set_test_audit_append_delay(Duration::from_millis(25));

        sink.enqueue_test_record(
            1,
            "node.audit.drop",
            AuditEventKind::TransportSecurityFailure,
            Some("subject-a".to_owned()),
            "first",
        )
        .expect("first audit enqueue should succeed");
        let started = Instant::now();
        for sequence in 2..=32 {
            sink.enqueue_test_record(
                sequence,
                "node.audit.drop",
                AuditEventKind::TransportSecurityFailure,
                Some(format!("subject-{sequence}")),
                "second",
            )
            .expect("audit enqueue should succeed even when dropped");
        }
        assert!(
            started.elapsed() < Duration::from_millis(10),
            "drop_newest audit enqueue should not block behind slow audit I/O"
        );
        assert!(
            sink.dropped_records() > 0,
            "drop_newest policy should drop at least one record under a slow one-slot worker"
        );
        clear_test_audit_append_delay();
        drop(sink);

        let contents = fs::read_to_string(&path).expect("audit log should be readable");
        assert!(contents.lines().count() < 32);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn audit_log_drop_policy_counts_dropped_records() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after epoch")
            .as_nanos();
        let root = env::temp_dir().join(format!("orion-audit-drop-count-{unique}"));
        let path = root.join("audit").join("events.jsonl");
        let sink = AuditLogSink::new(path, 1, AuditLogOverloadPolicy::DropNewest)
            .expect("audit log sink should build");
        set_test_audit_append_delay(Duration::from_millis(25));

        sink.enqueue_test_record(
            1,
            "node.audit.drop",
            AuditEventKind::TransportSecurityFailure,
            Some("subject-1".to_owned()),
            "message-1",
        )
        .expect("first audit enqueue should succeed");
        sink.enqueue_test_record(
            2,
            "node.audit.drop",
            AuditEventKind::TransportSecurityFailure,
            Some("subject-2".to_owned()),
            "message-2",
        )
        .expect("second audit enqueue should not fail even when dropped");

        for index in 3..=16 {
            sink.enqueue_test_record(
                index,
                "node.audit.drop",
                AuditEventKind::TransportSecurityFailure,
                Some(format!("subject-{index}")),
                "message-extra",
            )
            .expect("extra audit enqueue should not fail even when dropped");

            if sink.dropped_records() > 0 {
                break;
            }
        }

        clear_test_audit_append_delay();
        assert!(sink.dropped_records() > 0);
        drop(sink);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn peer_sync_classification_uses_request_failure_kind_when_transport_kind_is_typed() {
        let connectivity = NodeError::HttpTransport(HttpTransportError::RequestFailed {
            kind: HttpRequestFailureKind::Connectivity,
            message: "totally different wording".into(),
        });
        assert_eq!(
            classify_peer_sync_error(&connectivity),
            PeerSyncErrorKind::TransportConnectivity
        );

        let generic = NodeError::HttpTransport(HttpTransportError::RequestFailed {
            kind: HttpRequestFailureKind::Other,
            message: "connection refused but intentionally generic".into(),
        });
        assert_eq!(
            classify_peer_sync_error(&generic),
            PeerSyncErrorKind::TransportConnectivity
        );
    }

    #[test]
    fn peer_sync_classification_treats_typed_http_status_failures_as_generic_peer_sync() {
        let error = NodeError::HttpTransport(HttpTransportError::UnexpectedStatus(503));
        assert_eq!(
            classify_peer_sync_error(&error),
            PeerSyncErrorKind::PeerSync
        );
    }

    #[test]
    fn peer_sync_classification_maps_tls_trust_errors_at_transport_boundaries() {
        let error = NodeError::HttpTransport(HttpTransportError::Tls(
            "peer requires prior tls enrollment for this transport binding".into(),
        ));
        assert_eq!(
            classify_peer_sync_error(&error),
            PeerSyncErrorKind::TlsTrust
        );
    }

    #[test]
    fn peer_sync_classification_maps_tls_handshake_errors_at_transport_boundaries() {
        let error = NodeError::HttpTransport(HttpTransportError::Tls(
            "tls handshake failed: peer sent no certificates".into(),
        ));
        assert_eq!(
            classify_peer_sync_error(&error),
            PeerSyncErrorKind::TlsHandshake
        );
    }

    #[test]
    fn peer_sync_classification_keeps_message_fallback_for_untyped_request_failures() {
        let error = NodeError::HttpTransport(HttpTransportError::RequestFailed {
            kind: HttpRequestFailureKind::Other,
            message: "authentication failed because peer is revoked".into(),
        });
        assert_eq!(
            classify_peer_sync_error(&error),
            PeerSyncErrorKind::AuthPolicy
        );
    }

    #[test]
    fn peer_sync_classification_defaults_unknown_untyped_transport_errors_to_generic_peer_sync() {
        let error = NodeError::HttpTransport(HttpTransportError::RequestFailed {
            kind: HttpRequestFailureKind::Other,
            message: "completely novel upstream failure wording".into(),
        });
        assert_eq!(
            classify_peer_sync_error(&error),
            PeerSyncErrorKind::PeerSync
        );
    }

    #[test]
    fn peer_sync_classification_uses_typed_auth_errors_without_message_matching() {
        let error = NodeError::AuthenticatedPeerRequired {
            operation: crate::ControlOperation::Mutations,
        };
        assert_eq!(
            classify_peer_sync_error(&error),
            PeerSyncErrorKind::AuthPolicy
        );
    }
}
