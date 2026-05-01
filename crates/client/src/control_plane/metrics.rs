use orion_control_plane::{
    CommunicationFailureCountSnapshot, CommunicationFailureKind, CommunicationMetricsSnapshot,
    CommunicationRecentMetricsSnapshot, CommunicationStageMetricsSnapshot, LatencyMetricBuckets,
    estimate_wire_bytes,
};
use std::{collections::BTreeMap, time::Duration};

#[derive(Clone, Debug, Default)]
pub(super) struct ClientCommunicationMetrics {
    messages_sent_total: u64,
    messages_received_total: u64,
    bytes_sent_total: u64,
    bytes_received_total: u64,
    estimated_wire_bytes_sent_total: u64,
    estimated_wire_bytes_received_total: u64,
    failures_total: u64,
    failures_by_kind: BTreeMap<CommunicationFailureKind, u64>,
    reconnects_total: u64,
    last_success_at_ms: Option<u64>,
    last_failure_at_ms: Option<u64>,
    last_error: Option<String>,
    latency: LatencyMetricBuckets,
}

impl ClientCommunicationMetrics {
    pub(super) fn record_success(
        &mut self,
        bytes_sent: u64,
        bytes_received: u64,
        now_ms: u64,
        duration: Duration,
    ) {
        self.messages_sent_total = self.messages_sent_total.saturating_add(1);
        self.messages_received_total = self.messages_received_total.saturating_add(1);
        self.bytes_sent_total = self.bytes_sent_total.saturating_add(bytes_sent);
        self.bytes_received_total = self.bytes_received_total.saturating_add(bytes_received);
        self.estimated_wire_bytes_sent_total = self
            .estimated_wire_bytes_sent_total
            .saturating_add(estimate_wire_bytes(bytes_sent));
        self.estimated_wire_bytes_received_total = self
            .estimated_wire_bytes_received_total
            .saturating_add(estimate_wire_bytes(bytes_received));
        self.last_success_at_ms = Some(now_ms);
        self.last_error = None;
        self.latency.record(duration);
    }

    pub(super) fn record_reconnect(&mut self) {
        self.reconnects_total = self.reconnects_total.saturating_add(1);
    }

    pub(super) fn record_failure(
        &mut self,
        bytes_sent: u64,
        now_ms: u64,
        duration: Duration,
        kind: CommunicationFailureKind,
        error: impl Into<String>,
    ) {
        self.messages_sent_total = self.messages_sent_total.saturating_add(1);
        self.bytes_sent_total = self.bytes_sent_total.saturating_add(bytes_sent);
        self.estimated_wire_bytes_sent_total = self
            .estimated_wire_bytes_sent_total
            .saturating_add(estimate_wire_bytes(bytes_sent));
        self.failures_total = self.failures_total.saturating_add(1);
        let error = error.into();
        *self.failures_by_kind.entry(kind).or_default() += 1;
        self.last_failure_at_ms = Some(now_ms);
        self.last_error = Some(error);
        self.latency.record(duration);
    }

    pub(super) fn snapshot(&self) -> CommunicationMetricsSnapshot {
        CommunicationMetricsSnapshot {
            messages_sent_total: self.messages_sent_total,
            messages_received_total: self.messages_received_total,
            bytes_sent_total: self.bytes_sent_total,
            bytes_received_total: self.bytes_received_total,
            estimated_wire_bytes_sent_total: self.estimated_wire_bytes_sent_total,
            estimated_wire_bytes_received_total: self.estimated_wire_bytes_received_total,
            failures_total: self.failures_total,
            failures_by_kind: self
                .failures_by_kind
                .iter()
                .map(|(kind, count)| CommunicationFailureCountSnapshot {
                    kind: *kind,
                    count: *count,
                })
                .collect(),
            reconnects_total: self.reconnects_total,
            last_success_at_ms: self.last_success_at_ms,
            last_failure_at_ms: self.last_failure_at_ms,
            last_error: self.last_error.clone(),
            latency: self.latency.snapshot(),
            stages: CommunicationStageMetricsSnapshot::default(),
            recent: CommunicationRecentMetricsSnapshot::default(),
        }
    }
}
