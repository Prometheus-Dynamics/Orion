use orion::control_plane::{
    COMMUNICATION_RECENT_SAMPLE_LIMIT, COMMUNICATION_RECENT_WINDOW_MS,
    CommunicationFailureCountSnapshot, CommunicationFailureKind, CommunicationMetricsSnapshot,
    CommunicationRecentMetricsSnapshot, CommunicationStageMetricsSnapshot, LatencyMetricBuckets,
    OperationFailureCategory, OperationMetricsSnapshot, duration_ms_u64, estimate_wire_bytes,
};
use std::{
    collections::{BTreeMap, VecDeque},
    time::Duration,
};

#[derive(Clone, Debug, Default)]
pub(crate) struct OperationMetrics {
    success_count: u64,
    failure_count: u64,
    total_duration_ms: u64,
    last_duration_ms: Option<u64>,
    max_duration_ms: u64,
    latency: LatencyMetricBuckets,
    last_error_category: Option<OperationFailureCategory>,
    last_error: Option<String>,
}

impl OperationMetrics {
    pub(crate) fn record_success(&mut self, duration: Duration) {
        let duration_ms = duration_ms_u64(duration);
        self.success_count = self.success_count.saturating_add(1);
        self.total_duration_ms = self.total_duration_ms.saturating_add(duration_ms);
        self.last_duration_ms = Some(duration_ms);
        self.max_duration_ms = self.max_duration_ms.max(duration_ms);
        self.latency.record(duration);
        self.last_error_category = None;
        self.last_error = None;
    }

    pub(crate) fn record_failure(
        &mut self,
        duration: Duration,
        category: OperationFailureCategory,
        error: impl Into<String>,
    ) {
        let duration_ms = duration_ms_u64(duration);
        self.failure_count = self.failure_count.saturating_add(1);
        self.total_duration_ms = self.total_duration_ms.saturating_add(duration_ms);
        self.last_duration_ms = Some(duration_ms);
        self.max_duration_ms = self.max_duration_ms.max(duration_ms);
        self.latency.record(duration);
        self.last_error_category = Some(category);
        self.last_error = Some(error.into());
    }

    pub(crate) fn snapshot(&self) -> OperationMetricsSnapshot {
        OperationMetricsSnapshot {
            success_count: self.success_count,
            failure_count: self.failure_count,
            total_duration_ms: self.total_duration_ms,
            last_duration_ms: self.last_duration_ms,
            max_duration_ms: self.max_duration_ms,
            latency: self.latency.snapshot(),
            last_error_category: self.last_error_category,
            last_error: self.last_error.clone(),
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct CommunicationMetrics {
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
    stages: CommunicationStageMetrics,
    recent: VecDeque<CommunicationRecentSample>,
}

impl CommunicationMetrics {
    pub(crate) fn record_sent(&mut self, bytes: u64) {
        self.messages_sent_total = self.messages_sent_total.saturating_add(1);
        self.bytes_sent_total = self.bytes_sent_total.saturating_add(bytes);
        self.estimated_wire_bytes_sent_total = self
            .estimated_wire_bytes_sent_total
            .saturating_add(estimate_wire_bytes(bytes));
    }

    pub(crate) fn record_received(&mut self, bytes: u64) {
        self.messages_received_total = self.messages_received_total.saturating_add(1);
        self.bytes_received_total = self.bytes_received_total.saturating_add(bytes);
        self.estimated_wire_bytes_received_total = self
            .estimated_wire_bytes_received_total
            .saturating_add(estimate_wire_bytes(bytes));
    }

    pub(crate) fn record_success_exchange(
        &mut self,
        now_ms: u64,
        duration: Duration,
        bytes_sent: u64,
        bytes_received: u64,
    ) {
        self.record_success_exchange_with_stages(
            now_ms,
            duration,
            bytes_sent,
            bytes_received,
            &CommunicationStageDurations::default(),
        );
    }

    pub(crate) fn record_success_exchange_with_stages(
        &mut self,
        now_ms: u64,
        duration: Duration,
        bytes_sent: u64,
        bytes_received: u64,
        stages: &CommunicationStageDurations,
    ) {
        self.last_success_at_ms = Some(now_ms);
        self.last_error = None;
        self.latency.record(duration);
        self.stages.record(stages);
        self.push_recent(now_ms, true, duration, bytes_sent, bytes_received);
    }

    pub(crate) fn record_failure(
        &mut self,
        now_ms: u64,
        duration: Option<Duration>,
        error: impl Into<String>,
    ) {
        let error = error.into();
        let kind = classify_communication_failure(&error);
        self.record_failure_kind(now_ms, duration, kind, error);
    }

    pub(crate) fn record_failure_kind(
        &mut self,
        now_ms: u64,
        duration: Option<Duration>,
        kind: CommunicationFailureKind,
        error: impl Into<String>,
    ) {
        let error = error.into();
        self.failures_total = self.failures_total.saturating_add(1);
        *self.failures_by_kind.entry(kind).or_default() = self
            .failures_by_kind
            .get(&kind)
            .copied()
            .unwrap_or(0)
            .saturating_add(1);
        self.last_failure_at_ms = Some(now_ms);
        self.last_error = Some(error);
        if let Some(duration) = duration {
            self.latency.record(duration);
            self.push_recent(now_ms, false, duration, 0, 0);
        } else {
            self.push_recent(now_ms, false, Duration::ZERO, 0, 0);
        }
    }

    pub(crate) fn record_reconnect(&mut self) {
        self.reconnects_total = self.reconnects_total.saturating_add(1);
    }

    fn push_recent(
        &mut self,
        now_ms: u64,
        success: bool,
        duration: Duration,
        bytes_sent: u64,
        bytes_received: u64,
    ) {
        self.recent.push_back(CommunicationRecentSample {
            timestamp_ms: now_ms,
            success,
            duration_ms: duration_ms_u64(duration),
            bytes_sent,
            bytes_received,
        });
        self.prune_recent(now_ms);
    }

    fn prune_recent(&mut self, now_ms: u64) {
        let cutoff = now_ms.saturating_sub(COMMUNICATION_RECENT_WINDOW_MS);
        while self
            .recent
            .front()
            .map(|sample| sample.timestamp_ms < cutoff)
            .unwrap_or(false)
        {
            self.recent.pop_front();
        }
        while self.recent.len() > COMMUNICATION_RECENT_SAMPLE_LIMIT {
            self.recent.pop_front();
        }
    }

    pub(crate) fn snapshot(&self) -> CommunicationMetricsSnapshot {
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
            stages: self.stages.snapshot(),
            recent: self.recent_snapshot(),
        }
    }

    fn recent_snapshot(&self) -> CommunicationRecentMetricsSnapshot {
        if self.recent.is_empty() {
            return CommunicationRecentMetricsSnapshot::default();
        }
        let mut successes = 0_u64;
        let mut failures = 0_u64;
        let mut total_latency = 0_u64;
        let mut latency_samples = 0_u64;
        let mut max_latency = 0_u64;
        let mut bytes_sent = 0_u64;
        let mut bytes_received = 0_u64;
        for sample in &self.recent {
            if sample.success {
                successes = successes.saturating_add(1);
            } else {
                failures = failures.saturating_add(1);
            }
            total_latency = total_latency.saturating_add(sample.duration_ms);
            latency_samples = latency_samples.saturating_add(1);
            max_latency = max_latency.max(sample.duration_ms);
            bytes_sent = bytes_sent.saturating_add(sample.bytes_sent);
            bytes_received = bytes_received.saturating_add(sample.bytes_received);
        }
        CommunicationRecentMetricsSnapshot {
            window_ms: COMMUNICATION_RECENT_WINDOW_MS,
            successes,
            failures,
            avg_latency_ms: (latency_samples > 0).then_some(total_latency / latency_samples),
            max_latency_ms: max_latency,
            bytes_sent,
            bytes_received,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct CommunicationStageDurations {
    pub(crate) queue_wait: Option<Duration>,
    pub(crate) encode: Option<Duration>,
    pub(crate) decode: Option<Duration>,
    pub(crate) socket_read: Option<Duration>,
    pub(crate) socket_write: Option<Duration>,
    pub(crate) retry_delay: Option<Duration>,
    pub(crate) backpressure: Option<Duration>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct CommunicationStageMetrics {
    queue_wait: LatencyMetricBuckets,
    encode: LatencyMetricBuckets,
    decode: LatencyMetricBuckets,
    socket_read: LatencyMetricBuckets,
    socket_write: LatencyMetricBuckets,
    retry_delay: LatencyMetricBuckets,
    backpressure: LatencyMetricBuckets,
}

impl CommunicationStageMetrics {
    fn record(&mut self, stages: &CommunicationStageDurations) {
        if let Some(duration) = stages.queue_wait {
            self.queue_wait.record(duration);
        }
        if let Some(duration) = stages.encode {
            self.encode.record(duration);
        }
        if let Some(duration) = stages.decode {
            self.decode.record(duration);
        }
        if let Some(duration) = stages.socket_read {
            self.socket_read.record(duration);
        }
        if let Some(duration) = stages.socket_write {
            self.socket_write.record(duration);
        }
        if let Some(duration) = stages.retry_delay {
            self.retry_delay.record(duration);
        }
        if let Some(duration) = stages.backpressure {
            self.backpressure.record(duration);
        }
    }

    fn snapshot(&self) -> CommunicationStageMetricsSnapshot {
        CommunicationStageMetricsSnapshot {
            queue_wait: self.queue_wait.snapshot(),
            encode: self.encode.snapshot(),
            decode: self.decode.snapshot(),
            socket_read: self.socket_read.snapshot(),
            socket_write: self.socket_write.snapshot(),
            retry_delay: self.retry_delay.snapshot(),
            backpressure: self.backpressure.snapshot(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct CommunicationRecentSample {
    timestamp_ms: u64,
    success: bool,
    duration_ms: u64,
    bytes_sent: u64,
    bytes_received: u64,
}

fn classify_communication_failure(error: &str) -> CommunicationFailureKind {
    let error = error.to_lowercase();
    if error.contains("timeout") || error.contains("timed out") {
        CommunicationFailureKind::Timeout
    } else if error.contains("tls") || error.contains("certificate") {
        CommunicationFailureKind::Tls
    } else if error.contains("decode") || error.contains("deserialize") || error.contains("codec") {
        CommunicationFailureKind::Decode
    } else if error.contains("protocol") || error.contains("malformed") || error.contains("frame") {
        CommunicationFailureKind::Protocol
    } else if error.contains("peer") && (error.contains("unavailable") || error.contains("unknown"))
    {
        CommunicationFailureKind::UnavailablePeer
    } else if error.contains("cancel") || error.contains("closed") {
        CommunicationFailureKind::Canceled
    } else if error.contains("transport") || error.contains("connect") || error.contains("network")
    {
        CommunicationFailureKind::Transport
    } else {
        CommunicationFailureKind::Unknown
    }
}
