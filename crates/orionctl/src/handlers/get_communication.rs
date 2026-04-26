use orion_control_plane::{CommunicationEndpointSnapshot, NodeObservabilitySnapshot};
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};

use crate::cli::{CommunicationArgs, CommunicationSort, CommunicationView};

#[derive(Clone, Debug, Serialize)]
pub(super) struct CommunicationPeerSummary {
    peer: String,
    endpoint_count: u64,
    transports: Vec<String>,
    connected_endpoints: u64,
    disconnected_endpoints: u64,
    messages_sent: u64,
    messages_received: u64,
    bytes_sent: u64,
    bytes_received: u64,
    recent_successes: u64,
    recent_failures: u64,
    recent_failure_rate_per_mille: u64,
    max_latency_ms: u64,
    worst_endpoint: Option<String>,
    last_error: Option<String>,
}

pub(super) fn print_communication_summary(communication: &[CommunicationEndpointSnapshot]) {
    for endpoint in communication {
        println!(
            "communication id={} transport={} scope={} connected={} queued={} sent_messages={} received_messages={} sent_bytes={} received_bytes={} estimated_wire_sent_bytes={} estimated_wire_received_bytes={} recent_successes={} recent_failures={} recent_avg_latency_ms={} latency_samples={} latency_avg_ms={} latency_last_ms={} latency_max_ms={} latency_le_1_ms={} latency_le_5_ms={} latency_le_10_ms={} latency_le_50_ms={} latency_le_100_ms={} latency_le_500_ms={} latency_gt_500_ms={} failures={} failure_kinds={} reconnects={} last_error={}",
            endpoint.id,
            endpoint.transport,
            endpoint.scope,
            endpoint.connected,
            endpoint
                .queued
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_owned()),
            endpoint.metrics.messages_sent_total,
            endpoint.metrics.messages_received_total,
            endpoint.metrics.bytes_sent_total,
            endpoint.metrics.bytes_received_total,
            endpoint.metrics.estimated_wire_bytes_sent_total,
            endpoint.metrics.estimated_wire_bytes_received_total,
            endpoint.metrics.recent.successes,
            endpoint.metrics.recent.failures,
            endpoint
                .metrics
                .recent
                .avg_latency_ms
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_owned()),
            endpoint.metrics.latency.samples_total,
            if endpoint.metrics.latency.samples_total == 0 {
                "-".to_owned()
            } else {
                (endpoint.metrics.latency.total_duration_ms
                    / endpoint.metrics.latency.samples_total)
                    .to_string()
            },
            endpoint
                .metrics
                .latency
                .last_duration_ms
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_owned()),
            endpoint.metrics.latency.max_duration_ms,
            endpoint.metrics.latency.bucket_le_1_ms,
            endpoint.metrics.latency.bucket_le_5_ms,
            endpoint.metrics.latency.bucket_le_10_ms,
            endpoint.metrics.latency.bucket_le_50_ms,
            endpoint.metrics.latency.bucket_le_100_ms,
            endpoint.metrics.latency.bucket_le_500_ms,
            endpoint.metrics.latency.bucket_gt_500_ms,
            endpoint.metrics.failures_total,
            endpoint
                .metrics
                .failures_by_kind
                .iter()
                .map(|failure| format!("{}:{}", failure.kind, failure.count))
                .collect::<Vec<_>>()
                .join(","),
            endpoint.metrics.reconnects_total,
            endpoint.metrics.last_error.as_deref().unwrap_or("-"),
        );
    }
}

pub(super) fn print_peer_communication_summary(summaries: &[CommunicationPeerSummary]) {
    for summary in summaries {
        println!(
            "peer={} endpoints={} transports={} connected={} disconnected={} sent_messages={} received_messages={} sent_bytes={} received_bytes={} recent_successes={} recent_failures={} recent_failure_rate_per_mille={} max_latency_ms={} worst_endpoint={} last_error={}",
            summary.peer,
            summary.endpoint_count,
            summary.transports.join(","),
            summary.connected_endpoints,
            summary.disconnected_endpoints,
            summary.messages_sent,
            summary.messages_received,
            summary.bytes_sent,
            summary.bytes_received,
            summary.recent_successes,
            summary.recent_failures,
            summary.recent_failure_rate_per_mille,
            summary.max_latency_ms,
            summary.worst_endpoint.as_deref().unwrap_or("-"),
            summary.last_error.as_deref().unwrap_or("-"),
        );
    }
}

pub(super) fn communication_peer_summaries(
    communication: &[CommunicationEndpointSnapshot],
    sort: CommunicationSort,
    limit: Option<usize>,
) -> Vec<CommunicationPeerSummary> {
    let mut grouped: BTreeMap<String, PeerAccumulator> = BTreeMap::new();
    for endpoint in communication
        .iter()
        .filter(|endpoint| peer_key(endpoint).is_some())
    {
        let peer = peer_key(endpoint).expect("filtered endpoint should have peer key");
        grouped.entry(peer).or_default().record(endpoint);
    }
    let mut summaries = grouped
        .into_iter()
        .map(|(peer, accumulator)| accumulator.finish(peer))
        .collect::<Vec<_>>();
    sort_peer_summaries(&mut summaries, sort);
    if let Some(limit) = limit {
        summaries.truncate(limit);
    }
    summaries
}

pub(super) fn filtered_communication(
    snapshot: &NodeObservabilitySnapshot,
    args: &CommunicationArgs,
) -> Result<Vec<CommunicationEndpointSnapshot>, String> {
    let label_filters = args
        .labels
        .iter()
        .map(|filter| {
            filter
                .split_once('=')
                .map(|(key, value)| (key.to_owned(), value.to_owned()))
                .ok_or_else(|| format!("communication label filter `{filter}` must use key=value"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let mut communication = snapshot
        .communication
        .iter()
        .filter(|endpoint| {
            args.id
                .as_ref()
                .map(|value| endpoint.id.contains(value))
                .unwrap_or(true)
                && args
                    .transport
                    .as_ref()
                    .map(|value| endpoint.transport == *value)
                    .unwrap_or(true)
                && args
                    .scope
                    .as_ref()
                    .map(|value| endpoint.scope == *value)
                    .unwrap_or(true)
                && args
                    .peer
                    .as_ref()
                    .map(|value| {
                        endpoint
                            .labels
                            .get("peer_node_id")
                            .map(|peer| peer.contains(value))
                            .unwrap_or(false)
                    })
                    .unwrap_or(true)
                && (!args.connected || endpoint.connected)
                && (!args.disconnected || !endpoint.connected)
                && (!args.failed || endpoint.metrics.failures_total > 0)
                && label_filters.iter().all(|(key, value)| {
                    endpoint
                        .labels
                        .get(key)
                        .map(|label_value| label_value == value)
                        .unwrap_or(false)
                })
        })
        .cloned()
        .collect::<Vec<_>>();
    sort_communication(&mut communication, args.sort);
    if args.view == CommunicationView::Endpoints
        && let Some(limit) = args.limit
    {
        communication.truncate(limit);
    }
    Ok(communication)
}

fn peer_key(endpoint: &CommunicationEndpointSnapshot) -> Option<String> {
    endpoint
        .labels
        .get("peer_node_id")
        .cloned()
        .or_else(|| endpoint.remote.clone())
}

#[derive(Default)]
struct PeerAccumulator {
    endpoint_count: u64,
    transports: BTreeSet<String>,
    connected_endpoints: u64,
    disconnected_endpoints: u64,
    messages_sent: u64,
    messages_received: u64,
    bytes_sent: u64,
    bytes_received: u64,
    recent_successes: u64,
    recent_failures: u64,
    max_latency_ms: u64,
    worst_endpoint: Option<String>,
    last_error: Option<String>,
}

impl PeerAccumulator {
    fn record(&mut self, endpoint: &CommunicationEndpointSnapshot) {
        self.endpoint_count = self.endpoint_count.saturating_add(1);
        self.transports.insert(endpoint.transport.to_string());
        if endpoint.connected {
            self.connected_endpoints = self.connected_endpoints.saturating_add(1);
        } else {
            self.disconnected_endpoints = self.disconnected_endpoints.saturating_add(1);
        }
        self.messages_sent = self
            .messages_sent
            .saturating_add(endpoint.metrics.messages_sent_total);
        self.messages_received = self
            .messages_received
            .saturating_add(endpoint.metrics.messages_received_total);
        self.bytes_sent = self
            .bytes_sent
            .saturating_add(endpoint.metrics.bytes_sent_total);
        self.bytes_received = self
            .bytes_received
            .saturating_add(endpoint.metrics.bytes_received_total);
        self.recent_successes = self
            .recent_successes
            .saturating_add(endpoint.metrics.recent.successes);
        self.recent_failures = self
            .recent_failures
            .saturating_add(endpoint.metrics.recent.failures);
        if endpoint.metrics.latency.max_duration_ms >= self.max_latency_ms {
            self.max_latency_ms = endpoint.metrics.latency.max_duration_ms;
            self.worst_endpoint = Some(endpoint.id.clone());
        }
        if endpoint.metrics.last_error.is_some() {
            self.last_error = endpoint.metrics.last_error.clone();
        }
    }

    fn finish(self, peer: String) -> CommunicationPeerSummary {
        let recent_total = self.recent_successes.saturating_add(self.recent_failures);
        CommunicationPeerSummary {
            peer,
            endpoint_count: self.endpoint_count,
            transports: self.transports.into_iter().collect(),
            connected_endpoints: self.connected_endpoints,
            disconnected_endpoints: self.disconnected_endpoints,
            messages_sent: self.messages_sent,
            messages_received: self.messages_received,
            bytes_sent: self.bytes_sent,
            bytes_received: self.bytes_received,
            recent_successes: self.recent_successes,
            recent_failures: self.recent_failures,
            recent_failure_rate_per_mille: if recent_total == 0 {
                0
            } else {
                self.recent_failures.saturating_mul(1_000) / recent_total
            },
            max_latency_ms: self.max_latency_ms,
            worst_endpoint: self.worst_endpoint,
            last_error: self.last_error,
        }
    }
}

fn sort_communication(
    communication: &mut [CommunicationEndpointSnapshot],
    sort: CommunicationSort,
) {
    match sort {
        CommunicationSort::Id => {
            communication.sort_by(|left, right| left.id.cmp(&right.id));
        }
        CommunicationSort::Slowest => {
            communication.sort_by_key(|endpoint| {
                std::cmp::Reverse(endpoint.metrics.latency.max_duration_ms)
            });
        }
        CommunicationSort::Failures => {
            communication
                .sort_by_key(|endpoint| std::cmp::Reverse(endpoint.metrics.failures_total));
        }
        CommunicationSort::FailureRate => {
            communication.sort_by_key(|endpoint| {
                let total = endpoint
                    .metrics
                    .recent
                    .successes
                    .saturating_add(endpoint.metrics.recent.failures)
                    .max(1);
                std::cmp::Reverse(
                    endpoint.metrics.recent.failures.saturating_mul(1_000_000) / total,
                )
            });
        }
        CommunicationSort::Bytes => {
            communication.sort_by_key(|endpoint| {
                std::cmp::Reverse(
                    endpoint
                        .metrics
                        .bytes_sent_total
                        .saturating_add(endpoint.metrics.bytes_received_total),
                )
            });
        }
    }
}

fn sort_peer_summaries(summaries: &mut [CommunicationPeerSummary], sort: CommunicationSort) {
    match sort {
        CommunicationSort::Id => {
            summaries.sort_by(|left, right| left.peer.cmp(&right.peer));
        }
        CommunicationSort::Slowest => {
            summaries.sort_by_key(|summary| std::cmp::Reverse(summary.max_latency_ms));
        }
        CommunicationSort::Failures => {
            summaries.sort_by_key(|summary| std::cmp::Reverse(summary.recent_failures));
        }
        CommunicationSort::FailureRate => {
            summaries
                .sort_by_key(|summary| std::cmp::Reverse(summary.recent_failure_rate_per_mille));
        }
        CommunicationSort::Bytes => {
            summaries.sort_by_key(|summary| {
                std::cmp::Reverse(summary.bytes_sent.saturating_add(summary.bytes_received))
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use orion_control_plane::{
        CommunicationMetricsSnapshot, CommunicationRecentMetricsSnapshot,
        CommunicationStageMetricsSnapshot, LatencyMetricsSnapshot,
    };

    #[test]
    fn peer_summaries_sort_after_aggregation() {
        let endpoints = vec![
            endpoint("http/peer-sync/node-a", "node-a", 10, 1, 9, 1),
            endpoint("tcp/data-plane/node-b", "node-b", 20, 5, 5, 2),
            endpoint("quic/data-plane/node-a", "node-a", 30, 0, 1, 10),
        ];

        let by_failure_rate =
            communication_peer_summaries(&endpoints, CommunicationSort::FailureRate, None);
        assert_eq!(by_failure_rate[0].peer, "node-b");
        assert_eq!(by_failure_rate[1].peer, "node-a");

        let by_bytes = communication_peer_summaries(&endpoints, CommunicationSort::Bytes, Some(1));
        assert_eq!(by_bytes.len(), 1);
        assert_eq!(by_bytes[0].peer, "node-a");
        assert_eq!(by_bytes[0].endpoint_count, 2);
    }

    fn endpoint(
        id: &str,
        peer: &str,
        bytes_sent: u64,
        recent_failures: u64,
        recent_successes: u64,
        max_latency_ms: u64,
    ) -> CommunicationEndpointSnapshot {
        CommunicationEndpointSnapshot {
            id: id.to_owned(),
            transport: orion_control_plane::CommunicationTransportKind::from_label(
                id.split('/').next().unwrap_or("http"),
            ),
            scope: orion_control_plane::CommunicationEndpointScope::PeerSync,
            local: None,
            remote: None,
            labels: BTreeMap::from([("peer_node_id".to_owned(), peer.to_owned())]),
            connected: true,
            queued: None,
            metrics: CommunicationMetricsSnapshot {
                messages_sent_total: 1,
                messages_received_total: 1,
                bytes_sent_total: bytes_sent,
                bytes_received_total: 0,
                estimated_wire_bytes_sent_total: bytes_sent,
                estimated_wire_bytes_received_total: 0,
                failures_total: recent_failures,
                failures_by_kind: Vec::new(),
                reconnects_total: 0,
                last_success_at_ms: None,
                last_failure_at_ms: None,
                last_error: None,
                latency: LatencyMetricsSnapshot {
                    samples_total: 1,
                    total_duration_ms: max_latency_ms,
                    last_duration_ms: Some(max_latency_ms),
                    max_duration_ms: max_latency_ms,
                    ..Default::default()
                },
                stages: CommunicationStageMetricsSnapshot::default(),
                recent: CommunicationRecentMetricsSnapshot {
                    window_ms: 300_000,
                    successes: recent_successes,
                    failures: recent_failures,
                    avg_latency_ms: Some(max_latency_ms),
                    max_latency_ms,
                    bytes_sent,
                    bytes_received: 0,
                },
            },
        }
    }
}
