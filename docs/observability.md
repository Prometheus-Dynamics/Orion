# Observability Surface

`orion-node` exposes three primary runtime status views:

- health
- readiness
- observability

These are available through the node control surface and, when configured, the probe HTTP surface.

## Health

Health is intended to answer whether the process is alive and whether it is degraded.

Current health fields include:

- replay completion and replay success
- HTTP, IPC unary, and IPC stream bind status
- degraded peer count
- degraded communication endpoint count folded into health reasons
- human-readable reasons

## Readiness

Readiness is intended to answer whether the node is ready to participate in normal operation.

Current readiness fields include:

- replay completion and replay success
- HTTP, IPC unary, and IPC stream bind status
- initial peer-sync completion
- ready, pending, and degraded peer counts
- human-readable reasons

## Observability Snapshot

The observability snapshot is the richer operator/debugging view.

It currently includes:

- desired, observed, and applied revisions
- host OS and process metrics, including hostname, OS/kernel information, uptime, load,
  memory/swap, process id, and process RSS when the host exposes `/proc`
- configured, ready, pending, and degraded peer counts
- effective peer-sync parallel in-flight cap
- operation metrics for replay, peer sync, reconcile, and mutation apply
- persistence worker queue capacity plus queue-send wait, reply wait, and operation duration metrics
- audit-log queue capacity, backpressure mode, and dropped-record count
- client-session metrics including registered clients, live stream clients, and queued client events
- transport metrics including malformed input counts, HTTP request failures, TLS failures, and IPC reconnect count
- local rate-limit counters and recent structured rate-limit events
- communication endpoint metrics for node-owned paths, including HTTP control/probe requests,
  local IPC unary, local IPC stream, peer HTTP sync endpoints, and managed TCP/QUIC data-plane
  frame handlers
- recent structured observability events

Communication endpoint metrics use one transport-agnostic shape so operator tooling can render a
single table without each transport inventing its own counters. Each endpoint carries:

- a stable subsystem-owned id such as `ipc/local-unary/orionctl.get`,
  `ipc/local-stream/executor.camera`, `http/peer-sync/node-b`, or
  `quic/data-plane/node-b/resource.camera`
- `transport` and `scope` fields
- optional local/remote endpoint strings
- subsystem labels such as client name, client role, peer node id, sync status, PID, UID, and GID
- sent/received message counts, serialized payload bytes, estimated wire bytes, failures,
  reconnects, and latency summary counters
- normalized failure-kind counters
- recent rolling-window success/failure/latency counters
- stage latency counters for endpoint pipeline phases that Orion can observe
- latency bucket counts for `<=1ms`, `<=5ms`, `<=10ms`, `<=50ms`, `<=100ms`, `<=500ms`, and
  `>500ms`

`bytes_sent_total` and `bytes_received_total` are application payload counters at Orion-owned
serialization boundaries. HTTP control/probe endpoints count serialized request and response
payloads and intentionally exclude HTTP headers, TLS framing, TCP/IP framing, and kernel socket
accounting. IPC and managed TCP/QUIC data-plane endpoints follow the same rule for their encoded
Orion envelopes or frames. `estimated_wire_bytes_*` adds Orion's local framing estimate to the
payload counters; it is useful for comparing endpoint pressure, but it is still not packet-level
wire accounting. Use packet or socket-level telemetry when exact wire bytes are required.

Health uses communication metrics to report a degraded node when critical communication endpoints
are disconnected or their most recent rolling window contains failures without a later success.
Readiness remains startup/control-plane oriented: a started node does not become NotReady solely
because a peer endpoint is degraded.

Use `orionctl get host` for the host/process view and `orionctl get communication` to view the
endpoint-oriented surface directly. The full observability snapshot also includes the same data
under `host` and `communication`.

`orionctl` can render observability views as Prometheus/OpenMetrics-style text without running a
scrape server:

- `orionctl get observability -o metrics`
- `orionctl get host -o metrics`
- `orionctl get communication -o metrics`

The metrics output is generated from the same control-plane snapshots as the structured outputs.
Communication filters apply before rendering, so commands such as
`orionctl get communication --transport ipc --scope local_unary -o metrics` export only the
matching endpoints. Endpoint metric labels intentionally promote a small stable set of labels,
including `peer_node_id`, `resource_id`, `client_name`, `role`, `binding`, and `sync_status`.
Arbitrary endpoint labels are not exported as metric labels by default to avoid unbounded
cardinality. The exporter also bounds endpoint id and label value length and exports at most 512
communication endpoints per scrape by default. Override those limits with
`ORION_METRICS_MAX_COMMUNICATION_ENDPOINTS` and `ORION_METRICS_MAX_LABEL_VALUE_LEN`, or for
`orionctl` metrics output with `--metrics-max-communication-endpoints` and
`--metrics-max-label-value-len`. If more endpoints are present, the omitted count is emitted as
`orion_communication_export_dropped_endpoints`.

When the probe HTTP surface is configured, the node also serves the same metrics text at
`GET /metrics`. This route is intentionally mounted only on the probe surface, alongside health
and readiness. It is not mounted on the peer/control HTTP surface, so enabling peer HTTP control
does not accidentally expose unauthenticated metrics. TLS and network exposure follow the probe
surface configuration deliberately; deployments that expose probes outside a trusted local network
should put the probe listener behind the same network controls used for health and readiness.

Exported metric families:

| Metric family | Type | Labels | Notes |
| --- | --- | --- | --- |
| `orion_host_uptime_seconds` | gauge | `node_id` | Present when `/proc/uptime` is available. |
| `orion_host_load1`, `orion_host_load5`, `orion_host_load15` | gauge | `node_id` | Load averages from `/proc/loadavg`. |
| `orion_host_memory_total_bytes`, `orion_host_memory_available_bytes` | gauge | `node_id` | Host memory from `/proc/meminfo`. |
| `orion_host_swap_total_bytes`, `orion_host_swap_free_bytes` | gauge | `node_id` | Host swap from `/proc/meminfo`. |
| `orion_process_id` | gauge | `node_id` | Current Orion process id. |
| `orion_process_rss_bytes` | gauge | `node_id` | Process RSS when `/proc/self/statm` is available. |
| `orion_peer_count` | gauge | `node_id`, `status` | `status` is `configured`, `ready`, `pending`, or `degraded`. |
| `orion_peer_sync_parallel_in_flight_cap` | gauge | `node_id` | Effective peer-sync parallelism cap. |
| `orion_audit_log_dropped_records_total` | gauge | `node_id` | Dropped audit records under overload policy. |
| `orion_client_sessions` | gauge | `node_id`, `state` | Currently exported states are `registered` and `live_stream`. |
| `orion_operation_success_total`, `orion_operation_failure_total` | counter | `node_id`, `operation` | Operation is `replay`, `peer_sync`, `reconcile`, or `mutation_apply`. |
| `orion_operation_duration_ms_total` | counter | `node_id`, `operation` | Total operation duration in milliseconds. |
| `orion_operation_duration_ms_max` | gauge | `node_id`, `operation` | Maximum observed operation duration. |
| `orion_operation_duration_ms` | histogram | `node_id`, `operation`, `le` | Buckets: `1`, `5`, `10`, `50`, `100`, `500`, `+Inf` ms. |
| `orion_communication_messages_sent_total`, `orion_communication_messages_received_total` | counter | endpoint labels | Message counters per communication endpoint. |
| `orion_communication_bytes_sent_total`, `orion_communication_bytes_received_total` | counter | endpoint labels | Serialized Orion payload bytes, not wire bytes. |
| `orion_communication_estimated_wire_bytes_sent_total`, `orion_communication_estimated_wire_bytes_received_total` | counter | endpoint labels | Payload bytes plus Orion's local framing estimate. |
| `orion_communication_failures_total`, `orion_communication_reconnects_total` | counter | endpoint labels | Failure and reconnect counters per endpoint. |
| `orion_communication_failures_by_kind_total` | counter | endpoint labels, `kind` | Normalized failure taxonomy: `timeout`, `refused`, `tls`, `protocol`, `decode`, `unavailable_peer`, `canceled`, `transport`, or `unknown`. |
| `orion_communication_connected` | gauge | endpoint labels | `1` when connected, `0` otherwise. |
| `orion_communication_queued` | gauge | endpoint labels | Emitted only when an endpoint reports queue depth. |
| `orion_communication_latency_ms` | histogram | endpoint labels, `le` | Buckets: `1`, `5`, `10`, `50`, `100`, `500`, `+Inf` ms. |
| `orion_communication_stage_latency_ms` | histogram | endpoint labels, `stage`, `le` | Stage-level pipeline latency when a stage is observable. Current populated stages include HTTP response write timing, IPC envelope encode/decode/read/write timing, and managed TCP/QUIC frame encode/decode plus request/response timing. |
| `orion_communication_recent_successes_total`, `orion_communication_recent_failures_total` | counter | endpoint labels | Recent rolling-window counters. |
| `orion_communication_recent_avg_latency_ms` | gauge | endpoint labels | Average latency in the recent rolling window. |
| `orion_communication_export_dropped_endpoints` | gauge | `node_id` | Endpoints omitted from metrics export by cardinality limits. |

Endpoint labels always include `node_id`, `id`, `transport`, and `scope`. The exporter also
promotes the stable optional labels `peer_node_id`, `resource_id`, `client_name`, `role`,
`binding`, and `sync_status` when present.

`orion-client` also exposes `LocalControlPlaneClient::local_communication_metrics()` for the
client process's own IPC unary request metrics. Those counters describe the client's observed
request/response latency and payload volume; they are not fetched from the node observability
snapshot.

`orionctl get communication` supports endpoint filtering before rendering:

- `--id` matches a substring of the endpoint id
- `--transport` matches transport exactly, such as `ipc`, `http`, `tcp`, or `quic`
- `--scope` matches scope exactly, such as `local_unary`, `local_stream`, `peer_sync`, or
  `data_plane`
- `--peer` matches the `peer_node_id` label when present
- `--label key=value` can be repeated to match endpoint labels exactly
- `--connected` keeps only currently connected endpoints
- `--disconnected` keeps only currently disconnected endpoints
- `--failed` keeps only endpoints with at least one recorded failure
- `--view endpoints|peers` switches between per-endpoint rows and one peer-focused summary row
- `--sort id|slowest|failures|failure-rate|bytes` sorts endpoint rows or peer summaries
- `--limit N` keeps only the first `N` endpoints or peer summaries after filtering and sorting

For `--view peers`, sorting is applied after endpoint aggregation:

- `id`: peer id or remote endpoint string
- `slowest`: highest `max_latency_ms`
- `failures`: highest recent failure count
- `failure-rate`: highest recent failure rate per mille
- `bytes`: highest sent plus received payload bytes

## Structured Field Reference

`orionctl get observability -o json|yaml|toml` returns `NodeObservabilitySnapshot`.
The most commonly consumed fields are:

| Field | Shape | Notes |
| --- | --- | --- |
| `node_id` | string | Local node id. |
| `desired_revision`, `observed_revision`, `applied_revision` | integer | Cluster-state revision markers. |
| `maintenance` | object | Current maintenance mode and reason fields. |
| `peer_sync_paused`, `remote_desired_state_blocked` | boolean | Runtime control-plane gates. |
| `host` | `HostMetricsSnapshot` | Host/process facts and counters. |
| `configured_peer_count`, `ready_peer_count`, `pending_peer_count`, `degraded_peer_count` | integer | Peer state counts. |
| `peer_sync_parallel_in_flight_cap` | integer | Effective configured peer-sync concurrency. |
| `replay`, `peer_sync`, `reconcile`, `mutation_apply` | `OperationMetricsSnapshot` | Operation counters and latency buckets. |
| `persistence` | `PersistenceMetricsSnapshot` | Persistence operation, worker queue-send wait, reply wait, and total operation metrics. |
| `audit_log_queue_capacity`, `audit_log_backpressure_mode`, `audit_log_dropped_records` | integer/string/integer | Audit-log overload state. |
| `client_sessions` | `ClientSessionMetricsSnapshot` | Registered clients, live streams, queue depth, and churn counters. |
| `transport` | `TransportMetricsSnapshot` | HTTP/IPС malformed input, TLS, frame, and reconnect counters. |
| `communication` | array of `CommunicationEndpointSnapshot` | Endpoint-level communication metrics. |
| `recent_events` | array of `ObservabilityEvent` | Recent bounded event log. |

`HostMetricsSnapshot` fields:

| Field | Shape | Notes |
| --- | --- | --- |
| `hostname`, `os_version`, `kernel_version` | string or null | Linux host identity where available. |
| `os_name`, `architecture` | string | Runtime OS and architecture. |
| `uptime_seconds` | integer or null | `/proc/uptime`. |
| `load_1_milli`, `load_5_milli`, `load_15_milli` | integer or null | Load average multiplied by 1000. |
| `memory_total_bytes`, `memory_available_bytes` | integer or null | `/proc/meminfo`. |
| `swap_total_bytes`, `swap_free_bytes` | integer or null | `/proc/meminfo`. |
| `process_id` | integer | Orion process id. |
| `process_rss_bytes` | integer or null | Process resident set size. |

`CommunicationEndpointSnapshot` fields:

| Field | Shape | Notes |
| --- | --- | --- |
| `id` | string | Stable subsystem-owned endpoint id. |
| `transport` | string | `ipc`, `http`, `tcp`, `quic`, or another transport string. |
| `scope` | string | Endpoint role such as `local_unary`, `local_stream`, `peer_sync`, `data_plane`, `health`, `readiness`, or `metrics`. |
| `local`, `remote` | string or null | Local/remote endpoint identity when known. |
| `labels` | object | Subsystem labels. Not all labels are promoted to metrics labels. |
| `connected` | boolean | Current connection state. |
| `queued` | integer or null | Queue depth when the endpoint reports one. |
| `metrics` | `CommunicationMetricsSnapshot` | Counters, latency, failure taxonomy, stages, and recent window. |

`CommunicationMetricsSnapshot` fields:

| Field | Shape | Notes |
| --- | --- | --- |
| `messages_sent_total`, `messages_received_total` | integer | Message counters. |
| `bytes_sent_total`, `bytes_received_total` | integer | Serialized Orion payload bytes. |
| `estimated_wire_bytes_sent_total`, `estimated_wire_bytes_received_total` | integer | Payload bytes plus Orion's local framing estimate. |
| `failures_total`, `reconnects_total` | integer | Failure and reconnect counters. |
| `failures_by_kind` | array | `{ kind, count }` entries using the normalized communication failure taxonomy. |
| `last_success_at_ms`, `last_failure_at_ms` | integer or null | Unix epoch milliseconds. |
| `last_error` | string or null | Last failure text for operator troubleshooting. |
| `latency` | `LatencyMetricsSnapshot` | Overall endpoint latency buckets. |
| `stages` | `CommunicationStageMetricsSnapshot` | Stage latency buckets for `queue_wait`, `encode`, `decode`, `socket_read`, `socket_write`, `retry_delay`, and `backpressure`. Unobserved stages are zeroed. |
| `recent` | `CommunicationRecentMetricsSnapshot` | Rolling-window successes, failures, latency, and bytes. |

`LatencyMetricsSnapshot` fields:

| Field | Shape | Notes |
| --- | --- | --- |
| `samples_total`, `total_duration_ms`, `max_duration_ms` | integer | Sample count, duration sum, and maximum. |
| `last_duration_ms` | integer or null | Most recent sample duration. |
| `bucket_le_1_ms`, `bucket_le_5_ms`, `bucket_le_10_ms`, `bucket_le_50_ms`, `bucket_le_100_ms`, `bucket_le_500_ms`, `bucket_gt_500_ms` | integer | Non-cumulative bucket counts in structured snapshots. Metrics export renders cumulative Prometheus buckets. |

`orionctl get communication --view peers -o json|yaml|toml` returns an array of peer summaries:

| Field | Shape | Notes |
| --- | --- | --- |
| `peer` | string | `peer_node_id` when present, otherwise remote endpoint string. |
| `endpoint_count` | integer | Number of endpoints included in the peer summary. |
| `transports` | array of strings | Distinct transports seen for that peer. |
| `connected_endpoints`, `disconnected_endpoints` | integer | Endpoint connection counts. |
| `messages_sent`, `messages_received` | integer | Aggregated message counters. |
| `bytes_sent`, `bytes_received` | integer | Aggregated payload-byte counters. |
| `recent_successes`, `recent_failures` | integer | Aggregated recent-window counts. |
| `recent_failure_rate_per_mille` | integer | Recent failures per 1000 recent samples. |
| `max_latency_ms` | integer | Worst endpoint max latency. |
| `worst_endpoint` | string or null | Endpoint responsible for `max_latency_ms`. |
| `last_error` | string or null | Most recent retained endpoint error while aggregating. |

Peer-sync failures are categorized into stable runtime buckets:

- `tls_trust`
- `tls_handshake`
- `auth_policy`
- `transport_connectivity`
- `peer_sync`

These categories now come from typed node and transport errors wherever Orion owns the error
surface. Free-text fallback is intentionally limited to external boundary cases where the upstream
HTTP transport layer still exposes only an unstructured message, such as raw TLS failures or
untyped request failures.

## Queue Pressure

Queue pressure is currently represented through:

- `client_sessions.queued_client_events` in the observability snapshot
- `persistence.worker_queue_capacity` plus queue-send wait, reply wait, and operation duration counters in the observability snapshot
- `audit_log_queue_capacity` in the observability snapshot
- `audit_log_backpressure_mode` in the observability snapshot
- `audit_log_dropped_records` in the observability snapshot when the audit overload policy drops
  events
- sampled runtime warnings when the audit-log queue is full and the overload policy is
  `drop_newest`

This means the node exposes backlog or saturation context for client streams, persistence worker
enqueue pressure, and audit-log overload handling.

Audit-log overload handling is runtime configurable through
`ORION_NODE_AUDIT_LOG_OVERLOAD_POLICY`:

- `drop_newest`: do not block the request path when the audit queue is saturated; count dropped
  records instead
- `block`: preserve the previous blocking behavior and wait for queue capacity

## Troubleshooting Queue Pressure

Use the observability snapshot as the first pass when local control or persistence appears slow:

- If `persistence.worker_queue_wait_count` and `persistence.worker_queue_wait_ms_max` are
  climbing, callers are waiting to enqueue persistence work and the worker queue is saturated.
- If `persistence.worker_reply_wait_count` or `persistence.worker_operation_ms_max` is climbing
  while queue wait stays flat, the durable operation itself is slow rather than the queue send.
- Compare `persistence.worker_queue_capacity` against observed wait growth before raising the
  capacity. A larger queue can smooth bursts, but it also hides slower durable storage.
- If `audit_log_backpressure_mode=drop_newest` and `audit_log_dropped_records` is increasing, the
  node is protecting request latency by shedding audit events. Treat that as an operator signal to
  reduce audit volume or speed up the underlying filesystem.
- If client-facing control requests are slow while persistence wait counters remain low, the
  remaining likely causes are synchronous local control handling or executor/provider work in the
  reconcile loop rather than worker-queue saturation.

## Troubleshooting Rejections And Transport Failures

Use one observability snapshot to separate malformed input, rejected requests, TLS failures, and
rate limiting:

- `transport.http_malformed_input_count` and `transport.ipc_malformed_input_count` indicate decode
  or protocol-shape failures before a request becomes a valid control message.
- `transport.http_tls_failures` and `transport.http_tls_client_auth_failures` isolate TLS setup or
  client-auth failures from application-level rejections.
- `client_sessions.rate_limited_total` plus recent `client_rate_limited` events indicate local
  control clients exceeding `ORION_NODE_LOCAL_RATE_LIMIT_MAX_MESSAGES`.
- Communication endpoint `failures_by_kind` uses structured buckets where Orion owns the error
  type. Remaining `unknown` failures should be treated as boundary errors that need their
  `last_error` text inspected.
- Application-level `Rejected` responses are valid control messages. They are visible as sent
  responses on the relevant local IPC or HTTP communication endpoint, while malformed or failed
  transport attempts increment the transport/error counters above.

## Peer Sync and Replay Coverage

The observability snapshot already includes dedicated operation metrics for:

- replay
- peer sync
- reconcile
- mutation apply

The readiness and health snapshots also surface peer-sync and replay state in their top-level
status and reasons.

## Peer Sync Failure Categories

Use peer-sync category together with the troubleshooting hint to narrow runtime failures quickly:

- `tls_trust`: trust enrollment or advertised transport binding is wrong for the peer identity.
- `tls_handshake`: TLS mode, presented certificate, or remote trust configuration is mismatched.
- `auth_policy`: the peer is authenticated but not allowed, revoked, or missing required auth
  policy state.
- `transport_connectivity`: the remote node is unreachable, timed out, or refused the connection.
- `peer_sync`: the failure is still peer-sync related but does not fit the narrower typed buckets.
