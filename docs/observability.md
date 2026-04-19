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
- configured, ready, pending, and degraded peer counts
- effective peer-sync parallel in-flight cap
- operation metrics for replay, peer sync, reconcile, and mutation apply
- persistence worker queue capacity plus enqueue wait metrics
- audit-log queue capacity, backpressure mode, and dropped-record count
- client-session metrics including registered clients, live stream clients, and queued client events
- transport metrics including malformed input counts, HTTP request failures, TLS failures, and IPC reconnect count
- recent structured observability events

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
- `persistence.worker_queue_capacity` plus enqueue wait counters in the observability snapshot
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

- If `persistence.worker_enqueue_wait_count` and `persistence.worker_enqueue_wait_ms_max` are
  climbing, the persistence worker is the likely bottleneck rather than peer sync.
- Compare `persistence.worker_queue_capacity` against observed wait growth before raising the
  capacity. A larger queue can smooth bursts, but it also hides slower durable storage.
- If `audit_log_backpressure_mode=drop_newest` and `audit_log_dropped_records` is increasing, the
  node is protecting request latency by shedding audit events. Treat that as an operator signal to
  reduce audit volume or speed up the underlying filesystem.
- If client-facing control requests are slow while persistence wait counters remain low, the
  remaining likely causes are synchronous local control handling or executor/provider work in the
  reconcile loop rather than worker-queue saturation.

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
