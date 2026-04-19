# `orion-node` Environment Reference

This document covers the runtime environment variables consumed by `orion-node`.
Startup now prefers typed parsing through `NodeProcessConfig::try_from_env()` and
`NodeConfig::try_from_env()`. Invalid typed values fail startup with `NodeError::Config`
instead of silently falling back.

## Identity and Bindings

| Variable | Default | Valid values | Failure behavior |
| --- | --- | --- | --- |
| `ORION_NODE_ID` | `node.local` | Any valid Unicode string | Invalid Unicode fails startup. |
| `ORION_NODE_HTTP_ADDR` | `127.0.0.1:9100` | Socket address like `127.0.0.1:9100` | Invalid address fails startup. |
| `ORION_NODE_IPC_SOCKET` | `${TMPDIR}/orion-<node-id>-control.sock` | Filesystem path | Invalid Unicode fails startup. |
| `ORION_NODE_IPC_STREAM_SOCKET` | `${TMPDIR}/orion-<node-id>-control-stream.sock` | Filesystem path | Invalid Unicode fails startup. |
| `ORION_NODE_HTTP_PROBE_ADDR` | unset | Socket address | Invalid address fails startup. |
| `ORION_NODE_RECONCILE_MS` | `250` | Integer milliseconds, minimum effective value `1` | Invalid integer fails startup. |

## Peer and Auth Controls

| Variable | Default | Valid values | Failure behavior |
| --- | --- | --- | --- |
| `ORION_NODE_PEERS` | unset | Comma-separated `node-id=http://host:port` entries, optional `|ca=/path` and trusted key segments | Invalid entry format fails startup. |
| `ORION_NODE_PEER_AUTH` | `optional` | `disabled`, `optional`, `required` | Invalid mode fails startup. |
| `ORION_NODE_PEER_SYNC_MODE` | `parallel` | `serial`, `parallel` | Invalid mode fails startup. |
| `ORION_NODE_PEER_SYNC_MAX_IN_FLIGHT` | `4` | Integer, minimum effective value `1` | Invalid integer fails startup. |
| `ORION_NODE_HTTP_MTLS` | `disabled` | `disabled`, `optional`, `required` | Invalid mode fails startup. |
| `ORION_NODE_LOCAL_AUTH` | `same-user` | `disabled`, `same-user`, `same-user-or-group` | Invalid mode fails startup. |

## Persistence and Logging

| Variable | Default | Valid values | Failure behavior |
| --- | --- | --- | --- |
| `ORION_NODE_STATE_DIR` | unset | Filesystem path | Invalid Unicode fails startup. |
| `ORION_NODE_AUDIT_LOG` | unset | Filesystem path | Invalid Unicode fails startup. |
| `ORION_NODE_SHUTDOWN_AFTER_INIT_MS` | unset | Integer milliseconds | Invalid integer fails startup. Intended for tests and controlled automation, not steady-state production. |

## HTTP TLS

| Variable | Default | Valid values | Failure behavior |
| --- | --- | --- | --- |
| `ORION_NODE_HTTP_TLS_CERT` | unset | Filesystem path | Invalid Unicode fails startup. |
| `ORION_NODE_HTTP_TLS_KEY` | unset | Filesystem path | Invalid Unicode fails startup. |
| `ORION_NODE_HTTP_TLS_AUTO` | `false` | `1`, `0`, `true`, `false`, `yes`, `no`, `on`, `off` | Invalid boolean fails startup. |

`ORION_NODE_HTTP_TLS_CERT` and `ORION_NODE_HTTP_TLS_KEY` must either both be set or both be unset.

## Runtime Tuning

All of the following accept integer values. Invalid integers fail startup. Queue and count values
are normalized to a minimum effective value of `1`. Duration values are interpreted as milliseconds
and normalized to a minimum effective value of `1ms`.

Programmatic callers can tune the same surface through `NodeRuntimeTuning` fluent setters and
apply it with `NodeConfig::with_runtime_tuning(...)` or `NodeConfig::with_runtime_tuning_mut(...)`.

| Variable | Default | Notes |
| --- | --- | --- |
| `ORION_NODE_MAX_MUTATION_HISTORY` | `256` | Max mutation history batches retained in memory. |
| `ORION_NODE_MAX_MUTATION_HISTORY_BYTES` | `1048576` | Max retained mutation history bytes. |
| `ORION_NODE_SNAPSHOT_REWRITE_CADENCE` | `1` | Snapshot rewrite cadence in persist cycles. |
| `ORION_NODE_PEER_SYNC_BACKOFF_BASE_MS` | `250` | Base peer sync retry backoff. |
| `ORION_NODE_PEER_SYNC_BACKOFF_MAX_MS` | `5000` | Max peer sync retry backoff. |
| `ORION_NODE_PEER_SYNC_BACKOFF_JITTER_MS` | `150` | Peer sync retry jitter. |
| `ORION_NODE_PEER_SYNC_SMALL_CLUSTER_THRESHOLD` | `4` | Cluster size threshold for small-cluster parallelism. |
| `ORION_NODE_PEER_SYNC_SMALL_CLUSTER_CAP` | `4` | Max in-flight peers for small clusters. |
| `ORION_NODE_PEER_SYNC_LARGE_CLUSTER_CAP` | `3` | Max in-flight peers for larger clusters. |
| `ORION_NODE_PEER_SYNC_NO_STAGGER_THRESHOLD` | `4` | Cluster size threshold before spawn staggering applies. |
| `ORION_NODE_PEER_SYNC_SPAWN_STAGGER_STEP_MS` | `5` | Per-peer stagger step in parallel sync. |
| `ORION_NODE_PEER_SYNC_SPAWN_STAGGER_MAX_MS` | `20` | Upper bound for stagger delay. |
| `ORION_NODE_PEER_SYNC_FOLLOWUP_STAGGER_MS` | `5` | Delay before follow-up sync fanout. |
| `ORION_NODE_LOCAL_RATE_LIMIT_WINDOW_MS` | `1000` | Local control-plane rate-limit window. |
| `ORION_NODE_LOCAL_RATE_LIMIT_MAX_MESSAGES` | `256` | Max local messages per rate-limit window. |
| `ORION_NODE_LOCAL_SESSION_TTL_MS` | `300000` | Local session TTL. |
| `ORION_NODE_LOCAL_STREAM_SEND_QUEUE_CAPACITY` | `64` | Per-stream send queue capacity. |
| `ORION_NODE_LOCAL_CLIENT_EVENT_QUEUE_LIMIT` | `256` | Local client event backlog limit. |
| `ORION_NODE_OBSERVABILITY_EVENT_LIMIT` | `128` | Retained observability events. |
| `ORION_NODE_PERSISTENCE_WORKER_QUEUE_CAPACITY` | `64` | Persistence worker queue capacity. |
| `ORION_NODE_AUTH_STATE_WORKER_QUEUE_CAPACITY` | `128` | Auth state worker queue capacity. |
| `ORION_NODE_AUDIT_LOG_QUEUE_CAPACITY` | `1024` | Audit log worker queue capacity. |
| `ORION_NODE_IPC_STREAM_HEARTBEAT_INTERVAL_MS` | `5000` | `50` in test builds. IPC stream heartbeat interval. |
| `ORION_NODE_IPC_STREAM_HEARTBEAT_TIMEOUT_MS` | `15000` | `125` in test builds. IPC stream heartbeat timeout. |
