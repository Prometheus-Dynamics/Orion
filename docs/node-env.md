# `orion-node` Environment Reference

This document covers the runtime environment variables consumed by `orion-node`.
Startup now prefers typed parsing through `NodeProcessConfig::try_from_env()` and
`NodeConfig::try_from_env()`. Invalid typed values fail startup with `NodeError::Config`
instead of silently falling back.

## Identity and Bindings

| Variable | Default | Valid values | Failure behavior |
| --- | --- | --- | --- |
| `ORION_NODE_ID` | `node.local` | Any valid Unicode string | Invalid Unicode fails startup. |
| `ORION_NODE_HTTP_ADDR` | `127.0.0.1:9100` | Socket address like `127.0.0.1:9100`, or `off` / `disabled` / `none` to skip the HTTP control listener | Invalid address fails startup. `off` combined with `ORION_NODE_PEERS` or HTTP TLS settings fails startup. |
| `ORION_NODE_IPC_SOCKET` | `${TMPDIR}/orion-<node-id>-control.sock` | Filesystem path | Invalid Unicode fails startup. |
| `ORION_NODE_IPC_STREAM_SOCKET` | `${TMPDIR}/orion-<node-id>-control-stream.sock` | Filesystem path | Invalid Unicode fails startup. |
| `ORION_NODE_HTTP_PROBE_ADDR` | unset | Socket address | Invalid address fails startup. |
| `ORION_NODE_RUNTIME_WORKER_THREADS` | unset (one per CPU core) | Positive integer | Zero or invalid integer fails startup. |
| `ORION_NODE_RUNTIME_MAX_BLOCKING_THREADS` | unset (Tokio default `512`) | Positive integer | Zero or invalid integer fails startup. |
| `ORION_NODE_RECONCILE_MS` | `250` | Integer milliseconds, minimum effective value `1` | Invalid integer fails startup. Minimum idle gap between reconcile passes (see below); also the peer sync interval. |

### Reconcile scheduling

The reconcile loop is event-driven. It runs one pass at startup and then sleeps until something
that can change the outcome of a pass is committed: a desired-state commit (local or remote
mutations, snapshot adoption, peer sync, lease or assignment changes), an observed-state merge,
provider or executor state published over local IPC, a maintenance change, or a newly registered
in-process integration. Wake-ups coalesce for up to 5ms so a burst of updates collapses into one
pass. A pass that dispatched commands to an in-process executor, or saw an in-process integration
snapshot change, schedules a follow-up pass so in-process integrations keep converging.

- `ORION_NODE_RECONCILE_MS` keeps its name and default but now means the minimum idle gap between
  the end of one pass and the start of the next. Under continuous load the loop therefore runs no
  more often than before; when idle it does not run at all until woken or the backstop fires.
- `ORION_NODE_RECONCILE_BACKSTOP_MS` (default `5000`, runtime tuning below) is the longest the loop
  stays idle before a periodic safety pass. It is clamped to at least `ORION_NODE_RECONCILE_MS`,
  so setting it at or below that value restores the previous fixed-interval polling.
- In-process integrations whose snapshots change outside Orion can call
  `NodeApp::request_reconcile()` to be picked up before the next backstop pass.
- While the loop runs, local IPC provider/executor state updates and applied mutations defer their
  follow-up reconcile to the loop instead of reconciling inline. Reconcile failures are then
  reported through reconcile metrics and the `reconcile failed` log instead of rejecting the
  already committed update. Without a running loop (embedded use) they still reconcile inline.

### Single-node appliance profile

A standalone node that only serves local IPC clients can shed most of its network surface:

- build with `cargo build -p orion-node --release --no-default-features` to drop the TCP and QUIC data-plane transports
- set `ORION_NODE_HTTP_ADDR=off` to skip the HTTP control listener (the optional probe listener on `ORION_NODE_HTTP_PROBE_ADDR` still works)
- set `ORION_NODE_RUNTIME_WORKER_THREADS=1` or `2` and lower `ORION_NODE_MAX_MUTATION_HISTORY*` and worker queue capacities to fit the device memory budget
- on glibc targets, `MALLOC_ARENA_MAX=2` limits per-thread malloc arenas, which otherwise dominate anonymous memory on small multi-core devices

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
| `ORION_NODE_TRANSPORT_MAX_PAYLOAD_BYTES` | `8388608` | Max inbound HTTP, IPC, TCP, and QUIC payload bytes before transport decode. |
| `ORION_NODE_TRANSPORT_IO_TIMEOUT_MS` | `5000` | Per-operation transport connect/read/write/handshake timeout. |
| `ORION_NODE_TRANSPORT_MAX_CONCURRENT_CONNECTIONS` | `1024` | Max in-flight accepted connections per managed transport listener. |
| `ORION_NODE_PERSISTENCE_WORKER_QUEUE_CAPACITY` | `64` | Persistence worker queue capacity. |
| `ORION_NODE_AUTH_STATE_WORKER_QUEUE_CAPACITY` | `128` | Auth state worker queue capacity. |
| `ORION_NODE_AUDIT_LOG_QUEUE_CAPACITY` | `1024` | Audit log worker queue capacity. |
| `ORION_NODE_RECONCILE_BACKSTOP_MS` | `5000` | Longest idle period before the event-driven reconcile loop runs a periodic backstop pass. Clamped to at least `ORION_NODE_RECONCILE_MS`. |
| `ORION_NODE_IPC_STREAM_HEARTBEAT_INTERVAL_MS` | `5000` | `50` in test builds. IPC stream heartbeat interval. |
| `ORION_NODE_IPC_STREAM_HEARTBEAT_TIMEOUT_MS` | `15000` | `125` in test builds. IPC stream heartbeat timeout. |
