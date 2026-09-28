# Changelog

All notable changes to this workspace should be documented in this file.

The format is based on Keep a Changelog and this project follows Semantic Versioning.

## [1.0.0] - 2026-04-19

- Standardized the workspace layout, docs, CI, linting, and helper scripts.
- Added repo-level testing guidance for default, Docker, perf, and soak validation surfaces.
- Added `scripts/check-file-sizes.sh`, `scripts/ci.sh`, and `scripts/repo-clean.sh`.
- Preserved the richer Orion-specific operational and performance workflow split.

### Added

- `ORION_NODE_HTTP_ADDR=off` runs `orion-node` without the HTTP control listener for IPC-only appliances.
- `ORION_NODE_RUNTIME_WORKER_THREADS` and `ORION_NODE_RUNTIME_MAX_BLOCKING_THREADS` size the node's Tokio runtime.
- Single-node appliance profile guidance in `docs/node-env.md`, and an `appliance_memory_soak` suite that checks memory slope and caps under provider/executor/mutation load.
- Opt-in `alloc-jemalloc` and `alloc-mimalloc` features that set the `orion-node` binary's global allocator (jemalloc wins if both are on), with allocator soak measurements in `docs/node-env.md`. The soak gains `ORION_SOAK_NODE_WORKER_THREADS` and `ORION_SOAK_NODE_BIN`.
- `ResourceEndpoint::Custom` accepts any valid URI scheme (for example `styx-frame-lease+unix://`), with `CustomEndpointScheme` for typed downstream endpoints and `Display`/`FromStr` round-tripping.
- `UnixFdLatestServer` / `UnixFdLatestClient` in `orion-transport-ipc`: a bounded latest-value channel that hands out dup'd fds (for example dmabuf frame leases) with sequence waits, max age, and max clients.
- `resource_usage` section in node observability snapshots (process RSS/PSS, state record counts, mutation history vs caps, local stream backlog, worker queue depth, registry sizes), `orionctl get memory`, and matching Prometheus families.
- `orion_client::prelude` plus consumer views: `AssignedWorkload`, `BoundResource`, `assigned_workloads`, `resources_bound_to`, and `LocalExecutorService::watch_assigned_workloads`.

### Changed

- The node reconcile loop is event-driven: it wakes (coalesced for up to 5ms) on desired-state commits, observed-state merges, provider/executor state published over IPC, maintenance changes, and integration registration, plus a periodic backstop (`ORION_NODE_RECONCILE_BACKSTOP_MS`, default 5000). `ORION_NODE_RECONCILE_MS` is now the minimum idle gap between passes. With the loop running, IPC provider/executor updates and applied mutations defer their reconcile to it.
- Successful reconcile passes log at `debug` and only record a recent observability event when they changed something; reconcile counters and latency still cover every pass.
- `ResourceEndpoint` and `ResourceEndpointError` are now `#[non_exhaustive]`; unknown but valid schemes parse as `Custom` instead of failing with `UnsupportedScheme`.
- Snapshot additions change the rkyv control-protocol layout, so `orionctl` and `orion-node` must be upgraded together.
- Explicit control-protocol versioning: `orion_core::CONTROL_PROTOCOL_VERSION` (now 2, for the `resource_usage` layout) is carried in a fixed 4-byte preamble on every local IPC message (unary and stream frames, including the `ClientHello`/`ClientWelcome` handshake) and in the `x-orion-control-protocol` HTTP header, and checked before any rkyv payload is decoded. Version skew now fails with `IpcTransportError`/`HttpTransportError`/`ClientError::ProtocolMismatch { local, remote }` telling operators to upgrade orionctl and orion-node together, instead of opaque rkyv decode errors. A layout fingerprint test (`crates/orion/tests/control_protocol_layout.rs`) forces a version bump on future layout changes. See `docs/protocol-compatibility.md`.
- CI workflows install the pinned 1.94.0 toolchain (matching `rust-toolchain.toml`) instead of `stable`.

### Fixed

- `send_unix_fd_frame_async` / `recv_unix_fd_frame_async` no longer spin at 100% CPU on idle connections; readiness is now cleared through `try_io`.
- Background loops no longer busy-spin when their `ReconcileLoopHandle` is dropped without shutdown (for example after a startup error).
- State persistence no longer fails with "cannot rewrite desired snapshot without encoded desired bytes" when a mutation races the snapshot capture; the rewrite is deferred to the next persist.
- Executor and provider subscriptions no longer drop all but the first matching event of a batched `ClientEvents` frame.
