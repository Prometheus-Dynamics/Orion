# Changelog

All notable changes to this workspace should be documented in this file.

The format is based on Keep a Changelog and this project follows Semantic Versioning.

## [Unreleased]

### Added

- `ORION_NODE_HTTP_ADDR=off` runs `orion-node` without the HTTP control listener for IPC-only appliances.
- `ORION_NODE_RUNTIME_WORKER_THREADS` and `ORION_NODE_RUNTIME_MAX_BLOCKING_THREADS` size the node's Tokio runtime.
- Single-node appliance profile guidance in `docs/node-env.md`.
- `ResourceEndpoint::Custom` accepts any valid URI scheme (for example `styx-frame-lease+unix://`), with `CustomEndpointScheme` for typed downstream endpoints and `Display`/`FromStr` round-tripping.
- `UnixFdLatestServer` / `UnixFdLatestClient` in `orion-transport-ipc`: a bounded latest-value channel that hands out dup'd fds (for example dmabuf frame leases) with sequence waits, max age, and max clients.
- `resource_usage` section in node observability snapshots (process RSS/PSS, state record counts, mutation history vs caps, local stream backlog, worker queue depth, registry sizes), `orionctl get memory`, and matching Prometheus families.
- `orion_client::prelude` plus consumer views: `AssignedWorkload`, `BoundResource`, `assigned_workloads`, `resources_bound_to`, and `LocalExecutorService::watch_assigned_workloads`.

### Changed

- `ResourceEndpoint` and `ResourceEndpointError` are now `#[non_exhaustive]`; unknown but valid schemes parse as `Custom` instead of failing with `UnsupportedScheme`.

- Snapshot additions change the rkyv control-protocol layout, so `orionctl` and `orion-node` must be upgraded together.

### Fixed

- `send_unix_fd_frame_async` / `recv_unix_fd_frame_async` no longer spin at 100% CPU on idle connections; readiness is now cleared through `try_io`.
- Background loops no longer busy-spin when their `ReconcileLoopHandle` is dropped without shutdown (for example after a startup error).
- Executor and provider subscriptions no longer drop all but the first matching event of a batched `ClientEvents` frame.

## [1.0.0] - 2026-04-19

- Standardized the workspace layout, docs, CI, linting, and helper scripts.
- Added repo-level testing guidance for default, Docker, perf, and soak validation surfaces.
- Added `scripts/check-file-sizes.sh`, `scripts/ci.sh`, and `scripts/repo-clean.sh`.
- Preserved the richer Orion-specific operational and performance workflow split.
