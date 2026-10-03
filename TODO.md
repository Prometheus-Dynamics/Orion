# TODO

Tracks the Orion ↔ HeliOS integration and appliance hardening work. See
[CHANGELOG.md](CHANGELOG.md) for the user-facing summary of what landed.

## Done

### HeliOS integration

- [x] IPC-only node profile: `ORION_NODE_HTTP_ADDR=off`, `ORION_NODE_RUNTIME_WORKER_THREADS`,
      `ORION_NODE_RUNTIME_MAX_BLOCKING_THREADS` ([docs/node-env.md](docs/node-env.md), "Single-node appliance profile").
- [x] `transport-http` is an optional (default-on) node feature; `--no-default-features` builds an
      IPC-only node (~2.5 MB vs ~5.6 MB, 74 vs 166 crates, no axum/hyper/rustls/quinn).
- [x] Latest-value fd channel: `UnixFdLatestServer` / `UnixFdLatestClient` in `orion-transport-ipc`.
- [x] Custom resource endpoint schemes: `ResourceEndpoint::Custom` + `CustomEndpointScheme`.
- [x] Consumer client API: `orion_client::prelude`, `AssignedWorkload`, `BoundResource`,
      `watch_assigned_workloads`.
- [x] Memory/backlog diagnostics: `resource_usage` observability section, `orionctl get memory`,
      Prometheus families.
- [x] Appliance memory soak (`crates/node/tests/appliance_memory_soak.rs`, nightly in `ci-soak.yml`).

### Efficiency

- [x] Event-driven reconcile loop with `ORION_NODE_RECONCILE_BACKSTOP_MS` (default 5000); soak CPU
      17.8% → 8.9% of a core at equal throughput.
- [x] Successful reconciles log at debug and only record an event when something changed.
- [x] Opt-in `alloc-jemalloc` / `alloc-mimalloc` features (measured; glibc + `MALLOC_ARENA_MAX=2` stays
      the recommendation).

### Compatibility and CI

- [x] Control-protocol version preamble on IPC and HTTP (`CONTROL_PROTOCOL_VERSION = 2`) with clear
      mismatch errors and a layout-fingerprint guard test ([docs/protocol-compatibility.md](docs/protocol-compatibility.md)).
- [x] CI pinned to toolchain 1.94.0; node feature matrix and allocator-feature jobs added.
- [x] Oversized files split; `scripts/check-file-sizes.sh` is clean.

### Bugs fixed (each with a regression test)

- [x] `send_unix_fd_frame_async` / `recv_unix_fd_frame_async` busy-spun on idle connections.
- [x] Background loops busy-spun when their `ReconcileLoopHandle` was dropped.
- [x] Executor/provider subscriptions dropped all but the first event of a batched frame.
- [x] Provider subscription desynced when its lease and state streams raced (non-cancel-safe reads).
- [x] Node IPC stream server desynced when a heartbeat tick interrupted a partial frame.
- [x] IPC server ignored shutdown while the connection limit was saturated.
- [x] Persistence failed when a mutation raced the desired-snapshot capture.
- [x] Lock-order deadlock between peer-sync responses and desired-state commits.
- [x] Order-dependent rustls provider panic in HTTP tests; peer-sync timing race in `process_http`.

## Open

### Needs the device (HeliOS CM5)

- [ ] Run the appliance soak with a release aarch64 build on the CM5. x86 release builds plateau at
      3–5 MiB PSS, so the observed ~42 MiB is not reproduced off-device.
- [ ] Check transparent huge pages on the device (prime suspect for the anonymous-memory gap):
      `/sys/kernel/mm/transparent_hugepage/enabled`, `getconf PAGESIZE`, and `AnonHugePages` in
      `/proc/$(pidof orion-node)/smaps_rollup`. If large, set THP to `madvise` in the Gaia image.
- [ ] Verify an aarch64 release link locally (missing aarch64 `libgcc_s` in the installed sysroot) or
      via `cross`.

### CI

- [ ] Watch the first GitHub run of the new jobs: node feature matrix, IPC-only tests, allocator
      features with the aarch64 cross-build, appliance soak.
- [ ] Timing-sensitive tests (`control_queries_remain_fast_*`,
      `concurrent_peer_sync_*_remains_responsive`, `audit_log_drops_newest_when_queue_is_full`) can
      fail on heavily loaded machines; relax or restructure if they flake in CI.

### Release

- [ ] Tag a release so downstreams can pin by tag instead of `rev` (version stays 1.0.0 until first ship).

### HeliOS side (tracked here for coordination; changes live in the HeliOS repo)

- [ ] Pin `orion` by `rev`/tag for both the backend crates and the Gaia `orion-node` artifact (same rev:
      the control-protocol layout must match).
- [ ] Build `orion-node` for the image with `--no-default-features`; set `ORION_NODE_HTTP_ADDR=off`,
      `MALLOC_ARENA_MAX=2`, worker threads, and lower history/queue caps in the systemd unit.
- [ ] Replace hand-rolled frame-lease servers with `UnixFdLatestServer`/`UnixFdLatestClient`, and the
      `shm://` metadata file (per-frame write) with a typed custom endpoint.
- [ ] Use `watch_assigned_workloads` in the engine instead of its own retry loop.
- [ ] Move FrameLease descriptor/backing types into Styx.

### Nice to have

- [ ] Optional blocking client for the fd latest-value channel.
- [ ] Single multiplexed local IPC socket instead of separate unary and stream sockets.
- [ ] Field-reorder/same-size type changes are not caught by the protocol layout fingerprint; consider
      hashing archived fixture bytes as well.
