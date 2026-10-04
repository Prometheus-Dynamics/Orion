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

### no_std / MCU

- [x] `orion-core`, `orion-data-plane`, `orion-control-plane`, `orion-auth`, `orion-runtime`,
      `orion-cluster` build `no_std` + `alloc` (default `std` feature; `#![cfg_attr(not(feature =
      "std"), no_std)]`, `core::`/`alloc::` paths). The `orion` facade's model features work with
      `default-features = false`.
- [x] Only host-only bits are gated behind `std`: Prometheus metrics export (env-driven config) and
      the filesystem helpers on `SharedMemoryEndpoint`/`UnixEndpoint`. Typed config decoding keeps
      `serde_path_to_error` field paths without `std`.
- [x] Workspace deps use `default-features = false` (model crates, serde, serde_json, thiserror,
      rkyv, ed25519-dalek); std crates opt into `std` explicitly. rkyv raised to 0.8.16 for 32-bit
      `pointer_width_64` support; wire layout and `CONTROL_PROTOCOL_VERSION` unchanged.
- [x] Canonical-encoding fixture test (`crates/auth/tests/canonical_encoding.rs`) runs in std and
      no_std builds and checks byte-identical archives (also covers the field-reorder gap of the
      layout fingerprint for the messages it encodes).
- [x] `scripts/check-no-std.sh` + CI `no-std` job (thumbv7em-none-eabihf, riscv32imac-unknown-none-elf;
      thumbv8m.main-none-eabihf locally).
- [x] `orion-link` framing layers: CRC-32C frames, COBS streams, CAN / CAN FD segmentation; no
      allocator, panic-free.
- [x] `orion-link` `alloc`: postcard link messages with stable kind numbers and a wire fingerprint
      fixture (`crates/link/tests/link_encoding.rs`); sans-IO `DeviceSession<T, RX, TX>` over
      `Stream` or `Packet` with fixed buffers (handshake, reliable newest-snapshot publishing,
      heartbeat, host-loss detection, leases).
- [x] `orion-link` `std`: sans-IO `HostSession` / `HostBus` (allowlist, acks, lease piggyback,
      device-loss detection, multi-device CAN demux).
- [x] Example MCU firmware crate outside the workspace (`examples/mcu-template`): links the device
      session, postcard, and the model crates with a real allocator into a bare-metal staticlib (CI
      builds it for thumbv7em-none-eabihf and riscv32imac-unknown-none-elf; `scripts/mcu-size.sh`
      reports flash/RAM).
- [x] Node gateway (`orion-node` `link-gateway` feature): `ORION_NODE_LINKS` serial and SocketCAN
      links (termios / `PF_CAN` over `libc` + `AsyncFd`), `HostSession` / `HostBus` driven per link,
      `ProviderState` applied through the IPC provider path (gateway-owned `node_id`, provider and
      resource ownership checks), resources unavailable on `DeviceLost` and restored on reconnect,
      leases refreshed on desired commits, `NodeApp::link_status()` counters, pty and in-memory CAN
      tests, `link_device_sim` example.
- [ ] Gateway follow-ups: persist which providers belong to link devices, so a node that crashed
      (no graceful shutdown) can mark them unavailable at startup instead of waiting for the device;
      expose `link_status` over the control protocol (`orionctl get links`) with the next
      `CONTROL_PROTOCOL_VERSION` bump; run the `vcan0` test in CI.
- [ ] Executor role on the link (`ExecutorState` / `Workloads`, kinds reserved) and the volatile
      `Status` lane.
- [ ] Real hardware bring-up: run the template and the gateway on real hardware (one Cortex-M and
      one RISC-V board, a USB-serial port and a SocketCAN adapter) and record round-trip timing at
      115200 baud and on classic CAN.

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

### Decided model, not yet implemented

Decisions from 2026-10-03, prompted by HeliOS multi-camera design questions. Orion stays generic;
none of these are HeliOS-specific features.

- [ ] **Durable vs volatile observed state.** Records hold durable facts only (existence,
      health/availability, workload phase).
  - [ ] Observed-state persistence is coalesced: at most one write per configurable interval, flushed
        on shutdown. Today every provider or executor state change does an fsynced rewrite
        (`apply_*_state_update` → `persist_state`).
  - [ ] New volatile status lane: latest value per (subject, key), in memory only, never persisted,
        with a TTL, watchable over client streams and queryable via `orionctl`.
  - [ ] Bulk and high-rate data (frames, detections) stays out of Orion and uses resource endpoints.
- [ ] **Cross-node binding.** A workload may bind another node's resource. Orion resolves,
      authorizes and leases it; bytes flow over the resource's own endpoint. Orion's generic data
      plane (`RemoteBinding`, TCP/QUIC frames) is not on the main path.
- [ ] **Discovery.** mDNS for discovery only. Trust stays enrollment-based (ed25519 peer keys or a
      shared enrollment key); discovered peers are never trusted automatically.
- [ ] **Placement.** Node labels plus workload constraints (node selector, co-locate with resource X,
      any eligible node), with a deterministic leaderless choice, owned by `orion-cluster`
      (`ClusterCoordinator` is currently unused).
- [ ] **Timebase.** Nodes publish their clock source and sync state (PTP/chrony, offset estimate) as an
      observed node fact. Orion does not discipline clocks; producers timestamp in a declared
      timebase.
- [x] **Lighter peer sync.** Peer sync over a transport lighter than full HTTP so IPC-only builds
      can cluster: the `peer-tcp` feature (`orion+tcp://` peers, `ORION_NODE_PEER_ADDR`) with
      signed requests and responses; the sync engine is transport-independent
      (`PeerSyncTransport`). See `docs/peer-sync.md`.
- [x] **Per-object conflict resolution** for desired state using a hybrid logical clock (HLC):
      per-object stamps and tombstones, last writer wins, bounded drift, tombstone retention, and
      migration of older state directories. See `docs/peer-sync.md`.
  - [ ] Tombstone retention is time-based only; a node offline longer than
        `ORION_NODE_TOMBSTONE_RETENTION_MS` can resurrect deleted objects. Consider refusing to
        sync stale nodes (last successful sync older than the retention) until they are reset.
- [ ] **Remove `ResourceOwnershipMode::ExclusiveOwnerPublishesDerived`.** It is enforced exactly
      like `Exclusive` and only appears in two client examples.

### Nice to have

- [ ] Optional blocking client for the fd latest-value channel.
- [ ] Single multiplexed local IPC socket instead of separate unary and stream sockets.
- [ ] Field-reorder/same-size type changes are not caught by the protocol layout fingerprint; consider
      hashing archived fixture bytes as well. (Partly covered: `crates/auth/tests/canonical_encoding.rs`
      pins the bytes of representative control/auth messages; extend it to the remaining types.)
