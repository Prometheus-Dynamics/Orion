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
- [x] Slim `orionctl`: `orion-transport-http` split into `client` / `server` features; `orionctl`
      features `http`, `yaml`, `toml` (default on). IPC + JSON build is 1.80 MiB / 57 crates vs
      4.38 MiB / 139 crates default (2.65 MiB with YAML + TOML).

### Efficiency

- [x] Event-driven reconcile loop with `ORION_NODE_RECONCILE_BACKSTOP_MS` (default 5000); soak CPU
      17.8% → 8.9% of a core at equal throughput.
- [x] Successful reconciles log at debug and only record an event when something changed.
- [x] Opt-in `alloc-jemalloc` / `alloc-mimalloc` features (measured; glibc + `MALLOC_ARENA_MAX=2` stays
      the recommendation).

### Packaging (owned by Orion)

- [x] `systemd-notify` node feature: `READY=1` after every listener, `STATUS=`, `STOPPING=1`, and a
      watchdog tied to reconcile-loop progress; no libsystemd. `orion-node` handles `SIGTERM`.
- [x] `packaging/`: systemd unit, environment file (appliance profile), sysusers entry, Buildroot
      users table, preset, and the importable Gaia layer `packaging/gaia/orion-node.toml`
      ([packaging/README.md](packaging/README.md)). Images such as HeliOS import the layer instead of
      carrying their own orion-node artifact, unit and env file (HeliOS side tracked below).
- [x] aarch64 release link verified locally (Fedora aarch64 glibc sysroot, LLVM libunwind standing
      in for `libgcc_s`): 3.07 MiB stripped appliance build, runs under qemu-user, 64 KiB `PT_LOAD`
      alignment pinned by `.cargo/config.toml` for 16 KiB page kernels. CI cross-links it and checks
      the alignment (`appliance-aarch64` job).

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
      session into a bare-metal staticlib (CI builds it for thumbv6m, thumbv7em, riscv32imc, and
      riscv32imac; `scripts/mcu-size.sh` reports flash/RAM).
- [x] `orion-link` minimal device path (`device`): hand-written postcard-compatible codec with
      borrowed views (byte-identical to the records, checked against the fixture and by property
      tests), no allocator, no `core::fmt`, no atomics; the template's UART device is about 8 KB
      flash and 0.9 KB RAM on Cortex-M0+, with a CI size budget.
- [ ] Go smaller: drop the `memcpy` the CAN reassembler pulls in (1 KB of compiler-builtins on
      Cortex-M0+), make the status buffer optional for devices that never publish status (`TX`
      bytes of RAM), and put `DeviceStats` behind a feature (56 bytes of RAM, about 100 bytes of
      flash).
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
- [ ] Install the packaged service on the CM5 image (16 KiB pages) and confirm `READY=1`, watchdog
      pings and a watchdog restart (`kill -STOP $(pidof orion-node)`) under the real systemd.

### CI

- [ ] Watch the first GitHub run of the new jobs: node feature matrix, IPC-only tests, allocator
      features with the aarch64 cross-build, appliance soak, `appliance-aarch64` (cross-link,
      alignment check, `systemd-analyze verify`).
- [ ] Build the Gaia layer's docker image (`packaging/gaia/docker/aarch64-cross.Dockerfile`) once in
      a real Gaia run; it was only validated with `gaia validate` / `gaia plan`, not built.
- [ ] Timing-sensitive tests (`control_queries_remain_fast_*`,
      `concurrent_peer_sync_*_remains_responsive`, `audit_log_drops_newest_when_queue_is_full`) can
      fail on heavily loaded machines; relax or restructure if they flake in CI.

### Release

- [ ] Tag a release so downstreams can pin by tag instead of `rev` (version stays 1.0.0 until first ship).

### HeliOS side (tracked here for coordination; changes live in the HeliOS repo)

- [ ] Pin `orion` by `rev`/tag for both the backend crates and the Gaia `orion-node` artifact (same rev:
      the control-protocol layout must match).
- [ ] Import `packaging/gaia/orion-node.toml` from the pinned Orion source (source id `orion`) and drop
      the HeliOS-side `orion-node` artifact, install, unit and env file; keep device-specific
      settings in a later layer (its own `orion-node-env` file or unit drop-ins). Create the `orion`
      user at build time (squashfs root: add `packaging/buildroot/orion-users.table` to
      `BR2_ROOTFS_USERS_TABLES`) or override `User=root` in a drop-in, and give IPC clients
      `Group=orion` with `ORION_NODE_LOCAL_AUTH=same-user-or-group`.
- [ ] Replace hand-rolled frame-lease servers with `UnixFdLatestServer`/`UnixFdLatestClient`, and the
      `shm://` metadata file (per-frame write) with a typed custom endpoint.
- [ ] Use `watch_assigned_workloads` in the engine instead of its own retry loop.
- [ ] Move FrameLease descriptor/backing types into Styx.

### Decided model, not yet implemented

Decisions from 2026-10-03, prompted by HeliOS multi-camera design questions. Orion stays generic;
none of these are HeliOS-specific features.

- [ ] **Durable vs volatile observed state.** Records hold durable facts only (existence,
      health/availability, workload phase).
  - [x] Observed-state persistence is coalesced: at most one write per configurable interval
        (`ORION_NODE_OBSERVED_PERSIST_INTERVAL_MS`), flushed on shutdown and carried by every
        desired-state commit.
  - [x] New volatile status lane: latest value per (subject, key), in memory only, never persisted,
        with a TTL, watchable over client streams and queryable via `orionctl get status`; link
        devices publish with the `Status` kind. Not replicated to peers yet.
  - [ ] Bulk and high-rate data (frames, detections) stays out of Orion and uses resource endpoints.
- [x] **Cross-node binding.** A workload may bind another node's resource. Orion resolves,
      authorizes and leases it; bytes flow over the resource's own endpoint. Orion's generic data
      plane (`RemoteBinding`, TCP/QUIC frames) is not on the main path. (Leases with per-holder
      entries in the HLC-merged desired state, owner-side capacity arbitration, `RemoteBinding`
      endpoints/availability on `ResourceBinding`, unavailable on owner loss, re-resolved after
      the grace period. See `docs/placement.md`.)
  - [ ] Replicate node liveness or relay observed slices so nodes that are not directly peered can
        judge each other (today placement and binding need direct peering).
  - [ ] Fencing for failover: a partitioned former assignee keeps running until it hears of the
        new assignment.
- [x] **Discovery.** mDNS for discovery only. Trust stays enrollment-based (ed25519 peer keys or a
      shared enrollment key); discovered peers are never trusted automatically. (Feature
      `discovery-mdns`: `_orion._tcp` with node id, public key, ports, protocol version and cluster
      in TXT; `orionctl get discovered-peers`, `orionctl peers enroll <node-id>` with fingerprint
      confirmation, `orionctl peers remove`; shared-key HMAC + ed25519 handshake over `orion+tcp`;
      see [docs/discovery.md](docs/discovery.md).)
  - [ ] Initiate the shared-key handshake over `https://` peers too (served on both transports,
        initiated over `orion+tcp` only).
- [x] **Placement.** Node labels plus workload constraints (node selector, co-locate with resource X,
      any eligible node), with a deterministic leaderless choice, owned by `orion-cluster`
      (`ClusterCoordinator`: rendezvous hashing over eligible nodes, chosen node writes its own
      assignment, grace-period hysteresis, explicit assignments authoritative;
      `ORION_NODE_LABELS`, `ORION_NODE_LIVENESS_TIMEOUT_MS`, `ORION_NODE_PLACEMENT_GRACE_MS`. See
      `docs/placement.md`.)
  - [ ] Capacity-aware placement (resource counts, load) instead of hash spreading only.
- [x] **Timebase.** Nodes publish their clock source and sync state (PTP/chrony, offset estimate) as an
      observed node fact. Orion does not discipline clocks; producers timestamp in a declared
      timebase. (`NodeRecord::clock` / `NodeClockFacts` from read-only `adjtimex`, declared with
      `ORION_NODE_CLOCK_SOURCE` and `ORION_NODE_TIMEBASE`; each node now pushes its own observed
      node record to its peers at the end of every sync round, see `docs/peer-sync.md`.)
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
- [x] **Remove `ResourceOwnershipMode::ExclusiveOwnerPublishesDerived`.** It is enforced exactly
      like `Exclusive` and only appears in two client examples.

### Decisions (2026-10-05)

- Frame leases stay in the producer (Styx `FrameSocket` + its public lease codec). Orion carries
  only discovery and typed endpoint records (`ResourceEndpoint::Custom`); `UnixFdLatest*` stays a
  generic latest-value fd channel without hold/release, because buffer-pool reuse and back-pressure
  are producer-specific.
- Orion owns the generic systemd unit and an importable Gaia fragment (`packaging/`); images import
  it rather than each packaging `orion-node` separately.

### Update / recovery

Design only so far: [docs/update-recovery.md](docs/update-recovery.md). Orion carries update
intents and progress; the writer (A/B slots, verification, bootloader) is an external device
manager, which keeps out-of-band paths that never depend on Orion. Milestones in order:

- [ ] **U0 Prerequisites** (parallel work): generic action mechanism (`ActionRequest` /
      `ActionResult`); host facts with OS / image version on the observed `NodeRecord`.
- [ ] **U1 Supervision**: feature `systemd` with `READY=1`, `WATCHDOG=1` from the reconcile loop and
      `STATUS=`; example unit (watchdog, restart limits, `OnFailure=` safe mode); wedged-loop test.
- [ ] **U2 Safe mode**: `ORION_NODE_SAFE_MODE`; quarantine undecodable or newer-format state instead
      of failing startup; no workloads, receive-only sync, health reason, `orion_safe_mode` metric.
- [ ] **U3 Data model**: `UpdateIntentRecord` desired section and `UpdateStatusRecord` in the
      observed slice (protocol bump, batched with other layout changes); `orionctl get updates`;
      requester authorization (`ORION_NODE_UPDATE_REQUESTERS`); audit records.
- [ ] **U4 Delivery and resume**: deliver intents as actions keyed by
      `(intent, version, generation)`, re-delivery with backoff, immediate persistence of phase
      transitions, status-lane progress keys, drain integration, fake device manager example and
      kill-at-every-phase tests.
- [ ] **U5 External rollout controller**: `RolloutRecord` with waves, `max_unavailable`, health
      gates and monotonic halt; `orionctl rollout`.
- [ ] **U6 Cross-version rollouts**: frozen rollout beacon readable across one protocol bump; keep
      the legacy state-dir copy until commit and use it on downgrade; N / N+1 rollout test.
- [ ] **U7 Leaderless rollouts** in `orion-cluster`: rendezvous wave order, own-intent writes,
      convergent halt, property tests.
- [ ] **U8 MCU firmware over the link**: reserve link kinds `0x20`–`0x2F`; `DeviceInfo`, chunked
      transfer with CRC and resume (`xfer` feature), A/B trial + confirm or bootloader handoff,
      `link.release` / `link.attach` actions, simulator test, size budget.
- [ ] **U9 Hardware validation**: real A/B writer with power-cut and watchdog fault injection; an
      MCU with an A/B bootloader over UART and CAN.

### Remote operator client (next, after protocol v4)

- [ ] Embeddable remote operator client (`orion-client` `remote` feature or `orion-remote` crate)
      for desktop/fleet tools: ed25519 operator identity, enrollment (operator approval or shared
      key), signed `orion+tcp` requests; list/watch node records and host facts, query/watch status
      lanes, send `ActionRequest` / watch `ActionResult`, optional mDNS discovery. Nodes treat it as
      an operator principal (no desired-state replica, no placement/liveness role), with
      per-identity action authorization. First consumer: Atlas (`atlas-driver-orion`).

### Nice to have

- [ ] Optional blocking client for the fd latest-value channel.
- [ ] Single multiplexed local IPC socket instead of separate unary and stream sockets.
- [ ] Default `orionctl` is still 4.38 MiB, almost all reqwest + rustls + hyper client for `--http`.
      A smaller HTTP/1.1 client (hyper-util client or a hand-rolled one over tokio-rustls) could
      replace reqwest if the CLI size matters; trimming clap's `color` / `suggestions` would also
      save a little but changes help/error output.
- [ ] Field-reorder/same-size type changes are not caught by the protocol layout fingerprint; consider
      hashing archived fixture bytes as well. (Partly covered: `crates/auth/tests/canonical_encoding.rs`
      pins the bytes of representative control/auth messages; extend it to the remaining types.)
