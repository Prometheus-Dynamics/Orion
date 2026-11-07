# TODO

Tracks Orion's integration with its first consumers and appliance hardening work. See
[CHANGELOG.md](CHANGELOG.md) for the user-facing summary of what landed.

## Alpha (reference board)

The user, 2026-11-03: the next release is an alpha for the reference board hardware only. These come
first; the other items below are still needed, after it.

- [x] **aarch64 appliance soak** on the reference board (below, under freeze items). Curve in docs/node-env.md; 20 h, not 24.
- [x] **Unknown arguments start a node** (below): `orion-node --version` on the reference board. Done in 6bf46ce; on dev.
- [x] **Byte-bounded watch queues** (below): the reference board runs the consuming product's watchers. Done in 737579d; on dev. Byte total now in the usage snapshot and metrics (protocol 1).
- [ ] **Protocol 1 on the reference board.** Orion's side is done (protocol 1 is on dev). Waiting on
      the consumers' window: the device library's protocol 1 merge, the consuming products
      together, and the images' orion-node. Owner: the coordinator's integration order.

## Next release (1.0.0)

The release list: the 2026-10-27 feature freeze plus the hardware-support work added on 2026-10-30
(the last addition). Everything else stays under the headings below. Every item: what, why, done
when, blocked by.

### 1. Freeze items still open

- [x] **aarch64 appliance soak.** Why: the frozen soak item; orion-node runs on every board. Earlier
      data (2026-10-31, reference board #1, the packaged node under the device runtime's real load):
      7 days up, 0 restarts, PSS 9.8 MiB / RSS 11.3 MiB. Done: the release node's curve on the same
      board and load from 2026-11-02 11:55 UTC (board clock), every 10 min, in docs/node-env.md. 122
      samples over 20.0 h, stopped early by decision: 0 restarts, one process, 9 threads, resting PSS
      8.4 MiB from hour 4, one 0.15 MiB step at hour 17. It measures the board's pin, not protocol 1,
      whose node the board's consumers cannot talk to yet. Board files and timers removed.
- [x] **API dedup pass.** Why: the freeze rule for every repo. Done when: no duplicate public types,
      aliases or client methods remain across orion-client/control-plane/node, and docs match.
      Blocked by: nothing. Done: removed `RemoteOperator::action` (kept `query_action`), the
      `link_gateway::LinkStatus` alias, node's mirror `PeerSyncStatus` and `PeerTransportSecurityMode`
      and node's private `HttpMutualTlsMode` (all now the control-plane types); exported
      `NodeRuntimeTuning`/`AuditLogOverloadPolicy`; narrowed unreachable items to `pub(crate)`. Rules:
      one name per concept (the wire type in orion-control-plane wins over a mirror); same-name
      types with different meaning stay (`ClientSession`, `TcpEndpoint`, `ClientRole` re-exports);
      wire types are not renamed or changed; `pub` items that docs or the facade name stay public.
      Changes listed in CHANGELOG [Unreleased].
- [x] **Unknown arguments start a node.** Why: `orion-node --version` on a reference board (2026-10-31)
      started a second node (`node.local`, sockets in /tmp) instead of printing a version or
      refusing. Done when: `--version` and `--help` work, and an unknown argument exits non-zero
      without starting anything. Blocked by: nothing. Done in 6bf46ce (exit 2 on unknown arguments).
- [x] Retraction, action-watch reconnect, private-link peer trust, supervision and safe mode
      (3f86056, bbc505b, 83fce6b, 3a00507, 2ec9f08, a380034, 9ad7991).

### 2. Hardware support and memory (added 2026-10-30)

- [x] **Byte-bounded watch queues.** Why: a non-polling executor/lease watcher can still hold up to
      256 full-list events; the K230D has 128 MB for everything. Done when: per-client queues are
      capped in bytes (a plain knob with a default) or coalesce to the latest list, and the
      device-like footprint harness shows a slow watcher holding at most the cap. Blocked by:
      nothing. Done in 737579d (byte cap on client event queues). Still open: the byte total in the
      usage snapshot and metrics, queued under "4. Protocol 1" below.
- [x] **riscv64 and armv7 builds.** Why: the K230D (riscv64) and Flame (RV1106G2, Cortex-A7) run
      orion-node and orionctl. Done in 81406f9 (CI job `cross32`, `scripts/ci-jobs.sh cross32`); the
      IPC-only build's dead-code warning was already handled upstream. The IPC-only orion-node and orionctl build in release for
      riscv64gc-unknown-linux-gnu and armv7-unknown-linux-gnueabihf; the orion-node tests (235 unit
      plus all integration binaries) pass under qemu-riscv64 with `--test-threads=1`, and orionctl's
      tests pass under it too. Stripped release sizes (opt-level z, fat LTO): riscv64gc orion-node
      3,166,224 B, orionctl 1,844,704 B; armv7 orion-node 3,052,712 B, orionctl 1,851,432 B. Checked
      locally with zig cc against glibc 2.39 and a Debian trixie sysroot; the GitHub job is the
      first run on ubuntu-latest. Blocked by: nothing.
- [x] **Memory on the device.** The packaged unit's malloc settings (6a70758: one arena, 64 KiB
      mmap/trim/top-pad) on reference board #1 under the device runtime's load: PSS 9.8 MiB, RSS 11.3 MiB, private dirty
      6.9 MiB after 7 days (13.6 MiB before 6a70758). Recorded in docs/node-env.md.
### 3. Release mechanics

For when the user calls the release; only the user decides when. Until then the workspace stays on
its dev version.

- [ ] Version 1.0.0 (target agreed with the user on 2026-10-30); CHANGELOG.md for the release.
- [ ] Tag the release; the consuming products and the Gaia `orion` source pin the tag instead of a `rev`.
- [ ] Gates on the release commit: fmt, clippy -D warnings, tests, the node feature matrix and the
      file-size check.

### 4. Protocol 1

Landed on dev (the merge "Protocol 1"): nothing has been released, so every Orion protocol and
on-disk format is version 1 and the compatibility paths for older versions are gone. Every queued
wire change is in it, so protocol 1 is the layout the consumers adopt together.

- [x] **Version reset.** `CONTROL_PROTOCOL_VERSION`, `ENROLLMENT_PROTOCOL_VERSION` and the snapshot
      format are 1. The snapshot format 3 and 4 migrations and every legacy decode path are deleted.
      A state directory in another format (or with an undecodable manifest) moves to
      `<state dir>/unsupported-format-<unix ms>/`; the node logs one error and starts fresh instead of
      crash-looping (docs/node-env.md, "State directory format"). Peers, `orionctl` and clients that
      announce another version are refused with a message naming "protocol 1". Kept:
      `CompatibilityState` / `FeatureFlag` (transport preference, part of the layout) and the
      data-plane `negotiate_with` (inert with one version; public API of every transport).
- [x] **Client event queue bytes** in `NodeResourceUsageSnapshot::local_streams`, `orionctl get
      memory` and `orion_local_client_event_queue_bytes` (docs/node-env.md).
- [x] **Action counters** in `NodeResourceUsageSnapshot::actions` and `orion_action_entries`
      (docs/actions.md, "Counters").
- [x] **U3 update records.** `UpdateIntentRecord` desired section with HLC stamps and tombstones,
      `UpdateStatusRecord` in the observed slice, `orionctl get updates` / `apply update` / `delete
      update`, `ORION_NODE_UPDATE_REQUESTERS`, audit records (docs/update-recovery.md, U3).
- [x] **Stale peer state.** `PeerTrustRecord::last_successful_sync_ms` and `stale`, shown by
      `orionctl get peers` (docs/peer-sync.md, "Stale peers").
- [x] **Replicated host metrics.** `ObservedStateUpdate::host` (`ReplicatedHostMetrics`, fixed
      size, on change and at least every 30 s), held in memory per peer in
      `NodeObservabilitySnapshot::peer_host_metrics` (docs/host-facts.md, "Replicated load").
- [x] **Peer watches, wire and owner side.** `ActionQuery::forwarded_by`; a configured peer may open
      a push session that watches only the actions it forwarded (docs/actions.md, "Peer watches").
- [x] **Link protocol.** Kinds `0x20`-`0x2F` reserved for firmware transfer; `ActionRequest`
      (`0x30`) / `ActionResult` (`0x31`) in the `alloc` message layer. The minimal device build is
      unchanged in size (riscv32imac `alloc` +18 B).

Follow-ups that do not change the wire:

- [x] **Peer watches, forwarder side:** one push session per owner peer, opened lazily, closed after
      60 s idle; pushes verified against the session's own `OpenSession` (signature, increasing
      sequence, watched action ids); fallback to the `RunAction` `wait_ms` resend when the session is
      refused or drops. Tests in `tests/cluster/peer_watch.rs`: a real owner refusing the session
      (`ORION_NODE_PEER_PUSH_SESSIONS=off`), and forged pushes from a fake owner on a real socket (bad
      signature, another `OpenSession`'s binding, replayed and out-of-order sequence, unwatched
      action), each refused with the action completing by resend. The sequence check is shared:
      `orion_auth::peer_tcp::check_push_sequence` (node and `orion-client`).
- [x] **Link gateway actions:** route an action for a link-owned provider to its device as
      `ActionRequest` and report the device's `ActionResult`; the device session queues and answers
      requests (docs/link-protocol.md, docs/actions.md). Done on link-actions: one request in flight
      per device (`busy` refusal), progress and output, disconnect fails in-flight actions. Cancel
      companion and name declarations landed on link-cancel: `ActionCancel` (kind `0x32`) reaches a
      busy device, `ActionCatalog` (kind `0x33`) declares names that the node checks before sending,
      and both are listed in the link status (docs/actions.md, "Link devices";
      docs/link-protocol.md, "Cancel" and "Action catalog").
- [x] **U8 firmware transfer** in the reserved kinds (`DeviceInfo`, chunked transfer): landed in
      `orion-link` (feature `xfer`) and in the node gateway for serial and CAN links.

Not protocol changes: Prometheus metric names, env knobs, and persisted side files with their own
format versions (`link-devices.rkyv`, `peer-sync-times.json`).

## Done

### Integration

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

- [x] **What the device agent needs on the SystemCore** (all four in Orion; the device agent adopts them next):
      blob push over local IPC, `BeginBlob`/`WriteBlobChunk`/`CommitBlob`/`QueryBlobs`/`DeleteBlob`,
      resumable and digest-checked, `LocalControlPlaneClient::push_blob` ([docs/blobs.md](docs/blobs.md),
      02cafc3); long-running jobs on the action lane, `ActionState::Cancelled` and the `<name>.cancel`
      companion, one-hour deadline cap ([docs/actions.md](docs/actions.md#long-running-jobs), 4a66027);
      provider rights for clients other than the device runtime, `ORION_NODE_LOCAL_PROVIDER_ALLOW` ([docs/node-env.md](docs/node-env.md#local-ipc-access), 416b67b);
      status values up to `ORION_NODE_STATUS_MAX_VALUE_BYTES` (4 KiB) under `ORION_NODE_STATUS_MAX_BYTES`
      (4 MiB), 1c2f1bf. Protocol 1.

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
      ([packaging/README.md](packaging/README.md)). Images import the layer instead of carrying their
      own orion-node artifact, unit and env file.
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
- [x] Go smaller (`209c58e`): the CAN reassembler no longer calls `memcpy` (the 7-byte `Result`
      move in `Reassembler::push`, about 0.55 KB of compiler-builtins on Cortex-M0+ and RV32); the
      status buffer is sized by `ST` (0 in the C template); `DeviceStats` is behind feature `stats`
      (off in the template minimal builds). mcu-size, uart flash / RAM bytes, before -> after:
      thumbv6m 8022 -> 7814 / 908 -> 724; thumbv6m CAN 8838 -> 8066 / 932 -> 748; thumbv7em
      7870 -> 7690 / 908 -> 724; thumbv7em CAN 8114 -> 7934 / 932 -> 748; riscv32imc 9818 -> 9652 /
      908 -> 724; riscv32imc CAN 10380 -> 9792 / 932 -> 748. Budgets lowered to measured + 18%.
- [x] Node gateway (`orion-node` `link-gateway` feature): `ORION_NODE_LINKS` serial and SocketCAN
      links (termios / `PF_CAN` over `libc` + `AsyncFd`), `HostSession` / `HostBus` driven per link,
      `ProviderState` applied through the IPC provider path (gateway-owned `node_id`, provider and
      resource ownership checks), resources unavailable on `DeviceLost` and restored on reconnect,
      leases refreshed on desired commits, `NodeApp::link_status()` counters, pty and in-memory CAN
      tests, `link_device_sim` example.
- [x] `link_status` over the control protocol (control protocol 1): `NodeObservabilitySnapshot::links`
      (`LinkStatusSnapshot`), `orionctl get links`, `orion_link_*` Prometheus families.
- [x] Gateway follow-ups: persist which providers belong to link devices, so a node that crashed
      (no graceful shutdown) can mark them unavailable at startup instead of waiting for the device;
      run the `vcan0` test in CI. Done: `<state dir>/link-devices.rkyv` (own format version, written
      on change, restored by `start_link_gateway` as unavailable and reserved for the device until it
      reconnects; links no longer configured are forgotten), tests in `link_gateway/tests/persist.rs`.
      Startup window closed: `orion-node` restores before its IPC servers accept clients, so an IPC
      provider cannot claim a persisted id first (test `an_ipc_provider_cannot_claim_a_persisted_id_at_startup`).
      vcan0 decision: the `lints` job runs `vcan0_up` (`scripts/ci-jobs.sh`: `sudo -n modprobe vcan`,
      `ip link add dev vcan0 type vcan`, `ip link set up`), best effort, never failing the job, then
      `can_gateway_on_vcan0 -- --ignored`, which skips itself when `vcan0` is missing. GitHub's
      ubuntu-latest runners have passwordless sudo and the vcan module in linux-modules-extra, so the
      test runs there: verified in CI run 41585251104 (`vcan0 is up`, `can_gateway_on_vcan0 ... ok`).
- [x] Executor role on the link: moved into the stable interface's step 5 (records and the
      executor role, revision 3). The volatile `Status` lane on the link is done (`Status`, step 3).
- [ ] Real hardware bring-up: run the template and the gateway on real hardware (one Cortex-M and
      one RISC-V board, a USB-serial port and a SocketCAN adapter) and record round-trip timing at
      115200 baud and on classic CAN. Blocked: no MCU board or adapter is attached to the build host
      (same blocker as U8's board port).

### Control plane (control protocol 1)

- [x] Host facts ([docs/host-facts.md](docs/host-facts.md)): injectable `HostFactsSource` (default
      `LinuxHostFactsSource`; `NodeAppBuilder::with_host_facts_source` / `with_host_facts_overlay`
      for hardware-abstraction crates); identity facts (incl. board serial/model, machine id) in the
      observed `NodeRecord::host`, volatile metrics in the status lane under `node/<id>`;
      `ORION_NODE_HOST_FACTS_REFRESH_MS`, `ORION_NODE_IMAGE_VERSION_FILE`; `orionctl get nodes` /
      `describe node` host section; `orion_node_host_*` metrics.
- [x] Generic actions ([docs/actions.md](docs/actions.md)): `RunAction` / `QueryActions` /
      `WatchActions` / `WatchActionRequests` / `ClaimNodeActions` / `ReportActionResult`; in-process
      node handlers (`NodeAppBuilder::with_action_handler`), out-of-process node action claims,
      provider/executor/resource handlers over IPC, one-hop forwarding over the signed peer
      transport, deadlines and a bounded in-memory registry; `orionctl action run`,
      `orionctl get actions`; well-known names (`reboot`, `restart-unit`, `locate`, `self-test`,
      `update`) and the `action.<id>.*` status-lane convention.
- [x] Operator access: `same-user-or-group-or-root`, `ORION_NODE_LOCAL_AUTH_ALLOW`, supplementary
      groups via `SO_PEERGROUPS`; optional `packaging/gaia/orionctl.toml` layer.
- [x] Device-agent contract ([docs/device-agent.md](docs/device-agent.md)): claim-scoped
      `<name>.*` node status keys, `update_action` names, `ActionReporter`, example agent with a
      fake updater and contract tests.
- [x] CPU utilisation and temperatures in `HostMetricsSnapshot` / `HostMetricsSample`,
      `ControlPlaneEventStream::subscribe_status` (consumers can drop their `/proc` polling).
- [x] Actions: peer watches, action counters and link devices as action handlers all landed in
      protocol 1 ("4. Protocol 1": peer watches, action counters, link gateway actions).
- [x] Host facts: replicated host metrics landed in protocol 1 (`ObservedStateUpdate::host`).

### Bugs fixed (each with a regression test)

- [x] NT4 client clock offset: the smallest RTT ever seen won and never expired, so a slow start
      stayed in the offset and drift was never followed. A 30 s window (521aa14) regressed steady
      state on a real server, whose RTT cycles over 60 s. Now the lowest of half the RTT plus 20 ppm
      of age wins, with pings every 1.0076 s to cover a 20 ms loop. A lost connection drops the
      estimate, and values wait for the first sample. Bench check on the real server is the reporter's.
- [x] `send_unix_fd_frame_async` / `recv_unix_fd_frame_async` busy-spun on idle connections.
- [x] Background loops busy-spun when their `ReconcileLoopHandle` was dropped.
- [x] Executor/provider subscriptions dropped all but the first event of a batched frame.
- [x] Provider subscription desynced when its lease and state streams raced (non-cancel-safe reads).
- [x] Node IPC stream server desynced when a heartbeat tick interrupted a partial frame.
- [x] IPC server ignored shutdown while the connection limit was saturated.
- [x] Persistence failed when a mutation raced the desired-snapshot capture.
- [x] Lock-order deadlock between peer-sync responses and desired-state commits.
- [x] Order-dependent rustls provider panic in HTTP tests; peer-sync timing race in `process_http`.
- [x] Snapshot write ordering race (persistence). A persist captured its bundle under the state read
      lock, then queued it to the persistence worker without any ordering. Two concurrent persists could
      queue in the reverse order of capture. A bundle that reuses the observed bytes (its observed
      revision matched the manifest on disk at capture time) then wrote a manifest naming a revision
      that the newer bundle's observed file had replaced, so `load_snapshot_sections` failed with
      "snapshot manifest revisions do not match section files". Fixed: capture and queueing run under
      one persist-order gate, and the async path reserves queue space before taking the gate. Regression
      test `older_persist_cannot_commit_over_a_newer_snapshot` (fails before the fix). Stress: 200 of
      200 green under load; the coalescing test 120 of 120 green.

## Open

State of every open item (2026-11-06, when the leap started). Nothing else is open.

- **The leap** (in progress, "Decided, starts when the user says" below): steps 5 to 8 are done
  (step 7 as `liborion`'s first version, on the shared ABI crate). Open: the static/shared build
  modes for the other dependencies. Streams on serial and CAN, `orionctl` without reqwest,
  `liborion`'s actions, records and stream subscriptions, and its Rust wrapper are done
  (2026-11-07).
- **Waiting on the user:** release mechanics (version 1.0.0, tag, release gates).
- **Waiting on others:** protocol 1 on the reference board (the consumers' window).
- **Blocked on hardware:** real MCU bring-up, the U8 board port, U9 hardware validation (no MCU
  board, debug probe or CAN adapter on the build host).
- **After the leap** (decided, not started; none blocks it): U4 to U7 (update delivery and
  rollouts; U6 needs the stable interface), relayed liveness and failover fencing, the
  shared-key handshake over `https://`, capacity-aware placement, the on-disk layout
  fingerprint.
- **CI (decided 2026-11-06, item dropped):** the user said no to making dev the default branch
  or moving main. Dev stays covered by per-push CI; the heavier jobs (`cross32`, `ci-perf`,
  `ci-docker`, `ci-soak`) are dispatched on dev by hand (`gh workflow run <file> --ref dev`)
  after changes in those areas. Their scheduled runs test main and are not dev's results.

### Test robustness (flaky under the full parallel suite)

Each failed once in a full `cargo test --workspace --all-features` run on 2026-11-02 and passed when rerun
alone. Fix the test so it does not depend on timing or on other tests' logs.

- [x] `link_gateway::tests::unix_backlog::queries_past_the_answer_cap_are_refused_and_the_session_stays_up`
      (fixed 2026-11-06). Failed under load: answers out of order, or a session timed out.
      Causes: a full session output queue dropped its oldest frame for every new `Pong`, so a
      client pinging without reading lost parts of queued answers (a real bug: fixed in
      orion-link, control frames get their own reserve and are dropped themselves); and the
      test's 300 ms heartbeat timeout was within a starved process's scheduling delay (the test
      now uses 400 ms heartbeats, 1.2 s timeouts). Stress: 144 concurrent runs on 2 cores, 0
      failures (5 of 144 before, then 144 of 144 once the heartbeat no longer masked the drop).
- [x] `link_gateway::tests::unix_burst::a_burst_past_the_budget_waits_without_losing_the_session`
      (fixed 2026-11-06). Failed once in the gates at 1.496 s against a fixed 1.5 s floor (1.67 s
      alone). The budget is spent per read, so the last read's frames (up to 4 KiB of them,
      fuller reads on a busy machine) pass without their wait. The floor now counts that read
      from the frame size (1.02 s for 21-byte frames, asserted at 90%); unthrottled, the burst
      takes milliseconds.
- [x] `tests::cluster::binding::binding_becomes_unavailable_when_the_owner_disappears_and_recovers`
      (fixed 2026-11-07). Timed out waiting for the remote binding: with a 300 ms liveness, one
      round on a starved machine outlasted the window, so peers lapsed between rounds. It now
      uses 2 s (still well inside each phase's 10 s wait). 48 concurrent runs on 2 cores: 47
      failures before, 0 after.
- [x] **orionctl `get events` test.** Closed 2026-11-06: the polling fix is in, and no failure
      since (200 runs under load, every CI run). Reopen if it fails again. Failure: `crates/orionctl/tests/cli_get.rs:421`, "assertion failed:
      events_stdout.contains("kind=mutation_apply")" in `orionctl_get_reports_health_readiness_observability_and_snapshot`.
      Suspected race: the test reads the events before the node has recorded this one. Poll until it appears.
      The test polls `get events` for up to 5 s (`poll_until_event`).
- [x] **Replayed nonce after restart.** Failed once in a full `ci-jobs.sh workspace` run on 2026-11-03
      (`crates/node/src/auth/tests/persistence.rs`, `replayed_nonce_is_rejected_after_restart`). First
      candidate: the trust-store persist-order hole (an older capture could reach the disk after a newer
      one); fixed with `PersistOrder` (tests in `auth/store.rs`), but it was not the cause. Reproduced
      under load: whole node lib binary looped beside looping workspace test runs, 2 of 151 runs failed
      on the code before that fix with `node app should build: Config("ORION_NODE_SAFE_MODE has invalid
      boolean value `maybe`; ...")`. Cause: config tests set `ORION_NODE_*` variables in the process
      environment (behind a lock only they take), and any node built at that moment in the same test
      binary read them; `ORION_NODE_SAFE_MODE=1` from the same test silently starts a node in safe mode.
      Fix: every config test that sets the environment moved to its own binary
      (`crates/node/tests/config_env.rs`). Runs: 400 isolated + 150 whole-binary on the persist-order
      code, 0 failures (the window is narrow). Beside it, 8 of 57 looping `cargo test --workspace`
      runs on that code failed some node build with the same `maybe` error. With the move: 150
      whole-binary + 300 isolated runs under the same load, 0 environment failures.
      Left: `crates/client/src/tests/local_defaults.rs` and `crates/client/src/graph.rs` still set
      variables in the client's lib test binary (valid values, own lock); move them if one ever fails.
- [x] **Unsupported-runtime warning missing under load.** `truly_unsupported_type_is_reported_once_and_does_not_block_others`
      (`unsupported_runtime.rs:269`, WARN left 0 right 1): 3 of 180 whole node lib binary runs beside
      looping workspace tests. Cause: with one dispatcher registered, tracing-core 0.1.36 caches a
      callsite's interest from the default of whichever thread registers it (`Rebuilder::JustOne`), so
      another test's thread without a subscriber reaching the warning first caches "never" for the
      process. Shown with a standalone program (captured 0 without, 1 with a second dispatcher). Fix:
      `with_logs` keeps a second, process-lived dispatcher registered. After: 0 of 210 runs.
- [x] **Blocked snapshot hangs the test binary.** `tick_async_keeps_state_reads_available_while_{provider,executor}_snapshot_blocks`
      hung one whole workspace run for 14 h under load: a 100 ms / 250 ms `recv_timeout` expired, the
      panic unwound, and dropping the runtime waited for a snapshot whose gate was never opened. Fix:
      `ReleaseOnDrop` opens the gate on every exit, and the read waits 30 s, since it finishing with the
      gate shut is the proof. An injected panic now fails the test instead of hanging it.
- [x] **Link sim device deadlock.** One whole-binary run hung (24 min) in
      `serial_device_becomes_provider_gets_leases_and_survives_reconnect`: `SimDevice::last_leases`
      held the events lock (a temporary living to the end of the statement) while taking the device
      lock; the device task takes them the other way round. Same code in the CAN `BusDevice`. Fix: read
      the events flag first, then lock the device. After: 0 hangs in 210 runs.
- [x] **Reconcile-loop wake tests.** `desired_state_mutation_wakes_reconcile_loop_promptly` and
      `observed_update_wakes_reconcile_loop` asserted the woken pass came within 500 ms; under heavy
      load it took 650-730 ms (3 of 210 runs). The property is "woken, not the backstop": the tests now
      use a one-hour backstop, so any second pass is the wake-up, and wait for it without a latency
      bound. `idle_reconcile_loop_only_runs_backstop_passes` (once "observed 5", on code with the
      environment leak, where safe mode adds a pass) bounds passes by the measured elapsed time.
- [x] **Unsupported-runtime log count.** Failure: `crates/node/src/tests/runtime/unsupported_runtime.rs:230`,
      "left: 0, right: 1" for `logs.count("nobody.exec.v1", "WARN")`. Cause: the IPC server reconciles on a
      runtime worker thread after a publish; the test's subscriber was thread-local (`set_default` on the
      test thread), so when that pass saw the workload first its warning went nowhere and the test thread's
      passes stayed quiet under the warn-once rule. Proven by forcing the window (a pass on a spawned task
      before the test's own ticks): fails every run with the old capture. Fix: `with_logs` installs the
      subscriber on every runtime thread (`on_thread_start`), rebuilds callsite interest, and the asserts
      wait for the warning. The forced pass stays in the test. Side finding: the same capture showed
      `register_all` sends the provider before the executor, so a workload already desired warns once in
      between; the "reconciles silently" test now registers first.
- [x] **Competing leases on one exclusive camera.** Failure: `tests::cluster::binding::competing_workloads_for_an_exclusive_resource_resolve_deterministically`
      (`crates/node/src/tests/cluster/binding.rs`), `holders(&a, "resource.cam-b").len()` 0 vs 1 after node-a's
      first pass, 1 of 400 runs beside looping `cargo test --workspace --all-features`. Cause: a node leases a
      remote resource only while its owner is live on that node, and liveness is wall-clock (the test's 300 ms
      `FAST`). The work between the last sync round and node-a's first pass can stretch past 300 ms under load;
      then node-b counts as gone and no lease is written. Proven: a 400 ms pause before that pass makes node-b
      unreachable from node-a (`unreachable_peers`) and the assertion fails every run. Fix: the test is about
      how competing leases resolve, not liveness, so it uses `NEVER` liveness (the placement suite's convention,
      moved to `tests/cluster/fixtures.rs`). Runs: 1 of 400 failed before; 0 of 400 after, under the same load.
- [x] **Forwarded action stuck at Accepted.** Failure: `tests::cluster::action_calls::forwarded_actions_finish_in_one_round_trip`
      (`crates/node/src/tests/cluster/action_calls.rs`), `fwd-long` left Accepted at the 2 s cap. Measured 10 of
      400 runs before. Cause: the test's provider handler polled its node every 2 ms, about 500 messages a second,
      over the default local rate limit of 256 messages per 1 s window. Once the budget is spent the owner refuses
      the handler's `ReportActionResult` (`RateLimitedClient`). The test ignored the refusal, so the owner never
      learned the result and the forwarder kept waiting. Probe: every failing run shows that refusal on the
      `fwd-long` report. Fix: poll every 5 ms (at most 200 messages per window, within the limit by construction),
      and count refused messages, which must be zero. Runs: 10 of 400 failed before; 0 of 400 after, under the
      same load.
- [x] **A refused final report left the action Accepted until its deadline.** The test fix above left the
      product gap: a handler over the rate limit lost its result. The final report of an open action, from the
      client it was delivered to, now passes the limit. Each admitted action owes one, so these stay bounded.
      Progress, repeats and reports nothing owes still count (`a_rate_limited_handler_still_delivers_each_final_result_once`).
- [x] **orionctl tests share control sockets.** Failures: `cli_get::orionctl_get_reports_health_readiness_observability_and_snapshot`
      ("failed to connect to unix socket: Connection refused") and `cli_describe::orionctl_describe_reports_full_object_state_and_blockers`
      ("resource `resource.describe` not found"), about 1 in 40 full workspace runs under load. Cause: the harness
      bound each node's control and stream sockets at fixed paths in the shared temp dir, named only by node id. The
      server's bind removes whatever is at its path, so a second test process with the same node id took over the
      first one's socket: connections were refused, or they reached a node without the test's state. Not a
      server-not-listening race: the bind completes before `start` returns, and state is applied in-process before
      the CLI runs. Proven: solo runs pass 40 of 40, eight at a time the describe test fails 35 of 40 and the get
      test 43 of 80 with those two messages. Fix: each harness gets unique socket paths (process id and a sequence
      number), removed on drop. Regression test `harnesses_with_the_same_node_id_keep_their_own_sockets` fails 3 of 3
      on the old harness. Runs after: get 300 of 300 and describe 300 of 300, eight at a time, the two binaries
      running concurrently.

- [x] **Removed peer re-enrolled under load.** Failure: `discovery::tests::sync::removed_peers_are_not_re_enrolled_by_the_shared_key`
      (`crates/node/src/discovery/tests/sync.rs:221`, "assertion failed: !a.app.is_registered_peer(&b.id())").
      Cause: a check-then-act race in `enroll_trusted_peer`. It checked the revocation, then wrote the
      trust store and registered the peer. A `remove_peer` landing in that gap revoked and unregistered
      the peer, and the trust write (`replace_trusted_peer_key`) cleared the revocation, so the stale
      enrollment registered the removed peer again. Trace of the failing run: the enrollment passed its
      check at 934.6 ms, the removal ran at 949 ms, and the enrollment saw `revoked_now=true` at 951.4 ms.
      Fix: a peer membership lock (`PeerRegistryState::membership`, taken before the security and peer
      locks) held by enrollment, restore, removal and revocation. Regression test
      `removal_is_not_undone_by_an_enrollment_past_its_revocation_check` parks an enrollment between its
      check and its write and removes the peer meanwhile; it fails without the lock and passes with it.
      Runs: 300 of 300 whole-binary runs green under load after the fix (the pre-fix probe build failed
      1 of 160 whole-binary runs; the pristine batch was at 249 of 300 with 0 failures when the stress was
      stopped).
- [x] **Stalled-handshake test: capacity release under load.**
      `tests::tls::https_server_times_out_stalled_tls_handshakes_and_releases_capacity` (`crates/transport-http/src/tests/tls.rs`).
      Failure: "valid HTTPS client should acquire capacity after stalled handshake times out:
      RequestFailed { kind: Other, ... }". Cause: the test set the server io timeout to 20 ms. That
      timeout bounds every connection's TLS handshake, not only the stalled one, so under load the valid
      client's handshake missed it. The server dropped the valid connection, and the client saw
      "Connection reset by peer" (probe run: server timeout 26 ms on the valid connection). The test also
      waited a fixed 60 ms for the stall to time out. Second signature: `kind: Timeout` at the client's
      500 ms connect timeout (reqwest's connect timeout covers DNS, TCP and TLS). Fix: the test waits for
      the server to close the stalled socket (bounded to 60 s); the server io timeout is the product
      default (5 s); TLS test clients use `HttpClient::with_tls_and_connect_timeout` with the same 5 s
      (new public constructors, default 500 ms unchanged, documented in CHANGELOG [Unreleased]).
      Runs: pristine code 3 of 3000 whole-binary runs failed (2 this test, 1 metrics test); after the
      io-timeout change 1 of 1000 failed with the Timeout signature; with the stall test's client on the
      new connect timeout, 1 of 500 failed in the roundtrip test (same 500 ms signature, which was not
      yet on the new client); with every TLS test client on it, 0 of 500 failed under load.

### Needs the device (arm64 board)

- [x] Transparent huge pages: not applicable on the reference board; its kernel has no THP
      (`/sys/kernel/mm/transparent_hugepage` absent), and AnonHugePages is 0.
- [x] The packaged service on the board image (16 KiB pages): `Type=notify`, `WatchdogUSec=30s`,
      StatusText "serving node=node-…", 0 restarts in 7 days (reference board #1, 2026-10-31).

### Persistence

- [x] Find the device trigger of the 5 s idle persist. Found 2026-11-02 on reference board #1 with
      `RUST_LOG=orion_node=debug` (35 s capture): every reconcile pass logged `changed=false
      runtime_changed=false applied_revision_changed=false`, so it was not a reconcile flag. The
      board's build (pre-f84e233) counted an executor's republish of an identical snapshot with a
      fresh `observed_at_ms` as an observed change; the observed coalescer (2 s default then) wrote
      it on the next 5 s backstop pass. The bench did not reproduce it because its test publishers
      sent no fresh timestamps. Fixed on dev by f84e233 (timestamp-only republishes are not
      changes; default interval 30 s) and 9e2f85e (observed state is never written). To confirm on
      the board once it runs a protocol 1 node: `state persistence succeeded` (debug level now) no
      longer appears while idle. Side note from the capture: right after the restart the stream
      socket saw ~11 session opens/s for ~35 s (clients reconnecting); no churn afterwards.

### CI

- [x] The per-push jobs (node feature matrix, IPC-only tests, allocators with the aarch64
      cross-build, `appliance-aarch64`) are green on dev: every push since 2026-11-04 (for example
      run 42318176402).
- [x] Gaia layer's docker image: handed to the image build, which builds it in a real run. Orion
      only ships the fragment.
- [x] Timing-sensitive tests no longer flake under load. The injected-delay tests
      (`control_queries_remain_fast_*`, `concurrent_peer_sync_*_remains_responsive`,
      `audit_log_drops_newest_when_queue_is_full`) hold `tests::INJECTED_DELAY_LOCK` and assert
      ordering, not wall-clock time: a control call returns while the operation it would wait
      behind is still in flight. `observability_reports_ipc_stream_communication_metrics` runs with
      its heartbeat out of the window. The coalescing flush, the process-level watch test and the
      restart test no longer depend on fixed sleeps or the one-second client timeout. 20 of 20
      full-suite runs green at load 54 to 113.
- [x] CI on `dev` pushes: `ci.yml` runs on `dev`, `main` and `master` pushes (skipped for pushes
      that only touch `docs/` or Markdown), with per-ref concurrency and cancel-in-progress. The
      long jobs run on schedule or dispatch only: `cross32`, and the Docker suites in
      `ci-docker.yml` (which no longer run on pull requests or pushes). Per-push wall time on the
      first dev push: 7 min 40 s (run 41434714329); the longest job is `docs-and-lints` at 7.5 min.

### Release

- [x] Release version agreed: 1.0.0 (see "Next release").

### Decided model, not yet implemented

Decisions from 2026-10-03, prompted by multi-camera design questions. Orion stays generic;
none of these are app-specific features.

- [x] **Durable vs volatile observed state.** Records hold durable facts only (existence,
      health/availability, workload phase).
  - [x] Observed-state persistence is coalesced: at most one write per configurable interval
        (`ORION_NODE_OBSERVED_PERSIST_INTERVAL_MS`), flushed on shutdown and carried by every
        desired-state commit.
  - [x] New volatile status lane: latest value per (subject, key), in memory only, never persisted,
        with a TTL, watchable over client streams and queryable via `orionctl get status`; link
        devices publish with the `Status` kind. Not replicated to peers yet.
  - [x] Bulk and high-rate data (frames, detections) stays out of Orion and uses resource
        endpoints. A rule, not work. Streams (step 4) carry sampled values only.
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
      and the reset to snapshot format 1 (older state directories are moved aside, see `docs/node-env.md`).
  - [x] Tombstone retention is time-based only; a node offline longer than
        `ORION_NODE_TOMBSTONE_RETENTION_MS` can resurrect deleted objects. Done: a peer whose last
        completed sync round is older than the retention is refused in both directions and marked
        stale until an operator resets it (`orionctl peers remove`, then `orionctl peers enroll`).
        The sync clock is persisted in `peer-sync-times.json`. Exposing the clock in the peer
        status needs a wire change, deferred to control protocol 1. See `docs/peer-sync.md`,
        "Stale peers".
- [x] **Remove `ResourceOwnershipMode::ExclusiveOwnerPublishesDerived`.** It is enforced exactly
      like `Exclusive` and only appears in two client examples.

### Decisions (2026-10-05)

- Frame leases stay in the producer (its frame socket and lease codec). Orion carries
  only discovery and typed endpoint records (`ResourceEndpoint::Custom`); `UnixFdLatest*` stays a
  generic latest-value fd channel without hold/release, because buffer-pool reuse and back-pressure
  are producer-specific.
- Orion owns the generic systemd unit and an importable Gaia fragment (`packaging/`); images import
  it rather than each packaging `orion-node` separately.

### Update / recovery

Design only so far: [docs/update-recovery.md](docs/update-recovery.md). Orion carries update
intents and progress; the writer (A/B slots, verification, bootloader) is an external device
manager, which keeps out-of-band paths that never depend on Orion. Milestones in order:

- [x] **U0 Prerequisites**: the generic action mechanism is in (`ActionRequest`, `ActionCaller`, `wait_ms`, 4fadba9).
- [x] **U1 Supervision**: done (`READY=1`, `WATCHDOG=1`, see the release section: 2ec9f08, a380034).
- [x] **U2 Safe mode**: done (a380034, 9ad7991; the safe-mode unit is never enabled by default).
- [x] **U3 Data model**: done (scoped against the device-agent contract in
      [docs/device-agent.md](docs/device-agent.md), "Durable update records"). `UpdateIntentRecord`
      desired section and `UpdateStatusRecord` in the observed slice (protocol layout changed,
      version bump batched at integration); `orionctl get updates`; requester authorization
      (`ORION_NODE_UPDATE_REQUESTERS`); audit records.
- [ ] **U4 Delivery and resume**: deliver intents as actions keyed by
      `(intent, version, generation)`, re-delivery with backoff, immediate persistence of phase
      transitions, status-lane progress keys, drain integration, fake device manager example and
      kill-at-every-phase tests.
- [ ] **U5 External rollout controller**: `RolloutRecord` with waves, `max_unavailable`, health
      gates and monotonic halt; `orionctl rollout`.
- [ ] **U6 Cross-version rollouts**: a rollout beacon on the stable message interface (under "Decided,
      starts when the user says"), readable across versions, instead of a separate frozen layout; keep a
      snapshot of the state directory until commit and restore it on downgrade; N / N+1 rollout test.
      docs/update-recovery.md still describes the frozen beacon.
- [ ] **U7 Leaderless rollouts** in `orion-cluster`: rendezvous wave order, own-intent writes,
      convergent halt, property tests.
- [x] **U8 MCU firmware over the link**: link kinds, `xfer` device and host sessions, simulator
      test and size budget, and the node actions `link.release`, `link.attach` and
      `link.firmware.update` on serial links landed (see docs/update-recovery.md, U8).
- [x] **U8 firmware over CAN links**: `link.firmware.update` on a SocketCAN link drives
      `FirmwareTransfer` from the CAN link task (chunks of at most 32 CAN frames, device and frame
      limits applied), one job per device; `link.release` and `link.attach` release one device and
      keep the bus open. Simulated-bus tests, and `can_firmware_on_vcan0` in the `lints` job.
- [x] **U8 end-to-end `RolledBack` test over a pty**: a trial whose health check fails rolls back
      through the node's serial gateway to `Failed` with the rolled-back reason, and a bootloader that
      reverts before the host sees the trial is `Failed` as not activated.
- [x] **U8 explicit rollback status**: the device reports a reverted trial in `DeviceInfo` (a
      `RollbackReason`: not confirmed, boot failure, verify failed, requested, other, and a short
      note), the host ends `RolledBack { reason }`, and `link.firmware.update` fails `rolled back: <reason>`
      with `rollback_reason` in its result. A bootloader that reports nothing keeps the `NotActivated`
      fallback. Tested in the link simulator (both reason paths, the fallback), the pty gateway and the
      in-memory CAN bus. Hardware bootloaders must implement `FirmwareSink::last_trial`.
- [x] **U8 two-slot firmware for the MCU template** (host-tested only): `slots::SlotFirmware<F>`
      with a persistent CRC-protected boot record (two copies), `slots::boot` (trial boots once;
      revert with `NotConfirmed`, `BootFailure` or `VerifyFailed`), the rollback report cleared by the
      next transfer. Tests in `examples/mcu-template/tests/slots`, including the host driver's
      `RolledBack { NotConfirmed }`. The C API keeps the placeholder unless a port enables `ffi-slots`.
- [x] **U8 C route and real size for the slots** (host-tested only): feature `ffi-slots` runs
      `SlotFirmware` over `orion_port_*` flash hooks and adds `orion_boot`. `mcu-size` variant
      `uart-slots` links it: +8076 to +8566 B flash and +216 B static RAM over `uart-xfer`, without the
      board's flash driver. `tests/port_hooks.rs` reverts an unconfirmed trial through the hooks.
- [ ] **U8 board port for the template slots**: implement the hooks and the reset reason for one
      real MCU, run the bootloader stage from its reset path, and check the flash semantics and the
      watchdog classification the host cannot. Not started; the slot logic has never run on hardware.
      Blocked: no target MCU is chosen, and none with a debug probe is attached to the build host.
- [ ] **U9 Hardware validation**: real A/B writer with power-cut and watchdog fault injection; an
      MCU with an A/B bootloader over UART and CAN.

### Remote operator client (next, after protocol 1)

- [x] Embeddable remote operator client (`orion-client` `remote` feature or `orion-remote` crate)
      for desktop/fleet tools: ed25519 operator identity, enrollment (operator approval or shared
      key), signed `orion+tcp` requests; list/watch node records and host facts, query/watch status
      lanes, send `ActionRequest` / watch `ActionResult`, optional mDNS discovery. Nodes treat it as
      an operator principal (no desired-state replica, no placement/liveness role), with
      per-identity action authorization. First consumer: a fleet tool.
      Done: `docs/remote-operator.md`; status queries are forwarded to the owning node.
- [x] The remote operator client builds on Windows and macOS (`orion-transport-ipc`'s frame code
      and `ControlTcpClient` are cross-platform, the Unix socket parts `#[cfg(unix)]`; CI checks
      the Windows and macOS targets and runs the client unit tests on `windows-latest`).
- [x] Remote operator follow-ups: folded into the stable interface. Push watches come with its
      remote transport (step 6), action policies scoped to targets with its authorization, and
      cluster-wide operator trust stays per node until a consumer needs it.

### Decided, starts when the user says

Decided by the user on 2026-11-03; none of it has started. New work stays paused until the user
starts it.

- The link format is extended, rather than adding a second rkyv rule. Nothing gets frozen.
- The rkyv control protocol stays internal to same-build software, and mixed versions on it stay
  refused. Software built outside the image uses the stable interface instead.
- Static and shared build modes both happen, with a core, surface and wrapper for each dependency.
  The image build's LTO test informs how shared gets built, not whether.
- Hardware access is a kernel module in the hardware repository, with drivers in Rust in userspace.
  No Orion item follows from that yet.

Items:

Started 2026-11-05 at the user's word (relayed by the coordinator): the interface, `liborion` and
the versioned template. The plan and its rules: [docs/design/stable-interface.md](docs/design/stable-interface.md).
The shared ABI primitives wait until the hardware repository says they are ready.

- [ ] **One stable, versioned message interface with versioned clients.** Extend the link format
      (stable kinds, version byte, recorded fixtures). Missing today: streams, status query and watch,
      client actions, workloads, a local Unix listener with peer identity, auth and rate limits, and
      a remote transport for tools built outside the image.
      Its message types stay separate from the internal records, so a record change does not change
      the wire. Measure the packaged node's footprint when the listener lands: `link-gateway` is not a
      default feature. Rough size: 6 to 8 engineer-weeks with `liborion`.
      Progress (steps in the design): 1 done (link-owned body types, fixtures unchanged; `revision`
      in `Hello` and `Welcome`). 2 done (`unix:` links: a session per connection, local policy at
      connect, rate limit by backpressure, 64 KiB output per connection). Footprint, x86_64 release:
      the gateway adds 431 KiB stripped (6.68 to 7.10 MiB) and about 0.4 MiB PSS idle (6.2 to
      6.6 MiB); 32 idle connections add about 0.4 MiB. Decided: Unix frames stay at 4096 bytes
      until the stream kinds need more, because the host's receive buffer is sized by one constant
      for every session, serial and CAN included. 3 done (revision 1: the client role on Unix links,
      status query and watch, action run, query and watch; each session is a local control-plane
      client, so the control sockets' authorization and rate limit apply per request). Decided:
      answers larger than a frame go in parts (`more`), all parts or none; a record larger than a
      frame ends its request with `limit`; serial and CAN links do not serve the client role.
      4 done (revision 2: streams on Unix links; a provider session declares and publishes streams
      of its own provider, a client session subscribes, at most 8 per session). Decided: frames
      stay at 4096 bytes on every transport, stream data larger than a frame goes in parts by
      chunk and chunks larger than a part are split at sample boundaries (byte-exact), so no
      receive buffer grows. Serial and CAN devices publish streams (2026-11-07): the device session
      (`orion-link` feature `streams`) declares one stream of at most 64 series, sent once its
      snapshot is acknowledged and repeated until the host answers (`StreamDemand` accepts it,
      `RequestError` refuses it for the session; declarations are idempotent on the node), and
      keeps one batch waiting at a time (`StreamError::Pending` is the device's backpressure);
      the demand is a 64-bit series mask. The gateway registers each declaring device as
      publisher `link:<link>/<device>` and removes it when the session ends.
      Measured (2026-11-06, `unix_link_latency`, docs/testing.md): p50 about 45 us from a
      provider session's publish to a client's `StreamData` or `StatusChanges`.
      5 done (revision 3: the executor role and record reads on Unix links). Decided: an executor
      session registers a second local client, `link:<link>/<device>#executor` (Executor role,
      provider lane), and the gateway sets its node id; the wire workload carries no requirements
      or placement; a record watch recomputes its filtered records when desired or observed state
      changes and keeps a 64-bit digest per record, not the record, so it sends only changes; one
      record watch per session. Tests: `tests/session/executor.rs` (link),
      `link_gateway/tests/unix_executor.rs` (node).
      6 done (2026-11-06, no new kinds: `tcp:` links for remote operators, client role only,
      watches pushed as on Unix links). Decided: off by default, one switch
      `ORION_NODE_REMOTE_AUTH=keys`; operators' enrolled ed25519 keys, not TLS. Handshake: X25519
      ephemeral keys, the node's identity key and the operator's key each sign the transcript,
      HKDF-SHA256 per direction. Records: ChaCha20-Poly1305 (no AES hardware on the K230, and it
      needs none on the Cortex-A76), the record's number as nonce (replays and reordering end the
      connection), rotation every 10 minutes or 2^20 records. Pure Rust, `no_std`: X25519 from
      curve25519-dalek 5 (already in the tree through ed25519-dalek; x25519-dalek 2 would add a
      second copy), chacha20poly1305 0.10, HKDF from the existing hmac. No ring or OpenSSL.
      Measured on a reference board (Cortex-A76, release, `link_channel_cost`): handshake about 1.1 ms of
      CPU for both sides; per frame, seal and open each 1.3 us at 64 B, 1.8 us at 256 B, 4.7 us at
      1 KiB and 16 us at 4 KiB (256 MB/s), with ChaCha20's NEON backend, which
      `.cargo/config.toml` selects for aarch64 (the portable code: 1.7 us and 37 us).
      Decided (2026-11-06): stay on chacha20poly1305 0.10 (chacha20 0.9), whose NEON backend needs
      `--cfg chacha20_force_neon` in the top-level build; products building Orion as a
      dependency set it for aarch64. chacha20 0.10 (chacha20poly1305 0.11) picks NEON without a
      cfg, but under `opt-level = "z"` its NEON code takes 55 us per 4 KiB (6 us at
      `opt-level = 3`), slower than 0.9's portable code, and fat LTO overrides per-package opt
      levels. `orion_auth::link_channel::PORTABLE_CHACHA20` (build.rs) reports a build without
      the cfg; the node warns when a `tcp:` link starts on it, an aarch64 unit test fails on it,
      and `link_channel_cost` with `LINK_CHANNEL_MAX_SEAL_4K_NS=25000` fails above 25 us. Each
      request is authorized by the operator's policy at the time, and a removed operator's
      connections close within a second (its watches stop). Tests: `link_gateway/tests/remote_link.rs`, `link_channel` unit tests.
- [x] **A throttled Unix connection could be declared lost** (fixed 2026-11-06). Unix links
      shared the control sockets' 256 frames per 1 s window, and a throttled read waited for the
      window to end, so a session with a heartbeat timeout under 1 s was lost. Decided: each
      connection gets its own token bucket (`max_frames_per_s`, 8192, one second's worth of
      burst), a throttled read waits only for the next frame's turn, and a held connection counts
      as heard (`HostSession::mark_heard`). Tests: `unix_burst.rs`, 300 ms timeout, 2,000
      frames at once, and 600 frames against 200 a second.
- [x] **Answers a Unix session holds are bounded in bytes** (2026-11-06). A client could send
      queries without reading, and each copied its whole answer into the session's queue.
      Decided: 256 KiB of answers not yet taken per session (`ANSWER_BYTES`), past it queries are
      refused with `limit` and the connection is not read until they drain (held counts as
      heard); writes run in their own task, so a slow reader's pings are still read. Watches,
      runs and subscriptions answer through the node's byte-capped client event queue. Test:
      `unix_backlog.rs`. Fuzzing (`crates/link/fuzz`, docs/testing.md) found one panic, a blank
      peer id at decode, now refused.
- [x] **`liborion` over it** (2026-11-06, docs/capi.md). `crates/capi` (the Rust core and the
      `extern "C"` surface) and `crates/liborion` (`liborion.a`, `liborion.so.1`). Link client
      sessions with status queries and watches (pollable fd, `orion_link_service_in_ms`, pings
      while waiting) and a blocking fd latest-value client. Decided: the filter is call
      parameters, not a struct, so every struct is pointer-free and fixed-width and
      `ORION_LAYOUT_HASH` (`0x2801f6607019ee2a`) is one value on every target (asserted at
      compile time); the ABI crate is the git dependency `abi` pinned at its 1.0 freeze
      (`abc9908`), not on crates.io, so both crates are `publish = false`; its name appears only
      in `Cargo.toml`, `Cargo.lock`, `crates/capi/include/` and `cbindgen.toml` (allowlisted in
      `scripts/check-neutral-names.sh`). CI job `capi`.
- [x] **`liborion` actions, records, streams** (2026-11-07, docs/capi.md "Actions, records and
      streams"): five functions, answers as `OrionEvent`s on the same `orion_link_next`.
      Decided: no new struct (the layout hash stays); JSON for action arguments (an object of
      scalars), results and records, since their shapes are open-ended and C callers already
      parse JSON elsewhere; samples as packed native-endian `{uint64_t t_us; double v}` pairs
      (every series kind as a double, the kind in `value_type`); record kinds reuse
      `ORION_SUBJECT_*`. No ABI minor bump on dev (frozen-interfaces dev rule); the release
      decides the version.
- [x] **Rust wrapper** (2026-11-07, `crates/liborion-rs`, docs/capi.md "Rust"): a safe API
      (`Link`, `Latest`, `Event`) over the C functions, the same declarations in both modes:
      `static` (default) compiles `orion-capi` in, `shared` links `-lorion` (`LIBORION_LIB_DIR`).
      Decided: both features on means `static` wins instead of a `compile_error!`, because the
      workspace's CI builds `--all-features` (as with the opt-in allocators); neither is a
      `compile_error!`. The wrapper declares the structs itself and asserts the layout hash at
      compile time (and, statically, that its constants are the core's); `scripts/capi.sh check`
      runs its tests in both modes and checks the shared test binary needs `liborion.so.1`.
- [x] **Versioned MCU template** (2026-11-07, step 8). The device session announces its
      interface revision in `Hello` (`DeviceConfig::revision`, default `LINK_INTERFACE_REVISION`,
      2 bytes more per `Hello`; 0 keeps the old bytes) and keeps the session's from `Welcome`
      (`DeviceSession::session_revision`). The template's C API gains `orion_api_version`
      (`ORION_MCU_API_VERSION`, 1.0, packed major << 16 | minor), `orion_link_revision` and
      `orion_session_revision`; `include/orion_mcu.h` is generated by cbindgen and checked in the
      `capi` job, which also compiles it in the UART, CAN and slots builds. Decided: the template
      keeps its own small version, not the ABI crate's struct (no dependency, no 16-byte struct
      on an MCU).
- [x] **Shared ABI primitives** (2026-11-06): `liborion` uses them (above). Use the hardware repository's ABI primitives crate where they fit.
      Ready: the hardware repository's ABI crate 1.0 is frozen (layout hash 0xa02cfb4a8dd4545b).
      Decided: `orion-capi` uses its `Handle`, `Status`, `BufferDesc` and `FenceDesc` directly; the
      shared library's version is the SONAME major plus a handshake call (rustc's cdylib version
      script overrides named symbol version nodes).
- [ ] **Link path allocations** (visibility done 2026-11-07, docs/performance-concurrency.md
      "Link Path Costs", `crates/node/tests/link_costs.rs`): 12 allocations and 1.4 KB per
      provider status update on a `unix:` link, 16 and 2.5 KB with one watch, 12 and 2.2 KB per
      50-sample stream batch, 70 allocations per second idle. Next: cut per-update allocations
      (borrowed decode of status entries, no clone into the lane), then measure serial and CAN
      and the board's PSS (7.9 MB on the test board; the goal is 64 MB boards).
- [x] **Static and shared build modes** (2026-11-07, scope from the coordinator): each repository
      splits itself into a core, a small versioned C surface and a Rust wrapper (static by
      default, shared for consumers outside the image). Orion's split is done: the core crates,
      `liborion` (the surface) and `liborion-rs` (the wrapper, both modes); an outside consumer
      (`examples/liborion-consumer`) builds against the wrapper only, in both modes, in CI.
      Decided: consuming other repositories' surfaces is not Orion's job; the hardware, vision
      and other repositories provide their own wrappers and consumers adopt them when they exist.
      `orion-node` is not built against other repositories' shared libraries; static fat LTO
      stays the default, and per-frame hot loops stay inside one library.

### Nice to have

- [x] Single multiplexed local IPC socket: dropped. Software built outside the image uses the
      stable interface's one Unix link socket. The two rkyv sockets stay internal.
- [x] Moved into the leap (approved 2026-11-06: lean is the default): `orionctl` without reqwest by
      default. Done 2026-11-07: `orion-transport-http`'s `client` is a hand-rolled HTTP/1.1 client
      over tokio and tokio-rustls (httparse for the response head; Content-Length, chunked and
      close-delimited bodies; up to 4 kept-alive connections, idle 30 s; a reused connection the
      server closed before any response byte is retried once on a new one). Decided: `--http`
      stays in the default build; https without `--ca-cert` uses the system roots
      (rustls-native-certs); reqwest stays only as a test dependency. Measured: release `orionctl`
      4,339,384 B, 106 crates (`--no-default-features --features yaml,toml`: 3,111,192 B). reqwest
      itself was only ~250 KiB; the rest of the HTTP half is rustls + ring. Was: default `orionctl` is still 4.38 MiB, almost all reqwest + rustls + hyper client for `--http`.
      A smaller HTTP/1.1 client (hyper-util client or a hand-rolled one over tokio-rustls) could
      replace reqwest if the CLI size matters; trimming clap's `color` / `suggestions` would also
      save a little but changes help/error output. Measure again after the shared build mode, which
      changes what this size costs.
- [ ] Field-reorder/same-size type changes are not caught by the protocol layout fingerprint. On the
      wire this matters less now that rkyv is same-build only; what remains is the state directory
      across an upgrade. Consider hashing archived fixture bytes of the on-disk types. (Partly covered:
      `crates/auth/tests/canonical_encoding.rs` pins the bytes of representative control/auth messages.)
