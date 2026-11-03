# TODO

Tracks Orion's integration with its first consumers and appliance hardening work. See
[CHANGELOG.md](CHANGELOG.md) for the user-facing summary of what landed.

## Alpha (reference board)

The user, 2026-11-03: the next release is an alpha for the reference board hardware only. These come
first; the other items below are still needed, after it.

- [ ] **aarch64 appliance soak** on the reference board (below, under freeze items).
- [x] **Unknown arguments start a node** (below): `orion-node --version` on the reference board. Done in 6bf46ce; on dev.
- [x] **Byte-bounded watch queues** (below): the reference board runs the consuming product's watchers. Done in 737579d; on dev. Byte total now in the usage snapshot and metrics (protocol 1).
- [ ] **Protocol 1 on the reference board:** once the current pin is proven on the board, the protocol 1
      window (the device library's protocol 1 merge, the consuming products together, the images' orion-node).

## Next release (1.0.0)

The release list: the 2026-10-27 feature freeze plus the hardware-support work added on 2026-10-30
(the last addition). Everything else stays under the headings below. Every item: what, why, done
when, blocked by.

### 1. Freeze items still open

- [ ] **aarch64 appliance soak.** Why: the frozen soak item; orion-node runs on every board. Data so
      far (2026-10-31, reference board #1, the packaged node under the device runtime's real load): 7 days up, 0 restarts,
      PSS 9.8 MiB / RSS 11.3 MiB. Left: a release aarch64 build's 24 h PSS curve after warm-up,
      recorded in docs/node-env.md. Running since 2026-11-02 11:55 UTC (board clock) on reference
      board #1: the installed packaged release node under the device runtime's load, sampled every
      10 min into `/var/lib/orion-soak/pss.csv` (epoch, pid, NRestarts, PSS/RSS/private dirty/anon
      kB, threads) by `/var/lib/orion-soak/sample.sh` from the transient `orion-pss-soak.timer`,
      which `orion-pss-soak-stop.timer` stops after 24 h. Transient timers do not survive a reboot:
      a gap in the epochs means the board rebooted. First sample: PSS 5.4 MiB, RSS 8.2 MiB, private
      dirty 4.1 MiB, 9 threads (just after a restart, so warm-up starts there). It measures the
      board's current pin, not protocol 1, whose node the board's consumers cannot talk to yet.
      Then: read the CSV, record the curve in docs/node-env.md.
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
      per device (`busy` refusal), progress and output, disconnect fails in-flight actions; no
      cancel companion (the device is busy) and no name declarations (docs/actions.md, "Link devices").
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
- [ ] Executor role on the link (`ExecutorState` / `Workloads`, kinds reserved) and the volatile
      `Status` lane.
- [ ] Real hardware bring-up: run the template and the gateway on real hardware (one Cortex-M and
      one RISC-V board, a USB-serial port and a SocketCAN adapter) and record round-trip timing at
      115200 baud and on classic CAN.

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
- [ ] Actions: an `ActionWatch` for peers (today a forwarding node polls the owner); action counters
      in `resource_usage`; link devices as action handlers (needs a link message kind).
- [ ] Host facts: replicate selected volatile host metrics to peers (the status lane is node-local).

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

### Test robustness (flaky under the full parallel suite)

Each failed once in a full `cargo test --workspace --all-features` run on 2026-11-02 and passed when rerun
alone. Fix the test so it does not depend on timing or on other tests' logs.

- [ ] **orionctl `get events` test.** Failure: `crates/orionctl/tests/cli_get.rs:421`, "assertion failed:
      events_stdout.contains("kind=mutation_apply")" in `orionctl_get_reports_health_readiness_observability_and_snapshot`.
      Suspected race: the test reads the events before the node has recorded this one. Poll until it appears.
      Status: the test now polls `get events` for up to 5 s (`poll_until_event`). Not reproduced: 200
      runs under load on the old code, 0 failures, so the fix is not proven under load yet.
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

- [ ] Watch the first GitHub run of the new jobs: node feature matrix, IPC-only tests, allocator
      features with the aarch64 cross-build, appliance soak, `appliance-aarch64` (cross-link,
      alignment check, `systemd-analyze verify`).
- [ ] Build the Gaia layer's docker image (`packaging/gaia/docker/aarch64-cross.Dockerfile`) once in
      a real Gaia run; it was only validated with `gaia validate` / `gaia plan`, not built.
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
- [ ] Watch `cross32` and `ci-docker` on the next weekly schedule: they have no per-push run now.

### Release

- [x] Release version agreed: 1.0.0 (see "Next release").

### Decided model, not yet implemented

Decisions from 2026-10-03, prompted by multi-camera design questions. Orion stays generic;
none of these are app-specific features.

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
- [ ] **U6 Cross-version rollouts**: frozen rollout beacon readable across one protocol bump; keep
      a snapshot of the state directory until commit and restore it on downgrade; N / N+1 rollout test.
- [ ] **U7 Leaderless rollouts** in `orion-cluster`: rendezvous wave order, own-intent writes,
      convergent halt, property tests.
- [x] **U8 MCU firmware over the link**: link kinds, `xfer` device and host sessions, simulator
      test and size budget, and the node actions `link.release`, `link.attach` and
      `link.firmware.update` on serial links landed (see docs/update-recovery.md, U8).
- [x] **U8 firmware over CAN links**: `link.firmware.update` on a SocketCAN link drives
      `FirmwareTransfer` from the CAN link task (chunks of at most 32 CAN frames, device and frame
      limits applied), one job per device; `link.release` and `link.attach` release one device and
      keep the bus open. Simulated-bus tests, and `can_firmware_on_vcan0` in the `lints` job.
- [ ] **U8 end-to-end `RolledBack` test over a pty**: a trial whose health check fails rolls back
      through the node's serial gateway to `Failed` with the rolled-back reason; the device's explicit
      rollback status, if the protocol gets one.
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
- [ ] Remote operator follow-ups: push-based watches over `orion+tcp` instead of polling; action
      policies scoped to targets; optional cluster-wide operator trust (today per node).

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
