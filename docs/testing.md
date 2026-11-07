# Testing

Orion splits validation into default workspace checks, Docker-backed cluster coverage, and longer perf or soak runs.

## Default Surface

- `cargo fmt --check`
- `./scripts/check-file-sizes.sh`
- `./scripts/check-neutral-names.sh` (no product or board names outside the allowlist in the script)
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- `cargo test --workspace --all-features`
- `cargo doc --workspace --no-deps`
- `cargo audit`

## Docker Surface

The main containerized suites exercise the node and cluster behavior inside `testing/docker/orion-node.Dockerfile`:

- `cargo test -p orion-node --test docker_cluster_baseline -- --ignored --nocapture`
- `cargo test -p orion-node --test docker_cluster_failure -- --ignored --nocapture`
- `cargo test -p orion-node --test docker_cluster_adversarial -- --ignored --nocapture`
- `cargo test -p orion-node --test docker_cluster_scale -- --ignored --nocapture`
- `cargo test -p orion-node --test docker_client_examples -- --ignored --nocapture`

## Appliance Memory Soak

`crates/node/tests/appliance_memory_soak.rs` spawns the real `orion-node` binary in the single-node appliance profile (`ORION_NODE_HTTP_ADDR=off`, `ORION_NODE_RUNTIME_WORKER_THREADS=2`, IPC sockets only) and drives it through the client SDK: a provider publishes capture heartbeats (observed resource state) at high rate and watches leases and state, an executor watches its assigned workloads and reports their observed state, and a control-plane client keeps adding, updating, and removing workloads so mutation history cycles through its cap. It samples the node's `resource_usage` diagnostics over IPC, prints one row per sample, asserts that mutation history, stream queues, worker queues, and registries stay within their caps, and fails when the least-squares slope after warm-up of either process memory (PSS, or `VmRSS` when PSS is unavailable) or anonymous RSS exceeds the limit. Anonymous RSS is checked separately because PSS includes file-backed pages that the kernel can reclaim under host memory pressure, which can mask heap growth.

```sh
# CI length (10 minutes)
cargo test -p orion-node --test appliance_memory_soak -- --ignored --nocapture
# quick local check
ORION_SOAK_DURATION_SECS=60 cargo test -p orion-node --test appliance_memory_soak -- --ignored --nocapture
```

| Variable | Default | Meaning |
| --- | --- | --- |
| `ORION_SOAK_DURATION_SECS` | `600` | Total soak length (minimum 10). |
| `ORION_SOAK_WARMUP_SECS` | duration/5, clamped to 10–120 | Samples before this are excluded from the slope. |
| `ORION_SOAK_SAMPLE_MS` | `2000` | Sampling interval. |
| `ORION_SOAK_MAX_SLOPE_KIB_PER_MIN` | `256` | Maximum allowed post-warm-up memory slope. |
| `ORION_SOAK_MIN_SLOPE_WINDOW_SECS` | `120` | Shorter post-warm-up windows report the slope without enforcing it. |
| `ORION_SOAK_PROVIDER_HZ` / `ORION_SOAK_RESOURCES` | `50` / `4` | Heartbeat rate and resources per heartbeat. |
| `ORION_SOAK_EXECUTOR_HZ` | `5` | Executor observed-state report rate. |
| `ORION_SOAK_MUTATION_INTERVAL_MS` / `ORION_SOAK_WORKLOAD_SLOTS` | `250` / `6` | Desired-state churn rate and workload count. |
| `ORION_SOAK_MAX_MUTATION_HISTORY` | `64` | `ORION_NODE_MAX_MUTATION_HISTORY` passed to the node, so the cap is reached early. |
| `ORION_SOAK_NODE_WORKER_THREADS` | `2` | `ORION_NODE_RUNTIME_WORKER_THREADS` for the node; `0` leaves it unset (one worker per usable core). |
| `ORION_SOAK_NODE_BIN` | the `orion-node` built with the test | Node binary to spawn, for comparing separately built binaries (for example allocator features) with one test build. |

The test needs Linux `/proc` memory counters and skips with a note elsewhere. PSS keeps rising for the first two to three minutes while allocator arenas and caches warm up, so a 60 second run checks the caps and prints the slope but does not enforce it; use the full duration to judge growth. The node inherits the test's environment, so `RUST_LOG`, `MALLOC_ARENA_MAX`, `_RJEM_MALLOC_CONF`, and `MIMALLOC_*` reach it, and so does CPU affinity from `taskset`. The final `memory:` line reports start, first post-warm-up, end, and peak memory, plus anonymous RSS and the node's thread count.

### Comparing allocators

Measure with release binaries. Debug builds allocate differently. `cargo test --release` builds the
node with the test's features, so an allocator feature on the test build also applies to the node:

```sh
# glibc (default), 2 workers
ORION_SOAK_DURATION_SECS=600 cargo test --release -p orion-node --test appliance_memory_soak -- --ignored --nocapture
# glibc with two malloc arenas, one worker per core, pinned to 4 CPUs like a small arm64 board
MALLOC_ARENA_MAX=2 ORION_SOAK_NODE_WORKER_THREADS=0 taskset -c 0-3 \
  cargo test --release -p orion-node --test appliance_memory_soak -- --ignored --nocapture
# jemalloc / mimalloc
cargo test --release -p orion-node --features alloc-jemalloc --test appliance_memory_soak -- --ignored --nocapture
MIMALLOC_ALLOW_THP=0 cargo test --release -p orion-node --features alloc-mimalloc --test appliance_memory_soak -- --ignored --nocapture
```

To run several variants in parallel without rebuilding, copy each `cargo build --release -p orion-node [--features …]` output aside and point `ORION_SOAK_NODE_BIN` at it. Results and the resulting recommendation (keep glibc, set `MALLOC_ARENA_MAX=2`, check transparent huge pages) are in [`node-env.md`](node-env.md#allocator-measurements). On hosts with THP set to `always`, jemalloc and mimalloc use far more memory unless THP is disabled, so record `/sys/kernel/mm/transparent_hugepage/enabled` with any comparison.

## Link Gateway

`cargo test -p orion-node --no-default-features --features link-gateway` (also part of
`--all-features`) runs the gateway tests without hardware or root: config parsing, serial end to end
over pseudo-terminal pairs (`posix_openpt`; a simulated `StreamDevice` on the master, the gateway on
the slave: the provider appears, leases reach the device, a disconnect marks resources unavailable,
a reconnect restores them, allowlist and ownership-collision rejection), and CAN bridging of several
devices through `HostBus` over an in-memory bus.

The restart tests (`link_gateway/tests/persist.rs`) cover the persisted link-owned providers: a crash
leaves them unavailable after the restart, a reconnect makes them available, and a link removed from
the config is forgotten.

One test needs a real virtual CAN interface and is `#[ignore]`d; it skips itself when `vcan0` is
missing. The `lints` CI job (`scripts/ci-jobs.sh`, step "vcan0 for the ignored SocketCAN test") brings
`vcan0` up with passwordless sudo on the GitHub runner and then runs the test; if the `vcan` module
cannot be loaded, the step says so and the test skips. Locally, the same commands are:

```sh
sudo modprobe vcan
sudo ip link add dev vcan0 type vcan && sudo ip link set up vcan0
cargo test -p orion-node --features link-gateway --lib can_gateway_on_vcan0 -- --ignored
cargo test -p orion-node --features link-gateway --lib can_firmware_on_vcan0 -- --ignored
```

### Latency and allocations

`unix_link_latency` (`link_gateway/tests/unix_latency.rs`, ignored, run in the perf workflow)
times one value's trip through the node over a Unix link. A provider session publishes, and a
client session receives. Each value waits for the previous one, so nothing coalesces:

```sh
cargo test -p orion-node --release --features link-gateway --lib unix_link_latency -- --ignored --nocapture
```

Measured 2026-11-06, release, in-process, 2,000 values per path, three runs each:

| Path | Machine | p50 | p90 | p99 |
|---|---|---|---|---|
| stream publish to `StreamData` | aarch64 board, Cortex-A76 (load 0.3) | 45 us | 48 to 49 us | 74 to 136 us |
| status publish to `StatusChanges` | aarch64 board, Cortex-A76 (load 0.3) | 56 to 57 us | 60 to 61 us | 113 to 119 us |
| stream publish to `StreamData` | x86_64 workstation (load ~110) | 34 to 51 us | 50 us to 1.6 ms | 0.1 to 2 ms |
| status publish to `StatusChanges` | x86_64 workstation (load ~110) | 45 to 53 us | 57 to 141 us | 77 us to 1.7 ms |

On the board the tail stays near the median. The workstation's millisecond tail came from its
load: threads waiting to be scheduled, not a timer in the path. The board binary is the test
binary cross-built for `aarch64-unknown-linux-musl` (`zig cc` as the C compiler, `rust-lld` as
the linker) and run as `orion_node-* unix_link_latency --ignored --nocapture`.

`unix_burst.rs` sends bursts at a link with a 300 ms heartbeat timeout. 2,000 frames at once
pass inside the default budget in well under a second. 600 frames against a 200-a-second budget
wait about two seconds, and no session is lost.

Allocation counts on the hot paths (`crates/link/tests/alloc_count.rs` and
`crates/nt4/tests/alloc_count.rs`) fail when a change adds allocations. Steady state:

| Path | Allocations |
|---|---|
| link heartbeat answer | 0 |
| link stream delivery (a 10-sample batch to a client) | 0 |
| link status from a device | 0.15 |
| link stream publish received | 4 |
| NT4 encode and scalar decode | 0 |
| NT4 relayed value (client, server and subscriber) | 4.7 |

## Fuzzing

`crates/link/fuzz` holds cargo-fuzz targets for the bytes a peer controls. It is its own
workspace, built with nightly:

| Target | Input |
| --- | --- |
| `decode_payload` | A frame kind and payload into `Message::decode_payload`. A body that decodes must encode and decode back to the same message. |
| `host_session` | A byte stream into a host session serving clients and streams, fed in chunks. At most 1024 frames may go out. |
| `device_session` | A byte stream into a stream device. |

```bash
cd crates/link/fuzz && cargo +nightly fuzz run decode_payload -- -max_total_time=60
```

`scripts/ci-jobs.sh fuzz` runs all three for `FUZZ_SECONDS` each. CI runs that job on the weekly
schedule and on dispatch, and keeps any crash input as an artifact. A crash found so far gets a
regression test in `crates/link/tests` with its bytes. The first was a blank executor id in a
status query, which panicked the node; ids from the peer are now checked when they decode.

## Peer Discovery

`cargo test -p orion-node --no-default-features --features peer-tcp,discovery-mdns` (also part of
`--all-features`) runs the discovery and enrollment tests without multicast: announcements travel
over `MemoryDiscoveryBus`, an in-memory stand-in for an mDNS segment. They cover TXT encoding,
browsing, TTL expiry and goodbyes, cluster filtering, protocol-version mismatch, the shared-key
handshake (success, wrong key, replayed confirmation, mismatched node ids, removed peers), operator
enrollment and removal through the local control path, persistence across a restart, and two
nodes with real `orion+tcp` listeners on loopback that discover each other and sync only after
they are enrolled.

One test uses real mDNS (the `mdns-sd` backend) and is `#[ignore]`d, because it needs multicast on
a network interface, which CI containers usually lack. Run it on a host or VM where mDNS works
(a firewall must allow UDP 5353):

```sh
cargo test -p orion-node --features discovery-mdns --lib real_mdns -- --ignored
```

It starts two nodes on the same host, waits until they discover each other and enroll with a
shared key, and syncs an artifact between them.

## Additional Coverage

- Perf thresholds and baselines live in `testing/ci/perf-baselines.json`
- Perf and soak suites stay separate from the default local loop
- Dependency vulnerability checks run in CI with `cargo audit`
- File-size linting is warning-only, supports `FILE_SIZE_EXCLUDE_DIRS=path1:path2`, and tracks current exceptions through `testing/ci/file-size-baseline.txt`
