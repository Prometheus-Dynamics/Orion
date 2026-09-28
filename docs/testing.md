# Testing

Orion splits validation into default workspace checks, Docker-backed cluster coverage, and longer perf or soak runs.

## Default Surface

- `cargo fmt --check`
- `./scripts/check-file-sizes.sh`
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

The test needs Linux `/proc` memory counters and skips with a note elsewhere. PSS keeps rising for the first two to three minutes while allocator arenas and caches warm up, so a 60 second run checks the caps and prints the slope but does not enforce it; use the full duration to judge growth. `RUST_LOG` and `MALLOC_ARENA_MAX` are passed through to the node.

## Additional Coverage

- Perf thresholds and baselines live in `testing/ci/perf-baselines.json`
- Perf and soak suites stay separate from the default local loop
- Dependency vulnerability checks run in CI with `cargo audit`
- File-size linting is warning-only, supports `FILE_SIZE_EXCLUDE_DIRS=path1:path2`, and tracks current exceptions through `testing/ci/file-size-baseline.txt`
