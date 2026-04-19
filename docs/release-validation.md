# Release Validation

This document describes the intended release-validation surface for Orion.

## Default Local Validation

These commands are expected to pass before pushing routine changes:

```bash
cargo fmt --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
```

This covers the typed config paths, node runtime, client SDK, operator CLI, and transport crates.

## Ignored Test Suites

Some suites are intentionally `#[ignore]` by default because they require heavier external setup
 or are intended for periodic benchmarking rather than every local run.

### Docker cluster suites

Location:

- `crates/orion-node/tests/docker_cluster_baseline.rs`
- `crates/orion-node/tests/docker_cluster_failure.rs`
- `crates/orion-node/tests/docker_cluster_adversarial.rs`
- `crates/orion-node/tests/docker_cluster_scale.rs`
- `crates/orion-node/tests/docker_cluster_soak.rs`
- `crates/orion-node/tests/docker_cluster_perf.rs`
- `crates/orion-node/tests/docker_client_examples.rs`

These require Docker Compose and are intended to validate multi-node convergence, restart behavior,
partition handling, and operator examples against real processes.

Recommended invocation:

```bash
cargo test -p orion-node --test docker_cluster_baseline -- --ignored
cargo test -p orion-node --test docker_cluster_failure -- --ignored
cargo test -p orion-node --test docker_cluster_adversarial -- --ignored
cargo test -p orion-node --test docker_cluster_scale -- --ignored
cargo test -p orion-node --test docker_client_examples -- --ignored
```

`docker_cluster_soak` and `docker_cluster_perf` should run on scheduled or release-candidate CI,
not every pull request.

### Perf baseline suites

Location:

- `crates/orion-node/tests/perf_baselines.rs`
- `crates/orion-node/tests/perf_release.rs`

These are baseline harnesses for replay time, idle CPU, and memory growth. They are intended for
comparison across release candidates, not as hard pass/fail unit tests on developer machines.

Recommended invocation:

```bash
cargo test -p orion-node --test perf_baselines -- --ignored
cargo test -p orion-node --test perf_release -- --ignored
```

`perf_release` additionally expects a release binary path or release build environment.

The most recent recorded baseline captured during release cleanup is in
[`docs/perf-baselines-2026-04-18.md`](perf-baselines-2026-04-18.md).

## Suggested CI Split

Recommended release pipeline structure:

1. Fast PR validation
   Run `fmt`, `clippy`, and `cargo test --workspace --all-features`.
2. Integration validation
   Run the Docker cluster baseline, failure, adversarial, scale, and example suites on a runner
   with Docker Compose available.
3. Scheduled deep validation
   Run soak and perf suites on a schedule or for release candidates.

## Minimum Public Release Gate

Before a public release tag, the repository should have:

- a green default local validation surface
- a green Docker integration pass
- a recorded perf-baseline run for comparison against the previous candidate
- up-to-date env-var and audit-log documentation
