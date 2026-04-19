# Testing

Orion splits validation into fast, Docker-backed, perf, and soak surfaces.

## Default Surface

- `cargo fmt --check`
- `./scripts/check-file-sizes.sh`
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- `cargo test --workspace --all-features`
- `cargo doc --workspace --no-deps`

## Docker Surface

- `cargo test -p orion-node --test docker_cluster_baseline -- --ignored --nocapture`
- `cargo test -p orion-node --test docker_cluster_failure -- --ignored --nocapture`
- `cargo test -p orion-node --test docker_cluster_adversarial -- --ignored --nocapture`

These use [`testing/docker/orion-node.Dockerfile`](docker/orion-node.Dockerfile).

## Additional Coverage

- Perf thresholds and baselines live in [`testing/ci/perf-baselines.json`](ci/perf-baselines.json)
- Perf workflow: [`.github/workflows/ci-perf.yml`](../.github/workflows/ci-perf.yml)
- Soak workflow: [`.github/workflows/ci-soak.yml`](../.github/workflows/ci-soak.yml)
- File-size linting is warning-only, supports `FILE_SIZE_EXCLUDE_DIRS=path1:path2`, and tracks current exceptions through `testing/ci/file-size-baseline.txt`
