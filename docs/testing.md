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

## Additional Coverage

- Perf thresholds and baselines live in `testing/ci/perf-baselines.json`
- Perf and soak suites stay separate from the default local loop
- Dependency vulnerability checks run in CI with `cargo audit`
- File-size linting is warning-only, supports `FILE_SIZE_EXCLUDE_DIRS=path1:path2`, and tracks current exceptions through `testing/ci/file-size-baseline.txt`
