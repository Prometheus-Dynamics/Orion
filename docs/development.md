# Development

Orion follows the shared Prometheus Dynamics workspace layout:

- `crates/`: core library crates, transports, facades, binaries, and validation helpers
- `docs/`: repository-level guidance
- `testing/`: CI-facing Docker and perf assets
- `.github/workflows/`: GitHub Actions pipelines

## Validation Surface

Use these commands for the default local validation loop:

```bash
./scripts/repo-clean.sh
cargo fmt --check
./scripts/check-file-sizes.sh
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo doc --workspace --no-deps
```

Heavier Docker, perf, and soak suites are intentionally separate and are documented in [`testing/README.md`](../testing/README.md).
See [`testing.md`](testing.md) for the repo-level validation surfaces and suite split.

## Tooling

- Rust toolchain is pinned in [`rust-toolchain.toml`](../rust-toolchain.toml)
- Root dependency versions are aligned in [`Cargo.toml`](../Cargo.toml)
- Local validation entrypoint lives in [`scripts/ci.sh`](../scripts/ci.sh)
- Local cleanup entrypoint lives in [`scripts/repo-clean.sh`](../scripts/repo-clean.sh)
- CI entrypoints live in [`.github/workflows`](../.github/workflows)
