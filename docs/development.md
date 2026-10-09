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
./scripts/check-no-std.sh   # needs: rustup target add thumbv7em-none-eabihf riscv32imac-unknown-none-elf thumbv8m.main-none-eabihf
```

`check-no-std.sh` covers the `no_std` model crates; see "no_std support" in
[`architecture-crate-map.md`](architecture-crate-map.md).

Heavier Docker, perf, and soak suites are intentionally separate and are documented in [`testing/README.md`](../testing/README.md).
See [`testing.md`](testing.md) for the repo-level validation surfaces and suite split.

## CI jobs

The Linux jobs of [`.github/workflows/ci.yml`](../.github/workflows/ci.yml) run
[`scripts/ci-jobs.sh`](../scripts/ci-jobs.sh), so a local run executes exactly the same commands:

```bash
./scripts/ci-jobs.sh lints            # one job: workspace, lints, no-std, link, allocators, appliance
./scripts/ci-jobs.sh all              # everything (the link and appliance jobs need the MCU and
                                      # aarch64 targets, and an aarch64 cross linker for appliance)
CI_JOBS_TIMINGS=t.tsv ./scripts/ci-jobs.sh all   # per-step wall and CPU seconds as TSV
```

Every step reports its wall and CPU time, and the script prints a summary table at the end.
The remote-client jobs for Windows and macOS, the dependency audit, and the package checks stay
inline in the workflow.

The feature matrix is chosen for distinct `cfg` combinations, not as a cross product:

- **orion-node clippy**: the empty build, each optional feature alone (`transport-http`,
  `transport-tcp`, `transport-quic`, `peer-tcp`, `link-gateway`, `systemd-notify`, and
  `discovery-mdns` with the `peer-tcp` it needs), the shipped appliance sets (with and without
  `link-gateway`), the defaults, and everything (`--workspace --all-features`). Unions of these
  (defaults plus one feature, `peer-tcp` plus one transport) add no `cfg` combination that one of
  them does not have, so they are not linted separately.
- **orion-node tests** run with all features (`workspace` job) and in the three builds that compile
  different code paths: IPC-only (`--no-default-features`), the packaged appliance
  (`peer-tcp,discovery-mdns,systemd-notify`, which includes the orion+tcp, remote-operator,
  discovery and sd_notify tests), and the IPC-only gateway (`link-gateway`).
- **orionctl / orion-transport-http**: the non-default feature sets only; their default sets equal
  their all-features builds. `cargo test -p orionctl --no-default-features` builds the slim CLI.
- **orion-link**: clippy for every feature set on three cores that differ in atomics (thumbv6m:
  load/store only, riscv32imc: none, thumbv7em: full; thumbv8m.main and riscv32imac lint
  identically to thumbv7em), release builds of `[]`, `device` and `alloc` on all five cores, and
  host tests without features, with `device`, with `alloc`, and with everything (table CRC,
  adapters, std sessions). `mcu-size.sh` builds and links every MCU template variant on every
  core and checks the budgets, so the template job only lints those builds.
- **Allocators**: each opt-in allocator alone, in the appliance feature set, linted, built,
  started and cross-built for aarch64 (one job, so the two builds share their dependencies).
- **Appliance**: the release aarch64 cross-build with its 64 KiB segment alignment, a type-check of
  the `link-gateway` variant, and `systemd-analyze verify` against the cross-built binary.

Debug builds use line tables only for workspace crates and no debug info for dependencies
(`[profile.dev]` in [`Cargo.toml`](../Cargo.toml)); set `CARGO_PROFILE_DEV_DEBUG=full` to debug.

## Tooling

- Rust toolchain is pinned in [`rust-toolchain.toml`](../rust-toolchain.toml)
- Root dependency versions are aligned in [`Cargo.toml`](../Cargo.toml)
- Local validation entrypoint lives in [`scripts/ci.sh`](../scripts/ci.sh)
- Local cleanup entrypoint lives in [`scripts/repo-clean.sh`](../scripts/repo-clean.sh)
- CI entrypoints live in [`.github/workflows`](../.github/workflows)
