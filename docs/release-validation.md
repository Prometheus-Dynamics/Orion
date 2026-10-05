# Release Validation

This document describes the intended release-validation surface for Orion.

## Default Local Validation

These commands are expected to pass before pushing routine changes:

```bash
cargo fmt --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps
cargo audit
```

This covers the typed config paths, node runtime, client SDK, operator CLI, and transport crates.

## Validation Tooling

Required release tools:

```bash
cargo install cargo-audit --locked
```

Optional dependency-cleanup tools:

```bash
cargo install cargo-machete --locked
cargo install cargo-udeps --locked
```

`cargo audit` is a required CI gate. `cargo machete` and `cargo udeps` are useful periodic cleanup
tools; `cargo udeps` may require a nightly toolchain depending on the installed version.

Scheduled and release-branch CI also runs:

```bash
cargo tree -d --workspace
cargo machete
```

`cargo tree -d` is reported so duplicate dependency movement is visible during release review.
`cargo machete` is a hard gate for unused dependency candidates in release-candidate hygiene runs.

## Release Binary Feature Profiles

`orion-node` release binaries enable both data-plane transports by default:

- `transport-tcp`
- `transport-quic`

That is the intended public release artifact because it lets one binary participate in either
managed data-plane topology. The facade crate, `orion`, keeps transports opt-in so embedders do not
pull the heavier transport stacks unless they request them.

For constrained builds, use an explicit minimal transport profile:

```bash
cargo build -p orion-node --release --no-default-features --features transport-tcp
cargo build -p orion-node --release --no-default-features --features transport-quic
```

Do not publish a release binary built with a reduced transport profile unless the artifact name or
release notes make that limitation explicit.

### Packaged appliance build

The systemd packaging (`packaging/README.md`, `packaging/gaia/orion-node.toml`) ships a separate
appliance profile for Linux images:

```bash
cargo build -p orion-node --release --target aarch64-unknown-linux-gnu \
  --no-default-features --features peer-tcp,discovery-mdns,systemd-notify
```

Before tagging a release that images will pin, check:

- `cargo test -p orion-node --no-default-features --features peer-tcp,systemd-notify` passes. The
  `systemd_notify` integration test runs the binary against a fake `NOTIFY_SOCKET` (`READY=1`
  only after the listeners accept, watchdog pings, `STOPPING=1` on timer and `SIGTERM` shutdown,
  `WATCHDOG_PID` handling) and the `systemd::tests` unit tests cover abstract sockets and a
  wedged reconcile loop.
- The aarch64 build links and every `PT_LOAD` segment is aligned to `0x10000`
  (`readelf -lW orion-node | grep LOAD`), so it loads on 16 KiB and 64 KiB page kernels. The CI
  `appliance-aarch64` job checks both and runs `systemd-analyze verify` on the unit.
- On a device or VM with systemd: `systemctl start orion-node` reaches `active (running)` with the
  `serving ...` status line, `systemctl show -p WatchdogTimestamp orion-node` advances, and
  `kill -STOP $(pidof orion-node)` leads to a watchdog restart within `WatchdogSec=`.
- `gaia validate` of an image build that imports `packaging/gaia/orion-node.toml` reports no errors.

Without a device, `qemu-aarch64-static -L <aarch64 sysroot>` runs the aarch64 binary (with
`ORION_NODE_HTTP_ADDR=off`, IPC sockets under a short `/tmp` path, and
`ORION_NODE_SHUTDOWN_AFTER_INIT_MS`); memory numbers from qemu-user include the emulator itself.

## Package And Publish Validation

Orion crates are versioned together and publish in dependency order when targeting crates.io. CI
verifies the release package surface before any crate has been published by packaging every
publishable crate, unpacking those `.crate` files into a temporary workspace, patching internal
Orion dependencies to the unpacked tarballs, and checking that isolated workspace:

```bash
./scripts/verify-package-tarballs.sh
```

This catches missing files and tarball-only build failures without requiring the new version to
already exist in crates.io. It is the pre-publish package verification gate. During the actual
crates.io release, still publish in dependency order so each crate's registry dependencies are
available by the time the next crate is published.

For a crates.io release, publish and verify in this order:

1. `orion-core`
2. `orion-control-plane`
3. `orion-auth`
4. `orion-data-plane`
5. `orion-runtime`
6. `orion-transport-common`
7. `orion-service`
8. `orion-macros`
9. `orion-cluster`
10. `orion-transport-http`
11. `orion-transport-ipc`
12. `orion-transport-tcp`
13. `orion-transport-quic`
14. `orion-client`
15. `orion`
16. `orion-node`
17. `orionctl`

`orion-perf-check` is marked `publish = false` and is intended as repository-local CI tooling.

Optional command per crate after its internal dependencies are available in the target registry:

```bash
cargo package -p <crate> --allow-dirty
```

Only publish after the tarball verification script succeeds. Per-crate verification remains useful
as an extra check while stepping through the publish order, but the CI gate is the tarball workspace
verification script above.

## Ignored Test Suites

Some suites are intentionally `#[ignore]` by default because they require heavier external setup
 or are intended for periodic benchmarking rather than every local run.

### Docker cluster suites

Location:

- `crates/node/tests/docker_cluster_baseline.rs`
- `crates/node/tests/docker_cluster_failure.rs`
- `crates/node/tests/docker_cluster_adversarial.rs`
- `crates/node/tests/docker_cluster_scale.rs`
- `crates/node/tests/docker_cluster_soak.rs`
- `crates/node/tests/docker_cluster_perf.rs`
- `crates/node/tests/docker_client_examples.rs`

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

- `crates/node/tests/perf_baselines.rs`
- `crates/node/tests/perf_release.rs`

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

## Package Contents

Before tagging a release candidate, inspect the workspace package surface:

```bash
cargo package --workspace --allow-dirty --no-verify --list
```

The tarballs may include source tests, examples, benches, and README/license metadata when those are
useful to downstream users. Generated files, editor backups, local reports, and accidental
local-only files should not appear. `Cargo.toml.orig` is expected in these package lists because
Cargo generates it when normalizing manifests that inherit workspace fields. If any other generated
or local-only file appears in this list, exclude it in the owning crate manifest before publishing.

## Suggested CI Split

Recommended release pipeline structure:

1. Fast PR validation
   Run `fmt`, `clippy`, `cargo test --workspace --all-features`, warning-free docs, and
   `cargo audit`.
2. Integration validation
   Run the Docker cluster baseline, failure, adversarial, scale, and example suites on a runner
   with Docker Compose available.
3. Scheduled deep validation
   Run soak and perf suites on a schedule or for release candidates.

## Minimum Public Release Gate

Before a public release tag, the repository should have:

- a green default local validation surface
- a green dependency audit
- warning-free generated docs
- verified package tarballs for each publishable crate in dependency order
- a green Docker integration pass
- a recorded perf-baseline run for comparison against the previous candidate
- up-to-date env-var and audit-log documentation
