# Orion

Orion is a Rust workspace for a distributed node runtime with a facade crate, node binary, client SDK, transport adapters, and operator CLI.

The repository is split into focused crates so runtime, transport, client, and operational surfaces can evolve independently.

## Workspace Layout

- `crates/orion`: consumer-facing facade and public API re-exports
- `crates/node`: node binary and runtime orchestration
- `crates/client`: Rust SDK for local and daemon clients
- `crates/orionctl`: operator CLI
- `crates/runtime`, `crates/cluster`, `crates/control-plane`, `crates/data-plane`: core runtime and protocol crates
- `crates/transport-*`: HTTP, TCP, QUIC, and IPC transport adapters
- `crates/auth`, `crates/service`, `crates/macros`, `crates/core`: shared support crates

Additional repository notes live under [docs/README.md](docs/README.md).

## Getting Started

Build the workspace:

```bash
cargo build --workspace
```

Run the node:

```bash
cargo run -p orion-node
```

Run the CLI:

```bash
cargo run -p orionctl -- --help
```

Run the default validation surface:

```bash
cargo fmt --check
./scripts/check-file-sizes.sh
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
```

## Development

Common workspace commands:

```bash
./scripts/repo-clean.sh
cargo fmt --check
./scripts/check-file-sizes.sh
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo doc --workspace --no-deps
```

Heavier Docker, perf, and soak suites are intentionally separate and are documented in [docs/testing.md](docs/testing.md).

## Documentation Index

- [docs/README.md](docs/README.md): repository documentation index
- [docs/architecture-crate-map.md](docs/architecture-crate-map.md): how the workspace crates fit together
- [docs/development.md](docs/development.md): repo layout, validation commands, and CI expectations
- [docs/testing.md](docs/testing.md): test surfaces, Docker suites, perf, and soak notes
- [CHANGELOG.md](CHANGELOG.md): release history and notable workspace changes
- [testing/README.md](testing/README.md): local and CI validation entry points
- [scripts/repo-clean.sh](scripts/repo-clean.sh): pre-commit cleanup and verification entry point
- [docs/node-env.md](docs/node-env.md): runtime environment contract
- [docs/release-validation.md](docs/release-validation.md): release validation checklist
- [docs/observability.md](docs/observability.md): health, readiness, and observability notes
- [docs/logging.md](docs/logging.md): runtime logging behavior
- [docs/public-api.md](docs/public-api.md): public constructors and compatibility shims

## Configuration

The node is configured primarily through environment variables. The typed entrypoints live in `orion-node` and use `try_*` constructors instead of panic-based startup helpers.

Important env vars include:

- `ORION_NODE_ID`
- `ORION_NODE_HTTP_ADDR`
- `ORION_NODE_IPC_SOCKET`
- `ORION_NODE_PEERS`
- `ORION_NODE_PEER_AUTH`
- `ORION_NODE_PEER_SYNC_MODE`
- `ORION_NODE_STATE_DIR`
- `ORION_NODE_HTTP_MTLS`
- `ORION_NODE_LOCAL_AUTH`
- `ORION_NODE_HTTP_PROBE_ADDR`
- `ORION_NODE_AUDIT_LOG`

For the full runtime contract, defaults, and failure behavior, see [`docs/node-env.md`](docs/node-env.md).
For release validation and ignored-suite guidance, see [`docs/release-validation.md`](docs/release-validation.md).
For audit-log behavior and operator guidance, see [`docs/audit-logging.md`](docs/audit-logging.md).
For health/readiness/observability coverage, see [`docs/observability.md`](docs/observability.md).
For runtime logging behavior and operator guidance, see [`docs/logging.md`](docs/logging.md).
For preferred public constructors versus compatibility shims, see [`docs/public-api.md`](docs/public-api.md).
For the current locking, blocking, and peer-sync concurrency audit, see [`docs/performance-concurrency.md`](docs/performance-concurrency.md).
For the crate layout and layering, see [`docs/architecture-crate-map.md`](docs/architecture-crate-map.md).

## Features

The facade crate `orion` is feature-gated by subsystem.

- Default features cover `core`, `auth`, `control-plane`, `data-plane`, and `runtime`.
- `client` enables the Rust SDK and implies `runtime`.
- `service`, `macros`, and `cluster` are explicit opt-ins.
- Transport layers stay opt-in through `transport-http`, `transport-ipc`, `transport-tcp`, and `transport-quic`.
- `orion-client` defaults to local IPC support through its `ipc` feature.

For production consumers that want a narrow dependency surface, prefer direct crate dependencies or disable default features on the facade and opt in explicitly.

## Operational Surface

`orion-node` exposes:

- health and readiness endpoints
- observability snapshots
- local IPC control and stream sockets
- optional HTTP/TCP/QUIC transport security and peer sync support
- optional audit logging

Current high-value runtime endpoints and surfaces:

- HTTP control surface on `ORION_NODE_HTTP_ADDR`
- optional HTTP probe surface on `ORION_NODE_HTTP_PROBE_ADDR`
- local IPC unary socket on `ORION_NODE_IPC_SOCKET`
- local IPC stream socket on `ORION_NODE_IPC_STREAM_SOCKET`

Observability and runtime debugging rely on:

- health snapshots
- readiness snapshots
- observability snapshots with recent events and transport counters
- structured tracing from the node runtime
- optional audit log records for trust and transport-security lifecycle events

## Notes On Performance

Most of the runtime and transport surface is written in an allocation-conscious style, but not every API is a zero-cost abstraction. In particular, `orion-service` intentionally uses `Arc<dyn Trait>` middleware for ergonomics at the control-plane boundary.

## License

Licensed under either:

- MIT
- Apache-2.0
