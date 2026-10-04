# Architecture Crate Map

Orion is intentionally layered. Higher-level crates build on shared contracts and typed runtime state instead of reaching directly across the workspace.

## Layering

1. `orion-core`
   Shared IDs, revisions, protocol versions, and core error/value types.
2. `orion-auth`
   Authentication and signing contracts used by node and transport boundaries.
3. `orion-control-plane`
   Canonical control messages, snapshots, mutations, observability payloads, and operator-facing records.
4. `orion-data-plane`
   Data-link negotiation, transport binding, and peer exchange vocabulary.
5. `orion-cluster`
   Cluster-level state helpers and coordination primitives built on control-plane contracts.
6. `orion-runtime`
   Local reconcile planning, provider/executor integrations, workload validation, and command application.
7. `orion-service`
   Ergonomic request/middleware primitives for control-boundary composition.
8. `orion-client`
   Rust SDK for local IPC and daemon-facing client flows.
9. `orionctl`
   Operator CLI built on the public client/control surfaces.
10. `orion`
    Facade crate that re-exports the public building blocks and keeps transports feature-gated.

## Transport Crates

- `orion-transport-common` holds shared TLS and connection-task helpers used by the transport adapters.
- `orion-transport-http` implements the HTTP control-plane transport.
- `orion-transport-ipc` implements same-device IPC control and data transport.
- `orion-transport-tcp` implements TCP frame transport for data-plane traffic.
- `orion-transport-quic` implements QUIC transport for data-plane traffic.

- `orion-link` implements the [link protocol](link-protocol.md) for microcontrollers on UART, RS-485, USB-CDC, classic CAN, and CAN FD. The default build is only the framing layers (CRC-32C message frames, COBS byte streams, CAN segmentation): `no_std`, allocation-free, and independent of other Orion crates. The `alloc` feature adds postcard-encoded link messages and the sans-IO `DeviceSession` (still `no_std`, built on `orion-core` / `orion-control-plane` without `std`); the `std` feature adds the sans-IO `HostSession` / `HostBus` that the node gateway will drive. `examples/mcu-template` (outside the workspace) is the chip-agnostic firmware starting point.

These crates keep transport-specific codecs, listeners, and TLS behavior local while sharing only the narrow common helpers that are truly transport-agnostic.

## Node And Operations

- `orion-node` composes runtime, auth, transports, persistence, observability, and startup/shutdown behavior into the daemon binary.
- `orion-perf-check` is a CI helper for perf-threshold enforcement and release validation.
- `orion-macros` contains optional procedural macros used by public Orion crates.

## no_std support

The foundation/model crates build `no_std` + `alloc`, so the Orion state model, wire messages, and
auth payloads can run on microcontrollers (any target with a global allocator; no specific chip is
assumed). Each has a default `std` feature; build with `default-features = false` for bare metal.

| Crate | Without `std` | Needs `std` |
| --- | --- | --- |
| `orion-core` | IDs, `Revision`, type names, `OrionError`, protocol constants, rkyv `encode_to_vec`/`decode_from_slice`/length-prefixed helpers | nothing |
| `orion-data-plane` | link, binding, peer capability and negotiation types | nothing |
| `orion-control-plane` | records, messages, mutations, cluster state, typed config decoding (`deserialize_config` keeps field-path diagnostics via `serde_path_to_error`, which is itself `no_std`), endpoint parsing, `LatencyMetricBuckets` | Prometheus export (`render_*_metrics`, `MetricsExportConfig`, which reads env vars); filesystem helpers on `SharedMemoryEndpoint` (`path`, `read_*`, `ORION_SHM_ROOT`) and `UnixEndpoint` (`path_buf`, `read_*`) |
| `orion-auth` | peer request / transport binding types and canonical signing bytes | nothing |
| `orion-runtime` | reconcile planning, local runtime store, provider/executor integration traits | nothing |
| `orion-cluster` | membership, admission, assignment helpers | nothing |

The `orion` facade has a default `std` feature too. With `default-features = false` its `core`,
`auth`, `control-plane`, `data-plane`, `runtime`, `cluster`, and `macros` features work `no_std`;
`client`, `service`, and every `transport-*` feature imply `std`. `orion-node`, the transports,
`orion-client`, `orion-service`, `orionctl`, and `orion-perf-check` are std-only.

Dependency notes:

- Workspace dependencies on the model crates and on `serde`, `serde_json`, `thiserror`, `rkyv`, and
  `ed25519-dalek` are declared with `default-features = false`; every std crate enables
  `features = ["std"]` explicitly, so std builds are unchanged.
- rkyv keeps `little_endian` + `pointer_width_64` on every target (requires rkyv >= 0.8.16 on
  32-bit targets), so archives are byte-identical between hosts and MCUs, and between std and no_std
  builds. `crates/auth/tests/canonical_encoding.rs` checks canonical messages against a recorded
  fixture in both the std and the `--no-default-features` build.
- Signing itself (ed25519) lives in `orion-node`; `orion-auth` only produces the canonical bytes, so
  MCU code can sign them with any `no_std` ed25519 implementation.

`scripts/check-no-std.sh` (also the CI `no-std` job) builds each crate separately for
`thumbv7em-none-eabihf`, `riscv32imac-unknown-none-elf`, and `thumbv8m.main-none-eabihf`, and runs
clippy plus the host tests without `std`.

`orion-link` has its own CI job (`link-no-std`): it builds and lints the crate for the same targets
with no features (framing only, no allocator) and with `alloc` (messages and the device session on
top of the no_std model crates), runs its tests in both the `alloc` and the `std` build, and builds
`examples/mcu-template` as a bare-metal staticlib. `scripts/mcu-size.sh` reports the template's
linked flash/RAM footprint.

## Typical Flow

1. A client or peer sends a typed control or data-plane request through an Orion transport.
2. `orion-node` authenticates and authorizes the request at the control boundary.
3. Control-plane mutations and queries operate on the shared typed state model from `orion-control-plane`.
4. `orion-runtime` reconciles desired state into provider/executor commands and local observations.
5. Peer sync and transport adapters exchange typed state using the control-plane and data-plane contracts.
6. Consumers can build on the `orion` facade or depend on the lower-level crates directly for a narrower surface.
