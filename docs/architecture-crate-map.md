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

These crates keep transport-specific codecs, listeners, and TLS behavior local while sharing only the narrow common helpers that are truly transport-agnostic.

## Node And Operations

- `orion-node` composes runtime, auth, transports, persistence, observability, and startup/shutdown behavior into the daemon binary.
- `orion-perf-check` is a CI helper for perf-threshold enforcement and release validation.
- `orion-macros` contains optional procedural macros used by public Orion crates.

## Typical Flow

1. A client or peer sends a typed control or data-plane request through an Orion transport.
2. `orion-node` authenticates and authorizes the request at the control boundary.
3. Control-plane mutations and queries operate on the shared typed state model from `orion-control-plane`.
4. `orion-runtime` reconciles desired state into provider/executor commands and local observations.
5. Peer sync and transport adapters exchange typed state using the control-plane and data-plane contracts.
6. Consumers can build on the `orion` facade or depend on the lower-level crates directly for a narrower surface.
