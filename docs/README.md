# Documentation

This directory holds repository-level documentation for the Orion workspace.

## Guides

- [development.md](development.md): repository layout, validation commands, and CI expectations
- [architecture-crate-map.md](architecture-crate-map.md): explains how the workspace crates fit together
- [testing.md](testing.md): default, Docker, perf, and soak validation surfaces
- [node-env.md](node-env.md): runtime environment contract
- [release-validation.md](release-validation.md): default and release-time validation expectations
- [observability.md](observability.md): health, readiness, observability, and audit surfaces
- [logging.md](logging.md): structured tracing behavior and operator guidance
- [public-api.md](public-api.md): preferred constructors and compatibility shims

## Where To Start

- Using Orion: start with the root [README.md](../README.md) and [`crates/orion/README.md`](../crates/orion/README.md)
- Operating a node: read [`crates/orion-node/README.md`](../crates/orion-node/README.md), [node-env.md](node-env.md), and [observability.md](observability.md)
- Client and control surfaces: read [`crates/orion-client/README.md`](../crates/orion-client/README.md), [`crates/orionctl/README.md`](../crates/orionctl/README.md), and [`crates/orion-control-plane/README.md`](../crates/orion-control-plane/README.md)
- Transport layers: read [`architecture-crate-map.md`](architecture-crate-map.md) and the `crates/orion-transport-*` crate READMEs
- Running validation: read [testing.md](testing.md) and [`../testing/README.md`](../testing/README.md)
