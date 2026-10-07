# Documentation

This directory holds repository-level documentation for the Orion workspace.

## Guides

- [development.md](development.md): repository layout, validation commands, and CI expectations
- [architecture-crate-map.md](architecture-crate-map.md): explains how the workspace crates fit together
- [testing.md](testing.md): default, Docker, perf, and soak validation surfaces
- [node-env.md](node-env.md): runtime environment contract, including running under systemd
- [../packaging/README.md](../packaging/README.md): systemd unit, environment file, and Gaia layer for running `orion-node` on Linux images
- [release-validation.md](release-validation.md): default and release-time validation expectations
- [observability.md](observability.md): health, readiness, observability, and audit surfaces
- [logging.md](logging.md): structured tracing behavior and operator guidance
- [public-api.md](public-api.md): preferred constructors and compatibility shims
- [protocol-compatibility.md](protocol-compatibility.md): control-protocol wire version, IPC preamble / HTTP header handshake, and how to bump it
- [placement.md](placement.md): node labels, leaderless workload placement, and cross-node binding (leases)
- [host-facts.md](host-facts.md): host identity facts and volatile host metrics, the replaceable host-facts source
- [actions.md](actions.md): generic actions on nodes, providers, resources, and executors (routing, authorization, handlers)
- [device-agent.md](device-agent.md): the device-agent contract: claiming node actions (`update`, `reboot`, `locate`) over local IPC, progress, `update.*` status keys that survive reboots, result semantics, disconnects
- [remote-operator.md](remote-operator.md): the embeddable remote operator client (`orion-client` feature `remote`), operator enrollment, per-operator authorization, threat model
- [update-recovery.md](update-recovery.md): design (not implemented) for software update intents, progress, staged rollouts, rollback, and recovery paths that never depend on Orion
- [link-protocol.md](link-protocol.md): MCU link protocol (UART/CAN framing, link messages, device bring-up) for no_std devices

## Where To Start

- Using Orion: start with the root [README.md](../README.md) and [`crates/orion/README.md`](../crates/orion/README.md)
- Operating a node: read [`crates/node/README.md`](../crates/node/README.md), [node-env.md](node-env.md), [observability.md](observability.md), and [`packaging/README.md`](../packaging/README.md) for systemd-managed devices
- Client and control surfaces: read [`crates/client/README.md`](../crates/client/README.md), [`crates/orionctl/README.md`](../crates/orionctl/README.md), and [`crates/control-plane/README.md`](../crates/control-plane/README.md)
- Transport layers: read [`architecture-crate-map.md`](architecture-crate-map.md) and the `crates/transport-*` crate READMEs
- Running validation: read [testing.md](testing.md) and [`../testing/README.md`](../testing/README.md)
