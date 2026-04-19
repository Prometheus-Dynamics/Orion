# orion

Facade crate for the Orion distributed node substrate.

This crate re-exports the main workspace surface behind feature gates so consumers can opt into
only the parts they need. Default features cover the core typed protocol/runtime surface; `service`,
`macros`, `cluster`, and the transport crates are explicit opt-ins.

The default feature set is intentionally narrow for a first public release:

- `core`, `auth`, `control-plane`, `data-plane`, and `runtime` are enabled because they define the
  typed Orion substrate most library consumers build against.
- Transport adapters remain opt-in so pulling the facade crate does not automatically bring in HTTP,
  IPC, TCP, or QUIC stacks.
- The `client` feature remains non-default because it currently implies the local IPC client path;
  consumers that only need the protocol/runtime contracts should not pay for that surface by
  default.
