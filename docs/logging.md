# Logging

`orion-node` emits structured tracing logs intended for operators and release validation.

## Default Behavior

Logging is initialized from `RUST_LOG` through `tracing-subscriber`.

If `RUST_LOG` is unset, the node defaults to `info`.

Example:

```bash
RUST_LOG=orion_node=debug cargo run -p orion-node
```

## What Is Logged

The node currently logs:

- startup configuration summary after initialization
- replay success and replay failure with duration and failure category
- reconcile success and reconcile failure with duration and failure category
- peer sync success and peer sync failure with peer id, duration, and failure category
- mutation apply success and mutation apply failure with duration and failure category
- HTTP TLS failures and audit-log backpressure warnings
- graceful shutdown errors for HTTP, probe, IPC unary, and IPC stream surfaces

Peer-sync failure logs now align with the runtime observability categories:

- `tls_trust`
- `tls_handshake`
- `auth_policy`
- `transport_connectivity`
- `peer_sync`

Those categories are sourced from typed errors when Orion owns the failure surface. Message-based
fallback remains only at narrow transport boundaries that still report free-text TLS or request
errors.

## Operator Guidance

For routine production operation:

- use `info` for normal lifecycle visibility
- use `debug` when investigating peer sync or transport behavior

For incident investigation, combine:

- structured logs
- the health/readiness/observability snapshots
- the optional audit log

## Common Failure Signals

Useful log messages to watch for:

- `replay failed`
- `peer sync failed`
- `reconcile failed`
- `mutation apply failed`
- `http tls failure`
- `graceful shutdown reported an error`
- `audit log queue full; dropping newest event`
