# orion-transport-common

Shared transport helpers for the Orion workspace.

This crate is internal support code for the transport adapters. It centralizes small pieces of reusable transport infrastructure, such as TLS-loading helpers and connection task management, without flattening the HTTP, IPC, TCP, or QUIC crates into one generic transport layer.

## What Lives Here

- shared TLS material loading and normalization
- connection task tracking helpers
- cross-transport constants used by the transport adapters

## What Does Not Live Here

- HTTP route handling or codecs
- IPC frame handling
- TCP or QUIC socket-specific behavior
- node startup, binding, or control middleware logic

Consumers should normally use the transport crates directly rather than depending on this crate.
