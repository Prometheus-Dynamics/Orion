# Public API Notes

## Preferred Constructors

Prefer these typed entrypoints:

- `NodeProcessConfig::try_from_env()`
- `NodeConfig::try_from_env()`
- `NodeApp::try_new(...)`
- `NodeApp::builder()`
- `HttpClient::try_new(...)`

These return typed errors instead of aborting on malformed configuration or client-construction
failures.

## Operator Surface vs Internal Helpers

Prefer documented environment variables and top-level builder/config APIs over internal helper
functions.

Examples:

- use `ORION_NODE_IPC_STREAM_SOCKET` instead of depending on
  `NodeConfig::default_ipc_stream_socket_path_for(...)`
- use documented `ORION_NODE_*` env vars instead of internal `*_from_env()` helper methods
- use health/readiness/observability endpoints and docs rather than internal status helper methods
