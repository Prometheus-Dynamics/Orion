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

## Control Protocol Version

The rkyv control protocol is versioned by `orion_core::CONTROL_PROTOCOL_VERSION`. Clients and
nodes built from different Orion releases reject each other with a typed `ProtocolMismatch
{ local, remote }` error (`IpcTransportError`, `HttpTransportError`, `ClientError`) before any
payload is decoded. If you write raw IPC frames yourself, use `control_preamble()` /
`check_control_preamble()` from `orion-transport-ipc`. See
[protocol-compatibility.md](protocol-compatibility.md).

## Config Decode

Prefer the explicit free function:

```rust
use orion::control_plane::deserialize_config;

let decoded: MyConfig = deserialize_config(&config.payload)?;
```

with plain Serde models:

```rust
#[derive(serde::Deserialize)]
struct MyConfig {
    graph: GraphConfig,
}
```

For lower-level manual access, use `ConfigMapRef`. Avoid introducing per-record decode helper methods
unless there is a concrete need they satisfy better than `deserialize_config(...)`.

## Resource Endpoints

`ResourceRecord::endpoints` holds plain `scheme://payload` strings; parsing into
`ResourceEndpoint` happens only on read, so the wire and storage format is just those strings.

Built-in schemes parse into dedicated variants: `shm://name`, `ipc://address`, `unix://path`,
`tcp://host:port`, `http://...` and `https://...`. Any other scheme that is valid RFC 3986 syntax
(an ASCII letter followed by letters, digits, `+`, `-` or `.`) parses into
`ResourceEndpoint::Custom(CustomEndpoint)`. Schemes are matched case-insensitively and custom
schemes are stored in lowercase. `ResourceEndpoint` implements `Display`/`FromStr`, and
`parse(endpoint.to_string())` returns an equal value.

Parsing fails with `MissingScheme`, `InvalidScheme` or `EmptyPayload`. `UnsupportedScheme` is
kept for compatibility but `parse` no longer returns it.

A `+` suffix names the transport underneath a custom protocol, e.g. `styx-frame-lease+unix`:
`CustomEndpoint::base_scheme()` returns `styx-frame-lease` and `transport_suffix()` returns
`Some("unix")`.

Downstream crates add typed endpoints by implementing `CustomEndpointScheme`. Every such type
is also a `TypedResourceEndpoint`, so `ResourceRecord::endpoint::<T>()` can look it up:

```rust
use orion::control_plane::CustomEndpointScheme;

struct FrameLeaseEndpoint {
    socket_path: String,
}

impl CustomEndpointScheme for FrameLeaseEndpoint {
    const SCHEME: &'static str = "styx-frame-lease+unix";

    fn from_payload(payload: &str) -> Option<Self> {
        Some(Self { socket_path: payload.to_owned() })
    }
}

// advertise: .endpoint(FrameLeaseEndpoint::endpoint_string("/run/helios/cam0.sock"))
let lease = resource.endpoint::<FrameLeaseEndpoint>()?;
```

`SCHEME` must not be a built-in scheme, because built-in schemes never parse as `Custom`.
`CustomEndpoint::new` rejects them with `ReservedScheme`.
