# orion-client

Rust client SDK for talking to an Orion daemon over local IPC and related control-plane flows.

Prefer the typed client and app wrappers over raw session plumbing for common workflows.

The crate enables the `ipc` feature by default on purpose. The primary release-ready client story is
same-host daemon control over local IPC, so the default install/build surface keeps that path
available without extra feature flags. If you only need the higher-level typed contracts and do not
want the local IPC transport linked in, depend on `orion-client` with `default-features = false`.

## Default Local Clients

When your daemon uses Orion's default local socket layout, prefer `connect_default(...)` on the
typed local helpers:

```rust
use orion_client::LocalProviderClient;
use orion_control_plane::ProviderRecord;

# async fn demo() -> Result<(), Box<dyn std::error::Error>> {
let provider = ProviderRecord::builder("provider.camera", "node-a")
    .resource_type("camera.device")
    .build();
let client = LocalProviderClient::connect_default("provider-camera")?;

client.register_provider(provider).await?;
# Ok(())
# }
```

`connect_default(...)` uses the default IPC stream socket and reuses the negotiated session for
repeated local calls from the same client or app instance. Use `connect_at(...)` or
`connect_at_with_local_address(...)` when you need an explicit socket path or a custom local
address for multi-client tests, examples, or custom runtime layouts.
