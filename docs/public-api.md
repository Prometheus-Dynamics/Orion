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
payload is decoded (version 3 adds the status-lane messages; version 4 host facts, actions, and
link status). If you write raw IPC frames yourself, use `control_preamble()` /
`check_control_preamble()` from `orion-transport-ipc`. See
[protocol-compatibility.md](protocol-compatibility.md).

## Volatile Status Lane

Providers and executors publish fast-moving, non-durable values (latest value per key, with a
TTL, in node memory only) instead of rewriting observed records:

```rust
use orion_client::prelude::*;

let camera = LocalProviderService::new(runtime, "camera", provider_record);
camera.register().await?; // the status publisher must own the provider
camera
    .publish_status([
        camera.status_entry("fps", TypedConfigValue::UInt(30)),
        camera
            .status_entry("mode", TypedConfigValue::String("streaming".into()))
            .with_ttl_ms(10_000),
    ])
    .await?;

let entries = camera.query_status(StatusQuery::all().with_key_prefix("fps")).await?;
let mut watch = camera.watch_status(StatusQuery::all()).await?;
let change = watch.next().await?; // bootstrap first, then coalesced changes
```

- `LocalProviderService` / `LocalExecutorService`: `status_entry`, `publish_status`,
  `query_status`, `watch_status` (returns `StatusWatch`). The same publish/query methods exist on
  `LocalProviderApp`, `LocalProviderClient`, `LocalExecutorApp`, and `LocalExecutorClient`, and
  `LocalControlPlaneClient::query_status` reads the lane.
- Types (re-exported by `orion_client::prelude` and `orion::control_plane`): `StatusSubject`,
  `StatusEntry`, `StatusKey`, `StatusQuery`, `StatusChange`.
- A client may publish only for subjects it owns (its provider or executor, their resources, and
  its executor's assigned workloads); other batches are rejected as a whole. Status is local to
  the node and not replicated. See `docs/observability.md` ("Volatile Status Lane").

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

## Node Clock Facts

`NodeRecord::clock: Option<NodeClockFacts>` carries a node's self-reported clock source
(`ClockSourceKind`), synchronization state, offset and error estimates, and declared timebase. It
is only meaningful in observed state, where each node publishes its own record; it is `None` in
desired records. `NodeObservabilitySnapshot::clock` holds the latest sample and
`orion::control_plane::render_clock_metrics` renders it as Prometheus gauges. See
[observability.md](observability.md#clock-facts).

`orion-node` reads the kernel with `KernelClockStatusSource` (read-only `adjtimex`).
`NodeApp::spawn_clock_facts_loop()` refreshes the facts every
`NodeRuntimeTuning::clock_refresh_interval`; `NodeApp::refresh_clock_facts_from(&source)` runs one
check with any `ClockStatusSource` (for example a fake `KernelClockReading` in tests), and
`NodeApp::published_clock_facts()` returns what the observed record currently holds.

## Host Facts

See [host-facts.md](host-facts.md). `orion-control-plane`: `NodeHostFacts`, `HostMetricsSample`,
`HostTemperature`, `HostFacts` (with `merge`), `NodeRecord::host` (`NodeRecordBuilder::host`),
`NodeObservabilitySnapshot::host_facts`, `StatusSubject::Node`, `render_host_facts_metrics`.
`orion-node`: the `HostFactsSource` trait (implemented by external crates to supply facts),
`LinuxHostFactsSource` (`with_root`, `with_image_files`), `LayeredHostFactsSource`, the
`host_facts` parsers, `HostFactsTuning` (`NodeRuntimeTuning::host_facts`),
`NodeAppBuilder::{with_host_facts_source, with_host_facts_overlay}`, and
`NodeApp::{refresh_host_facts, refresh_host_facts_from, published_host_facts, latest_host_facts,
spawn_host_facts_loop}`.

## Actions

See [actions.md](actions.md). `orion-control-plane`: `ActionTarget` (`FromStr`/`Display`,
`status_subject`), `ActionRequest`, `ActionState`, `ActionResult` (`new`, `status_entries`,
`as_resource_action_result`), `ActionReport`, `ActionQuery`, `action_names`, `action_status_keys`,
and the control messages `RunAction`, `QueryActions`, `WatchActions`, `WatchActionRequests`,
`ClaimNodeActions`, `ReportActionResult`, `ActionResults`. `orion-node`: `actions::{ActionHandler,
ActionContext, ActionOutcome, ActionFuture}`, `NodeAppBuilder::with_action_handler`,
`NodeApp::{run_action, query_actions, action_handler_names}`, `ActionTuning`
(`NodeRuntimeTuning::actions`). `orion-client`: `LocalControlPlaneClient::{run_action,
query_actions, wait_for_action}`, `ControlPlaneEventStream::subscribe_actions`, `ActionWatch`,
`LocalProviderService` / `LocalExecutorService` `::{watch_action_requests, claim_node_actions}`,
and `ActionRequestWatch` (`next`, `report`, `progress`, `succeed`, `fail`, `reject`,
`publish_action_status`).

## Link Gateway Status

`NodeObservabilitySnapshot::links: Vec<LinkStatusSnapshot>` carries `NodeApp::link_status()` over
the control protocol (empty without the `link-gateway` feature);
`orion_node::link_gateway::LinkStatus` is an alias of `LinkStatusSnapshot`, and
`render_link_metrics` renders the per-link Prometheus families.

## Peer Sync and Per-Object Versions

See [peer-sync.md](peer-sync.md) for the model. The public surface:

- `orion_core::{HlcTimestamp, HybridLogicalClock, HlcClockSkew, hlc_node_tag}` (`no_std`).
- `DesiredClusterState::{stamps, tombstones}` (`DesiredObjectStamps`), `version_of`,
  `apply_stamped` (the merge rule), `force_stamped` (history replay), `stamped_batch`,
  `stamped_mutation_for`, `max_stamp`, `collect_tombstones`; `DesiredObjectKey` and
  `DesiredStateMutation::key`. The plain `put_*`/`remove_*` helpers edit records without stamps;
  `orion-node` stamps every write it commits, so clients keep building states and unstamped
  `MutationBatch::new(base_revision, mutations)` batches as before.
- `MutationBatch::{stamps, stamped, is_stamped, check_stamps, versions}`.
- `orion-node`: `NodeApp::start_peer_tcp_server(addr)` (feature `peer-tcp`), `NodeApp::hlc_now()`,
  `NodeApp::collect_expired_tombstones()`, `PeerTransportKind`, `PEER_TCP_SCHEME`,
  `PeerTcpError`, `NodeStorage::migrate_legacy_state` / `StateMigrationReport`, and
  `NodeRuntimeTuning::{with_hlc_max_drift, with_tombstone_retention}`.
  `ControlSurface::PeerTcp` (with `ControlSurface::is_peer`) marks requests from `orion+tcp`
  peers for custom middleware.

## Placement and Cross-Node Binding

See [placement.md](placement.md). The public surface:

- `orion_control_plane`: `WorkloadRecord::placement` (`Option<WorkloadPlacement>`, `#[serde(default)]`),
  `WorkloadRecordBuilder::placement`, `WorkloadRecord::{has_explicit_assignment,
  is_placement_managed}`, `WorkloadPlacement::{any, require_label, colocate_with}`,
  `LabelRequirement::{equals, exists, parse, matches}`, `PlacementDecision`, `PlacementReason`,
  `parse_node_labels`, `split_label`; `ResourceBinding::remote` (`Option<RemoteBinding>`,
  `#[serde(default)]`), `ResourceBinding::{remote(..), is_remote, is_available}`,
  `RemoteBinding { endpoints, available }`; `LeaseRecord::holders` (`Vec<LeaseHolder>`, rkyv only:
  `#[serde(skip)]` so the postcard MCU link wire is unchanged), `LeaseRecord::{held_by,
  all_holders, is_held_by}`, `LeaseHolder`.
- `orion-cluster` (`no_std`, re-exported as `orion::cluster` and `orion_node::cluster`):
  `ClusterView`, `NodeCandidate`, `eligibility`, `eligible_nodes`, `Ineligibility`,
  `choose_node`, `rendezvous_score`, `rendezvous_choice`, `resource_host`,
  `ClusterCoordinator::{new, plan, status, assign_in_place}` (replaces the unused unit struct and
  its `assign`), `PlacementStatus`, and the `leases` module (`LeaseEdit`, `capacity`,
  `resource_matches`, `remote_candidates`, `arbitrate_owned_leases`, `with_holder`,
  `without_holders`, ...).
- `orion-runtime`: `ReconcileReport::unsatisfied` (`UnsatisfiedRequirement`),
  `LocalRuntimeStore::{unreachable_nodes, remote_leases_for, resource_owner}`, `RemoteLease`.
- `orion-client`: `AssignedWorkload::{remote_bindings, placement, has_explicit_assignment}`,
  `BoundResource::{from_binding, is_remote, is_available}`; prelude exports `LabelRequirement`,
  `LeaseHolder`, `PlacementReason`, `RemoteBinding`, `WorkloadPlacement`.
- `orion-node`: `PlacementTuning` (`NodeRuntimeTuning::placement`: `labels`,
  `liveness_timeout`, `grace`, with `with_labels` / `with_liveness_timeout` / `with_grace`),
  `NodeApp::cluster_view()`, `NodeApp::unreachable_peers()`.

## Resource Ownership Modes

`ResourceOwnershipMode` is `Exclusive`, `SharedRead`, or `SharedLimited { max_consumers }`.
`ExclusiveOwnerPublishesDerived` was removed: it was enforced exactly like `Exclusive`. Use
`Exclusive` for the source resource and publish derived resources with their own mode (typically
`SharedRead`) and `source_resource` / `realized_for_workload` links.
