# Host Facts

Every node reports facts about the machine it runs on: who it is (hostname, OS, system image,
kernel, board, CPU count, memory) and how it is doing (uptime, load, available memory,
temperatures). Orion stays generic: it reads standard Linux files by default and lets an embedder
replace or extend the source; it never interprets the values.

The two halves have different lifetimes and travel differently:

| Half | Type | Where it goes | Replicated to peers |
| --- | --- | --- | --- |
| Identity (slow-changing) | `NodeHostFacts` | The node's own observed `NodeRecord::host`, replaced only when a value changes | Yes, with the observed slice (like clock facts, see [peer-sync.md](peer-sync.md)) |
| Metrics (volatile) | `HostMetricsSample` | The volatile status lane under the subject `node/<id>`, and the observability snapshot | No (the status lane is node-local) |

So peers and `orionctl get nodes` see every node's hostname, image version and boot id, while
uptime, load and temperatures are read from the node itself (`orionctl get status --subject
node/<id>`, `orionctl describe node`, Prometheus) without churning records or the disk.

## Wire shapes

`orion-control-plane` (`no_std` + `alloc`), control protocol v4:

```rust
pub struct NodeHostFacts {
    pub hostname: Option<String>,
    pub os_id: Option<String>,          // os-release ID
    pub os_name: Option<String>,        // os-release NAME
    pub os_version: Option<String>,     // os-release VERSION_ID
    pub image_name: Option<String>,     // image file, else os-release IMAGE_ID
    pub image_version: Option<String>,  // image file, else os-release IMAGE_VERSION
    pub kernel_release: Option<String>, // uname -r
    pub architecture: Option<String>,
    pub boot_id: Option<String>,        // changes on every boot
    pub board_serial: Option<String>,   // raw; consumers normalize
    pub board_model: Option<String>,
    pub machine_id: Option<String>,     // /etc/machine-id
    pub cpu_count: Option<u32>,
    pub memory_total_bytes: Option<u64>,
    pub labels: BTreeMap<String, String>, // extra facts from a custom source
}

pub struct HostTemperature {
    pub sensor: String,
    pub millidegrees_c: i32,
}

pub struct HostMetricsSample {
    pub uptime_seconds: Option<u64>,
    pub load_1_milli: Option<u64>,      // load average x 1000
    pub load_5_milli: Option<u64>,
    pub load_15_milli: Option<u64>,
    pub memory_available_bytes: Option<u64>,
    pub cpu_busy_milli: Option<u32>,    // all CPUs, per mille, since the previous sample
    pub cpu_core_busy_milli: Vec<u32>,  // per CPU, same window
    pub temperatures: Vec<HostTemperature>,
    pub extra: BTreeMap<String, TypedConfigValue>, // extra metrics from a custom source
}

pub struct HostFacts {
    pub identity: NodeHostFacts,
    pub metrics: HostMetricsSample,
    pub sampled_at_ms: u64,
}

// NodeRecord gains `host: Option<NodeHostFacts>` (`#[serde(default)]`), and
// NodeObservabilitySnapshot gains `host_facts: Option<HostFacts>` (the latest sample).
// StatusSubject gains `Node(NodeId)` (`node/<id>`).
```

Every field is optional; a source reports what its platform knows.

## Status-lane keys

Each sample is published as one batch under `StatusSubject::Node(<local node id>)` by the
publisher `node:host-facts`, with a TTL of three refresh intervals (capped by
`ORION_NODE_STATUS_MAX_TTL_MS`), so a stalled sampler lets them expire:

| Key | Value |
| --- | --- |
| `host.uptime_seconds` | `UInt` |
| `host.load1_milli`, `host.load5_milli`, `host.load15_milli` | `UInt` (load x 1000) |
| `host.memory_available_bytes`, `host.memory_total_bytes` | `UInt` |
| `host.cpu_busy_milli` | `UInt`, busy share of all CPUs per mille (0 to 1000) since the previous sample |
| `host.cpu<N>_busy_milli` | `UInt`, the same per CPU (`host.cpu0_busy_milli`, ...; at most 64) |
| `host.temperature.<sensor>` | `Int` (millidegrees Celsius) |
| `host.extra.<key>` | the source's value, for the first 64 extra metrics |

Unknown values are left out. Local clients cannot publish for `node/<id>`, except a client that
holds a node action claim: `action.*` keys and `<claimed name>.*` keys (see
[device-agent.md](device-agent.md)); `host.*` stays the node's own.

## The default Linux source

`LinuxHostFactsSource` reads (each file is optional):

| Fact | Source |
| --- | --- |
| `hostname` | `/proc/sys/kernel/hostname` |
| `os_id`, `os_name`, `os_version`, `image_name`, `image_version` | `/etc/os-release` (or `/usr/lib/os-release`): `ID`, `NAME`, `VERSION_ID`, `IMAGE_ID`, `IMAGE_VERSION` |
| image name and version override | the first readable file of `ORION_NODE_IMAGE_VERSION_FILE` (none by default; Orion hardcodes no product paths) |
| `kernel_release` | `/proc/sys/kernel/osrelease` |
| `architecture` | the build's target architecture |
| `boot_id` | `/proc/sys/kernel/random/boot_id` |
| `board_serial` | `/proc/device-tree/serial-number`, else `/sys/class/dmi/id/product_serial`, else `/sys/class/dmi/id/board_serial` (trailing NULs and whitespace trimmed; DMI files are often root-only, then the fact stays unset) |
| `board_model` | `/proc/device-tree/model`, else `/sys/class/dmi/id/product_name` |
| `machine_id` | `/etc/machine-id` |
| `cpu_count` | `/sys/devices/system/cpu/online` |
| `memory_total_bytes`, `memory_available_bytes` | `MemTotal`, `MemAvailable` in `/proc/meminfo` |
| `uptime_seconds` | `/proc/uptime` |
| load averages | `/proc/loadavg` |
| CPU utilisation | `/proc/stat` deltas between two samples (`user`..`steal`, idle = `idle + iowait`); none on the first sample |
| temperatures | every `/sys/class/thermal/thermal_zone*/temp`, labeled by the zone's `type` (`<type>/<zone>` when several zones share a type), at most 32 |

An image file is either os-release style `KEY=value` lines (`IMAGE_ID`, `IMAGE_NAME`, `NAME`, or
`ID` for the name; `IMAGE_VERSION`, `VERSION_ID`, or `VERSION` for the version) or a single line
holding the version.

`LinuxHostFactsSource::with_root(dir)` reads below another root (tests and containers that mount
the host file system elsewhere); the parsers (`parse_os_release`, `parse_image_file`,
`parse_uptime_seconds`, `parse_loadavg`, `meminfo_bytes`, `parse_cpu_list`, `thermal_readings`,
`firmware_string`) are public for fixture tests.

## Live host metrics in observability snapshots

`NodeObservabilitySnapshot::host` (`HostMetricsSnapshot`, also `query_host_metrics`) is sampled
when the snapshot is taken, and carries, besides uptime, load, memory, swap and process counters:

| Field | Meaning |
| --- | --- |
| `cpu_busy_milli` | Busy share of all CPUs, per mille, over `cpu_window_ms` (`/proc/stat`). |
| `cpu_core_busy_milli` | The same per CPU, in kernel CPU order. |
| `cpu_window_ms` | The window those figures cover. The node keeps one baseline for every reader and starts a new window only when the previous one is at least 250 ms old, so a reader polling every second sees the last second, and two readers do not shrink each other's window. The baseline is set when the node starts, so the first snapshot already has figures. |
| `temperatures` | Every `/sys/class/thermal` zone (same labels as the host-facts source), plus sensors only a custom host-facts source reports (from its latest sample). |

Where `/proc` or `/sys` do not report them (another platform, a custom source), the fields are
filled from the latest host-facts sample. Prometheus gains `orion_host_cpu_busy_ratio` and
`orion_host_cpu_core_busy_ratio{cpu}` (0 to 1).

### Following host metrics without polling

A control-plane client (for example a device's web API) that today re-reads `/proc` and
`/sys` every second can follow the node instead:

```rust
let mut events = ControlPlaneEventStream::connect_default("helios-api").await?;
let node = events.node_id().clone();
events.subscribe_state(Revision::ZERO).await?;          // desired/observed changes
events.subscribe_status(
    StatusQuery::subject(StatusSubject::Node(node)).with_key_prefix("host."),
).await?;                                                // host.* metrics
loop {
    for event in events.next_events().await? {
        match event.event {
            ClientEventKind::Status(change) => { /* change.updated: host.cpu_busy_milli, ... */ }
            _ => { /* state changes */ }
        }
    }
}
```

The first status event is a bootstrap with every matching key; later ones carry only changed
keys (coalesced, newest value per key). Keys arrive once per host-facts sample, so set
`ORION_NODE_HOST_FACTS_REFRESH_MS` to the rate the UI needs (for example `2000`); a sample reads a
handful of small `/proc` and `/sys` files. For an on-demand value with a fresh CPU window, call
`LocalControlPlaneClient::query_host_metrics()` instead. One stream can carry the state, status
and action subscriptions together, and `orion-client` reconnect is the caller's: after
`next_events` fails, connect and subscribe again (the next status event is a new bootstrap).

## Custom sources

An external crate (for example a hardware-abstraction adapter) implements one trait in
`orion-node`:

```rust
pub trait HostFactsSource: Send + Sync {
    /// Takes one sample. `sampled_at_ms` may be left 0; the node stamps it.
    fn sample(&self) -> HostFacts;
}
```

and installs it on the builder:

```rust
let app = NodeApp::builder()
    .config(config)
    // Replace the default Linux source entirely:
    .with_host_facts_source(MyPlatformFacts::new())
    // Or keep it and merge richer facts on top (applied in registration order):
    .with_host_facts_overlay(MyBoardFacts::new())
    .try_build()?;
```

An overlay's set fields replace the base's, its `labels` and `extra` metrics are added, and its
temperatures replace readings of the same sensor (`HostFacts::merge`). `LayeredHostFactsSource`
does the same for embedders that compose sources themselves. Sources must be cheap: the node calls
them on a blocking thread once per refresh interval.

`NodeApp::refresh_host_facts_from(&source)` runs one sample with any source (tests);
`NodeApp::refresh_host_facts()` samples the configured one; `NodeApp::published_host_facts()`
returns the identity in the observed record and `NodeApp::latest_host_facts()` the last sample.
`orion-node` runs `NodeApp::spawn_host_facts_loop()`.

### Naming extra metrics and labels

Status entries carry no unit metadata, so the unit goes in the key, as the built-in keys do
(`host.load1_milli`, `host.memory_available_bytes`, `host.uptime_seconds`):

- Values are fixed-point integers (`Int` / `UInt`); the suffix names unit and scale: `_uv`, `_ua`,
  `_uw`, `_mv`, `_mg`, `_mdps`, `_rpm`, `_hz`, `_bytes`, `_seconds`, `_ms`. Dimensionless ratios
  and duty cycles use `_milli` (0..1000).
- Keys are lowercase, dot-separated paths with the axis or index last:
  `imu.acceleration.x_mg`, `power.bus_voltage_uv`, `fan.duty_milli`.
- A key never changes unit; add a new key instead.
- Temperatures belong in `temperatures` (typed, millidegrees) with stable sensor labels such as
  `imu0` or `hwmon:<name>`, not in `extra`. Reusing a `/sys/class/thermal` label replaces that
  reading on purpose.
- `labels` are replicated on the node record and usable in placement selectors, so keep them
  low-churn (board, revision, attached hardware); prefix them with the source's name
  (`<source>.board`).

`HostFactsSource` lives in `orion-control-plane` (`no_std` + `alloc`), so an adapter crate needs
only the model crate; `orion_node::HostFactsSource` re-exports it.

## Configuration

| Variable | Default | Meaning |
| --- | --- | --- |
| `ORION_NODE_HOST_FACTS_REFRESH_MS` | `10000` | Sampling interval; `0` turns host facts off. |
| `ORION_NODE_IMAGE_VERSION_FILE` | unset | `:`-separated image files, first readable wins. |

## Surfaces

- `orionctl get nodes`: `hostname`, `os`, `os_version`, `image`, `image_version`, `kernel`, and
  `arch` columns for every node (from the replicated observed records); `-o json|yaml|toml` include
  the full `host` value.
- `orionctl describe node <id>`: every identity fact (`host_*` lines), and uptime, load, available
  memory and temperatures when the queried node is the described one.
- `orionctl get status --subject node/<id>`: the volatile keys above.
- Prometheus (`/metrics` on the probe surface, `orionctl get observability -o metrics`):

| Metric | Labels | Notes |
| --- | --- | --- |
| `orion_node_host_info` | `node_id`, `hostname`, `os_id`, `os_version`, `image_name`, `image_version`, `kernel_release`, `architecture`, `board_model` | Always `1`. |
| `orion_node_host_cpu_count` | `node_id` | Omitted when unknown. |
| `orion_node_host_temperature_celsius` | `node_id`, `sensor` | One sample per sensor. |
| `orion_node_host_metric` | `node_id`, `key` | Numeric extra metrics (`Int`, `UInt`, `Bool`). |
| `orion_host_uptime_seconds`, `orion_host_load1/5/15`, `orion_host_memory_total_bytes`, `orion_host_memory_available_bytes` | `node_id` | The existing host section; read from `/proc` at scrape time, filled from the latest host-facts sample when `/proc` does not report them. |

## Upgrading from protocol v3

`NodeRecord::host` changes the archived layout of node records, so control protocol v4 refuses v3
peers, `orionctl` builds and client libraries with a `ProtocolMismatch` error; upgrade them
together. Persisted state directories are migrated on the first start: snapshot format 4 is
rewritten as format 5 with every desired and observed record, stamp, tombstone, mutation-history
batch and revision kept (node records get `host: None` until the node samples again). The old
files are kept in `<state dir>/legacy-format-4/`, an interrupted migration restarts from there, and
a directory that cannot be decoded fails startup without overwriting anything.
