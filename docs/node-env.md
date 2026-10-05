# `orion-node` Environment Reference

This document covers the runtime environment variables consumed by `orion-node`.
Startup now prefers typed parsing through `NodeProcessConfig::try_from_env()` and
`NodeConfig::try_from_env()`. Invalid typed values fail startup with `NodeError::Config`
instead of silently falling back.

## Identity and Bindings

| Variable | Default | Valid values | Failure behavior |
| --- | --- | --- | --- |
| `ORION_NODE_ID` | `node.local` | Any valid Unicode string | Invalid Unicode fails startup. |
| `ORION_NODE_HTTP_ADDR` | `127.0.0.1:9100` | Socket address like `127.0.0.1:9100`, or `off` / `disabled` / `none` to skip the HTTP control listener | Invalid address fails startup. `off` combined with `http://`/`https://` entries in `ORION_NODE_PEERS` or HTTP TLS settings fails startup (`orion+tcp://` peers do not need it). |
| `ORION_NODE_PEER_ADDR` | unset (no listener) | Socket address like `0.0.0.0:9200` for the `orion+tcp` peer listener, or `off` | Invalid address fails startup. Setting it in a build without the `peer-tcp` feature fails startup. Without it the node still syncs outbound with its `orion+tcp://` peers, but peers cannot connect to it. |
| `ORION_NODE_IPC_SOCKET` | `${TMPDIR}/orion-<node-id>-control.sock` | Filesystem path | Invalid Unicode fails startup. |
| `ORION_NODE_IPC_STREAM_SOCKET` | `${TMPDIR}/orion-<node-id>-control-stream.sock` | Filesystem path | Invalid Unicode fails startup. |
| `ORION_NODE_HTTP_PROBE_ADDR` | unset | Socket address | Invalid address fails startup. |
| `ORION_NODE_RUNTIME_WORKER_THREADS` | unset (one per CPU core) | Positive integer | Zero or invalid integer fails startup. |
| `ORION_NODE_RUNTIME_MAX_BLOCKING_THREADS` | unset (Tokio default `512`) | Positive integer | Zero or invalid integer fails startup. |
| `ORION_NODE_RECONCILE_MS` | `250` | Integer milliseconds, minimum effective value `1` | Invalid integer fails startup. Minimum idle gap between reconcile passes (see below); also the peer sync interval. |

### Reconcile scheduling

The reconcile loop is event-driven. It runs one pass at startup and then sleeps until something
that can change the outcome of a pass is committed: a desired-state commit (local or remote
mutations, snapshot adoption, peer sync, lease or assignment changes), an observed-state merge,
provider or executor state published over local IPC, a maintenance change, or a newly registered
in-process integration. Wake-ups coalesce for up to 5ms so a burst of updates collapses into one
pass. A pass that dispatched commands to an in-process executor, or saw an in-process integration
snapshot change, schedules a follow-up pass so in-process integrations keep converging.

- `ORION_NODE_RECONCILE_MS` keeps its name and default but now means the minimum idle gap between
  the end of one pass and the start of the next. Under continuous load the loop therefore runs no
  more often than before; when idle it does not run at all until woken or the backstop fires.
- `ORION_NODE_RECONCILE_BACKSTOP_MS` (default `5000`, runtime tuning below) is the longest the loop
  stays idle before a periodic safety pass. It is clamped to at least `ORION_NODE_RECONCILE_MS`,
  so setting it at or below that value restores the previous fixed-interval polling.
- In-process integrations whose snapshots change outside Orion can call
  `NodeApp::request_reconcile()` to be picked up before the next backstop pass.
- While the loop runs, local IPC provider/executor state updates and applied mutations defer their
  follow-up reconcile to the loop instead of reconciling inline. Reconcile failures are then
  reported through reconcile metrics and the `reconcile failed` log instead of rejecting the
  already committed update. Without a running loop (embedded use) they still reconcile inline.

### Single-node appliance profile

A standalone node that only serves local IPC clients can shed most of its network surface:

- build with `cargo build -p orion-node --release --no-default-features` to get an IPC-only binary. This build drops the HTTP stack (the `transport-http` feature: axum, hyper, reqwest, and rustls), the `orion+tcp` peer transport, and the TCP and QUIC data-plane transports. Add `--features transport-http`, `peer-tcp`, `transport-tcp`, or `transport-quic` to keep any of them.
- to cluster such appliances without the HTTP stack, build with `--no-default-features --features peer-tcp`, set `ORION_NODE_PEER_ADDR`, and list peers as `node-id=orion+tcp://host:port|<public-key-hex>` (see `docs/peer-sync.md` for the threat model: requests and responses are signed, traffic is not encrypted).
- with a default (HTTP-enabled) build, set `ORION_NODE_HTTP_ADDR=off` to skip the HTTP control listener. The optional probe listener on `ORION_NODE_HTTP_PROBE_ADDR` still works.
- in a build without `transport-http`, the HTTP listener is always off:
  - `ORION_NODE_HTTP_ADDR` must be unset or one of `off`, `disabled`, or `none`. Setting it to an address fails startup, because the build cannot serve it.
  - `http://`/`https://` entries in `ORION_NODE_PEERS`, `ORION_NODE_HTTP_PROBE_ADDR`, `ORION_NODE_HTTP_TLS_CERT`, `ORION_NODE_HTTP_TLS_KEY`, and `ORION_NODE_HTTP_TLS_AUTO` fail startup with an error that names the missing feature. `orion+tcp://` peers likewise need the `peer-tcp` feature.
  - Embedders that pass peers or HTTP TLS files to `NodeApp::builder()` get the same error from `try_build()`.
- set `ORION_NODE_RUNTIME_WORKER_THREADS=1` or `2` and lower `ORION_NODE_MAX_MUTATION_HISTORY*` and worker queue capacities to fit the device memory budget
- on glibc targets, set `MALLOC_ARENA_MAX=2` to limit per-thread malloc arenas
- keep the default allocator (glibc malloc). In every appliance soak run, the opt-in `alloc-jemalloc` and `alloc-mimalloc` features below used more memory than glibc
- check transparent huge pages (THP) on the device: `cat /sys/kernel/mm/transparent_hugepage/enabled` and `grep AnonHugePages /proc/$(pidof orion-node)/smaps_rollup`. With THP set to `always`, the kernel can back sparsely used heap regions with whole huge pages. On a kernel with 16 KiB pages, as Raspberry Pi OS ships for BCM2712 (Pi 5 / CM5), a PMD huge page is 32 MiB instead of 2 MiB, so check `getconf PAGESIZE` too. If `AnonHugePages` is a large share of `Anonymous`, boot with `transparent_hugepage=madvise` (or write `madvise` to that sysfs file)

#### Global allocator features

`orion-node` has two opt-in Cargo features that replace the global allocator of the binary. The
`orion-node` library never sets a global allocator. Both features are off by default.

| Feature | Allocator | Runtime tuning |
| --- | --- | --- |
| `alloc-jemalloc` | jemalloc 5.3 via `tikv-jemallocator` | Built-in options: `narenas:2,background_thread:true,max_background_threads:1,dirty_decay_ms:1000,muzzy_decay_ms:0,thp:never`. Override or extend them at runtime with `_RJEM_MALLOC_CONF` (the symbols are prefixed, so plain `MALLOC_CONF` is ignored). For example, `_RJEM_MALLOC_CONF=confirm_conf:true` prints the options in effect at startup. |
| `alloc-mimalloc` | mimalloc 3 via the `mimalloc` crate | `MIMALLOC_*` environment variables. Set `MIMALLOC_ALLOW_THP=0` on hosts with THP set to `always` (see the measurements below). `MIMALLOC_PURGE_DELAY` controls how soon freed memory is returned. |

```sh
cargo build -p orion-node --release --no-default-features --features alloc-jemalloc
```

Enable only one of them. If both are enabled, for example by `cargo clippy --all-features`,
jemalloc takes precedence and mimalloc is compiled but not used. They are not made mutually
exclusive with `compile_error!` because that would break workspace `--all-features` builds.
`MALLOC_ARENA_MAX` has no effect with either feature.

#### Allocator measurements

These numbers come from the appliance memory soak (`docs/testing.md`) on current `main`, which
includes the event-driven reconcile loop. Each run used release binaries with default features,
lasted 600 s, and was sampled every 2 s. The host was x86_64 with 24 cores, glibc 2.43, 4 KiB
pages, and THP `always`, and other builds were running on it. Memory values are in MiB. "Warm" is
the first sample after the 120 s warm-up. The slope is post-warm-up PSS growth. "Default" workers
means one per core the process may run on: 24, or 4 with `taskset -c`. The jemalloc rows use the
built-in options above.

| Allocator | Workers | CPUs | PSS warm | PSS end | Peak PSS | Anon end | Slope (KiB/min) | Threads |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| glibc | 2 | 24 | 3.7 | 4.5 | 4.5 | 3.1 | -27 | 10 |
| glibc + `MALLOC_ARENA_MAX=2` | 2 | 24 | 2.9 | 2.9 | 3.1 | 2.6 | 3 | 10 |
| mimalloc | 2 | 24 | 28.1 | 9.3 | 41.2 | 9.0 | -3307 | 11 |
| mimalloc + `MIMALLOC_ALLOW_THP=0` | 2 | 24 | 6.1 | 5.6 | 6.4 | 5.2 | -93 | 11 |
| jemalloc | 2 | 24 | 11.7 | 11.5 | 11.8 | 10.0 | -59 | 12 |
| glibc | default | 24 | 4.6 | 4.4 | 4.9 | 3.8 | -77 | 31 |
| glibc + `MALLOC_ARENA_MAX=2` | default | 24 | 3.7 | 3.6 | 3.8 | 3.2 | -16 | 32 |
| mimalloc | default | 24 | 43.0 | 15.2 | 54.1 | 14.9 | -4412 | 34 |
| mimalloc + `MIMALLOC_ALLOW_THP=0` | default | 24 | 8.8 | 7.8 | 9.0 | 7.5 | -125 | 33 |
| jemalloc | default | 24 | 14.7 | 13.8 | 15.0 | 13.1 | -88 | 33 |
| glibc | 2 | 4 | 3.5 | 3.4 | 4.0 | 3.1 | -39 | 10 |
| glibc + `MALLOC_ARENA_MAX=2` | 2 | 4 | 3.0 | 3.9 | 3.9 | 2.5 | -33 | 10 |
| mimalloc | 2 | 4 | 20.2 | 12.6 | 33.4 | 11.9 | -1441 | 10 |
| jemalloc | 2 | 4 | 12.6 | 12.5 | 13.2 | 12.0 | -62 | 11 |
| glibc | default | 4 | 4.5 | 4.6 | 4.9 | 3.9 | -69 | 13 |
| glibc + `MALLOC_ARENA_MAX=2` | default | 4 | 3.4 | 3.4 | 3.8 | 3.0 | -49 | 13 |
| mimalloc | default | 4 | 28.7 | 13.2 | 42.4 | 12.7 | -2814 | 12 |
| mimalloc + `MIMALLOC_ALLOW_THP=0` | default | 4 | 6.5 | 6.0 | 7.1 | 5.5 | -147 | 12 |
| jemalloc | default | 4 | 11.4 | 11.3 | 12.0 | 10.5 | -56 | 14 |
| jemalloc, THP off for the process | default | 4 | 5.6 | 6.0 | 6.0 | 4.6 | -105 | 13 |

Every run passed the soak's caps and its 256 KiB/min slope limit. glibc with `MALLOC_ARENA_MAX=2`
was the smallest configuration in every row group. It saved 0.5 to 1 MiB over plain glibc, and the
saving was largest with one worker per core. In this workload, glibc arenas stayed small: plain
glibc with 24 workers used about 1 MiB more than with 2 workers. That is far from the roughly
25 MiB of extra anonymous memory measured on HeliOS, so arenas alone are unlikely to explain it.

Transparent huge pages cause most of the jemalloc and mimalloc overhead here. In an earlier run of
this matrix, 200 s in with default workers on 24 CPUs, `AnonHugePages` was 12 MiB of mimalloc's
29 MiB anonymous memory, 6 MiB of jemalloc's 12 MiB, and 0 of glibc's 4 MiB. With THP disabled
(`MIMALLOC_ALLOW_THP=0`, or `prctl(PR_SET_THP_DISABLE)` set by a wrapper and inherited by the node),
both allocators come close to glibc but stay above it. jemalloc's own `thp:never` option only
recovers 2 to 5 MiB: the pre-merge runs without it ended at 13.8 and 17.0 MiB PSS, compared with
11.4 and 11.0 MiB with it. mimalloc also peaks high, then returns memory once its purge delay
expires, which produces the large negative slopes.

Stripped `opt-level = "z"` binary sizes were 5.39 MiB for glibc, 5.50 MiB for mimalloc
(+0.11 MiB), and 5.68 MiB for jemalloc (+0.29 MiB).

These measurements were taken on x86_64, not on the CM5. On the device, compare `RssAnon` and
`AnonHugePages` with and without `MALLOC_ARENA_MAX=2` and with THP set to `madvise` before you
switch allocators.

## Peer and Auth Controls

| Variable | Default | Valid values | Failure behavior |
| --- | --- | --- | --- |
| `ORION_NODE_PEERS` | unset | Comma-separated `node-id=<url>` entries where `<url>` is `http://host:port`, `https://host:port` (feature `transport-http`) or `orion+tcp://host:port` (feature `peer-tcp`); optional `|ca=/path` (HTTPS) and trusted public key segments. HTTP and TCP peers can be mixed. | Invalid entry format, an unknown scheme, or a scheme whose feature is not compiled in fails startup. |
| `ORION_NODE_PEER_AUTH` | `optional` | `disabled`, `optional`, `required` | Invalid mode fails startup. |
| `ORION_NODE_PEER_SYNC_MODE` | `parallel` | `serial`, `parallel` | Invalid mode fails startup. |
| `ORION_NODE_PEER_SYNC_MAX_IN_FLIGHT` | `4` | Integer, minimum effective value `1` | Invalid integer fails startup. |
| `ORION_NODE_HTTP_MTLS` | `disabled` | `disabled`, `optional`, `required` | Invalid mode fails startup. |
| `ORION_NODE_LOCAL_AUTH` | `same-user` | `disabled`, `same-user`, `same-user-or-group` | Invalid mode fails startup. |
| `ORION_NODE_LABELS` | unset | Comma-separated `key=value` or bare `key` labels, published in the node's observed record and matched by workload node selectors (see `docs/placement.md`). Blank terms are ignored; the last value of a repeated key wins. | Non-UTF-8 value fails startup. |

## Peer Discovery

mDNS/DNS-SD discovery and enrollment (feature `discovery-mdns`, off by default). Discovered peers
are never trusted automatically; see [discovery.md](discovery.md).

| Variable | Default | Valid values | Failure behavior |
| --- | --- | --- | --- |
| `ORION_NODE_DISCOVERY` | `off` | `off`, `mdns` | Invalid value fails startup. `mdns` fails startup without the `discovery-mdns` feature, without `ORION_NODE_PEER_AUTH=required`, or without `ORION_NODE_PEER_ADDR`. |
| `ORION_NODE_CLUSTER` | `default` | 1-63 characters of `[A-Za-z0-9._-]`; only peers advertising the same name are considered | Invalid name fails startup. |
| `ORION_NODE_DISCOVERY_TTL_MS` | `120000` | Positive integer milliseconds a discovered peer stays listed without a new announcement | Invalid value fails startup. |
| `ORION_NODE_DISCOVERY_INTERFACES` | unset (all interfaces) | Comma-separated interface names or addresses | Unknown names are ignored by the mDNS daemon. |
| `ORION_NODE_ENROLLMENT_KEY` | unset | Shared enrollment key, at least 32 bytes (for example `openssl rand -hex 32`); enables automatic mutual enrollment | Shorter keys, setting it together with `ORION_NODE_ENROLLMENT_KEY_FILE`, or setting it without `ORION_NODE_DISCOVERY=mdns` fails startup. |
| `ORION_NODE_ENROLLMENT_KEY_FILE` | unset | Path to a file containing the key (surrounding whitespace ignored) | Unreadable file or a short key fails startup. |

## Link Gateway

Requires the opt-in `link-gateway` cargo feature (`cargo build -p orion-node --features
link-gateway`, or `--no-default-features --features link-gateway` for an IPC-only appliance) and
Linux. Setting `ORION_NODE_LINKS` in a build without the feature, or on another OS, fails startup
with an error that says so. See the "Gateway" section of `docs/link-protocol.md`.

| Variable | Default | Valid values | Failure behavior |
| --- | --- | --- | --- |
| `ORION_NODE_LINKS` | unset (no links) | `;`-separated `<kind>:<target>[?key=value&...]` entries, `kind` = `serial` or `can` | Any malformed entry, unknown or misplaced key, out-of-range value, duplicate key, or duplicate link fails startup with `NodeError::Config` naming the entry. A port or interface that cannot be opened does not fail startup; the link retries every second and logs the error. |

Keys for every link:

| Key | Default | Valid values | Meaning |
| --- | --- | --- | --- |
| `allow` | unset (any device name) | Comma-separated device names | Only these `Hello` device names are accepted; others get `Reject { UnknownDevice }`. |
| `heartbeat_ms` | `1000` | `10`-`600000` | Ping interval announced to devices in `Welcome`. |
| `missed_heartbeats` | `3` | `>= 1` | Heartbeats without traffic before the gateway reports a device lost (its resources become unavailable). |
| `max_frame` | `4096` | `32`-`4096` | Host frame limit; the negotiated limit is the minimum of this and the device's. |

`serial:<path>` keys (raw mode, 8N1, no flow control, exclusive open):

| Key | Default | Valid values | Meaning |
| --- | --- | --- | --- |
| `baud` | `115200` | `1200`, `2400`, `4800`, `9600`, `19200`, `38400`, `57600`, `115200`, `230400`, `460800`, `500000`, `576000`, `921600`, `1000000`, `1152000`, `1500000`, `2000000`, `2500000`, `3000000`, `3500000`, `4000000` | Line rate. Ignored by USB-CDC and pseudo-terminals. |

`can:<interface>` keys (SocketCAN `CAN_RAW`, one kernel receive filter per device address):

| Key | Default | Valid values | Meaning |
| --- | --- | --- | --- |
| `device_base` | `0x600` | Decimal or `0x` hex | Device-to-host identifier base; device `a` sends on `device_base + a`. |
| `host_base` | `0x680` | Decimal or `0x` hex | Host-to-device identifier base; device `a` listens on `host_base + a`. |
| `addresses` | `1-16` | `N` or `A-B`, at most 512 addresses | Device addresses served. Every identifier must fit 11 bits (29 with `extended`), and the device and host identifier ranges must not overlap. |
| `fd` | `false` | `true`/`false` (`1`/`0`, `yes`/`no`, `on`/`off`) | CAN FD (64-byte frames, `CAN_RAW_FD_FRAMES`). The interface must be configured for FD. |
| `extended` | `false` | as `fd` | 29-bit identifiers. |

Example: `ORION_NODE_LINKS='serial:/dev/ttyAMA0?baud=115200&allow=imu-board;can:can0?device_base=0x600&host_base=0x680&addresses=1-16&allow=motor-a,motor-b'`.

## Persistence and Logging

| Variable | Default | Valid values | Failure behavior |
| --- | --- | --- | --- |
| `ORION_NODE_STATE_DIR` | unset | Filesystem path | Invalid Unicode fails startup. |
| `ORION_NODE_AUDIT_LOG` | unset | Filesystem path | Invalid Unicode fails startup. |
| `ORION_NODE_SHUTDOWN_AFTER_INIT_MS` | unset | Integer milliseconds | Invalid integer fails startup. Intended for tests and controlled automation, not steady-state production. |

A state directory written by an `orion-node` before control protocol v3 (snapshot format 3) is
migrated in place on the first start: the old files are kept in `<state dir>/legacy-format-3/`,
the desired state is rewritten with per-object versions, and observed state is rebuilt from live
reports. If the old files cannot be read, startup fails without overwriting anything. See
"Upgrading from protocol v2" in `docs/peer-sync.md`.

A state directory written with control protocol v3 (snapshot format 4) is rewritten in snapshot
format 5 (`NodeRecord::host`) on the first start, keeping every record, stamp, and revision; the
old files are kept in `<state dir>/legacy-format-4/`. See "Upgrading from protocol v3" in
`docs/host-facts.md`.

## Clock Facts

The node reports its clock source and synchronization state in its observed node record (see
[Clock Facts](observability.md#clock-facts)); Orion never adjusts the clock. The kernel reports
whether the clock is synchronized but not which daemon disciplines it, so operators declare it.

| Variable | Default | Valid values | Failure behavior |
| --- | --- | --- | --- |
| `ORION_NODE_CLOCK_SOURCE` | unset (`system` on Linux, `unknown` elsewhere) | `unknown`, `system`, `ntp`, `chrony`, `ptp`, `gps` (case-insensitive), or any other name, reported as `Other(name)` | Invalid Unicode fails startup. Blank is treated as unset. |
| `ORION_NODE_TIMEBASE` | unset | Any name, for example `UTC`, `TAI`, or `monotonic` | Invalid Unicode fails startup. Blank is treated as unset. |

The refresh interval is `ORION_NODE_CLOCK_REFRESH_MS` under Runtime Tuning. Programmatic callers
set all three through `NodeRuntimeTuning::with_clock_refresh_interval`, `with_clock_source`, and
`with_clock_timebase`.

## Host Facts

The node samples host facts (hostname, OS, image, kernel, board, CPU count, memory, uptime, load,
temperatures) every `ORION_NODE_HOST_FACTS_REFRESH_MS` (Runtime Tuning, default `10000`, `0`
turns host facts off). Identity facts go into the node's observed record, volatile metrics into
the status lane under `node/<id>`; see [host-facts.md](host-facts.md).

| Variable | Default | Valid values | Failure behavior |
| --- | --- | --- | --- |
| `ORION_NODE_IMAGE_VERSION_FILE` | unset (none) | `:`-separated list of files; the first readable one declares the system image (`KEY=value` lines with `IMAGE_ID`/`IMAGE_NAME`/`NAME`/`ID` and `IMAGE_VERSION`/`VERSION_ID`/`VERSION`, or a single version line). Overrides `IMAGE_ID`/`IMAGE_VERSION` from `os-release`. | Invalid Unicode fails startup. Unreadable files are skipped. |

Programmatic callers use `NodeRuntimeTuning::host_facts` (`HostFactsTuning::with_refresh_interval`,
`with_image_files`) and replace or extend the source with `NodeAppBuilder::with_host_facts_source`
and `with_host_facts_overlay`.

## Actions

Generic actions (see [actions.md](actions.md)) are tracked in memory only, bounded by
`ORION_NODE_ACTION_MAX_TRACKED`, and expire `ORION_NODE_ACTION_RESULT_TTL_MS` after they finish.
A request's `deadline_ms` of `0` uses `ORION_NODE_ACTION_DEFAULT_DEADLINE_MS`; longer deadlines are
capped at `ORION_NODE_ACTION_MAX_DEADLINE_MS` (all under Runtime Tuning). Programmatic callers use
`NodeRuntimeTuning::actions` (`ActionTuning`) and register node-side handlers with
`NodeAppBuilder::with_action_handler`.
### Observed-state write coalescing

Desired state is durable: every desired-state commit (local or remote mutations, snapshot
adoption, provider or executor record changes, leases) writes the state bundle and fsyncs it
before the commit is acknowledged.

Observed and applied state change far more often (every provider or executor snapshot, peer
observed updates, reconcile results) and are only a cache of what providers and executors report.
While the reconcile loop runs (`NodeApp::spawn_reconcile_loop`, which `orion-node` always starts),
those changes only mark the state dirty, and a background coalescer writes it at most once per
`ORION_NODE_OBSERVED_PERSIST_INTERVAL_MS` (default `2000`):

- the first change after an idle interval is written at once; changes within the interval after a
  write are collected into one write at the end of the interval;
- a desired-state commit writes the full bundle, so it also carries any pending observed change;
- graceful shutdown (stopping the reconcile loop) flushes pending changes;
- `0` restores the previous behaviour (every observed change is written immediately), and embedded
  users that never start the reconcile loop also keep immediate writes.

Crash semantics: after an unclean stop (power loss, `SIGKILL`) the persisted observed and applied
state can be up to one interval stale. Nothing durable is lost: desired state is unaffected,
providers and executors republish full snapshots when they reconnect (link devices resend theirs
on every session), and reconcile re-derives applied state. `resource_usage.observed_persistence`
(`orionctl get memory -o json`) reports the interval, whether a write is pending, and how many
changes were coalesced.

### Volatile status lane limits

The status lane (see `docs/observability.md`) is in memory only and bounded:

- `ORION_NODE_STATUS_MAX_ENTRIES` (default `4096`) caps entries node-wide;
- `ORION_NODE_STATUS_MAX_ENTRIES_PER_PUBLISHER` (default `256`, at most the node cap) caps the
  entries one local client or link device holds;
- `ORION_NODE_STATUS_MAX_TTL_MS` (default `300000`) is the longest TTL; entries published with
  `ttl_ms = 0` get this TTL, and longer TTLs are capped;
- keys are at most 128 bytes and string or byte values at most 1024 bytes.

A batch that would exceed a cap, or that has an invalid entry, is rejected as a whole and counted
in `resource_usage.status_lane.dropped_total`.

## HTTP TLS

| Variable | Default | Valid values | Failure behavior |
| --- | --- | --- | --- |
| `ORION_NODE_HTTP_TLS_CERT` | unset | Filesystem path | Invalid Unicode fails startup. |
| `ORION_NODE_HTTP_TLS_KEY` | unset | Filesystem path | Invalid Unicode fails startup. |
| `ORION_NODE_HTTP_TLS_AUTO` | `false` | `1`, `0`, `true`, `false`, `yes`, `no`, `on`, `off` | Invalid boolean fails startup. |

`ORION_NODE_HTTP_TLS_CERT` and `ORION_NODE_HTTP_TLS_KEY` must either both be set or both be unset.

## Runtime Tuning

All of the following accept integer values. Invalid integers fail startup. Queue and count values
are normalized to a minimum effective value of `1`. Duration values are interpreted as milliseconds
and normalized to a minimum effective value of `1ms`.

Programmatic callers can tune the same surface through `NodeRuntimeTuning` fluent setters and
apply it with `NodeConfig::with_runtime_tuning(...)` or `NodeConfig::with_runtime_tuning_mut(...)`.

| Variable | Default | Notes |
| --- | --- | --- |
| `ORION_NODE_MAX_MUTATION_HISTORY` | `256` | Max mutation history batches retained in memory. |
| `ORION_NODE_MAX_MUTATION_HISTORY_BYTES` | `1048576` | Max retained mutation history bytes. |
| `ORION_NODE_SNAPSHOT_REWRITE_CADENCE` | `1` | Snapshot rewrite cadence in persist cycles. |
| `ORION_NODE_PEER_SYNC_BACKOFF_BASE_MS` | `250` | Base peer sync retry backoff. |
| `ORION_NODE_PEER_SYNC_BACKOFF_MAX_MS` | `5000` | Max peer sync retry backoff. |
| `ORION_NODE_PEER_SYNC_BACKOFF_JITTER_MS` | `150` | Peer sync retry jitter. |
| `ORION_NODE_PEER_SYNC_SMALL_CLUSTER_THRESHOLD` | `4` | Cluster size threshold for small-cluster parallelism. |
| `ORION_NODE_PEER_SYNC_SMALL_CLUSTER_CAP` | `4` | Max in-flight peers for small clusters. |
| `ORION_NODE_PEER_SYNC_LARGE_CLUSTER_CAP` | `3` | Max in-flight peers for larger clusters. |
| `ORION_NODE_PEER_SYNC_NO_STAGGER_THRESHOLD` | `4` | Cluster size threshold before spawn staggering applies. |
| `ORION_NODE_PEER_SYNC_SPAWN_STAGGER_STEP_MS` | `5` | Per-peer stagger step in parallel sync. |
| `ORION_NODE_PEER_SYNC_SPAWN_STAGGER_MAX_MS` | `20` | Upper bound for stagger delay. |
| `ORION_NODE_PEER_SYNC_FOLLOWUP_STAGGER_MS` | `5` | Delay before follow-up sync fanout. |
| `ORION_NODE_LOCAL_RATE_LIMIT_WINDOW_MS` | `1000` | Local control-plane rate-limit window. |
| `ORION_NODE_LOCAL_RATE_LIMIT_MAX_MESSAGES` | `256` | Max local messages per rate-limit window. |
| `ORION_NODE_LOCAL_SESSION_TTL_MS` | `300000` | Local session TTL. |
| `ORION_NODE_LOCAL_STREAM_SEND_QUEUE_CAPACITY` | `64` | Per-stream send queue capacity. |
| `ORION_NODE_LOCAL_CLIENT_EVENT_QUEUE_LIMIT` | `256` | Local client event backlog limit. |
| `ORION_NODE_OBSERVABILITY_EVENT_LIMIT` | `128` | Retained observability events. |
| `ORION_NODE_TRANSPORT_MAX_PAYLOAD_BYTES` | `8388608` | Max inbound HTTP, IPC, TCP, and QUIC payload bytes before transport decode. |
| `ORION_NODE_TRANSPORT_IO_TIMEOUT_MS` | `5000` | Per-operation transport connect/read/write/handshake timeout. |
| `ORION_NODE_TRANSPORT_MAX_CONCURRENT_CONNECTIONS` | `1024` | Max in-flight accepted connections per managed transport listener. |
| `ORION_NODE_PERSISTENCE_WORKER_QUEUE_CAPACITY` | `64` | Persistence worker queue capacity. |
| `ORION_NODE_AUTH_STATE_WORKER_QUEUE_CAPACITY` | `128` | Auth state worker queue capacity. |
| `ORION_NODE_AUDIT_LOG_QUEUE_CAPACITY` | `1024` | Audit log worker queue capacity. |
| `ORION_NODE_RECONCILE_BACKSTOP_MS` | `5000` | Longest idle period before the event-driven reconcile loop runs a periodic backstop pass. Clamped to at least `ORION_NODE_RECONCILE_MS`. |
| `ORION_NODE_HLC_MAX_DRIFT_MS` | `300000` | Largest distance a peer's hybrid-logical-clock timestamp may be ahead of the local wall clock; desired-state versions stamped further ahead are rejected and counted (`desired_merge.clock_skew_rejections`). See `docs/peer-sync.md`. |
| `ORION_NODE_LIVENESS_TIMEOUT_MS` | `5000` | A peer not heard from (sync round, signed request, observed push) for this long is considered gone: placement stops choosing it and cross-node bindings to its resources become unavailable. See `docs/placement.md`. |
| `ORION_NODE_PLACEMENT_GRACE_MS` | `10000` | How long a workload's assignee (or a cross-node binding's owner) must stay gone or ineligible before the workload moves (or the lease is released and re-resolved). See `docs/placement.md`. |
| `ORION_NODE_TOMBSTONE_RETENTION_MS` | `604800000` | How long desired-state tombstones (deletes) are kept before collection. A node offline for longer than this can resurrect deleted objects. See `docs/peer-sync.md`. |
| `ORION_NODE_HOST_FACTS_REFRESH_MS` | `10000` | How often host facts are sampled; `0` turns them off. Status-lane host metrics live three intervals (see `docs/host-facts.md`). |
| `ORION_NODE_ACTION_DEFAULT_DEADLINE_MS` | `30000` | Deadline of actions submitted with `deadline_ms = 0` (at most the maximum). |
| `ORION_NODE_ACTION_MAX_DEADLINE_MS` | `600000` | Longest accepted action deadline; longer ones are capped. |
| `ORION_NODE_ACTION_RESULT_TTL_MS` | `600000` | How long a finished action's result stays queryable. |
| `ORION_NODE_ACTION_MAX_TRACKED` | `256` | Most actions tracked at once; the oldest finished results are evicted first, and new requests are refused while every tracked action is still running. |
| `ORION_NODE_CLOCK_REFRESH_MS` | `10000` | How often the node re-reads its clock state. The observed node record is only republished on meaningful change (see `docs/observability.md`, Clock Facts). |
| `ORION_NODE_OBSERVED_PERSIST_INTERVAL_MS` | `2000` | Shortest spacing between coalesced observed/applied state writes while the reconcile loop runs. `0` writes every change immediately (not normalized to `1`). Desired-state commits are never delayed. See "Observed-state write coalescing". |
| `ORION_NODE_STATUS_MAX_ENTRIES` | `4096` | Node-wide cap on volatile status lane entries. |
| `ORION_NODE_STATUS_MAX_ENTRIES_PER_PUBLISHER` | `256` | Status lane entries one local client or link device may hold (clamped to the node-wide cap). |
| `ORION_NODE_STATUS_MAX_TTL_MS` | `300000` | Longest status entry TTL, also used for entries published with `ttl_ms = 0`. |
| `ORION_NODE_IPC_STREAM_HEARTBEAT_INTERVAL_MS` | `5000` | `50` in test builds. IPC stream heartbeat interval. |
| `ORION_NODE_IPC_STREAM_HEARTBEAT_TIMEOUT_MS` | `15000` | `125` in test builds. IPC stream heartbeat timeout. |
