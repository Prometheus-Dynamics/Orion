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
| `ORION_NODE_RUNTIME_WORKER_THREADS` | `2` | Positive integer | Zero or invalid integer fails startup. |
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

A standalone node that only serves local IPC clients can shed most of its network surface. The
settings below are collected in `packaging/systemd/orion-node.env.example`, and "Running under
systemd" below covers the service side:

- build with `cargo build -p orion-node --release --no-default-features` to get an IPC-only binary. This build drops the HTTP stack (the `transport-http` feature: axum, hyper, reqwest, and rustls), the `orion+tcp` peer transport, and the TCP and QUIC data-plane transports. Add `--features transport-http`, `peer-tcp`, `transport-tcp`, or `transport-quic` to keep any of them.
- to cluster such appliances without the HTTP stack, build with `--no-default-features --features peer-tcp`, set `ORION_NODE_PEER_ADDR`, and list peers as `node-id=orion+tcp://host:port|<public-key-hex>` (see `docs/peer-sync.md` for the threat model: requests and responses are signed, traffic is not encrypted).
- with a default (HTTP-enabled) build, set `ORION_NODE_HTTP_ADDR=off` to skip the HTTP control listener. The optional probe listener on `ORION_NODE_HTTP_PROBE_ADDR` still works.
- in a build without `transport-http`, the HTTP listener is always off:
  - `ORION_NODE_HTTP_ADDR` must be unset or one of `off`, `disabled`, or `none`. Setting it to an address fails startup, because the build cannot serve it.
  - `http://`/`https://` entries in `ORION_NODE_PEERS`, `ORION_NODE_HTTP_PROBE_ADDR`, `ORION_NODE_HTTP_TLS_CERT`, `ORION_NODE_HTTP_TLS_KEY`, and `ORION_NODE_HTTP_TLS_AUTO` fail startup with an error that names the missing feature. `orion+tcp://` peers likewise need the `peer-tcp` feature.
  - Embedders that pass peers or HTTP TLS files to `NodeApp::builder()` get the same error from `try_build()`.
- set `ORION_NODE_RUNTIME_WORKER_THREADS=1` or `2` and lower `ORION_NODE_MAX_MUTATION_HISTORY*` and worker queue capacities to fit the device memory budget
- on glibc targets, keep the packaged unit's malloc settings: `MALLOC_ARENA_MAX=1` with 64 KiB `MALLOC_MMAP_THRESHOLD_`, `MALLOC_TOP_PAD_` and `MALLOC_TRIM_THRESHOLD_`. On a reference board with one pipeline, after three workload rewrites, they held 8.4 MiB PSS against 13.6 MiB with `MALLOC_ARENA_MAX=2` alone. On a reference board (reference board) under the device runtime's load, the packaged node after 7 days (2026-10-31): 9.8 MiB PSS, 11.3 MiB RSS, 6.9 MiB private dirty, 0 restarts
- on the same board and load, the packaged release node sampled every 10 min for 20.0 h (2026-11-02, stopped early, not a full 24 h; the board's pinned release, before protocol 1). One process throughout, 0 restarts, 9 threads:

  | Hours since first sample | PSS low to high (MiB) | RSS low to high (MiB) |
  |---|---|---|
  | 0 (restart) | 5.4 | 8.2 |
  | 0 to 2 | 8.1 to 9.3 | 9.6 to 10.0 |
  | 2 to 17 | 8.3 to 9.2 | 9.9 to 10.7 |
  | 17 to 20 | 8.5 to 8.8 | 10.1 to 10.2 |

  The resting PSS held at 8.4 MiB from hour 4, then stepped up 0.15 MiB at hour 17 and stayed there, with short peaks above it. That step is most of the fitted slope after hour 6, 9 KiB/h. A run this short cannot separate rare steps like it from the 9.8 MiB seen after 7 days.
- keep the default allocator (glibc malloc). In every appliance soak run, the opt-in `alloc-jemalloc` and `alloc-mimalloc` features below used more memory than glibc
- check transparent huge pages (THP) on the device: `cat /sys/kernel/mm/transparent_hugepage/enabled` and `grep AnonHugePages /proc/$(pidof orion-node)/smaps_rollup`. With THP set to `always`, the kernel can back sparsely used heap regions with whole huge pages. On a kernel with 16 KiB pages (common on small arm64 boards), a PMD huge page is 32 MiB instead of 2 MiB, so check `getconf PAGESIZE` too. If `AnonHugePages` is a large share of `Anonymous`, boot with `transparent_hugepage=madvise` (or write `madvise` to that sysfs file)

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
25 MiB of extra anonymous memory measured on a device image, so arenas alone are unlikely to explain it.

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

These measurements were taken on x86_64, not on the arm64 board. On the device, compare `RssAnon` and
`AnonHugePages` with and without `MALLOC_ARENA_MAX=2` and with THP set to `madvise` before you
switch allocators.

## Running under systemd

The `systemd-notify` cargo feature (off by default, Unix only, no libsystemd dependency) makes
`orion-node` a `Type=notify` service with a working watchdog. `packaging/systemd/` has the unit,
the environment file, and the user and preset files that go with it; `packaging/README.md`
explains how to install them and `packaging/gaia/orion-node.toml` packages them for Gaia images.

```sh
cargo build -p orion-node --release --no-default-features --features peer-tcp,discovery-mdns,systemd-notify
```

The node reads the variables systemd sets; it does not need any `ORION_NODE_*` setting for this.

| Variable | Set by | Effect |
| --- | --- | --- |
| `NOTIFY_SOCKET` | systemd (`Type=notify`) | Notification socket: an absolute path or an `@abstract` name (Linux). Unset or empty: no notifications. Other forms (for example `vsock:`) are ignored with a warning. |
| `WATCHDOG_USEC` | systemd (`WatchdogSec=`) | Enables watchdog pings every `WATCHDOG_USEC / 2`. Invalid values are ignored with a warning. |
| `WATCHDOG_PID` | systemd | When set to another PID than the node's, the node does not ping. |

What the node sends:

- `READY=1` with `STATUS=serving node=... ipc=... peer_tcp=... http=... links=... peers=...`, once
  state replay is done and every listener accepts connections: IPC and IPC stream sockets, the
  HTTP and probe listeners, the `orion+tcp` peer listener, the link gateway, and mDNS discovery.
  Units ordered `After=orion-node.service` therefore start against a node that answers.
- `WATCHDOG=1` every `WATCHDOG_USEC / 2`, but only while the node is live. Liveness is the
  background reconcile loop finishing passes (`NodeApp::reconcile_loop_heartbeat()`). At every
  ping interval the node checks that the loop completed a pass since the previous check, pings if
  it did, and then asks the loop for another pass (`NodeApp::request_reconcile()`), so an idle
  node still proves liveness without waiting for the reconcile backstop. A pass that never
  returns, a loop that stopped, or a runtime too starved to run the check stops the pings; the
  status line then reads `reconcile loop stalled; withholding watchdog pings`, and systemd kills
  and restarts the node once `WatchdogSec=` has passed since the last ping. Pings resume if the
  loop recovers first. The check costs at most one extra reconcile pass per ping interval.
- `STOPPING=1` when shutdown starts (`SIGTERM`, `SIGINT`, or `ORION_NODE_SHUTDOWN_AFTER_INIT_MS`),
  before the listeners close.

`orion-node` handles `SIGTERM` (the signal systemd and container runtimes stop services with) like
`SIGINT` in every build: it stops the loops, flushes coalesced observed state, and closes the
listeners before it exits.

Tuning `WatchdogSec=` (default `30s` in the unit): a reconcile pass must complete within about
half of it. Raise it on slow storage or very large desired state, or set `WatchdogSec=0` in a
drop-in to disable the watchdog. Start-up (state replay) is not covered by the watchdog but by
`TimeoutStartSec=` (`120s` in the unit).

## Peer and Auth Controls

| Variable | Default | Valid values | Failure behavior |
| --- | --- | --- | --- |
| `ORION_NODE_PEERS` | unset | Comma-separated `node-id=<url>` entries where `<url>` is `http://host:port`, `https://host:port` (feature `transport-http`) or `orion+tcp://host:port` (feature `peer-tcp`); optional `|ca=/path` (HTTPS) and trusted public key segments. HTTP and TCP peers can be mixed. | Invalid entry format, an unknown scheme, or a scheme whose feature is not compiled in fails startup. |
| `ORION_NODE_PEER_AUTH` | `optional` | `disabled`, `optional`, `required` | Invalid mode fails startup. |
| `ORION_NODE_ROBOT_LINKS` | unset | Comma-separated IPv4 or IPv6 subnets (`192.0.2.0/24`, `fd00:7::/64`) or single addresses that are the device's own links (USB gadget network, private Ethernet). See "Private links" in `docs/discovery.md`. | Invalid subnet or prefix fails startup. |
| `ORION_NODE_PEER_AUTO_TRUST` | `private-link` with `ORION_NODE_PEER_AUTH=disabled`, otherwise `off` | `off`, `private-link` | Invalid value fails startup. `private-link` without `ORION_NODE_ROBOT_LINKS` fails startup when set explicitly; as the default it is off. With security on (`optional`, `required`) it never applies. |
| `ORION_NODE_PEER_SYNC_MODE` | `parallel` | `serial`, `parallel` | Invalid mode fails startup. |
| `ORION_NODE_PEER_SYNC_MAX_IN_FLIGHT` | `4` | Integer, minimum effective value `1` | Invalid integer fails startup. |
| `ORION_NODE_HTTP_MTLS` | `disabled` | `disabled`, `optional`, `required` | Invalid mode fails startup. |
| `ORION_NODE_LOCAL_AUTH` | `same-user` | `disabled`, `same-user`, `same-user-or-group`, `same-user-or-group-or-root` (see "Local IPC access" below) | Invalid mode fails startup. |
| `ORION_NODE_LOCAL_AUTH_ALLOW` | unset | Comma-separated extra callers admitted in every mode but `disabled`: `root`, `uid:<n>`, `gid:<n>`, `user:<name>`, `group:<name>` | A malformed entry or an unknown user or group name fails startup. |
| `ORION_NODE_LOCAL_PROVIDER_ALLOW` | unset | Comma-separated callers (same syntax as `ORION_NODE_LOCAL_AUTH_ALLOW`) admitted to the provider lane only: registering as a provider or executor, publishing its state, status and streams, and handling actions. Not admitted to control-plane operations. | A malformed entry or an unknown user or group name fails startup. |
| `ORION_NODE_LABELS` | unset | Comma-separated `key=value` or bare `key` labels, published in the node's observed record and matched by workload node selectors (see `docs/placement.md`). Blank terms are ignored; the last value of a repeated key wins. | Non-UTF-8 value fails startup. |

### Local IPC access

Every local IPC connection carries the caller's Unix credentials (`SO_PEERCRED`: uid and primary
gid; on Linux 4.13 and newer also the supplementary groups, `SO_PEERGROUPS`). The node admits a
caller when `ORION_NODE_LOCAL_AUTH` or `ORION_NODE_LOCAL_AUTH_ALLOW` says so:

| `ORION_NODE_LOCAL_AUTH` | Admits |
| --- | --- |
| `disabled` | anyone who can open the socket (file permissions are the only check) |
| `same-user` (default) | callers running as the node's user |
| `same-user-or-group` | also callers whose primary **or supplementary** groups include the node's primary group |
| `same-user-or-group-or-root` | also root (uid 0), for appliances where operators work in a root shell |

`ORION_NODE_LOCAL_AUTH_ALLOW` admits more callers on top of the mode (ignored with `disabled`):
`root` (uid 0), `uid:<n>`, `user:<name>`, and `gid:<n>` / `group:<name>` (members by primary or
supplementary group). Names are resolved once at startup from the system user database
(`getpwnam_r` / `getgrnam_r`), so the user or group must exist before the node starts.

The socket's file permissions still apply before any of this: the packaged service creates
`/run/orion` with mode 0750 and the sockets group-connectable (`UMask=0007`), so callers other
than root also need to be in the `orion` group, or the image adds an ACL. Typical settings:

```sh
# Appliance images: services in the orion group, operators as root.
ORION_NODE_LOCAL_AUTH=same-user-or-group-or-root
# The same with an explicit allow-list (for example an operator group that is also given
# access to /run/orion):
ORION_NODE_LOCAL_AUTH=same-user-or-group
ORION_NODE_LOCAL_AUTH_ALLOW=root,group:wheel
```

`ORION_NODE_LOCAL_PROVIDER_ALLOW` admits a narrower set: a caller in it that is not admitted by the
mode or `ORION_NODE_LOCAL_AUTH_ALLOW` may register as a **provider or executor** (`ClientHello` with
role `Provider` or `Executor`), publish its provider or executor state, status and streams, and
handle actions (`WatchActionRequests`, `ClaimNodeActions`, `ReportActionResult`). Every other
operation, such as `RunAction`, `Mutations`, `WatchState` or `PollClientEvents`, is refused for it.
This is how a device agent that is not the node's own user (for example an on-device agent)
can be a provider and handle actions without operator rights:

```sh
ORION_NODE_LOCAL_PROVIDER_ALLOW=user:device-agent
```

A refused caller gets an authorization error naming its uid, gid and groups and these variables. Embedders set the same with `NodeAppBuilder::with_local_authentication_mode` and
`with_local_access_allow(LocalAccessAllowList::new().allow_uid(0).allow_gid(..))`.

### Blob push limits

Blob push (see [blobs.md](blobs.md)) stores files that local clients push in chunks.

| Variable | Default | Valid values | Failure behavior |
| --- | --- | --- | --- |
| `ORION_NODE_BLOB_DIR` | unset (`<state dir>/blobs`) | Filesystem path | Blob push is off when neither this nor `ORION_NODE_STATE_DIR` is set. |
| `ORION_NODE_BLOB_MAX_BYTES` | `2147483648` | Integer bytes, minimum `1` | Invalid integer fails startup. |
| `ORION_NODE_BLOB_CHUNK_MAX_BYTES` | `1048576` | Integer bytes, clamped to `1`..`4194304` | Invalid integer fails startup. |

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

With a state directory the gateway keeps `<state dir>/link-devices.rkyv`: the providers link devices
own, restored at start as unavailable until their device reconnects (see "Restart" there). Removing a
link from `ORION_NODE_LINKS` forgets its providers at the next start. The file is not part of the
quarantined cluster state.

| Variable | Default | Valid values | Failure behavior |
| --- | --- | --- | --- |
| `ORION_NODE_LINKS` | unset (no links) | `;`-separated `<kind>:<target>[?key=value&...]` entries, `kind` = `serial`, `can` or `unix` | Any malformed entry, unknown or misplaced key, out-of-range value, duplicate key, or duplicate link fails startup with `NodeError::Config` naming the entry. A port, interface or socket that cannot be opened does not fail startup; the link retries every second and logs the error. |

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

`unix:<path>` keys (a Unix stream socket the node listens on; one session per connection, COBS
framed as on a serial port; the path must be absolute and shorter than 108 bytes):

| Key | Default | Valid values | Meaning |
| --- | --- | --- | --- |
| `max_connections` | `32` | `1`-`1024` | Connections served at once; a connection past it is closed at once. |
| `max_frames_per_s` | `8192` | `1`-`1000000` | Frames each connection may send per second, sustained; a burst of one second's worth is read at once. |

A Unix link judges each caller at connect by its `SO_PEERCRED` identity, like the control sockets:
`ORION_NODE_LOCAL_AUTH` and `ORION_NODE_LOCAL_AUTH_ALLOW` admit it, and so does
`ORION_NODE_LOCAL_PROVIDER_ALLOW` (a provider session needs no more). A caller admitted by none is
disconnected. Frames from one connection are read within its own budget (`max_frames_per_s`, a
token bucket holding one second's worth; not the control sockets' rate limit). Past it, each read
waits only for the next frame's turn, and the socket pushes back on the sender, so nothing is
dropped. A connection held back this way counts as heard: its session never times out for
frames the node made wait, whatever the heartbeat. Output waiting for a connection
is bounded at 64 KiB; a connection that takes no output for 5 s is closed. Answers a client
session has asked for but not yet taken are bounded at 256 KiB (each answer is at most 32
frames): past it, status and action queries are refused with `limit` and the connection is not
read until the answers drain, again without timing the session out. A second connection
whose `Hello` names a device already connected on the link is closed. A stale socket at the path is
replaced at start and removed at shutdown; any other file there is left alone and the link reports
the error. `link.release`, `link.attach` and firmware transfers do not apply to Unix links.

A Unix link session may take the client role (`docs/link-protocol.md`, "Revisions"). Its requests
are those of a local control-plane client named `link:<link>/<device name>`, authorized with the
caller's identity per request: a caller admitted only by `ORION_NODE_LOCAL_PROVIDER_ALLOW` is
refused (`not authorized`). A session holds one status watch, one action watch, at most 16
unfinished actions it ran, and at most 8 stream subscriptions. A provider session also declares
and publishes streams of its own provider (revision 2).

At revision 3 a session may also take the executor role. It registers a local executor client
named `link:<link>/<device name>#executor`, admitted like a provider session (the provider lane).
Its `ExecutorState` is applied as that executor's state, with the node's own id; it gets the
workloads the node assigns to it, then each change. A client session reads records (workloads with
their observed state, resources, providers, executors) by query, or holds one record watch, which
sends only the records that changed since its last answer and the keys of those gone.

Example: `ORION_NODE_LINKS='serial:/dev/ttyAMA0?baud=115200&allow=imu-board;can:can0?device_base=0x600&host_base=0x680&addresses=1-16&allow=motor-a,motor-b'`.

## Safe mode

Safe mode starts the node so that it can be inspected and repaired without doing harm
([update-recovery.md](update-recovery.md#safe-mode)). In safe mode the node runs in `maintenance`
with no workloads allowed (so it is unschedulable), refuses maintenance changes other than
`orionctl maintenance exit`, and receives peer state without pushing its own. Local IPC, health and
observability keep working. Health shows `safe_mode: <cause>` in its reasons and status `degraded`.

| Variable | Default | Valid values | Failure behavior |
| --- | --- | --- | --- |
| `ORION_NODE_SAFE_MODE` | `false` | `1`, `0`, `true`, `false`, `yes`, `no`, `on`, `off` | Invalid boolean fails startup. |

Safe mode is also entered without the variable when:

- persisted cluster state cannot be decoded (a desired or applied section, say). The state files (`snapshot-*.rkyv`, `mutation-history*.rkyv`) are moved to
  `<state dir>/quarantine-<unix ms>/` and never deleted, and the node starts with an empty desired
  state. The same goes for an unreadable maintenance state file, which is moved on its own.
- the node identity key (`node-identity.ed25519`) cannot be read. The node runs on a throwaway identity, does
  not touch the key file, and does not sync with peers at all.

The cause is written to `<state dir>/safe-mode.txt`, so a restart stays in safe mode until the
operator runs `orionctl maintenance exit` and restarts the node. Removing `ORION_NODE_SAFE_MODE`
alone is not enough while the marker exists.

## Persistence and Logging

| Variable | Default | Valid values | Failure behavior |
| --- | --- | --- | --- |
| `ORION_NODE_STATE_DIR` | unset | Filesystem path | Invalid Unicode fails startup. |
| `ORION_NODE_AUDIT_LOG` | unset | Filesystem path | Invalid Unicode fails startup. |
| `ORION_NODE_SHUTDOWN_AFTER_INIT_MS` | unset | Integer milliseconds | Invalid integer fails startup. Intended for tests and controlled automation, not steady-state production. |

### State directory format

The node reads one snapshot format, format 1, the layout of the first release. The manifest
(`snapshot-manifest.rkyv`) names the format. A state directory holding any other format, or a
manifest that does not decode with the current layout (a pre-release directory, or a damaged
manifest), is not read and never blocks startup:

- the cluster state files (`snapshot-*.rkyv`, `mutation-history*.rkyv`) are renamed into
  `<state dir>/unsupported-format-<unix ms>/`; nothing is deleted;
- the node logs one error that names the format it found, the format it reads (1), and the moved
  directory;
- the node starts with an empty desired state and gets its desired state back from peers and
  local clients, as on a fresh node.

This is not safe mode: the maintenance state and the other stores are left as they are. Damaged
state in a readable directory (a decodable manifest whose applied or desired section does not
decode) is quarantined to `<state dir>/quarantine-<unix ms>/` and starts safe mode, see "Safe
mode" above.

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

## Update intents

Update intents (`docs/update-recovery.md`, "Data model") say which version of a component a node
should install. They are desired state: any local control-plane client (`orionctl apply update`)
may write them, as it may write any desired state. Remote operators may write them only when they
are listed here:

| Variable | Default | Meaning |
| --- | --- | --- |
| `ORION_NODE_UPDATE_REQUESTERS` | unset (none) | Comma-separated operator ids (`operator:<name>`, or `<name>`) that may write update intents over the signed peer transport (`RemoteOperator::apply_update_intents`). A listed operator's batch is accepted only when every mutation is an update-intent write or removal; anything else is refused as a whole. Operators not listed are refused. Every accepted write and removal is audited (`update_intent_written`, `update_intent_removed`, see `docs/audit-logging.md`). |

Intents are validated before they are committed: the id must be `<node>/<component>`, strings are
bounded (component and versions 128 bytes, error text 512, argument names 64) and at most 32
arguments, a drain timeout is 1 ms to 24 h.

## Actions

Generic actions (see [actions.md](actions.md)) are tracked in memory only, bounded by
`ORION_NODE_ACTION_MAX_TRACKED`, and expire `ORION_NODE_ACTION_RESULT_TTL_MS` after they finish.
A `RunAction` with `wait_ms` over a unary exchange is held at most `ORION_NODE_ACTION_MAX_WAIT_MS`
before the node answers with the current result.
A request's `deadline_ms` of `0` uses `ORION_NODE_ACTION_DEFAULT_DEADLINE_MS`; longer deadlines are
capped at `ORION_NODE_ACTION_MAX_DEADLINE_MS` (all under Runtime Tuning). Programmatic callers use
`NodeRuntimeTuning::actions` (`ActionTuning`) and register node-side handlers with
`NodeAppBuilder::with_action_handler`.
### Observed-state write coalescing

Desired state and identities are durable: every desired-state commit (local or remote mutations,
snapshot adoption, provider or executor records, leases) writes the state bundle and fsyncs it
before the commit is acknowledged.

Observed state (provider, executor and resource status, workload observations) is never written.
It is runtime status that its owners republish: after a restart it starts empty ("pending"),
reconcile re-derives what it can, and executors and providers republish on reconnect (link devices
resend theirs on every session). Observed changes only wake the observed watchers.

Applied state (the applied revision) is still persisted. While the reconcile loop runs
(`NodeApp::spawn_reconcile_loop`, which `orion-node` always starts) its changes are coalesced by a
background coalescer, which writes at most once per `ORION_NODE_OBSERVED_PERSIST_INTERVAL_MS`
(default `30000`):

- the first change after an idle interval is written at once; changes within the interval after a
  write are collected into one write at the end of the interval;
- a desired-state commit writes the full bundle, so it also carries any pending applied change;
- graceful shutdown (stopping the reconcile loop) flushes pending changes;
- `0` writes every applied change immediately, and embedded users that never start the reconcile
  loop also keep immediate writes.

Crash semantics: after an unclean stop (power loss, `SIGKILL`) the persisted applied state can be up
to one interval stale. Nothing durable is lost: desired state is unaffected and observed state was
never durable. `resource_usage.observed_persistence` (`orionctl get memory -o json`) reports the
interval, whether a write is pending, and how many changes were coalesced.

A node upgraded from a format that stored observed state deletes the observed file on its first
write; the observed section is not read back.

### Volatile status lane limits

The status lane (see `docs/observability.md`) is in memory only and bounded:

- `ORION_NODE_STATUS_MAX_ENTRIES` (default `4096`) caps entries node-wide;
- `ORION_NODE_STATUS_MAX_ENTRIES_PER_PUBLISHER` (default `256`, at most the node cap) caps the
  entries one local client or link device holds;
- `ORION_NODE_STATUS_MAX_TTL_MS` (default `300000`) is the longest TTL; entries published with
  `ttl_ms = 0` get this TTL, and longer TTLs are capped;
- `ORION_NODE_STATUS_MAX_VALUE_BYTES` (default `4096`, at most `65536`) is the longest string or
  byte value; the lane refuses longer ones;
- `ORION_NODE_STATUS_MAX_BYTES` (default `4194304`, 4 MiB) caps the bytes of string and byte values
  held node-wide, so the lane's memory stays bounded whatever the value size. A batch that would
  exceed it is refused whole. With the default cap, a full lane's values fit in one 8 MiB control
  frame (`ORION_NODE_TRANSPORT_MAX_PAYLOAD_BYTES`) with its keys, so `QueryStatus` answers stay under
  the frame limit;
- keys are at most 128 bytes.

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
| `ORION_NODE_MAX_MUTATION_HISTORY_BYTES` | `262144` | Max retained mutation history bytes. The history keeps a full copy of each revision's records, so this is the main per-revision memory cost; a peer that lags further than the history catches up by snapshot. |
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
| `ORION_NODE_LOCAL_CLIENT_EVENT_QUEUE_LIMIT` | `256` | Local client event backlog limit (by count). |
| `ORION_NODE_LOCAL_CLIENT_EVENT_QUEUE_BYTES` | `1048576` | Local client event backlog limit by bytes. Full executor and provider lists are charged their encoded length; other events a fixed 256 bytes, and stream samples their data. Overflow drops the oldest events, as for the count limit (counted in `orion_local_client_dropped_events`); the newest event is always kept, so one event larger than the limit is held alone. The queued bytes and this limit are in the usage snapshot (`local_streams`, shown by `orionctl get memory`) and in `orion_local_client_event_queue_bytes` / `orion_local_client_event_queue_byte_limit`. |
| `ORION_NODE_OBSERVABILITY_EVENT_LIMIT` | `128` | Retained observability events. |
| `ORION_NODE_TRANSPORT_MAX_PAYLOAD_BYTES` | `8388608` | Max inbound HTTP, IPC, TCP, and QUIC payload bytes before transport decode. |
| `ORION_NODE_TRANSPORT_IO_TIMEOUT_MS` | `5000` | Per-operation transport connect/read/write/handshake timeout. |
| `ORION_NODE_TRANSPORT_MAX_CONCURRENT_CONNECTIONS` | `1024` | Max in-flight accepted connections per managed transport listener. |
| `ORION_NODE_PEER_PUSH_SESSIONS` | `on` | `on`, `off` | Invalid value fails startup. `off`: this node refuses its configured peers' push sessions (`OpenSession`); the peers' forwarded actions then resend `RunAction` until final. `RunAction`, sync and the rest are unchanged. See "Peer watches" in `docs/actions.md`. |
| `ORION_NODE_PERSISTENCE_WORKER_QUEUE_CAPACITY` | `32` | Persistence worker queue capacity. |
| `ORION_NODE_AUTH_STATE_WORKER_QUEUE_CAPACITY` | `128` | Auth state worker queue capacity. |
| `ORION_NODE_AUDIT_LOG_QUEUE_CAPACITY` | `1024` | Audit log worker queue capacity. |
| `ORION_NODE_AUDIT_LOG_MAX_BYTES` | `4194304` | Size at which `ORION_NODE_AUDIT_LOG` rotates to `<path>.1`, replacing the previous rotation (one rotated file is kept). The file stays open between records. A record larger than the cap is still written. Minimum 1. |
| `ORION_NODE_RECONCILE_BACKSTOP_MS` | `5000` | Longest idle period before the event-driven reconcile loop runs a periodic backstop pass. Clamped to at least `ORION_NODE_RECONCILE_MS`. |
| `ORION_NODE_HLC_MAX_DRIFT_MS` | `300000` | Largest distance a peer's hybrid-logical-clock timestamp may be ahead of the local wall clock; desired-state versions stamped further ahead are rejected and counted (`desired_merge.clock_skew_rejections`). See `docs/peer-sync.md`. |
| `ORION_NODE_LIVENESS_TIMEOUT_MS` | `5000` | A peer not heard from (sync round, signed request, observed push) for this long is considered gone: placement stops choosing it and cross-node bindings to its resources become unavailable. See `docs/placement.md`. |
| `ORION_NODE_PLACEMENT_GRACE_MS` | `10000` | How long a workload's assignee (or a cross-node binding's owner) must stay gone or ineligible before the workload moves (or the lease is released and re-resolved). See `docs/placement.md`. |
| `ORION_NODE_TOMBSTONE_RETENTION_MS` | `604800000` | How long desired-state tombstones (deletes) are kept before collection. A peer that has not completed a sync round with this node for longer than this is refused in both directions until it is reset (`orionctl peers remove`, then `orionctl peers enroll`). Its clock is kept in `peer-sync-times.json` in the state directory. See the "Stale peers" section of `docs/peer-sync.md`. |
| `ORION_NODE_HOST_FACTS_REFRESH_MS` | `10000` | How often host facts are sampled; `0` turns them off. Status-lane host metrics live three intervals (see `docs/host-facts.md`). |
| `ORION_NODE_ACTION_DEFAULT_DEADLINE_MS` | `30000` | Deadline of actions submitted with `deadline_ms = 0` (at most the maximum). |
| `ORION_NODE_ACTION_MAX_DEADLINE_MS` | `3600000` | Longest accepted action deadline (one hour, for long-running jobs); longer ones are capped. |
| `ORION_NODE_ACTION_RESULT_TTL_MS` | `600000` | How long a finished action's result stays queryable. |
| `ORION_NODE_ACTION_MAX_TRACKED` | `256` | Most actions tracked at once; the oldest finished results are evicted first, and new requests are refused while every tracked action is still running. |
| `ORION_NODE_ACTION_MAX_WAIT_MS` | `2000` | Longest time the node holds one unary answer to a `RunAction` with `wait_ms` (callers resend to keep waiting). Keep it below the transport I/O timeouts. |
| `ORION_NODE_OPERATOR_ACTIONS` | unset (none) | Comma-separated action-name patterns (`*`, `prefix*`, exact names) enrolled remote operators may run when their own policy names none (operators enrolled with the shared key, or approved without `--action`/`--no-actions`). Empty makes such operators read-only. See `docs/remote-operator.md`. |
| `ORION_NODE_OPERATOR_OPEN_READ` | unset (none) | Comma-separated `status`, `streams`, `state`: what operators the node has not enrolled may read (signed requests still; no actions), including push sessions for it. For appliances that are open on their own networks by design. See "Open read and auto-enrollment" in `docs/remote-operator.md`. |
| `ORION_NODE_OPERATOR_AUTO_ENROLL` | unset (none) | `<network>/<prefix>=<pattern>,...` rules separated by `;` (`172.31.0.0/16=reboot,usb-boot`): an operator whose hello comes from the network is enrolled with read access and those action patterns, without an administrator (never one an administrator removed, or one enrolled with another key). Persisted and audited as `network` enrollments. |
| `ORION_NODE_CLOCK_REFRESH_MS` | `10000` | How often the node re-reads its clock state. The observed node record is only republished on meaningful change (see `docs/observability.md`, Clock Facts). |
| `ORION_NODE_OBSERVED_PERSIST_INTERVAL_MS` | `30000` | Shortest spacing between coalesced applied-state writes while the reconcile loop runs. `0` writes every change immediately (not normalized to `1`). Desired-state commits are never delayed. See "Observed-state write coalescing". |
| `ORION_NODE_STATUS_MAX_ENTRIES` | `4096` | Node-wide cap on volatile status lane entries. |
| `ORION_NODE_STATUS_MAX_ENTRIES_PER_PUBLISHER` | `256` | Status lane entries one local client or link device may hold (clamped to the node-wide cap). |
| `ORION_NODE_STATUS_MAX_TTL_MS` | `300000` | Longest status entry TTL, also used for entries published with `ttl_ms = 0`. |
| `ORION_NODE_STATUS_MAX_VALUE_BYTES` | `4096` | Longest string or byte status value (at most 65536). |
| `ORION_NODE_STATUS_MAX_BYTES` | `4194304` | Node-wide budget for the bytes of string and byte status values; at least the value cap. |
| `ORION_NODE_STREAM_MEMORY_MAX_BYTES` | `4194304` | Memory for the history of all live streams together (`docs/streams.md`); a stream declared beyond it keeps proportionally less history. |
| `ORION_NODE_IPC_STREAM_HEARTBEAT_INTERVAL_MS` | `5000` | `50` in test builds. IPC stream heartbeat interval. |
| `ORION_NODE_IPC_STREAM_HEARTBEAT_TIMEOUT_MS` | `15000` | `125` in test builds. IPC stream heartbeat timeout. |
