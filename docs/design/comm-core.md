# Design: the communication core and the node logic (two layers, three tiers)

Status: design note, for review. Nothing is implemented yet. The hello and access slices go
ahead. The extraction, backplane and supervisor slices wait for the user's decision on how far to
split; [split-options.md](split-options.md) is the input for it. The access services (SSH, sftp,
forwarding, recovery shell, the built-in `reboot`) are detailed in
[access-ssh.md](access-ssh.md); this note places them inside the communication core.

## The split

Orion becomes two layers that run as separate processes:

| Layer | Holds | Must |
| --- | --- | --- |
| **Communication core** (`orion-comm`) | Every transport and link: `orion+tcp`, QUIC, HTTP, local IPC, serial/CAN/USB links. Discovery and identity (hello). Auth (signature checks, enrollment, local access). The status and stream relay. The access services: SSH, sftp, forwarding, recovery shell, `reboot`. | Never go down. When the node logic is gone it keeps every link and session up, keeps relaying, keeps answering identity, and lets people in. |
| **Node logic** (`orion-node`) | Desired state, reconcile, placement, jobs and workloads, cluster and peer sync semantics, providers and executors registry, actions registry, persistence, record semantics (ownership, TTL policy, queries), observability of the above. | May crash, restart, upgrade or be absent. Its absence is a reported state, not an outage. |

### Isolation model: separate processes, comm core in front

```
          network / links / IPC clients / SSH
                         │
  ┌──────────────────────▼───────────────────────┐   orion-comm.service
  │ orion-comm (one binary mode, root with a      │   Restart=always, WatchdogSec,
  │ bounded capability set)                       │   OOMScoreAdjust=-900
  │  transports · links · discovery · hello       │
  │  auth · status/stream relay · access (SSH)    │
  │  supervisor (only without systemd)            │
  └───────────────┬──────────────────────────────┘
                  │ backplane: one Unix stream socket, SO_PEERCRED checked,
                  │ framed and already-authenticated messages
  ┌───────────────▼──────────────────────────────┐   orion-node.service
  │ orion-node (user orion, no capabilities)      │   Restart=always
  │  desired state · reconcile · cluster · jobs   │
  │  providers · actions · persistence            │
  └──────────────────────────────────────────────┘
```

Why processes and not an in-process task set or a second runtime:

- **Failure classes an in-process split cannot contain:**
  - a memory leak that ends in an OOM kill;
  - `abort` from a dependency, a stack overflow or a double panic;
  - a blocking call inside a runtime;
  - a deadlock on a lock both layers use;
  - a corrupt state file that crashes start-up;
  - a bad upgrade of the node logic.

  Each of these takes down every task in the process. Only a process boundary survives all of
  them.
- **Privilege.** The access services must start shells as other users, so they need root. The
  node logic needs nothing (today's unit has an empty capability set). One process cannot be both.
  Two processes give each the rights it needs.
- **Upgrade.** The node logic can be replaced and restarted without dropping a single link, SSH
  session or remote operator session.
- **Recovery images.** The comm core runs alone, with no node logic at all (see "Recovery").

A three-process variant puts the SSH server in its own root process and keeps the comm core
unprivileged (`CAP_NET_BIND_SERVICE` only). This shrinks the root attack surface to SSH alone, at
the cost of one more process and one more local socket. The note keeps it as the alternative. The
recommendation is two processes, with the comm core holding this bounded capability set:
- `CAP_SETUID`, `CAP_SETGID`;
- `CAP_NET_BIND_SERVICE`;
- `CAP_DAC_READ_SEARCH` (to read `authorized_keys`);
- `CAP_SYS_BOOT`, `CAP_KILL`;
- `CAP_NET_ADMIN` only where the CAN bitrate is set by Orion.

Pre-auth parsing in a child without privileges is the hardening step that makes the two-process
form as tight as the three-process one (`access-ssh.md`, risks).

### The backplane

- **One Unix stream socket.** The comm core listens; the node logic connects. The comm core
  checks `SO_PEERCRED` against the configured node user, so nothing else can claim to be the node
  logic.
- **Messages are already authenticated.**
  - The comm core verifies every signature, session and local credential.
  - It forwards a request with the verified principal attached (`node:<id>`, `operator:<key
    fingerprint>`, `local:<uid>`), and the node logic authorizes against that principal.
  - Keys and enrollment stay in the comm core, so a node logic compromise does not leak the node
    identity key.
- **No decode on the relay path.** Control frames are rkyv. The comm core reads the routing header
  and forwards the bytes as they are. Batches are coalesced with the same writer that coalesces
  peer frames today.
- **When the node logic is down:**
  - control-plane requests get `Unavailable { reason: "node logic restarting" }`, with a retry
    hint;
  - status, streams, link sessions, discovery, hello and access are unaffected;
  - the hello reports `node: down`.
- **On reconnect** the node logic sends a resume hello, and the comm core replays the
  current relay state (latest status entries, open stream catalogs, link device inventory). This
  is the same bootstrap snapshot a new watcher gets today, so no new state machine is needed.

### Containment inside each process: one bad apple

The process split protects access from the node logic. Inside each process the same rule
holds: one misbehaving provider, job, plugin, link, peer, session or client must not stall or take
down the others. **Containing something never means switching it off.** A flaky link may be the
only way in, so it is limited, retried and reported, and it stays usable.

Rules that apply to every component:
- **Own task, own failure.** Each link, peer session, client session, provider session, job
  supervisor and SSH connection runs as its own task. A panic ends that task only; the release
  profile unwinds, and tokio catches the panic at the task boundary.
- **No shared lock across I/O or `await`.** Shared state (the status store, the provider
  registry, the desired state) is held only for in-memory work. No lock is taken inside another
  component's callback. A slow component therefore holds only its own queue.
- **Bounded queues per component.** Every component has a send queue with a cap. When it is
  full, the newest value replaces older ones (latest-value lanes, as the status lane and stream
  demand do today). Control messages for a client that never reads end in a disconnect of that
  client, as the NT4 server already does. The client then reconnects.
- **Per-component deadlines:** a request handler has its budget, a link write its deadline, and a
  peer sync request its timeout. A deadline that passes fails that request only.

The ladder (what triggers each step, and how bad it has to be):

| Step | Trigger | What happens | Comes back |
| --- | --- | --- | --- |
| 1. Fail the request | A request or write misses its deadline, or a handler returns an error. | That request fails with a reason. The component keeps running. | Next request. |
| 2. Restart the component | The task panics or returns an error, or its link or session drops (EOF, I/O error, liveness timeout: 3 s for NT4, the link's heartbeat timeout for links). | Restarted or reconnected after a backoff: 100 ms, doubling, **capped at 5 s for access paths** (links, SSH, serial, the USB link) and **10 s for everything else**. Never gives up. | Each attempt; the backoff resets after 30 s of health. |
| 3. Rate-limit the noise | More than 3 restarts or errors of one component within 10 s. | Logs drop to one line per 10 s with a count. Retries continue at the capped backoff. | Health for 30 s. |
| 4. Report | Any component in step 2 or 3. | Status keys `<component>.state` (`backing_off`, `restarting`, `healthy`), `.reason`, `.restarts`, `.next_retry_ms`, and `orion_component_restarts_total`. `orionctl status` lists them. | When healthy. |

There is no step 5 by default: Orion never disables a component by its own decision.

- **Manual off.** One command takes a part out of the retry loop: `orionctl link release`
  (this exists today as the `link.release` action), `orionctl provider disable <id>`, or
  `orionctl peer disable <id>`. A config entry with `enabled = false` does the same. It persists
  until it is turned back on, so nobody is stuck watching a crash loop.
- **Configured auto-off.** Allowed only as a policy someone wrote, for example
  `auto_off = { restarts = 20, within_s = 60 }` on a provider. It is never a default Orion picks.
  Access paths (links marked `access`, SSH, the serial console, the USB link) refuse such a
  policy.

| Part | Scope | Interfaces | Risk |
| --- | --- | --- | --- |
| One supervisor helper (backoff, rate limit, state keys) used by links, peers, sessions and providers | ~350 | additive status keys and one metric | low |
| `provider disable` and `peer disable` (the link one exists) | ~200 | two orionctl verbs and two node actions | low |
| Lock audit: no lock held across `await` or I/O; a clippy lint (`await_holding_lock`) set to deny | ~100 plus fixes found | none | low |

### Where the status lane and streams live

The hot paths carry live values, so they must not cross the backplane:
- a publisher's stream to a remote subscriber;
- a CAN encoder's records to a camera.

So the **relay** (latest value per key, bounded history ring per series, catalog, backfill,
resume, coalescing, demand) is in the comm core. The **semantics** (who may own a key, what an
expired record means for placement, persistence of observed state) stay in the node logic, which
reads the relay as a subscriber. Measured on the link path (`docs/performance-concurrency.md`,
"Link Path Costs"), a status update costs 12 allocations and a stream batch 12. In the comm core
these paths are the same code with the node bridge removed, so the hot paths do not get slower
from the split. Only control requests (desired-state writes, queries) gain one local hop: one
coalesced `write`/`read` pair on a Unix socket.

### Supervision

| Layer | Supervised by | Watchdog | Crash loop |
| --- | --- | --- | --- |
| Comm core | systemd (`Restart=always`, `StartLimitIntervalSec=0`, `RestartSteps`/`RestartMaxDelaySec`). Without systemd (initramfs) it is PID 1's child or PID 1 itself and supervises its own children. | Thread outside the runtime pets only while the accept loops and link loops make progress (`access-ssh.md`). | Backoff 1 s doubling to a 10 s cap, never gives up. Safe-mode start on broken config: built-in defaults, never wider access. |
| Node logic | systemd, or the comm core's built-in supervisor without systemd. | The node's existing health-gated watchdog (`crates/node/src/systemd.rs`). | Backoff 1 s doubling to a 10 s cap, never gives up; the comm core reports the restart count and last exit in hello. Safe mode only when someone configured it (the existing `OnFailure=` drop-in example) or asked for it; never by Orion's own decision. |

The comm core's built-in supervisor is small: fork and exec, a backoff timer, reaping children,
forwarding signals. It exists only so the initramfs and recovery images (no systemd) get the same
guarantees.

## The hardware layer and Orion: handles in, protocols out

Protocols stay Orion's. The hardware layer (the hardware repository's daemon, `docs/capi.md`) discovers hardware and hands Orion raw handles. Orion's comm core
speaks the link protocol (or NT4, or SSH) over them.

- **Interface:** one Unix socket from the hardware layer to the comm core. The hardware layer sends
  `HandleOffer { kind, device_ref, facts }` with the fd attached through `SCM_RIGHTS`:
  - `kind` is one of `tty`, `can`, `usb-bulk` (endpoint pair) or `usb-gadget` (a FunctionFS
    endpoint pair);
  - `device_ref` is the hardware layer's stable name;
  - `facts` are the hardware layer's device record fields Orion shows next to the link.

  Orion answers `Accepted { link }` or `Declined { reason }`. A `HandleRevoked` on removal closes
  the link.
- **Fallback with no hardware layer:** the existing `ORION_NODE_LINKS` paths, opened by Orion itself, as
  today.
- **Gap:** raw USB bulk endpoints. Today the link protocol runs over USB-CDC (a tty), and the
  stream transport handles it. A `usb-bulk` handle is packet-like, so it gets a third transport
  next to `Stream` and `Packet` in `orion-link`: framed bulk transfers with a 512-byte or 1-KiB
  MTU. The codec and sessions are unchanged.

## Recovery runs the comm core

Anything with Linux userspace runs the comm core:
- the normal image;
- the initramfs;
- the flasher/recovery image.

Recovery then speaks Orion (identity, hello, actions such as `update` staging and `reboot`,
status) and SSH (shell, sftp) like everything else. Only pre-userspace paths stay outside, such
as SoC ROM USB boot.

- **Minimal build, `comm-core-min`:**
  - `orion+tcp` and local IPC;
  - hello and identity (mDNS optional);
  - auth;
  - SSH with sftp and the recovery shell;
  - `reboot`, and `update` staging through the existing device-agent contract;
  - serial links, so it can flash MCUs from recovery.

  It drops the node logic, HTTP, QUIC, NT4 and the stream history rings (latest values only).
  It is linked static with musl, so it needs no libc on the initramfs.
- **Recovery shell:** a login that gets `/bin/sh` when the image has one (busybox
  initramfs). On images with no shell, the comm core offers a fixed built-in command set over
  `ssh exec`, with no parser beyond argv:
  - `ls`, `cat`, `cp`, `rm`, `mount`;
  - `dmesg`, `ip addr`, `reboot`, `update stage`, `update commit`.
- **Size:** measured below.
- **Relation to the update layering** (`docs/update-recovery.md`, "dependencies only point
  downwards"):
  - The comm core becomes the way *into* a recovery image: identity, actions, SSH.
  - It does not write disks. The device manager still writes slots and checks signatures, and it
    still works without Orion.
  - The bootloader still falls back without either of them.
  - A recovery image whose comm core fails to start is no worse off than today: the bootloader
    and the SoC ROM path are still below it.
  - `update-recovery.md` changes its "never depend on Orion" line to "never depend on the node
    logic" when this lands.

## Time sync in the comm core

On the test board, `systemd-timesyncd` costs 2.8 MB PSS. Its whole job is an SNTP client that
slews or steps the clock, plus a saved timestamp so a board without a working RTC never boots
into the past. The comm core can do the same job inside a process that is already running, with
two sources:

- **SNTP** (RFC 4330 client mode) to the configured servers. One UDP socket, one request per
  poll, with poll intervals from 64 s to 1024 s as timesyncd uses. Small offsets are slewed
  (`adjtimex` with `ADJ_OFFSET`); large ones (over 0.4 s) are stepped (`clock_settime`).
- **A peer or link time source:** the controller or a peer node over an Orion link or session.
  Orion already estimates a server clock offset on its links (the NT4 client's RTT-filtered
  offset, `crates/nt4/src/client/clock.rs`). The same filter feeds the clock discipline from an
  Orion peer, so a field network with no internet still gets one time.

The rules match timesyncd:
- the first good sample may step the clock;
- later samples only slew;
- the last synchronized time is saved to the state directory every poll, and at start the clock is
  moved forward to it if the clock is behind;
- the kernel's `STA_UNSYNC` is cleared once synchronized, so `timedatectl` and the node's
  existing clock facts (`clock_facts.rs`) report it.

| Item | Value |
| --- | --- |
| Lines | ~450: SNTP client ~200, discipline (slew or step, saved time) ~150, peer source glue ~100 |
| Interfaces | config (`time.servers`, `time.peer_source`), one status key group `time.*`; needs `CAP_SYS_TIME` in the comm core's capability set |
| Binary size | estimated 20 to 40 KB: no new crates; UDP, `adjtimex` and `clock_settime` come through `libc`, which is already linked |
| Memory | no new process; one socket and a timer that wakes once per poll interval |
| Saves | timesyncd's 2.8 MB PSS on the test board |
| Risk | medium: a clock discipline bug moves time for everything on the board. Mitigations: never step backwards after the first sync; a step needs two agreeing samples; and a mode to only report the offset, run side by side with timesyncd before switching |

An image switches by masking `systemd-timesyncd.service` and turning on `time.enabled`. Both
running together is refused at start (the comm core checks whether timesyncd is active over its
pid file and logs instead of disciplining).

## Tiers: the same protocols everywhere

| Tier | Runs on | Crates (today) | Speaks |
| --- | --- | --- | --- |
| **Device** | MCU, `no_std` (the link device path needs no allocator) | `orion-link` (`device`, `actions`, `streams`, `xfer`); the `no_std` model crates `orion-core`, `orion-data-plane`, `orion-control-plane`, `orion-auth` (with `alloc`) | The link protocol over serial, CAN, USB-CDC: records, streams, actions, firmware transfer. |
| **Comm core** | Every Linux device, including initramfs and recovery | `orion-transport-{tcp,quic,http,ipc,common}`, `orion-link` (`std`, host session), `orion-auth` (`crypto`), `orion-nt4` (opt-in); plus, today inside `orion-node`: `discovery/`, `peer_tcp/`, `link_gateway/`, `auth/`, `managed_transport/`, `systemd.rs`, `host_facts`, `app/status_lane`, `app/streams.rs`, local IPC serving | `orion+tcp`, QUIC, HTTP, IPC, links, SSH, hello. |
| **Node** | Where jobs and the cluster are needed | `orion-node` minus the above: `app/desired_*`, `reconcile*`, `placement`, `cluster_state`, `hlc_state`, `persistence`, `actions` registry, `peer_sync*` semantics, `operators`; `orion-cluster`, `orion-runtime`, `orion-service` | The same protocols, through the comm core. |

A CAN encoder (device tier) publishes records and a stream over its link. The comm core on the
gateway Linux board relays them. A camera elsewhere reads them over `orion+tcp` with a stream
subscription. No node logic is involved anywhere on that path; with the split, the gateway may
run the comm core alone.

### Gaps

| Gap | Scope | Interfaces | Risk |
| --- | --- | --- | --- |
| Extract the comm core from `orion-node` into `crates/comm`. The modules listed above move: about 16,700 lines out of `orion-node`'s 42,600 production lines. Most move unchanged; their seam to the app (`NodeApp` calls) becomes backplane messages. | ~16,700 moved, ~1,500 new (backplane, principal forwarding, resume) | new backplane (internal, versioned); none external | high: the largest refactor in the repo. Mitigated by moving module by module behind the existing integration tests, which keep running against the combined deployment. |
| Relay without node logic: status lane and streams serve link and network subscribers with no `NodeApp`. Today the link bridge registers link devices as node providers. | ~1,200 | none external | medium |
| Backplane and the node logic as a client of it | ~1,500 (counted above) | internal | medium: request routing for every control message kind |
| Comm-core supervisor for systemd-less images | ~250 | none | low |
| Hardware-layer handle offers (`SCM_RIGHTS`) | ~400 | new local socket with the hardware layer | low |
| `usb-bulk` / `usb-gadget` transport in `orion-link` | ~350 | new transport kind (link protocol unchanged) | medium: usbfs and FunctionFS error paths |
| Hello and board identity in the comm core (next section) | ~800 | the contract's port and path, a fragment directory, one `orion+tcp` request, additive TXT keys | medium (HTTP responder) |
| `comm-core-min` static musl build in CI, with a size gate | ~150 (scripts) | none | low |
| Access services (`access-ssh.md`) | ~2,600 + ~900 tests | new unit, CLI mode | see that note |

Device-tier gaps: none for the encoder case. MCUs already publish records and streams, run
actions and take firmware over links. A device talking IP directly (an MCU with Ethernet) would
need a `no_std` `orion+tcp` client. It is not needed for the encoder-through-gateway path, so it
is listed and not proposed.

## Identity in the comm core (hello)

Today a device's identity is spread across three places:
- the node key and id in the mDNS TXT record (`orion_auth::discovery::Advertisement`: `id`,
  `pk`, `cp`, `cl`, ports);
- host facts in the node's records (`machine_id`, model, boot ID, OS);
- whatever separate endpoint an image runs for board identity.

The board identity contract moves into the comm core, so a device running Orion needs no separate
identity endpoint.

**The contract (contract 1).** The fleet tool recognizes boards by a JSON identity document:
- It is served at `GET http://<device>:5899/.well-known/pd-device` and also written to
  `/run/board/identity.json`.
- Readers ignore properties they do not know, and later versions only add properties.
- Required fields:
  - `contract`;
  - `model`, `rev`, `serial` (lowercase hex);
  - `hostname`;
  - `os`;
  - `device_package`;
  - `update_methods`;
  - `manage_url`;
  - `macs`.
- Optional fields:
  - `bootloader`;
  - `endpoints`: `metrics`, `logs`, `actions` (`POST <actions>/<id>`), and `status`, `events`,
    `stream`, where `stream` may name another port, for example `:5898/stream`;
  - `actions`;
  - `camera_stream` and `camera_streams`;
  - `gadget`;
  - `diagnostics` (run over SSH);
  - `update` (the updater state, plus the slot list on A/B boards);
  - `usb_route`;
  - `security` (`on` or `off`);
  - `time` (unix seconds).

**Who fills what.** The comm core builds the document from two sources:

| Source | Fields |
| --- | --- |
| The comm core itself (host facts it already reads, `crates/node/src/host_facts.rs`) | `contract`, `model`, `rev`, `serial`, `hostname`, `os`, `macs`, `time`, `bootloader` when readable, `endpoints` (its own status, events, stream, actions paths), `actions` (the node actions it routes, `reboot` included) |
| The device package, as one JSON fragment file (for example `/etc/orion/identity.d/*.json`, merged in name order) or as `identity.*` status keys of a provider allowed to publish them | `device_package`, `update_methods`, `manage_url`, `update`, `camera_stream(s)`, `gadget`, `usb_route`, `security`, `diagnostics` |

**Live fields.** Three fields are computed on every request and never cached:
- `hostname`, from the kernel (`uname`), so a `hostname` change shows at once;
- `time`, as integer unix seconds;
- `security` (`on` or `off`), read from the package's security file in the package's precedence
  order (a writable data copy wins over the image default).

**The minimum to pair.** The fleet tool needs `contract`, `serial`, `model`, `rev`,
`hostname`, `os` (`name`, `version`), `device_package` (`version`, `commit`) and `macs`, plus
`gadget` on a board with a USB link. The signed Orion hello carries exactly these; the rest comes
from later calls. `update` is large and changes often, so over Orion it is a status key group and
a stream, not part of the hello.

**Passed through.** `update` is copied verbatim from the first line of the updater's state file
(`/run/board/update.json`), not rebuilt. Its shape (`state`, per-mille `progress`, `phase`, and
the slot fields on A/B layouts) belongs to the updater.

**Shapes the comm core checks before serving:**
- `usb_route` must carry the booleans `enabled` and `active`;
- each `camera_streams` entry must carry `url`, which may be `:<port>/path` on the same host;
- `endpoints` values are strings.

A fragment that breaks its field's shape is dropped and logged, never served half-valid.

The comm core never invents a package field. A missing required field is `null`, as the schema
allows, and is never guessed.

**Where it is served:**
- **The contract's own HTTP path and port**, by a minimal HTTP/1.1 `GET` responder in the comm
  core. It is not `axum` or `hyper`. It answers one path and the endpoints' paths, so it also
  works in `comm-core-min`, which has no HTTP stack.
- **The file** `/run/board/identity.json`, rewritten atomically when a non-live field changes. The
  HTTP answer is always built per request, so its live fields are current.
- **Signed over Orion:** the same document, plus the node id and public key, the comm-core and
  protocol versions, the tier state (`node: up | down | absent`) and the SSH host-key
  fingerprint. It is signed with the node key and served as one `orion+tcp` request, over local
  IPC, and over a link to a device that asks. The comm core answers it without the node logic.
- **mDNS:** the contract's own service, `_pd-device._tcp` on port 5899. The instance name is the
  hostname. The TXT keys are `contract`, `model`, `rev`, `serial`, `os`, `os_ver` and `path`.
  Every key is always present (a value may be empty), values are limited to `[A-Za-z0-9._:+-]`
  (anything else becomes `_`), and each is at most 200 characters. This sits next to Orion's own
  `_orion._tcp` advertisement.
- **Recovery images:** in the same way. A board in recovery is recognized as itself, with
  `update.state` saying so.

**Actions endpoint.** `POST <actions>/<id>` maps onto Orion actions on the local node. The comm
core routes it: through the node logic when it is up, and for `reboot` straight to the access
claim. Auth on that endpoint follows `security`:
- `off`: open, as today;
- `on`: only callers with an Orion operator signature or from a local socket.

| Part | Scope | Interfaces | Risk |
| --- | --- | --- | --- |
| Document builder, fragment merge, file writer | ~250 | fragment directory (new, documented) | low |
| Minimal HTTP responder (GET plus the actions POST) | ~250 | the contract's port and path, unchanged | medium: a hand-written HTTP parser, so it is fuzzed and has strict limits (8 KiB header cap, one request per connection) |
| Signed hello over `orion+tcp`, IPC, link; TXT keys | ~300 | one new request kind, additive TXT keys | low |

## Size

Release profile (`lto = "fat"`, `codegen-units = 1`, `opt-level = "z"`, `strip = "symbols"`),
measured on today's tree (`--bin orion-node`, bytes):

| Build of today's `orion-node` | Features | x86_64 glibc | aarch64 musl static |
| --- | --- | --- | --- |
| Default release | HTTP, TCP, QUIC data transports, `peer-tcp` | 6,366,720 | 5,215,304 |
| Default + SSH probe | as above + `russh`, `russh-sftp` | 7,429,464 | |
| IPC only | `--no-default-features` | 3,663,552 | |
| Comm-core-min feature set | `peer-tcp`, `link-gateway`, SSH probe | 5,975,288 | 4,928,128 |
| The same without SSH | `peer-tcp`, `link-gateway` | | 3,962,776 |
| The same + mDNS + sd_notify | `peer-tcp`, `link-gateway`, `discovery-mdns`, `systemd-notify`, SSH probe | 6,484,688 | |

On aarch64 the SSH part of the comm-core-min set is 965,352 B. A static musl binary needs no
libc in the initramfs.

These builds still contain all of the node logic, so the comm-core-min row is an **upper bound**
for `comm-core-min`. The extraction removes from it:
- desired state, reconcile, placement, cluster, persistence and the actions registry, about 26,000
  of `orion-node`'s 42,600 production lines;
- rkyv snapshot persistence.

mDNS and sd_notify together cost 509,400 B, nearly all of it `mdns-sd`. The initramfs build leaves it out by default and
answers hello over `orion+tcp` and on the link instead; an image that needs zero-configuration
discovery in recovery turns it on.

The comm core is a subset of today's `orion-node` code plus the access services. The tables above
give its bounds until the extraction lands; the size gate in CI then tracks the real binary.

## Order

The work comes in these slices, each landing on its own:

1. the hello document;
2. the access services in a separate process (`access-ssh.md`);
3. the comm core extraction, module by module;
4. the relay without node logic;
5. the backplane;
6. the systemd-less supervisor and `comm-core-min`;
7. Hardware-layer handle offers and the USB transport.

The order is by dependency, not by size. The access services come early because the comm core's
privilege and supervision model is the same one, so it gets proven first on a smaller piece.
