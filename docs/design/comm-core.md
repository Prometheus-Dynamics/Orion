# Design: the communication core and the node logic (two layers, three tiers)

Status: design note, for review. Nothing is implemented yet. The access services (SSH, sftp,
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
| Comm core | systemd (`Restart=always`, `StartLimitIntervalSec=0`, `RestartSteps`/`RestartMaxDelaySec`). Without systemd (initramfs) it is PID 1's child or PID 1 itself and supervises its own children. | Thread outside the runtime pets only while the accept loops and link loops make progress (`access-ssh.md`). | Backoff to 30 s, never gives up. Safe-mode start on broken config: built-in defaults, never wider access. |
| Node logic | systemd, or the comm core's built-in supervisor without systemd. | The node's existing health-gated watchdog (`crates/node/src/systemd.rs`). | After N fast crashes the node starts in its existing safe mode (`docs/update-recovery.md`). The comm core reports it in hello. |

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
| Hello and board identity in the comm core (next section) | ~400 | additive TXT keys and one `orion+tcp` request | low |
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
identity endpoint:

- **Hello document:**
  - node id and public key;
  - board model and serial;
  - machine id;
  - image name and version;
  - boot id;
  - comm-core version and protocol versions;
  - which tiers are present and up (`node: up | down | absent`);
  - service ports, with the SSH port and host-key fingerprint.

  The comm core signs it with the node key.
- **Where it is served:**
  - compact fields in the mDNS TXT record (additive keys; old parsers ignore them);
  - the full signed document over one `orion+tcp` request, answered by the comm core without node
    logic;
  - over local IPC;
  - in recovery images the same way.
- **The fields named by the board identity contract** are the ones to copy. This note lists
  Orion's side. The contract's exact field list and names come from its owner; Orion adds them
  unchanged as hello fields.

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
