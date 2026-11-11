# Orion: the design

The rebuild of Orion on `next`. It starts from what the system needs. `dev` at 7012e8d is the
reference. [`architecture.html`](architecture.html) is this design with drawings.

## What Orion is

Orion is orchestration and interconnect, inside a board and out of it.

**Orion is:**

- The one way in and out of a board.
- The only authority on what runs.
- The bus that values travel on.
- An open list of actions: whoever connects adds theirs.
- The same thing from a microcontroller up to a PC, in different sizes.

**Orion knows:**

- Names: of nodes, topics, actions, workloads.
- Who is asking, and whether they may.
- What should be running, and what really is.
- Which transports exist and which peers are alive.

**Orion does not know:**

- What a camera, a pose or a graph is.
- What is inside a topic beyond its type.
- How to encode video. A publisher gives it bytes.
- What FRC is. NT4 topic names come from the product as configuration.
- How to talk to hardware or update a board.

## Where it sits

| Library | Job |
| --- | --- |
| Styx | Cameras and media |
| Daedalus | Runs graphs |
| Eidos | Vision |
| Lemnos | Devices and drivers |
| Orion | Orchestration and interconnect |

A product such as HeliOS wires these together. **No library needs another one in order to work.**
Where two meet, the bridge is an optional feature owned by one of them, and the product turns it on.

## Six things

Today 11 separate mechanisms and 72 control messages do this work. The rebuild has six concepts, and
the whole protocol is made of them.

| Concept | Meaning | Replaces |
| --- | --- | --- |
| **Node** | Who is here: identity, peers, clock | Node and peer records, trust |
| **Resource** | A thing that exists, with topics and actions: a camera, a fan | Resources, providers, leases |
| **Topic** | A named value that moves: latest, a history, or shared buffers | Status lane, stream lane, the NT4 side path, the fd channel |
| **Action** | Do one thing once, through the check | Action routing and claims, board agent actions, update intents |
| **Workload** | Should be running; tracked until it is really gone | Workloads, executors, assignments |
| **File** | Bytes that land somewhere: an image, a plugin, a model | The local blob store |

## The system

```text
 browser   tools   robot code   other boards   microcontrollers   a person (SSH)
    |        |         |             |                |                 |
 ===+========+=========+=============+================+=================+=====
 | comm core: connections, the one check, topic broker, gateways,           |
 |            access (SSH, recovery shell), hello, discovery, time          |
 ===+==========+==============+==============+================+=============
    |          |              |              |                |
  node logic  helios-api    lemnosd        daedalusd       camera service
  (records,   (web API,     (devices,      (runs graphs,   (owns sensors)
  workloads,   a client)     actions)       an executor)
  actions)                       \              |              /
    |                             \             |             /
   disk                     topic rings in shared memory: values and buffers
```

The comm core is the only program with a door to the outside. Node logic and every program connect
to it once, on the same wire. Values and frames then move through rings, without Orion in the path,
unless someone off the board asked for them.

## Repository layout

```text
crates/
  core/      types, wire, msg-*, check, supervise, measure, fdpass, config, values, ring, ring-*
  device/    microcontroller side: core, topics, actions, firmware. no_std, size gated
  client/    connect, topics, buffers, records, actions, files, executor, provider, orion, capi, ctl
  comm/      server, unix, tcp, ws, link-*, broker*, topic-peers, outbound, keys, hello,
             discovery, time, self-status, device-*, ssh*, recovery, relay, comm-app
  node/      store, persist, workloads, workload-*, executors, sync, placement, actions*, files*,
             resources, audit, node-app, assembly
  gateways/  nt4, nt4-gateway, nt4-client-gateway
  bridges/   daedalus
  bins/      orion, orion-recovery
  testing/   wire-vectors, ring-model, ring-bench, drills
web/         TypeScript client and the browser video example
examples/    mcu-template, stm32g4-device, sample-executor. Their own workspaces
docs/  scripts/  packaging/  testing/board-runs/
```

The workspace is one glob, `crates/*/*`, so adding a crate never edits the root. Small crates are
what let many sessions work at once without conflicts. Fat LTO means they cost nothing in the
binary.

- `crates/comm/*` and `crates/node/*` never depend on each other. They share `crates/core/*`.
- `crates/core/*` and `crates/device/*` depend on nothing above them.
- Everything is behind features.

## Two programs, five forms

Two processes, comm core and node logic, so links stay up whatever happens to the logic. There is
no separate backplane protocol: node logic is the comm core's first client, on the same wire as
everyone else. They ship as one binary with modes `comm`, `node` and `ctl`, and share code pages.

| Form | Contains |
| --- | --- |
| Microcontroller | Wire, device core |
| Recovery (initramfs, flasher) | Wire, comm core, SSH and recovery shell |
| Board Orion manages | Wire, client, comm core, SSH and recovery shell, node logic |
| Guest (SystemCore, a PC) | Wire, client, comm core, node logic |
| PC tool (Atlas, orionctl) | Wire, client |

| If this dies | What keeps working | How it comes back |
| --- | --- | --- |
| Node logic | Links, SSH, topics, gateways. Requests for records answer "restarting" | Reconnects as a client |
| Comm core | Graphs and every same-board topic, frames included, because those hold their rings. No new connections | Restarts. Publishers re-offer their rings. Peers and browsers reconnect, access paths with no backoff |
| An executor | Everything else. Its records stay, marked lost | Orion restarts its workloads |
| Orion, both parts | Graphs keep running | Orion finds them by name and takes them back |

## The wire

One wire on every carrier, using the rules the link protocol already has:

- `[version][kind][seq][payload][crc]`, with postcard bodies.
- Stable kind numbers. Bodies are append-only. Unknown kinds are ignored.
- One revision number agreed at hello.
- Recorded bytes in tests pin every kind.
- Protocol version 1.

rkyv leaves the codebase. A daemon loading plugins built elsewhere cannot live with "same build or
refused". Hot data no longer goes through messages, so decode speed on control messages stops
mattering.

| Carrier | Form |
| --- | --- |
| Same board | One socket per client, kept open, with file-descriptor passing |
| Network, between nodes | `orion+tcp`: the same signed session over TCP, always push |
| Browser | WebSocket carrying the same frames |
| Links: UART, CAN, USB | COBS or segmentation, as today |

The HTTP control API, QUIC and the separate TLS `transport-tcp` crate are not carried over. None is
on an image and nothing uses them. With one wire, adding a carrier back is a small change.

## Topics

A topic is a named value with a type, and every sample carries the time it belongs to. Types are
bool, integers, floats, strings, bytes, arrays and structured values with a pinned type name, so a
pose is one sample. One declaration can hold several related fields.

### The ring

- The comm core makes a ring in shared memory, sized from the declaration, and hands the write end
  to the publisher and a read end to each subscriber, once.
- A publish writes in place: no reply, no system call, no timer.
- Each subscriber has a doorbell, rung only if that subscriber is asleep. It is pollable.
- Old entries are overwritten. A slow reader skips ahead and counts what it missed.
- Latest value, history and the lookup by frame time all read the same entries. A latest-value topic
  is a ring two deep with an expiry.
- Each field keeps its own history, so a busy field never erases a quiet one's newest value. An
  index keeps the order across fields. [`ring.md`](ring.md) pins the layout.
- Subscriber counts sit in the ring header, so a publisher can skip work with one memory read.
- Readers bounds-check everything. A writer can only damage its own topic.
- A program that can't write shared memory sends over its connection, and the comm core writes the
  ring for it. Remote and microcontroller publishers work this way. There is still one model.

### Time

Ask for the value at or before frame time *t*, optionally waiting a bounded time. Replies are hit,
lagged or missing. Time sync between boards already exists and carries over.

### Across boards

A subscriber names `<node>/<topic>`. The comm cores carry it over `orion+tcp` into a local copy of
the ring, only while someone asked. The subscriber's call is the same as on one board.

### Buffers: frames are topics

A camera is a topic like any other. There is no second path.

1. The publisher declares `cam0.frames` and gives Orion its buffers, once.
2. A subscriber subscribes and can say what it wants. Orion passes that to the publisher without
   reading it, so one capture can be planned for all subscribers.
3. Orion hands the subscriber the buffers, the ring and a doorbell, once.
4. Each frame is a ring entry: buffer numbers, frame time, then a wake. Orion is not in this part.

What a buffer topic adds to a value ring:

- Hold marks: each reader marks the buffers it holds in a page only that reader writes. A buffer is
  free when no reader marks it, and a dead reader's marks go away with its page. This is the one
  new thing in the ring.
- Limits on a subscription (frames held, how long), applied by the publisher, which sees the marks.
  The publisher never waits for buffers. It drops frames instead.
- One entry can name several buffers (image pyramid levels).

Measured on a CM5 at 120 Hz: passing fds on every frame 34.5 µs; a ring slot and wake 18.1 µs; a
full-frame copy 170 µs. Set-up costs a few system calls per buffer, once. As built, with hold
marks: 10.5 µs from publish to the subscriber holding the frame
([`B9.md`](../testing/board-runs/B9.md)).

A buffer number means nothing on another board. Off the board, video is an ordinary bytes topic. One
generic flag on a sample, "others depend on this one", lets Orion skip forward to the next
independent sample for a slow reader.

## Leaving the board

| Destination | Path |
| --- | --- |
| Browser | The page comes from the product's web server. It opens a WebSocket straight to the comm core and subscribes. Per-subscriber byte limits |
| Another board | `orion+tcp` into a local copy of the ring |
| Robot code | NT4 gateway: topics to NT4 and back, server or client. A topic reader in the comm core. Name mappings come from the product |
| Tools | The same client, locally, over the network or over USB |

## Workloads

Orion holds what should be running and tracks every run.

- States: Pending, Starting, Running, Stopping, Gone, plus Failed and Lost.
- Asking for a stop does its best. The record stays until the executor reports the run gone.
- Force remove exists for when a process will not die. It is the only way a record leaves early.
- A workload has a name, a kind (`graph`), an opaque spec, the files it needs, where it may run and
  a restart policy.
- An executor is any program that connects and says "I run this kind". It reports state per name and
  keeps no journal of its own.
- Orion makes sure every needed file is on the board, then tells the executor.
- If an executor restarts, Orion restarts its workloads. If Orion restarts, runs continue and Orion
  adopts them by name.
- Peer sync and placement carry over from `dev`.

## Actions and the one check

Actions have progress, cancel and detached running, so a long job carries on if the caller leaves
and the caller can reattach. Orion ships almost none of its own. Any program that connects can add
actions.

Every request of every kind passes one check: actions, record writes, topic opens, file writes, SSH
logins. It takes who is asking, what they want and its policy, and returns a `Checked<T>`. A handler
can only be called with a `Checked<T>`, and only the check can make one. Registering an action
requires a policy. So skipping it, or forgetting it, is a compile error.

Security is one switch. Off, the check costs nothing. On, it runs.

## Files

Chunked, resumable and digest-checked, from any connection, or pulled by URL. A file is written
straight to where it belongs, so a small board never holds it twice.

## Access and recovery

The comm core serves SSH and sftp itself, replacing sshd and OpenSSL, and holds the recovery shell.
It runs alone in the initramfs and the flasher image. Orion is the standard way to do everything,
with the USB console and then SSH as fallbacks.

## The failure rule

Never give up. Backoff capped at 5 to 10 s, and it clears itself. No default auto-off. Manual off is
easy. Auto-off exists only as a configured policy. Backoff state is a topic anyone can see. Access
paths are never backed off. One misbehaving connection, peer, provider or link is contained.

## Microcontrollers

The device side keeps its size gate: 9,228 B flash and 1,988 B RAM on the STM32G4 example. A device
sends topics, takes actions and receives firmware on the same wire, minus shared memory.

`crates/device/core` is the session: hello with the role `device`, welcome, pings, reconnect. It
has no allocator, no clock and no I/O; the port feeds bytes or CAN segments, passes a millisecond
counter that may wrap, and sends what it is handed. Its own frames are encoded by hand in the
postcard format, so neither serde nor postcard nor `core::fmt` reaches a firmware.
`scripts/device-size.sh` links the smallest firmware (`crates/device/probe`) for each MCU target
with only what its entry points reach. Today the session costs 5.8 KB flash and 512 B RAM on
thumbv7em, 6.3 KB on thumbv6m and 6.7 KB on riscv32imc, about 1.4 KB of it memmove.

`crates/device/topics` declares a firmware's topics, kept as constants in flash, when a session
starts, and publishes samples as `TopicSample` frames once the host has answered with `Opened`.
Whether anyone listens comes back as `Demand`. One topic adds 1.75 KB on thumbv7em. What a
device asks of the host once a session (a topic, an action, a file prefix) is asked again after a
second without an answer and never again once answered or refused, until the next session:
`Requests` in the device core.

`crates/device/actions` registers a firmware's actions the same way and hands it every
`ActionClaim` for one of them; the firmware answers with `ActionProgress` and `ActionResult` when
it likes. Nothing about a run is kept on the device: a result the link has no room for is sent
again by the firmware, and one that never comes is ended by the run's deadline on the host. One
action adds 1.7 KB on thumbv7em.

## Size and memory

Measured on a CM5 today, against the targets. The targets are unbuilt, and an allocation profile of
today's heap is the first step.

| Where | Today | Target |
| --- | --- | --- |
| Heap | 3.36 MB | 1.4 MB, both processes |
| Other anonymous memory | 1.35 MB | 0.3 MB |
| Static data and main stack | 0.22 MB | 0.3 MB |
| Topic rings (shared) | in the heap | 0.5 MB |
| **Data total** | **4.98 MB** | **2.5 MB** |
| Code resident | 2.84 MB | 2.0 MB |
| Threads | 9 | About 4 |
| Binary on disk | 3.8 MB, plus orionctl | About 5.5 MB, one file, with SSH replacing sshd and OpenSSL |

## Shared interfaces

Everyone builds against these. The named crates are the source of truth, and they change only
through an A-series or B-series PR.

The kind table, by range, with numbers assigned in `crates/core/wire` (A4):

| Range | Concept |
| --- | --- |
| `0x00` to `0x0F` | Session: Hello, Welcome, Reject, Ping, Pong, Refusal |
| `0x10` to `0x1F` | Records: Node, Resource, Lease; Put, Get, Watch, Changed |
| `0x20` to `0x2F` | Files |
| `0x30` to `0x3F` | Actions |
| `0x40` to `0x4F` | Topics |
| `0x50` to `0x5F` | Workloads and executors |
| `0x60` to `0x6F` | Peers and trust |
| `0x70` to `0x7F` | Time |

Roles at hello: client, provider, executor, node, peer, device.

```rust
// crates/core/check. Only `Check` can make a Checked<T>.
pub enum Principal { Anonymous, Local { uid: u32, gid: u32 }, Operator(KeyId), Peer(NodeName), Device(DeviceName) }
pub struct Checked<T> { /* private */ }
impl<T> Checked<T> { pub fn who(&self) -> &Principal; pub fn get(&self) -> &T; pub fn into_inner(self) -> T; }
impl Check {
    pub fn decide<T: Request>(&self, who: &Principal, request: T, policy: &Policy) -> Result<Checked<T>, Refused>;
}

// crates/comm/server. Handlers take Checked requests, so none can be reached around the check.
pub trait Carrier { /* accept connections that say who is there, move whole frames, pass descriptors if they can */ }
pub trait Service {
    fn kinds(&self) -> &'static [u8];
    fn asks<'a>(&self, frame: &FrameView<'a>) -> Result<(What<'a>, Policy), Error>;   // all the check is told
    fn handle(&mut self, request: Checked<Incoming<'_>>, cx: &mut Cx<'_>) -> Result<(), Error>;   // returns at once
    fn closed(&mut self, session: SessionId, sessions: &mut Sessions);
}

// crates/core/ring. One writer, many readers. Neither call makes a system call.
impl Writer {
    pub fn publish(&mut self, field: FieldId, time: FrameTime, flags: Flags, payload: &[u8]) -> Result<u64, PublishError>;
    pub fn subscribers(&self) -> u32;
}
impl Reader {
    pub fn next(&mut self) -> Next<'_>;                     // Sample, Empty, or Skipped(count)
    pub fn latest(&mut self, field: FieldId) -> Option<Sample<'_>>;
    pub fn at(&mut self, field: FieldId, lane_seq: u64) -> Option<Sample<'_>>;   // a field's history
}
// crates/core/ring-time
pub fn at_or_before(reader: &Reader, field: FieldId, time: FrameTime) -> Lookup<'_>;   // Hit, Lagged, Missing

// crates/core/ring-buffers and ring-holds
impl BufferWriter {
    pub fn take(&mut self, holders: &impl Holders) -> Result<BufferIndex, Dropped>;   // a free buffer, or drop the frame
    pub fn publish(&mut self, buffers: &[BufferIndex], time: FrameTime, meta: &[u8]) -> Result<u64, BufferError>;
}
impl BufferReader { pub fn next(&mut self) -> NextFrame<'_>; }   // Frame { time, meta, held }; dropping Held clears the mark
impl HoldTable { pub fn enforce(&mut self, now_ns: u64, pool_page: &impl MemMut) -> u32; }   // cuts off readers over a limit

// crates/client/orion. The same calls on any address.
impl Orion {
    pub fn connect(address: &Address, name: &str) -> Result<Self, ClientError>;
    pub fn declare(&mut self, topic: TopicDecl) -> Result<Publisher, ClientError>;
    pub fn subscribe(&mut self, topic: &TopicRef) -> Result<Subscriber, ClientError>;
    pub fn declare_buffers(&mut self, topic: TopicDecl, pool: BufferPool) -> Result<BufferPublisher, ClientError>;
    pub fn subscribe_buffers(&mut self, topic: &TopicRef, request: &[u8]) -> Result<BufferSubscriber, ClientError>;
    pub fn run(&mut self, action: ActionCall) -> Result<ActionHandle, ClientError>;
    pub fn register_action(&mut self, decl: ActionDecl, policy: Policy) -> Result<ActionRegistration, ClientError>;
}

// crates/client/executor. What daedalusd implements.
pub trait Executor {
    fn kinds(&self) -> &[WorkloadKind];
    fn start(&mut self, name: &WorkloadName, spec: &[u8]) -> Result<(), ExecError>;
    fn stop(&mut self, name: &WorkloadName);
    fn kill(&mut self, name: &WorkloadName);
    fn inventory(&self) -> Vec<(WorkloadName, WorkloadState)>;
}
```

Non-negotiable properties: 0 allocations on steady-state publish, read and request paths; bounded
queues everywhere; no timer on a data path; nothing on the wire names a kind of device.

## What comes from dev

| | What |
| --- | --- |
| **Ported** | Link wire and device code; time sync; discovery; auth keys and enrollment; HLC merge; placement; MCU examples and size gates; the NT4 crate, reworked as a gateway; fd framing |
| **Rewritten** | The client, as one instead of local and remote copies; status and streams, as topics; actions around the check; workloads with the new states; storage without rkyv; orionctl |
| **New** | Rings and buffer topics; WebSocket and the web client; SSH access; the executor protocol; URL pull; the Daedalus plugin |
| **Left behind** | HTTP control API, QUIC, `transport-tcp`; `data-plane`; the rkyv control messages; update intents; maintenance admin; metrics export (a part's state is a topic) |

## Known risks

- The ring is the riskiest part. Readers run against memory that can change underneath them. It gets
  fuzzing and a concurrency model check before anything uses it.
- Hold marks are new shared-memory code on the camera path. The 18 µs was measured without them.
- One async thread per process: a big SSH copy could delay a topic forward. Measure on a CM5.
  More threads are a plain knob.
- Browser video on plain HTTP: WebCodecs needs a secure page. JPEG chunks or H.264 through Media
  Source both work without one, and the publisher chooses.
- Per-subscriber limits and the drop rule have to hold under a real radio bandwidth cap.
- The heap target is a guess until profiled.
- Postcard is not self-describing, so saved records follow the wire's append-only rules.

## Settled decisions

- Clean-slate `next`; `dev` frozen as the reference.
- Six concepts, one wire, rkyv gone.
- Two processes, one binary, node logic as the comm core's first client.
- Frames are topics. Styx keeps its own way to pass frames for anyone not using Orion.
- Orion is the only authority on what runs. Executors keep no journal.
- NT4 is a gateway.
- Compile-time single check.
- SSH served by Orion.
- Node-to-node traffic stays on `orion+tcp`.

## Pinned

**Board management.** Update, slots, confirm, rollback, reboot, USB and board identity go with
Lemnos, as drivers that add their actions to Orion's open list, so Orion has no board knowledge. The
details come back later. Open when they do:

- An update needs lemnosd alive. A slot should only be confirmed if the update action answers, so a
  build with a broken update path rolls itself back. SSH and the USB flasher remain for a broken
  running slot.
- Storage is outside Lemnos today, so a driver would need to work with files and boot config.
- An update is far longer than a normal driver call, so drivers need a long-job form with progress
  and cancel.

Orion's part is built regardless: the open action list, the check, progress and File delivery.

## Open

- **What wakes a graph.** Every topic gives a subscriber something it can sleep on, so a frame, a
  topic or a timer all work.
- **Pinning a graph to a core.** A workload's placement has room for a CPU set.

## The PR list

102 PRs in 9 groups, in [`TODO.md`](../TODO.md), with one brief each in [`docs/prs/`](prs/). Waves
come from what each PR needs. A PR is ready when everything it needs has merged, and
`scripts/pr-status.py` lists those.

- **A. Foundation.** 18 PRs. Types, the wire, the check and the tools everyone builds on. A1 to A5 go one at a time, then the group fans out.
- **B. Rings.** 9 PRs. The shared-memory ring that values and frames travel through. The riskiest code in the rebuild, so it is proven alone first.
- **C. Comm core.** 15 PRs. The always-up program: connections, the broker, hello, discovery, time and links.
- **D. Client.** 11 PRs. One client with the same calls on any address, plus the C surface and orionctl.
- **E. Node logic.** 18 PRs. Records, peer sync, the workload tracker, actions and files.
- **F. Leaving the board.** 8 PRs. Other boards, browsers and NT4.
- **G. Access and recovery.** 6 PRs. SSH served by Orion, the recovery shell and the recovery build.
- **H. Microcontrollers.** 8 PRs. The device side on the same wire, with its size gate.
- **I. Finish.** 9 PRs. One binary, packaging, the Daedalus bridge, failure drills and the hardware proof.

## Proof on hardware

All on a CM5.

| Test | Passes when |
| --- | --- |
| Publish with a subscriber awake | No system call from the publisher |
| Frame topic at 120 fps, hold marks included | About 18 µs per frame, Orion using no CPU per frame |
| Graph to graph, nobody remote | Orion idle |
| Memory under the full robot setup | 2.5 MB data and 2 MB code, both parts together |
| Kill each part while running | Behaves as in the table above |
| Browser | Live values and video, a slow reader hurting nobody else |
| NT4 | Works against the SystemCore as server and as client |
| Recovery | SSH into the initramfs with only the comm core |
| MCU | Size gate holds, one real board runs against the gateway |
