# Design: the stable message interface

Status: decided on 2026-11-03 (TODO.md, "Decided, starts when the user says"). This document
fixes the rules and the order of the work; each step lands on `dev` on its own.

## Why

Orion's control protocol is rkyv. Archives are layout-exact, so both sides must come from the same
build, and a node refuses any other `CONTROL_PROTOCOL_VERSION` (protocol-compatibility.md). That
stays: the rkyv protocol is internal to same-build software (`orion-node`, `orionctl`,
`orion-client` built with the node).

Software built outside the image needs a wire that survives an Orion upgrade: C programs, other
languages, products on their own release cycle, microcontrollers. The link protocol already has
the rules that need (link-protocol.md, "Versioning"): stable kind numbers, a version byte, unknown
kinds ignored, appended fields allowed, and recorded fixtures that pin every byte. The stable
interface is the link protocol, extended. It is not a second format.

## Rules

These hold for every kind, old and new. `crates/link/tests/link_encoding.rs` enforces the bytes.

1. **Append only.** Nothing gets frozen, but nothing changes in place. A new capability is a new
   kind, or a field appended to the end of a body. Kind numbers are never reused.
2. **No change in the middle of a body.** Postcard is not self-describing, so a field added
   inside a nested struct shifts every byte after it. Nested types stay as they are; a body that
   needs more grows at its end, or a new kind replaces it.
3. **New enum variants only where the peer knows them.** A reader that predates a variant fails
   to decode the whole body, so a variant is appended to the enum, belongs to a revision, and a
   side sends it only when the session's revision includes it.
4. **Appended fields have defaults.** A body shorter than the reader's type is valid: missing
   trailing fields take their defaults. Postcard does not do this by itself, so step 1 adds it to
   the decoder. Longer bodies keep working as today (trailing bytes ignored).
5. **The wire types are the link's own.** Bodies are defined by types in `orion-link`, not by the
   node's records; `orion-link` converts between them. A record change in `orion-control-plane`
   does not change the wire: the conversions stop compiling until the change is decided for the
   link, and the fixtures catch any change to a wire type.
6. **`LINK_PROTOCOL_VERSION` stays 1.** A bump means every deployed device and client stops
   working with the new node, so it is reserved for a framing change. Additions use revisions
   (below).
7. **`Hello` and `Reject` keep their prefix.** Their existing fields stay first and unchanged,
   so a version mismatch can always be reported.

## Revisions and versioned clients

Unknown kinds are ignored, which keeps old peers working, but a client must also know whether the
host will answer a request at all. So both sides announce an interface revision:

- `Hello` appends `revision: u16`, the highest revision the client speaks. Missing means 0, the
  link protocol as it is today.
- `Welcome` appends `revision: u16`, the highest revision the host speaks.
- Each side uses `min(own, peer)`. A kind belongs to the revision that introduced it, and a side
  sends it only when the session's revision includes it.
- A client library is built against one revision and works with any host: with an older host it
  sees a lower session revision and reports the missing requests as unsupported, without sending
  them.

Revision 0 is the link protocol before revisions. Each step below that adds kinds raises the
revision by one, recorded in the table in link-protocol.md ("Revisions").

## Roles

`Hello::roles` keeps its meaning and gains one bit:

| Bit | Role | Session |
|---|---|---|
| `0x01` | provider | publishes a provider, its resources and status; gets its leases and actions (today). |
| `0x02` | executor | publishes an executor and its workloads; gets its assignments (kinds `0x12` and `0x13`, reserved today). |
| `0x04` | client | reads and watches, runs actions, subscribes to streams. Publishes nothing. |

A session may combine roles. A device on a serial or CAN link stays a provider (and later an
executor); the client role is for local and remote connections.

## Kinds

New kinds come in ranges, so a reader can tell what an unknown kind was for:

| Range | Use |
|---|---|
| `0x01`-`0x06` | session (today) |
| `0x10`-`0x14` | provider and executor state (today, `0x12`/`0x13` reserved for executor) |
| `0x20`-`0x2F` | firmware transfer (today) |
| `0x30`-`0x33` | actions run on the device (today) |
| `0x40`-`0x4F` | requests and their errors: `Request` correlation, `RequestError` |
| `0x50`-`0x5F` | status lane: query, watch, changes |
| `0x60`-`0x6F` | client actions: run, query, watch, results |
| `0x70`-`0x7F` | streams: declare, publish, subscribe, catalog, data, reset, demand |
| `0x80`-`0x8F` | records: workloads, resources and providers read by clients |

Every request carries a `req: u32` chosen by the client and echoed by every answer to it, so
requests can be pipelined and watches told apart. A request the host refuses is answered with
`RequestError { req, code, reason }`, `code` from a list that only grows.

## Transports

| Transport | Framing | Sessions | Status |
|---|---|---|---|
| serial | COBS, `0x00`-delimited | one | today |
| CAN, CAN FD | segments | one per address | today |
| Unix stream socket | COBS, as serial | one per connection | step 2 |
| remote (TCP) | COBS inside TLS or the `orion+tcp` signing | one per connection | step 6 |

A Unix link is configured like the others: `ORION_NODE_LINKS='unix:/run/orion/link.sock?...'`.
Its frames stay at `HOST_MAX_FRAME` (4096) for now. The host's receive buffer is sized by that
constant for every session, serial and CAN included, so raising it for all would cost each session
the larger buffer; a larger limit for Unix links comes with the step whose kinds need it (streams),
with the buffer sized per transport.

## Authorization and limits

- **Unix links:** the caller's `SO_PEERCRED` identity is checked like the local IPC sockets
  (`ORION_NODE_LOCAL_AUTH`, `ORION_NODE_LOCAL_AUTH_ALLOW`). The provider and executor roles also
  need the provider lane (`ORION_NODE_LOCAL_PROVIDER_ALLOW`). A session that asks for a role its
  caller may not have is refused with `Reject`. The link's `allow` list of device names still
  applies. A caller the policy admits to nothing is disconnected at connect; the client role's
  requests are authorized per request (see "Step 3 wire").
- **Ownership:** a session publishes only for its own provider or executor, as local clients do.
- **Rate limit:** each connection has its own frame budget (the link's `max_frames_per_s`,
  8192 by default, a token bucket of one second's worth). Past it, reads wait for the next
  frame's turn. Frames wait instead of being refused, so the final result of an action the
  session was handed is never lost, and no exemption is needed. A connection held back counts as
  heard, so the wait never times its session out. (Until 2026-11-06 this was the control
  sockets' budget, 256 frames per 1 s window, and a throttled read waited for the window to end.)
- **Queues:** host to client traffic is bounded in bytes per session. Watches coalesce to the
  latest value per key when the queue is full; stream data reports the gap with its cursor, as
  today.
- **Remote:** decided at step 6. The options are the operator identities of `orion+tcp` (ed25519,
  enrollment) or TLS client certificates.

## Step 3 wire (revision 1)

Decided 2026-11-06. Bodies are postcard, as every other kind; `String` is UTF-8 with a varint
length, `Option` and `Vec` are postcard's. All of them are link-owned types (`orion-link`), and each
enum below only grows at its end, inside a later revision (rule 3).

A session takes the client role with `Hello { roles: 0x04, revision: 1, .. }`; it may also be a
provider. A client-only session publishes nothing and gets no leases. Client kinds are sent only
when the session's revision is at least 1; a host that receives one in a revision-0 session ignores
it, as any unknown kind.

| Kind | Name | Direction | Body |
|---|---|---|---|
| `0x40` | `RequestError` | host → client | `req: u32, code: ErrorCode, reason: String` |
| `0x41` | `RequestEnd` | client → host | `req: u32`: ends the watch `req`; nothing answers it |
| `0x50` | `StatusQuery` | client → host | `req: u32, filter: StatusFilter` |
| `0x51` | `StatusWatch` | client → host | `req: u32, filter: StatusFilter` |
| `0x52` | `StatusChanges` | host → client | `req: u32, bootstrap: bool, more: bool, updated: Vec<StatusRecord>, expired: Vec<StatusKey>` |
| `0x60` | `ActionRun` | client → host | `req: u32, action_id: String, target: Target, name: String, args: Vec<(String, Value)>, deadline_ms: u64` |
| `0x61` | `ActionQuery` | client → host | `req: u32, filter: ActionFilter` |
| `0x62` | `ActionWatch` | client → host | `req: u32, filter: ActionFilter` |
| `0x63` | `ActionResults` | host → client | `req: u32, more: bool, results: Vec<ActionRecord>` |

Types:

- `Subject` and `Target`: an enum of a kind and an id string. `Subject` is
  `Provider, Executor, Resource, Workload, Node`; `Target` is `Node, Provider, Resource, Executor`,
  in that order (the node's own order).
- `Value`: the link's existing value encoding (`Bool, Int, UInt, String, Bytes, F64`).
- `StatusFilter { subject: Option<Subject>, key_prefix: Option<String> }`.
- `StatusRecord { subject: Subject, key: String, value: Value, ttl_ms: u64, published_at_ms: u64 }`;
  `StatusKey { subject: Subject, key: String }`.
- `ActionFilter { action_id: Option<String>, target: Option<Target> }`.
- `ActionRecord { action_id, target, name, state: ActionStateWire, output: Vec<(String, Value)>,
  handled_by: String, requested_by: String, created_at_ms: u64, updated_at_ms: u64 }`;
  `ActionStateWire` is `Accepted, Running { progress: Option<u16> }, Succeeded, Failed { reason },
  Rejected { reason }, TimedOut, Cancelled`.
- `ErrorCode` (`u16`, the list only grows): `1` unsupported (a request the host does not serve),
  `2` not authorized, `3` invalid (a malformed or refused request), `4` limit (too many open
  watches, or the rate limit for the request), `5` internal.

Answers:

- An answer larger than the session's frame size is split into parts, each a frame of the same
  kind and `req`; every part but the last has `more: true`. A client applies a `bootstrap` answer
  (all its parts) as the whole current set for its filter. A single record larger than a frame
  cannot be sent: the request gets `RequestError { code: limit }` and ends (a watch with it).
- A query is answered by one `StatusChanges` (`bootstrap: true`) or one `ActionResults`.
- A watch is answered by a first `bootstrap` change (status) or the matching results (actions,
  possibly none), then by changes until the client ends it (`RequestEnd`) or the session ends. Status changes coalesce to
  the newest value per key while the client's queue is full, as for local clients.
- `ActionRun` is answered with the action's current result, then with each update until the result
  is final; the request ends there. Resubmitting the same `action_id` and content returns the same
  action, as the control protocol does.
- A refused request gets one `RequestError` and ends.

Limits: one status watch and one action watch per session (a second is refused with `limit`; open
another connection for more), and at most 16 actions run by the session that are not yet final.

Authorization: the node handles client requests as requests of a local control-plane client
registered for the session (named `link:<link>/<device name>`), with the caller's peer identity,
so the control sockets' authorization and rate limit apply as they are: a caller admitted only to
the provider lane gets `not authorized` for every client request (its session is not refused, so
it can still be a provider).

Decided at implementation (2026-11-06):

- Only Unix links serve the client role (`HostConfig::serve_clients`): their callers have a peer
  identity to authorize. On serial and CAN links a client-only `Hello` is rejected with
  `UnsupportedRoles`, and client requests in a provider session get `unsupported`.
- The role needs revision 1 on both sides; a revision-0 `Hello` with the client bit is a provider
  session (or `UnsupportedRoles` without the provider bit). `Welcome` and `DeviceConnected` report
  only the roles the host serves for the session.
- A client-only session gets no `Leases`, and its provider kinds are ignored (counted as decode
  errors).
- Answers wait in the gateway while the session's output is full; the node's events for the client
  stay in its queue, where status coalesces to the newest value per key and action results to the
  newest per action. An answer is queued as all of its parts or none, and needs at most the
  session's output queue (32 frames).
- `RequestError` reasons are cut to 256 bytes at a character boundary.

## Step 4 wire (revision 2)

Decided 2026-11-06. Streams as the node serves them (`docs/streams.md`): a publisher declares
typed series and publishes batches of sample chunks; subscribers get a catalog, a backfill, then
every batch. Sample chunks keep their encoding (per series, `t0_us` then packed `(Δt, value)`
pairs). Stream kinds are sent only when the session's revision is at least 2.

| Kind | Name | Direction | Body |
|---|---|---|---|
| `0x70` | `StreamDeclare` | provider → host | `req: u32, id: String, series: Vec<Series>` |
| `0x71` | `StreamPublish` | provider → host | `id: String, chunks: Vec<Chunk>` |
| `0x72` | `StreamDemand` | host → provider | `id: String, subscribers: u32, series: Vec<u16>` |
| `0x73` | `StreamSubscribe` | client → host | `req: u32, owner: Subject, id: String, series: Vec<String>, min_interval_us: u32, backfill: Backfill` |
| `0x74` | `StreamCatalog` | host → client | `req: u32, epoch: u64, series: Vec<Series>, live: bool` |
| `0x75` | `StreamData` | host → client | `req: u32, epoch: u64, seq: u64, prev_seq: u64, backfill: bool, more: bool, chunks: Vec<Chunk>` |
| `0x76` | `StreamReset` | host → client | `req: u32, reason: ResetReason` |

Types:

- `Series { name: String, unit: Option<String>, kind: SeriesKind, period_us: u32,
  history: { full_ms: u32, reduced_ms: u32, reduced_period_ms: u32 }, sampling: Sampling }`;
  `SeriesKind` is `F32, F64, I64, U32`, `Sampling` is `Always, WhileSubscribed`.
- `Chunk { series: u16, t0_us: u64, count: u32, data: Vec<u8> }`, the node's `SampleChunk`.
- `Backfill` is `None, LastMs(u64), Since { epoch: u64, seq: u64 }`.
- `ResetReason` is `NewEpoch, OutOfRange, Overrun, Removed`.

Publishers:

- A session with the provider role declares streams of its own provider: the owner is implied,
  `provider/<the provider id of the session's last ProviderState>`, so a declaration before the
  first `ProviderState` is refused. A refused declaration gets `RequestError { req }`; an accepted
  one gets its first `StreamDemand`, and another whenever the demand changes. Declaring the same
  id with the same series again changes nothing; other series start a new epoch.
- `StreamPublish` is not answered: a batch for a stream the session did not declare, or that does
  not decode, is dropped and counted. The session's end leaves its streams without a publisher
  (`live: false`); their history is still served.

Subscribers:

- A session with the client role subscribes with `StreamSubscribe`, answered by `StreamCatalog`,
  the backfill (`StreamData { backfill: true }`), then each batch, until `RequestEnd { req }` or the
  session ends. A stream that is not declared yet is waited for. After a `StreamReset` (other than
  `Removed`) a catalog and the history follow; `Removed` ends nothing, the subscription waits for a
  new declaration.
- One subscription per stream per session (a second is refused with `limit` until the first is
  ended), and at most 8 per session.
- `StreamData` larger than a frame is split into parts by chunk (`more` on all but the last, the
  same cursor on all); a chunk larger than a part is split at sample boundaries first, so no batch
  is ever refused for its size. A subscriber that falls behind gets batches merged while they wait
  (one frame carries more samples), then `StreamReset { Overrun }` past the node's bound.

Frames stay at 4096 bytes on Unix links: parts carry larger answers, so the host's receive buffer
does not grow for any transport. Serial and CAN links do not serve streams yet
(`HostConfig::serve_streams`): their sessions' declarations are refused with `unsupported`.

## Step 5 wire (revision 3)

Decided 2026-11-06. The executor role, and client reads of the node's records. Kinds of this
step are sent only when the session's revision is at least 3. Only Unix links serve them, as for
the client role.

| Kind | Name | Direction | Body |
|---|---|---|---|
| `0x12` | `ExecutorState` | executor → host | `executor: Executor, workloads: Vec<Observed>, resources: Vec<Resource>` |
| `0x13` | `Workloads` | host → executor | `bootstrap: bool, more: bool, workloads: Vec<Workload>, removed: Vec<String>` |
| `0x80` | `RecordQuery` | client → host | `req: u32, filter: RecordFilter` |
| `0x81` | `RecordWatch` | client → host | `req: u32, filter: RecordFilter` |
| `0x82` | `Records` | host → client | `req: u32, bootstrap: bool, more: bool, updated: Vec<Record>, removed: Vec<RecordKey>` |

Types:

- `Executor { executor_id: String, node_id: String, runtime_types: Vec<String> }`. The host
  overwrites `node_id` with its own, as for provider state: an executor session runs on the node
  it talks to.
- `Observed { workload_id: String, state: ObservedState }`; `ObservedState` is `Pending,
  Assigned, Starting, Running, Stopped, Completed, Failed` (the node's order).
- `Resource`: the provider kinds' resource body (kind `0x10`).
- `Workload { workload_id, runtime_type, artifact_id: String, config: Option<Config>,
  desired: DesiredState, observed: ObservedState, assigned_node: Option<String>,
  bindings: Vec<Binding>, restart: RestartPolicy }`, with `Config { schema_id: String,
  payload: Vec<(String, Value)> }`, `Binding { resource_id: String, node_id: String,
  remote: Option<{ endpoints: Vec<String>, available: bool }> }`, `DesiredState` `Running,
  Stopped`, `RestartPolicy` `Never, OnFailure, Always`. A workload's requirements and placement
  are the node's inputs for choosing where it runs; the wire does not carry them.
- `RecordKind` is `Workload, Resource, Provider, Executor`. `RecordFilter { kind: RecordKind,
  id: Option<String>, node: Option<String> }` (`node`: the assigned node of a workload, the node
  of a provider or executor, the node of a resource's provider).
- `Record` is `Workload(Workload), Resource(Resource), Provider(Provider), Executor(Executor)`,
  `Provider` the provider kinds' body; `RecordKey { kind: RecordKind, id:
  String }`.

Executors:

- A session with the executor role (`0x02`) sends `ExecutorState` whenever its state changes:
  the node applies it as a local executor's state update, the workloads' records from the node's
  desired state with the observed state the executor reports. A workload the node does not
  assign to the executor is ignored. `ExecutorState` is acknowledged with `Ack`, as provider
  state.
- The host answers the first `ExecutorState` with `Workloads { bootstrap: true }`, the workloads
  assigned to the executor, then sends each change: changed or new workloads in `workloads`,
  unassigned ones by id in `removed`. Parts as for client answers (`more`). The session's end
  leaves the executor's workloads to the node's lease and liveness rules, as for a local
  executor that disconnects.

Records:

- `RecordQuery` is answered by one `Records { bootstrap: true }`; `RecordWatch` by a first
  bootstrap, then by changes until `RequestEnd` or the session's end. Each change carries the
  records that differ from what the client was sent last, and the keys of those gone. The host
  keeps a 64-bit digest per record a watch has sent, not the record.
- One record watch per session. Records larger than a frame end the request with `limit`, as
  status records do.

Authorization: an executor session is a local executor client of the session's caller and needs
the provider lane, like a provider session. Record reads are client requests, authorized like
status queries.

## Not covered

Desired-state writes, peers, enrollment, maintenance, observability and blobs stay on the rkyv
protocol, for same-build tools (`orionctl`). They can move later, one kind range at a time.

## Steps

Each step lands on `dev` with its fixtures, tests and docs, and raises the revision when it adds
kinds.

1. **Wire types and rules.** This document, then link-owned body types in `orion-link`; the
   fixtures stay byte for byte (proof the wire did not change). `revision` appended to `Hello` and
   `Welcome`. Still revision 0: nothing new to announce yet. Done.
2. **Unix listener.** `unix:` links with many sessions, peer identity, roles, rate limit and
   byte-bounded queues. The existing provider kinds work over it unchanged. Done.
3. **Client role, status and actions.** Request correlation, status query and watch, run, query
   and watch actions. Revision 1. Done.
4. **Streams.** Declare and publish for providers, subscribe for clients. Sample chunks keep
   their current encoding. Revision 2. Done.
5. **Records and the executor role.** Workloads and assignments (`0x12`, `0x13`), resource and
   provider reads. Revision 3. Done.
6. **Remote transport.** Revision unchanged unless it needs kinds.
7. **`liborion`.** A C library over the interface: opaque handles, byte buffers, a pollable fd,
   and a blocking client for the fd latest-value channel.
8. **Versioned MCU template.** The template's C API announces its revision and gains a version
   function.

The node's footprint is measured at step 2: the gateway (`link-gateway`) is not a default
feature today, and the packaged node would carry it.
