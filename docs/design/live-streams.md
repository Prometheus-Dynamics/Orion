# Design: push sessions and live streams (proposal, for review)

Status: proposal on branch `live-streams`, based on origin/main `dbc1fb3`. Nothing is
implemented yet.

## Why

A desktop operator (Atlas) wants a board's live readings at about 10 Hz or the source rate, for
as long as a page is open. Graphs must be full the moment the page opens. The board may spend
under 1% of one core on this (orion-node and the publisher together), with no forks. Today Orion
has neither part:

- **No push to remote clients.** `orion+tcp` is request/response. `RemoteOperator::watch_status`
  polls (`TODO.md`: "push-based watches over `orion+tcp` instead of polling").
- **No high-rate data.** The status lane keeps only the latest value per key, with no history
  and no sequence numbers (`TODO.md`: "Bulk and high-rate data ... uses resource endpoints").

This proposal adds both:
- **Operator sessions** over `orion+tcp`: one long-lived, signed, multiplexed connection on
  which the node pushes events.
- **Streams**: a publisher declares typed series. The node keeps a bounded history ring per
  series and serves catalog, backfill, live batches and resume to local and remote subscribers.

It also adds two policy knobs: anonymous read and auto-enrollment by source network. With them a
device package can keep its "open while the security switch is off" behaviour.

## 1. Operator sessions (push over `orion+tcp`)

A session runs on its own TCP connection, separate from the request/response `ControlTcpClient`,
so a slow query never delays pushes.

```text
client                                   node
  OperatorHello (signed, as today)  -->
                                    <--  OperatorWelcome (signed, as today)
  OpenSession { nonce } (signed)    -->
                                    <--  SessionOpened { session_id, node_epoch } (signed)
  SessionRequest { req_seq, Subscribe(..) | Unsubscribe(id) | Ack(..) } (signed)  -->
                                    <--  SessionEvent { ev_seq, sub_id, payload } (signed) ...
```

- **Framing:** the IPC stream framing, `[b'O' b'C'][version u16 LE][len u32 LE][rkyv archive]`.
  Every frame is signed like `AuthenticatedPeerRequest` / responses, so integrity holds in both
  directions. Each `SessionEvent` signature covers `(session_id, ev_seq, payload)`. The node signs
  one event per batch window, not one per sample. Batches go at most every 50 ms, which is at most
  20 ed25519 signatures a second per session: a few hundred microseconds a second on a Pi 5.
- **What can be subscribed:**
  - `Status(StatusQuery)`: bootstrap, then `StatusChange` deltas. This is the existing local
    watch machinery (`subscribe_status_watch`) behind a session.
  - `Actions(ActionQuery)`: `ActionResults` as they change. `wait_for_action` stops polling.
  - `State(rev)`: an observed-state watch, so node records and host facts push.
  - `Stream(StreamSubscription)`: section 2.
- **Delivery and backpressure:**
  - Each session has a bounded send queue.
  - Status and state events coalesce newest-per-key, as the local queue does.
  - Stream batches are never dropped silently. If the queue is full, the node drops the oldest
    batches of that subscription and sends `StreamGap{from, to}`. The client then resumes, which
    gets it a backfill.
  - The existing counters gain `session_dropped_*`.
- **Authorization:**
  - `authorize_operator` gains `OpenSession`, plus per-subscription checks: status, state and
    streams need `read`; action results need `read`.
  - Pushes are scoped to what the policy allows at subscribe time and re-checked on policy
    change. A revoked operator's session closes.
- **Limits:** `ORION_NODE_SESSION_MAX` (default 16) and `ORION_NODE_SESSION_QUEUE` (default 256
  frames). The heartbeat is a `SessionEvent::Ping` every 10 s, and a session with no reads for
  30 s closes.
- **Client:** `RemoteOperator::session() -> RemoteSession`, with these methods:
  - `subscribe_status(query) -> impl Stream<StatusChange>`;
  - `subscribe_actions(query)`;
  - `subscribe_state(rev)`;
  - `subscribe_stream(sub) -> StreamSubscriptionHandle` (below).

  The session reconnects with backoff and re-subscribes; stream subscriptions resume by cursor.
  `watch_status` and `watch_actions` become session-backed when the node supports sessions, and
  keep polling otherwise. The check is the protocol version in `OperatorWelcome`.

## 2. Streams

### Model

```rust
pub struct StreamId(String);                 // "board.live", unique per owner subject
pub struct StreamDecl {
    pub owner: StatusSubject,                // node/<id> | provider/<id> | resource/<id> ...
    pub id: StreamId,
    pub series: Vec<SeriesDecl>,
}
pub struct SeriesDecl {
    pub name: String,                        // "cpu-thermal.temp", "imu.accel_x", "led.0"
    pub unit: Option<String>,
    pub kind: SeriesKind,                    // F32 | F64 | I64 | U32 (e.g. 0xWWRRGGBB colours)
    pub nominal_hz: f32,                     // what the publisher samples at
    pub history: HistoryPolicy,
}
pub struct HistoryPolicy {
    pub full_rate: Duration,                 // e.g. 5 min (gauges at 10 Hz), 30 s (IMU at 100 Hz)
    pub reduced: Option<(Duration, f32)>,    // e.g. (5 min, 10 Hz): older samples downsampled
}
pub struct StreamCursor { pub epoch: u64, pub seq: u64 }   // epoch: publisher session (boot + restart)
```

### Publishing (local, a provider or a node-action agent such as board-agent)

- **`DeclareStream(StreamDecl)`:** idempotent. Redeclaring with new series bumps a `catalog_rev`.
  Only the subject's owner may declare, by the same rule as `PublishStatus`.
- **`PublishSamples { stream, epoch, batch: SampleBatch }`:** one IPC frame per batch window,
  20–50 ms. A sample is never its own frame.
- **`SampleBatch`** is columnar and compact:
  - per series index, a `t0_us`, then packed `(Δt varint in µs, value LE)`;
  - a series that didn't change in the window is left out;
  - slow gauges can publish only on change (`kind` and `nominal_hz` tell subscribers how to draw
    gaps).
- **Fast path:** a publisher on the same host may use the existing latest-value fd channel
  (`UnixFdLatestServer`) for a shared-memory ring instead. Not needed at these rates; noted for
  later.

### The node's ring

- **Size:** each series has a ring sized from its `HistoryPolicy`, with time deltas u16 in
  100 µs units (a re-anchor sample on overflow) and values in their kind.
- **Downsampling:** samples older than `full_rate` are averaged per `1/reduced_hz` bucket into
  the reduced ring.
- **Memory cap:** `ORION_NODE_STREAM_MEMORY_MAX` (default 4 MiB). A declaration whose policy
  would exceed it is clamped and reported in observability. Example: Atlas's board stream, with
  30 gauge series at 10 Hz for 5 min plus 6 IMU channels at 100 Hz for 30 s and reduced to 10 Hz
  for 5 min, needs about 0.8 MiB.
- **Lifetime:** rings live in memory and are never persisted. A publisher reconnect with a new
  epoch starts new rings and tells subscribers.
- **No subscriber, no network cost.** The publisher keeps publishing into the ring, so a late
  subscriber gets history. A publisher that wants to stop expensive sources when nobody listens
  asks `StreamSubscribers(stream)`, or watches `stream.<id>.subscribers` on the status lane.

### Subscribing (local IPC stream or a remote session)

```rust
pub struct StreamSubscription {
    pub owner: StatusSubject, pub id: StreamId,
    pub series: Vec<String>,                 // names or prefix patterns ("imu.*"); empty = all
    pub max_hz: Option<f32>,                 // server-side decimation per subscriber
    pub backfill: Backfill,                  // None | Last(Duration) | Since(StreamCursor)
}
```

The node answers in order:
1. `StreamCatalog { catalog_rev, series }`.
2. `StreamBackfill { cursor_end, chunks }`, one message: the requested history from the rings in
   the same columnar encoding, decimated to `max_hz`.
3. A `StreamBatch { cursor, chunks }` for every publisher batch, forwarded within one batch
   window. The rate is full and stays full: no slow-down after the first seconds.

On resume with `Since(cursor)`:
- Same epoch and the seq still in the ring: the node sends only the batches after it.
- Otherwise it sends `StreamReset { reason: NewEpoch | OutOfRange }`, then a fresh catalog and
  backfill.

### Cross-node

Phase 1 answers subscriptions for subjects the connected node owns. Atlas connects to each
board's own node. Forwarding a subscription to the owning node over the peer transport, as
`QueryStatus` is forwarded today, is phase 3.

## 3. Policy: anonymous read and auto-enrollment

Two new node settings, both off by default:

| Env | Example | Effect |
|---|---|---|
| `ORION_NODE_ANONYMOUS_READ` | `streams,status` | With `PEER_AUTH=optional`, unauthenticated callers may `OpenSession` and subscribe to streams and status (read only). Never actions, never writes. |
| `ORION_NODE_OPERATOR_AUTO_ENROLL` | `172.31.0.0/16=reboot,usb-boot,locate` | An operator `EnrollmentHello` from a source address in the CIDR is approved without an administrator, with that action policy (and `read`). It's recorded in the trust store as `method = auto(cidr)` and audited. |

**How a device package maps its security switch:**
- **Switch off:**
  - `PEER_ADDR=0.0.0.0:9200`;
  - `PEER_AUTH=optional`;
  - `ANONYMOUS_READ=streams,status`;
  - `OPERATOR_AUTO_ENROLL=<usb-link cidr>=reboot,usb-boot`.
- **Switch on:**
  - `PEER_AUTH=required`;
  - no anonymous read, no auto-enroll;
  - only operators enrolled by an administrator or with the enrollment key.

The node reads these at start. The device package restarts orion-node when the switch changes,
which is rare.

The listener stays integrity-only, without encryption, as `orion+tcp` documents.

## 4. Protocol version

This adds control messages and archived types, so `CONTROL_PROTOCOL_VERSION` goes 4 → 5:
- **Messages:** `OpenSession`, `SessionOpened`, `SessionRequest`, `SessionEvent`,
  `DeclareStream`, `PublishSamples`, `SubscribeStream`, `StreamCatalog`, `StreamBackfill`,
  `StreamBatch`, `StreamReset`, `StreamGap`.
- **Observability:** a `streams` section, plus the session counters.

Mixed versions aren't supported, as today. A v5 client talking to a v4 node keeps the polling
watches, because `OperatorWelcome` carries the version. Downstreams (HeliOS's orion-node
artifact, Atlas's crates) move to the same rev together.

## 5. Tests and measurement

- **Unit:**
  - ring sizing, wrap-around and downsampling;
  - columnar encode/decode round trips (with a fixture, like `link_encoding.rs`);
  - cursor resume, gap and reset;
  - policy (anonymous read limits, the auto-enroll CIDR).
- **Integration, in-process node:**
  - a publisher at 100 Hz, a remote session subscriber with a backfill of the last 5 min, then
    live;
  - a client disconnected for 2 s resumes with no gap;
  - a publisher restart gives a reset;
  - a slow subscriber gets a gap, not unbounded memory.
- **perf-check:**
  - CPU and allocations per second for orion-node with one publisher (40 series at 10 Hz plus
    6 at 100 Hz) and one session at 10 Hz. Target: under 0.5% of one Pi 5 core for orion-node;
  - time from subscribe to the last backfill byte.
- **Soak:** an hour of streaming in `appliance_memory_soak`, with RSS flat.

## 6. Users of this (outside Orion, for context)

- **board-agent (Atlas repo):**
  - declares `board.live` on `node/<id>`;
  - samples /sys and /proc at 10 Hz in Rust;
  - takes lemnosd devices at their rate;
  - publishes 50 ms batches over its existing IPC session;
  - publishes slow state (`board.*`) to the status lane on change;
  - claims more node actions.
- **Atlas:** one `RemoteSession` per board. It subscribes to `board.live` with a 5-minute
  backfill and to status `board.*`, and runs actions through Orion. The board's HTTP endpoints
  remain as the fallback for boards without this.
