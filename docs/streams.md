# Live streams

A **stream** is a set of typed series that a publisher on the node's host samples at a rate:
sensor readings, CPU and temperatures, an IMU at 100 Hz, a light's colours. The node keeps a short
history of each series in memory and serves subscribers, local or remote:

1. a catalog of the series;
2. one backfill with the history they ask for;
3. every new batch as it arrives, at full rate, for as long as they stay subscribed.

The point is a graph that is full the moment a page opens and stays live, without polling and
without a request per update.

Streams are for high-rate numbers. Slow facts that change now and then belong in the
[status lane](observability.md); durable state belongs in the observed records. Nothing in a
stream is persisted or replicated to peers.

```text
publisher (local)                    orion-node                         subscriber
StreamPublisher                      ring per series                    local ControlPlaneEventStream
  DeclareStream ------------------>  (history policy, memory cap)       or remote RemoteSession
  PublishSamples every 20-50 ms -->  catalog + backfill + batches  -->  StreamCatalog, StreamData
  <------- StreamDemand (who watches which series)                      (resume by cursor)
```

## Declaring

```rust
use orion::client::StreamPublisher;
use orion::control_plane::{HistoryPolicy, Sampling, SeriesDecl, SeriesKind, StatusSubject, StreamDecl};

let mut publisher = StreamPublisher::connect_default("board-agent").await?;
publisher.declare(StreamDecl {
    owner: StatusSubject::Node(publisher.node_id().clone()),
    id: "board.live".into(),
    series: vec![
        SeriesDecl::new("cpu-thermal.temp", SeriesKind::F32, 100_000)       // 10 Hz
            .with_unit("°C")
            .with_history(HistoryPolicy::full(300_000)),                       // 5 min
        SeriesDecl::new("imu.accel_x", SeriesKind::F32, 10_000)            // 100 Hz
            .with_history(HistoryPolicy::full(30_000).with_reduced(300_000, 100))
            .with_sampling(Sampling::WhileSubscribed),
    ],
}).await?;
```

- **Owner.** `owner` is `node/<this node>`, or the publisher's own `provider/<id>` or
  `executor/<id>` (publish that record first, as for the status lane). A stream's id is unique
  for its owner.
- **Series order is fixed.** A sample chunk names its series by index in the declaration.
- **Kinds.**

  | Kind | Encoding |
  |---|---|
  | `F32`, `U32` | 4 bytes |
  | `F64` | 8 bytes |
  | `I64` | zigzag varint |

  `U32` suits colours (`0xWWRRGGBB`).
- **Period.** `period_us` is the rate the publisher samples at; 0 means irregular, on change. It
  sizes the ring. A publisher a little faster than declared keeps its window; one much faster
  wraps sooner.
- **History.** `HistoryPolicy` keeps every sample of the last `full_ms`, and optionally averages
  over `reduced_period_ms` back to `reduced_ms`. For example, an IMU can keep 30 s at 100 Hz and
  5 minutes at 10 Hz.
- **Sampling.** `Sampling::WhileSubscribed` tells subscribers that the series' history starts
  with the first subscriber. The publisher samples it only while [`StreamDemand`](#demand) lists
  it, which suits a sensor that is expensive to read. `Sampling::Always` (the default) keeps the
  ring filling, so a late subscriber still gets its history.
- **Redeclaring.**
  - The same declaration again on the same connection changes nothing.
  - Other series, or a new connection (the publisher restarted), start a new **epoch** with new
    rings. Subscribers then get a `StreamReset { NewEpoch }`, the new catalog, and the new
    epoch's history.
- **Ownership while live.** While its publisher is connected, nobody else can declare or
  publish the stream.

## Publishing

```rust
use orion::control_plane::SampleChunkBuilder;

let mut temp = SampleChunkBuilder::new(0, SeriesKind::F32);
temp.push(now_us, 54.3);
publisher.publish(&stream_ref, vec![temp.take()]).await?;
```

- **Batching.** Publish a batch every 20–50 ms, with one chunk per series that has new samples.
  Never send a frame per sample.
- **Chunk layout.** A chunk is `t0_us`, then `(Δt varint µs, value)` pairs with times never
  decreasing. At 10 Hz an F32 sample costs about five bytes.
- **Times** are microseconds since the Unix epoch on the publisher's clock. It is the node's own
  clock, since publisher and node share the host. A subscriber that knows the host's clock
  offset corrects for it.
- **Validation.** A batch that doesn't decode, or that names a series the stream doesn't have,
  is refused whole.

## Subscribing

```rust
use orion::control_plane::{Backfill, StreamSubscription};

session.subscribe_stream(                          // RemoteSession, or a ControlPlaneEventStream
    StreamSubscription::all(stream_ref)
        .with_series(["cpu*", "imu.*"])           // names, or prefixes ending in `*`
        .with_backfill(Backfill::LastMs(300_000)),
).await?;
while let Some(event) = session.next_event().await {
    match event.event {
        ClientEventKind::StreamCatalog(catalog) => { /* series, epoch, live */ }
        ClientEventKind::StreamData(data) => { /* data.backfill, data.cursor, data.chunks */ }
        ClientEventKind::StreamReset(reset) => { /* catalog and backfill follow */ }
        _ => {}
    }
}
```

**What a subscription gets, in order:**
1. A `StreamCatalog`.
2. One `StreamData` with `backfill: true` carrying the history `Backfill` asks for: `None`,
   `LastMs(ms)`, or `Since(cursor)`.
3. A `StreamData` for each batch.

**Other behaviour:**
- **Filtering.** `min_interval_us` thins each series per subscriber. A batch with nothing for a
  subscriber isn't sent to it.
- **Before the stream exists.** A subscription to a stream that isn't declared yet waits, and
  starts when the publisher declares it.
- **Replacing.** Subscribing again on the same connection replaces the subscription.

**Resuming:**
- **Cursors.** Each `StreamData` carries `cursor = { epoch, seq }` and the `prev_seq` this
  subscriber got before it. When `prev_seq` isn't the seq it last saw, something was lost (a slow
  subscriber whose queued samples overran): resubscribe with `Backfill::Since(last_cursor)`.
- **After a reconnect**, do the same.
- **What `Since` returns.** `Since(cursor)` returns only what came after the cursor. For a cursor
  from another epoch, or one older than the kept history, the node sends a `StreamReset`, then
  the catalog and the whole history.

**Slow subscribers:**
- **Merging.** Live batches of one stream merge into the one event still queued for a subscriber,
  so it holds one growing event instead of losing batches.
- **Overrun.** Past 1 MiB queued, its samples are dropped for a `StreamReset { Overrun }`.

**When the publisher disconnects:**
- the stream and its history stay;
- subscribers get a catalog with `live: false`;
- the next declaration starts a new epoch.

**Remote operators** subscribe inside a push session ([remote-operator.md](remote-operator.md),
"Push sessions") and need read access.

## Demand

The publisher gets `ClientEventKind::StreamDemand` whenever the set of subscribers changes, and
after a declaration. It carries:
- the number of subscribers;
- the series indexes at least one of them wants.

`StreamPublisher::next_demand` (cancel-safe) and `take_demands` return them. Use them to start and
stop `WhileSubscribed` sources.

## Memory

- **Cost.** History costs 16 bytes a sample.
- **Cap.** `ORION_NODE_STREAM_MEMORY_MAX_BYTES` (default 4 MiB) caps all streams together. A
  declaration that would exceed what is left keeps proportionally less history.
- **Observability.** `resource_usage.streams` reports the number of streams, series, samples held,
  the budget, subscribers and counters.

For example, 40 gauges at 10 Hz for 5 minutes plus 6 IMU channels (30 s at 100 Hz, 5 minutes at
10 Hz) need about 2.6 MiB.

## Authorization

| Who | May |
|---|---|
| Local provider and executor clients | Declare and publish streams for subjects they own (`node/<this node>` for any of them). |
| Any local client | Subscribe. |
| Remote operators with read access | Subscribe, inside a push session. |

Nothing else may declare or publish.
