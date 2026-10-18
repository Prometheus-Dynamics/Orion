# orion-nt4

NetworkTables 4.1 (the FRC NT4 protocol used by WPILib 2024 and later, and by PhotonVision) for
Orion tools. One crate, two roles:

- **Client**: connect to a robot's or a camera's NT server, subscribe by name or prefix, publish
  topics, and estimate the server clock. Used by Atlas's live NetworkTables viewer and by HeliOS.
- **Server**: serve topics from a host app, with values that the host edits itself. Used by Atlas
  to run a local NT4 server, so PhotonVision or HeliOS cameras can be tested without a robot. It is
  not a robot simulator: it holds only the values you give it.

The crate does not depend on `orion-node`. Atlas and HeliOS depend on it alone, by git rev. It
needs only Tokio, a WebSocket client and server (`tokio-tungstenite`, no TLS), `rmp`/`rmpv` for
MessagePack, and serde_json. There is no `unsafe` code and no C dependency, so it cross-compiles
for `aarch64-unknown-linux-musl` and other musl targets.

Cargo features: `client` and `server`, both on by default. A consumer that needs one role builds
with `default-features = false, features = ["client"]` (or `"server"`) and gets only that half.

Flow control: a subscriber that falls behind keeps only the latest value per topic, while
announces and property changes are never dropped. A server client whose control backlog overflows
is disconnected. The persistence file is rewritten at most about once a second (compact JSON,
fsynced, atomic rename) and flushed when the server is dropped.

## Usage

Client, subscribing by prefix and publishing one topic:

```rust
use orion_nt4::{Client, ClientConfig, ClientEvent, SubscribeOptions, TopicEvent, Value};

let mut client = Client::start(ClientConfig::new("10.0.0.2", "helios"));
let handle = client.handle();

// Typed stream of this subscription only. Dropping it unsubscribes.
let mut cameras = handle.subscribe(&["/CameraPublisher/"], SubscribeOptions {
    prefix: true,
    ..Default::default()
})?;

// A Publisher is cheap to clone and to set from any thread: a set is one channel send.
let speed = handle.publish("/HeliOS/speed", "double")?;
speed.set(Value::Double(1.5))?;

loop {
    tokio::select! {
        Some(event) = client.next_event() => match event {
            ClientEvent::Connected => {}
            ClientEvent::Disconnected { reason } => eprintln!("lost: {reason}"),
            _ => {}
        },
        Some(event) = cameras.next() => match event {
            TopicEvent::Announced(info) => println!("topic {} [{}]", info.name, info.type_name),
            TopicEvent::Value { name, value, .. } => println!("{name} = {value:?}"),
            _ => {}
        },
    }
}
```

The client reconnects on its own with backoff (`ClientConfig::reconnect_min` and
`reconnect_max`). Subscriptions and publications are sent again after each connect, and the last
value of each publication is re-sent. Values set while disconnected are kept (the last one per
publication). `ClientHandle::server_time_us()` gives the server clock estimate, which RTT pings
(`[-1, ...]` binary frames) refine on every connect; `ClientEvent::TimeSync` reports each better
sample.

Server, with a host-owned topic that clients can subscribe to and edit:

```rust
use orion_nt4::{Properties, Server, ServerConfig, Value};

let server = Server::start(ServerConfig::default()).await?; // 0.0.0.0:5810
let host = server.handle();
host.publish("/Camera/exposure", "double", Properties::new())?;
host.set_value("/Camera/exposure", Value::Double(20.0))?; // relayed to subscribers
let current = host.value("/Camera/exposure");              // read back
host.delete("/Camera/exposure")?;
```

`ServerHandle` is the local API. `set_value` creates a topic from the value's type when it does
not exist and rejects values that do not fit the topic's type. `ServerHandle::events()` reports
client connections, values written by clients (`ValueChanged` with the client id), and protocol
warnings, so the host can show what clients are doing.

Set `ServerConfig::persist_path` to keep topics with the `persistent` property in a JSON file.
They are written on each change and restored on start. Off by default.

## Examples

```text
cargo run -p orion-nt4 --example nt4-server -- --topic /Camera/exposure=double:20
cargo run -p orion-nt4 --example nt4-dump -- --host 127.0.0.1 --prefix /
```

`nt4-server` takes `NAME VALUE` lines on stdin to edit a topic. It handles the scalar types only.
`nt4-dump` subscribes to a prefix (everything by default) and prints announces, values and
connection events.

## Measuring camera latency

`nt4-latency` measures a vision camera's end-to-end latency and timestamp accuracy with no robot.
The tool is the NT4 server. PhotonVision connects to it and syncs its clock to the tool's clock,
so the capture timestamps it publishes are on the tool's clock. A page on this machine shows an
AprilTag (tag36h11) that the tool switches on and off at random 300 to 700 ms intervals, logging
each flip as commanded and as painted. For each flip the tool measures:

- **a, true latency:** the page's paint of the flip to the arrival of the camera result that
  first shows it.
- **b, reported latency:** the camera's capture timestamp to that arrival.
- **c, timestamp error:** the capture timestamp minus the paint, minus `--display-delay-ms`. Zero
  means the capture timestamps are exact once the display lag is taken out.

Steps:

1. Start the tool: `cargo run -p orion-nt4 --example nt4-latency -- --camera cam0 --csv flips.csv`.
   `--camera` is the camera's name in PhotonVision (the `/photonvision/<name>/` table). `--count`
   sets the number of flips (default 60). `--nt-bind` defaults to port 5810.
2. In PhotonVision's networking settings, set the NetworkTables server to this machine's address,
   port 5810. Keep the camera name the same as `--camera`.
3. Open `http://127.0.0.1:8080/` on the screen, and make it fullscreen. Use `--web-port` to change
   the port.
4. Aim the camera at the screen so the tag fills a good part of the view and the screen is the
   brightest thing in it. The tool waits for the page and for PhotonVision's clock sync, then
   starts flipping.
5. Read the summary, and the per-flip CSV (`flip_cmd_us`, `flip_painted_us`, `capture_us`,
   `publish_us`, `nt_arrival_us`, and the three metrics in ms).

Measure the display delay once, from the paint to the light leaving the screen. Film the screen
with a phone in slow motion and count the frames between the flip and the visible change, or use
the monitor's specified input lag (often one to two frames plus the panel's response time). Pass it
as `--display-delay-ms`. Until then, the default of 0 makes c relative: it is right in shape but
offset by the display lag.

What the tool decodes from PhotonVision:

- `/photonvision/<camera>/hasTarget` (boolean) gives the on and off transitions.
- `/photonvision/<camera>/rawBytes` (raw) carries the `PhotonPipelineResult`. The tool reads only
  its first 32 bytes, the `PhotonPipelineMetadata`: four little-endian int64 values, `sequenceID`,
  `captureTimestampMicros`, `publishTimestampMicros` and `timeSinceLastPong`. This is the layout in
  PhotonVision's `photon-targeting` serde (`PhotonPipelineMetadataSerde`). The target list after it
  is not decoded. `latencyMillis` is not used.
- The arrival time is the tool's own receive time. PhotonVision adds its NT time offset to the
  capture and publish times before publishing, so they are on the tool's clock.

Matching: each `hasTarget` transition pairs with the `rawBytes` result that arrived closest to it
(within 100 ms). The result's capture time picks the nearest flip painted at or before it, and that
flip must show the new state. A flip with no matched transition is a drop. A result whose publish
time is more than 1 s from its arrival is not on the tool's clock (PhotonVision has not synced yet),
so it is excluded and counted.

Checks before a measurement run:

- The tag must be detected. `hasTarget` should follow the screen. If it does not, check the tag id
  and the family (tag36h11) in PhotonVision.
- Drops rise when flips come closer together than the camera's frame time. Raise
  `--min-interval-ms` if so.
- The page paints in `requestAnimationFrame`, and the change scans out about one frame later. That
  lag is part of the display delay, so measuring the delay with the same screen and refresh rate
  keeps it right.

## Wire format

| Part | Format |
| --- | --- |
| Transport | WebSocket, path `/nt/<client name>`, default port 5810, no TLS |
| Subprotocol | `v4.1.networktables.first.wpi.edu`, with `networktables.first.wpi.edu` accepted as the fallback |
| Control | Text frames: JSON arrays of `{"method", "params"}`. Client: `publish`, `unpublish`, `setproperties`, `subscribe`, `unsubscribe`. Server: `announce`, `unannounce`, `properties` |
| Values | Binary frames: MessagePack `[topic id, timestamp us, type id, value]`. One WebSocket message may hold several of these back to back (ntcore batches values this way), and the decoder reads them all |
| Clock | RTT pings: `[-1, 0, int, client time]`, answered with `[-1, server time, int, client time]` |

Batching: receiving handles any number of messages per binary frame, and a malformed one drops only the rest of its frame (a `ProtocolWarning`), not the connection. Sending is one value per frame, except that the client's reconnect replay of last values goes out as one batch. `encode_frames` builds a batch for callers that want to send several values per WebSocket message.

Type ids: boolean 0, double 1, int 2 (signed 64-bit), float 3, string 4, raw 5 (also `msgpack`,
`protobuf` and `struct:*` topics, which carry opaque bytes), boolean[] 16, double[] 17, int[] 18,
float[] 19, string[] 20. `type_id_for_name` maps type strings to ids.

## Clocks

Value timestamps are microseconds on the server's clock. `orion-nt4` servers use microseconds
since the server process started, so their clock starts near 0. A WPILib client's
`getServerTimeOffset()` against an orion server is therefore about minus the client's own Unix
time (for example -1.79e15 µs in 2026). WPILib servers use Unix time, so an orion client sees an
offset of about +1.79e15 µs there. Robot FPGA time also starts at 0, so this is the same
convention a robot uses.

A server stamps every value it stores or relays with its own clock. A timestamp the client sends
is ignored, so a relayed value's timestamp is always on the server's clock.

## Interop with WPILib

`scripts/nt4-interop.sh` checks both directions against WPILib's ntcore (the `pyntcore` package,
installed into `target/nt4-interop/venv`). It is optional and not part of CI. An orion server gets
a double[] and a string from an ntcore client, and the server's relay is checked for timestamps. An
ntcore server publishes double, boolean, string[], int and raw values, and an orion client must
receive all five.

## Bridging into Orion (future)

A later Orion bridge would turn NT topics into Orion data and back, for robots and devices that
speak NT on one side and Orion on the other. This crate is built for that use without changes to
it: subscribe-by-prefix streams give a typed event per subscription, a `Publisher` per topic is
cheap to clone and set, and the client needs only Tokio and no node runtime. The bridge itself is
not part of this crate and is not built yet.

## Scope

Implemented:

- Client and server for NT4.1 over WebSocket, with JSON control messages and MessagePack values.
- Every value type in the spec (scalars and arrays), with `Value` as the typed model and serde
  JSON as `{"type", "value"}`.
- Prefix and exact-name subscriptions with `all`, `topicsonly`, and `prefix` options. Own
  publications are echoed only to subscriptions with `all`.
- Late joiners get the last value. Retained topics stay after their publisher leaves (kept
  announced), and persistent topics also survive a restart when a persistence file is set.
- RTT-based server clock estimate (lowest-RTT sample per connection).
- Multiple clients, client-side reconnect with backoff, and a local server API for the host.

Not implemented or not verified:

- NT3 (the older protocol) is not supported. NT4.0 peers are accepted under the fallback
  subprotocol only, and the same messages are used. The 4.1 set is what is tested.
- TLS (`wss://`). Plain `ws://` only, as on the robot network.
- `struct:*`, `protobuf` and `msgpack` topics carry raw bytes. Schema decoding is the caller's job.
- The server does not throttle values for `periodic` subscriptions. Every change is sent at once,
  which is allowed but more traffic than WPILib's server sends.
- The server keeps the publisher's type on a re-publish with the same type and resets the value
  when the type changes. Re-publishing a topic owned by another client takes it over.
- Raw bytes serialize to JSON as an array of numbers. Non-finite floats serialize as `null`.

## Tests

`cargo test -p orion-nt4` runs unit tests (value types, MessagePack frames including the spec
bytes, JSON control messages) and in-process round trips over loopback: publish and prefix
subscribe, late joiners, own-echo rules, unpublish and unannounce, several clients, the local API
with client writes, every value type, RTT sync, and persistence across a restart.

`cargo test -p orion-nt4 --example nt4-latency` runs the latency tool's tests: the matching and
metric arithmetic on synthetic timelines, the metadata decoder, the clock estimate, the tag
rendering, and an in-process run with a fake camera (NT client) and a fake page (WebSocket client)
that checks the measured numbers against the simulated latency.
