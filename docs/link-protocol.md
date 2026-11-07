# Orion Link Protocol

The link protocol connects microcontrollers and other constrained devices to an `orion-node` over
low-level links such as UART, RS-485, USB-CDC, classic CAN, and CAN FD. A device takes part as an
ordinary Orion provider (and optionally executor): it publishes typed `ProviderRecord` /
`ResourceRecord` state and receives its leases, exactly like an IPC client. The node-side gateway
bridges link sessions into the node.

Goals, in priority order:

1. **Any MCU, minimal bring-up.** No chip, HAL, RTOS, or async runtime is assumed. A port supplies
   bytes in, bytes out, and a millisecond clock.
2. **Minimal footprint.** Framing and the device session need no allocator: a complete UART
   device fits in about 8 KB of flash and under 1 KB of RAM. Only the host side and optional
   full-record conveniences on the device use `alloc`.
3. **Robust on lossy links.** Every frame is CRC-checked. Every state message is an idempotent full
   snapshot, so loss is repaired by retransmission, never by replaying deltas.
4. **Same model as the rest of Orion.** Devices publish the shared `orion-core` /
   `orion-control-plane` records: either as borrowed `orion_link::wire` views that encode to the
   same bytes (no allocator), or as the records themselves (built `no_std` + `alloc`). The gateway
   maps them onto the node's normal provider path.

## Crates

| Crate | `std`? | Role |
| --- | --- | --- |
| `orion-link` | `no_std` (framing and the device path with its hand-written codec, feature `device`: no `alloc`, no dependencies; serde messages over the records: `alloc`; host session: `std`) | Framing codecs, link messages, the sans-IO device session, and the sans-IO host session. |
| `orion-node` (`link-gateway` feature) | std | Serves configured serial, SocketCAN and Unix socket links and bridges each device session into the node as a provider. |

Optional `orion-link` features add adapters for common trait ecosystems without changing the core:
`embedded-io` (blocking byte streams), `embedded-io-async`, and `embedded-can` (CAN frames). The core
API is sans-IO: the session never performs I/O and never reads a clock itself.

## Layering

```
 typed messages (postcard, LINK_PROTOCOL_VERSION)      ← orion-link::wire (device, no alloc)
                                                         orion-link::message (host, serde)
 ───────────────────────────────────────────────
 message frame: [header][payload][crc32c]              ← orion-link::frame
 ───────────────────────────────────────────────
 transport encoding                                     ← orion-link::stream | orion-link::packet
   byte streams:  COBS, 0x00-delimited
   packet links:  segmentation into CAN / CAN FD frames
 ───────────────────────────────────────────────
 physical link (user code: UART driver, CAN peripheral, USB-CDC, ...)
```

### Message frame

Every message, regardless of link, is one *message frame*:

| Field | Size | Notes |
| --- | --- | --- |
| `version` | 1 byte | `LINK_PROTOCOL_VERSION`. A mismatch is answered with `Reject { VersionMismatch }`. |
| `kind` | 1 byte | Message kind (see below). Unknown kinds are ignored, which allows additive extensions. |
| `seq` | 2 bytes LE | Sender sequence number, used for acks and duplicate suppression. |
| `payload` | variable | `postcard`-encoded body for `kind`. |
| `crc` | 4 bytes LE | CRC-32C (Castagnoli) over `version..payload`. Frames with a bad CRC are dropped silently. |

The maximum frame size is negotiated in the handshake and bounded at compile time on the device
(const-generic buffer sizes), so a device never allocates for framing.

Encoding choice: `postcard` instead of `rkyv` on the link, because it is compact (varints, no
alignment padding), which matters on classic CAN's 8-byte frames and slow UARTs. The node-side IPC
protocol remains `rkyv`; the gateway converts between the two.

### Byte streams (UART, RS-485, USB-CDC, TCP for testing)

Message frames are COBS-encoded and terminated with `0x00`. A receiver resynchronizes at the next
`0x00` after any corruption or overflow. Half-duplex RS-485 buses with several devices are out of
scope for v1 (one device per stream link).

### Packet links (classic CAN, CAN FD)

A message frame is split into segments, each carried in one CAN frame:

| Byte 0 bits | Meaning |
| --- | --- |
| 7 | start of message |
| 6 | end of message |
| 5..0 | segment counter (mod 64) |

Classic CAN carries 7 payload bytes per frame, CAN FD up to 63. A missing or out-of-order segment
discards the partial message; the frame CRC guards reassembly.

CAN identifiers are not fixed by the protocol. A link is configured with one identifier for
device→host and one for host→device traffic (standard or extended IDs), typically
`base + device_address`, so many devices share one bus. Lower identifiers win arbitration, so
deployments can prioritize devices by address.

### Framing rules

These rules pin down details that the layer summaries above leave open. `orion-link` implements
them exactly.

- **Version check.** Frame decoding validates length and CRC and reports the version byte without
  rejecting it, so the host can answer `Reject { VersionMismatch }` instead of dropping the frame.
- **Frame size.** There is no wire-level maximum; frames are bounded by the receiver's
  const-generic buffer, and oversized frames are dropped and counted as overflows.
- **COBS.** Canonical COBS: a final full `0xFF` block gets no trailing `0x01`. Empty packets
  (consecutive `0x00`) are ignored and not counted.
- **Leading delimiter.** Senders may emit a `0x00` before each frame. It terminates line noise, so
  noise cannot corrupt the next frame. Without it, garbage between frames costs exactly that frame.
- **Error accounting.** Each corrupt stream packet is counted once, when its delimiter arrives.
- **Segment counters** start at 0 on each start segment and increase by one per segment, mod 64.
  Receivers do not require the start counter to be 0.
- **No CAN FD padding.** Message frames carry no length field, so padding would be
  indistinguishable from payload. Full segments use the whole MTU, and the tail is split into
  segments whose lengths are each a valid CAN FD length (0–8, 12, 16, 20, 24, 32, 48, 64). This
  sometimes costs one or two extra CAN frames (an 8-byte frame on CAN FD is sent as 8 + 2 bytes).
  Smaller FD MTUs are allowed.
- **Duplicate segments.** A segment with the same counter and the same bytes as the previous one is
  ignored as a CAN-level retransmission. Comparing bytes ensures a new message after a lost tail is
  not mistaken for a duplicate.
- **Interrupted messages.** A start segment mid-message drops the partial message (counted as
  interrupted) and begins the new one.
- **Discard until end.** After a sequence error, overflow, or orphan segment, the rest of that
  message's segments are dropped until its end segment or the next start segment.
- **Duplicate messages.** A duplicated single-segment message is delivered twice by the framing
  layer; duplicate suppression by `seq` belongs to the session layer.
- **CAN identifiers.** `CanLinkIds::for_address(base, addr)` adds `addr` to both base identifiers and
  fails if either result leaves the 11-bit or 29-bit range or if the two collide.

## Messages

Kind numbers are stable constants (`orion_link::message::kind`) and are never reused. Bodies are
postcard encodings of the listed fields, in order.

| Kind | Name | Direction | Body | Notes |
| --- | --- | --- | --- | --- |
| `0x01` | `Hello` | device → host | `device_name: String`, `roles: u8` (bit 0 provider, bit 1 executor, bit 2 client), `max_frame: u32`, then `revision: u16` when not 0 | Opens or reopens a session. The protocol version is the frame header's `version`; the body carries no separate copy. `revision` is the highest interface revision the device speaks (see "Revisions"). |
| `0x02` | `Welcome` | host → device | `node_id`, `session_id: u32`, `heartbeat_ms: u32`, `max_frame: u32`, then `revision: u16` when not 0 | `max_frame` is the negotiated minimum of both sides. `revision` is the host's highest interface revision. |
| `0x03` | `Reject` | host → device | `reason: u8` | See the reject reasons below. |
| `0x04` | `Ping` | device → host (either direction is answered) | `now_ms: u64` | Sent by the device every heartbeat. |
| `0x05` | `Pong` | answer to `Ping` | `now_ms: u64` | Echoes the ping's `now_ms`, so the pinging side can measure the round trip. |
| `0x06` | `Ack` | host → device | `seq: u16` | Acknowledges the `ProviderState` frame sent with `seq`. |
| `0x10` | `ProviderState` | device → host | `ProviderRecord`, `Vec<ResourceRecord>` | Full snapshot; idempotent. The gateway owns `ProviderRecord::node_id`. |
| `0x11` | `Leases` | host → device | `Vec<LeaseRecord>` | Full set for this device's provider. |
| `0x12` | `ExecutorState` | device → host | | Reserved for the executor role. |
| `0x13` | `Workloads` | host → device | | Reserved for the executor role. |
| `0x14` | `Status` | device → host | `Vec<StatusEntry>`; each `key: String`, `value: TypedConfigValue`, `ttl_ms: u32` | Volatile status values (see "Status" below). Fire-and-forget: never acknowledged or retransmitted. Added in version 1 as an additive kind. |
| `0x20` | `DeviceInfo` | device → host | firmware state; see "Firmware transfer" (feature `xfer`) |
| `0x21`-`0x28` | firmware transfer | both | `XferBegin`, `XferChunk`, `XferAck`, `XferEnd`, `XferResult`, `XferAbort`, `Activate`, `Confirm` (feature `xfer`) |
| `0x29`-`0x2F` | reserved | | | Reserved for firmware transfer; ignored like unknown kinds. |
| `0x30` | `ActionRequest` | host → device | `id: u32`, `name: String`, `payload: Vec<u8>` | Asks the device to run a named action (see "Actions" below). Added in version 1 as an additive kind. |
| `0x31` | `ActionResult` | device → host | `id: u32`, `state` (postcard enum: `0` Running, `1` Succeeded, `2` Failed, `3` Rejected, `4` Cancelled), `payload: Vec<u8>` | The device's report on the request with the same `id`; the payload depends on the state (see "Actions"). Added in version 1 as an additive kind. |
| `0x32` | `ActionCancel` | host → device | `id: u32` | Asks the device to stop the running action with that `id`. Accepted while the device is busy with it (see "Actions"). Added in version 1 as an additive kind. |
| `0x33` | `ActionCatalog` | device → host | `names: Vec<String>` (a postcard sequence of strings) | The action names the device handles, declared with `DeviceSession::declare_actions` (see "Action catalog"). Added in version 1 as an additive kind. |
| `0x40` | `RequestError` | host → client | `req: u32`, `code: u16`, `reason: String` | Refuses the client request `req`, which ends. Codes: 1 unsupported, 2 not authorized, 3 invalid, 4 limit, 5 internal. Revision 1. |
| `0x41` | `RequestEnd` | client → host | `req: u32` | Ends the watch `req`; not answered. Revision 1. |
| `0x50` | `StatusQuery` | client → host | `req: u32`, `filter: { subject: Option<Subject>, key_prefix: Option<String> }` | Answered by one `StatusChanges` with `bootstrap`. Revision 1. |
| `0x51` | `StatusWatch` | client → host | as `StatusQuery` | Answered by a `bootstrap` change, then changes. One per session. Revision 1. |
| `0x52` | `StatusChanges` | host → client | `req: u32`, `bootstrap: bool`, `more: bool`, `updated: Vec<StatusRecord>`, `expired: Vec<StatusKey>` | One part of an answer; `more` until the last part. Revision 1. |
| `0x60` | `ActionRun` | client → host | `req: u32`, `action_id`, `target`, `name`, `args: Vec<(String, Value)>`, `deadline_ms: u64` | Answered by the action's result, then each update until it is final. At most 16 unfinished per session. Revision 1. |
| `0x61` | `ActionQuery` | client → host | `req: u32`, `filter: { action_id: Option<String>, target: Option<Target> }` | Answered by one `ActionResults`. Revision 1. |
| `0x62` | `ActionWatch` | client → host | as `ActionQuery` | Answered by the matching results, then updates. One per session. Revision 1. |
| `0x63` | `ActionResults` | host → client | `req: u32`, `more: bool`, `results: Vec<ActionRecord>` | One part of an answer. Revision 1. |
| `0x70` | `StreamDeclare` | provider → host | `req: u32`, `id: String`, `series: Vec<Series>` | Declares a stream of the session's provider. Answered by `StreamDemand`, or refused with `RequestError`. Revision 2. |
| `0x71` | `StreamPublish` | provider → host | `id: String`, `chunks: Vec<Chunk>` | A batch of samples; not answered. Revision 2. |
| `0x72` | `StreamDemand` | host → provider | `id: String`, `subscribers: u32`, `series: Vec<u16>` | Which series someone subscribes to; after a declaration and on every change. Revision 2. |
| `0x73` | `StreamSubscribe` | client → host | `req: u32`, `owner: Subject`, `id: String`, `series: Vec<String>`, `min_interval_us: u32`, `backfill: Backfill` | Answered by a catalog, the backfill, then each batch, until `RequestEnd`. Revision 2. |
| `0x74` | `StreamCatalog` | host → client | `req: u32`, `epoch: u64`, `series: Vec<Series>`, `live: bool` | First, and again after a reset. Revision 2. |
| `0x75` | `StreamData` | host → client | `req: u32`, `epoch: u64`, `seq: u64`, `prev_seq: u64`, `backfill: bool`, `more: bool`, `chunks: Vec<Chunk>` | One part of the samples; `more` until the last part. Revision 2. |
| `0x76` | `StreamReset` | host → client | `req: u32`, `reason: u8 enum` | The subscription's position no longer holds. Revision 2. |

Reject reasons (one byte; unknown codes are reported as `Other(code)`):

| Code | Reason | Device reaction |
| --- | --- | --- |
| 1 | `VersionMismatch` | Back off and retry. |
| 2 | `UnknownDevice` (empty name, or not on the link's allowlist) | Back off and retry. |
| 3 | `UnsupportedRoles` (no role the host serves: `provider`, and `client` on Unix links at revision 1) | Back off and retry. |
| 4 | `FrameTooSmall` (`max_frame` below the host minimum, default 32) | Back off and retry. |
| 5 | `NoSession` (session traffic from a device the host has no session for, for example after a host restart) | Reconnect immediately, without backoff and without a `Rejected` event. |

Rules for decoders:

- Unknown and reserved kinds are ignored by both sessions (after duplicate suppression they still
  count as liveness), so new kinds can be added without a version bump.
- Trailing bytes after a body are ignored, so a later version can append fields compatibly.
- `Hello` and `Reject` (kind numbers, and the fields they had in version 1) are frozen across
  versions; `Hello` only grows at its end. A host answers a
  `Hello` of another version with `Reject { VersionMismatch }` in its own version; a device accepts
  a `Reject` of any version (decoding the reason if it can, otherwise assuming `VersionMismatch`)
  and ignores every other frame of another version.
- Sequence lengths in bodies are untrusted: decoders preallocate at most a few elements and grow as
  elements actually decode, so a forged length cannot exhaust a small heap.

### Status

`Status` carries volatile, latest-value data such as a temperature or a mode string. The gateway
files every entry in the node's in-memory status lane under the device's provider subject
(`provider/<provider id>`), where local clients read it with `query_status` / `watch_status` and
operators with `orionctl get status`. Nothing is persisted.

- **Device.** `DeviceSession::publish_status(&[StatusView])` (or `&[StatusEntry]` with `alloc`)
  keeps only the newest unsent batch (encoded at once into the status buffer; `publish_status`
  fails with `TooLarge` if the frame cannot fit). The batch is
  sent once connected, after any due `Pong`, `Ping`, and provider snapshot, and at most every
  `DeviceConfig::status_min_interval_ms` (default 100 ms); publishing faster, or while
  disconnected, replaces the pending batch (`DeviceStats::status_replaced`). Batches are not
  re-sent after a reconnect: publish again, or rely on the next periodic publish. Each batch
  should contain every key that must stay current.
- **Host.** `HostSession` reports `HostEvent::Status { device_name, entries }` for every `Status`
  frame of the current session (sequence-number duplicate suppression applies) and never acks
  it. Status from a device without a session is answered with `Reject { NoSession }` like any
  other session traffic.
- **Gateway actions.** A `Running` result is forwarded to the node as progress; a final one ends the
  action. The gateway matches `id` to the requests it sent on this link and to the device they went
  to, so a result from another device or for an unknown id is dropped. A device lost, restarted, or
  a link closed while an action is in flight fails it (see "Actions" above and `docs/actions.md`).
- **Gateway.** Status is accepted only after the device's provider snapshot was accepted on that
  link; otherwise it is dropped and counted in `LinkStatus::status_rejects`. Accepted batches
  count in `status_batches`. TTL `0` means the node maximum (`ORION_NODE_STATUS_MAX_TTL_MS`),
  which also caps longer TTLs; node caps on keys, values, and entries per publisher apply (see
  `docs/node-env.md`), and a batch over a cap is refused whole.

### Actions

A device can be an action handler for its provider (`docs/actions.md`, "Link devices"). The node
routes an action whose target is that provider (or one of its resources) to the device; the host
sends it as `ActionRequest { id, name, payload }` (kind `0x30`), and the device reports
`ActionResult { id, state, payload }` (kind `0x31`) with the same `id`:

| `state` | Meaning | `payload` |
| --- | --- | --- |
| `0` `Running` | Still working. | Optional progress: a `u16` varint, per-mille (`0..=1000`); empty when unknown. |
| `1` `Succeeded` | Finished. | The output: an action map (below), empty for no output. |
| `2` `Failed` | Finished unsuccessfully. | The reason, UTF-8. |
| `3` `Rejected` | Refused without running (unknown name, bad arguments, busy). | The reason, UTF-8. |
| `4` `Cancelled` | Stopped at the host's `ActionCancel`; final. | Empty. |

`id` is chosen by the host and is unique among the requests it has outstanding to that device. The
name and the meaning of `payload` belong to the handler, as with node actions: the link layer only
carries them.

**Action map.** Arguments (request payload) and output (`Succeeded` payload) are the postcard
encoding of a map: a varint count, then `(key, value)` pairs in key order. A key is a string, and a
value is a varint variant index followed by its body, from `TypedConfigValue`: `0` `Bool` (one byte,
`0` or `1`), `1` `Int` (zigzag varint), `2` `UInt` (varint), `3` `String` (length-prefixed UTF-8),
`4` `Bytes` (length-prefixed), `5` `F64` (eight bytes, little endian). An empty payload is an empty
map. `orion_link::message::{encode_action_map, decode_action_map}` implement it.

**Device side.** One action runs at a time per device. `DeviceSession::action()` (feature
`actions` of `orion-link`) returns the request until the first report on it;
`report_progress` / `report_action` queue `Running` and final results, and `DeviceEvent::
ActionRequested` announces a request. A request for another id while one runs is answered at once
with `Rejected` and the reason `busy`.

**Cancel.** `ActionCancel { id }` (kind `0x32`) is the one message a busy device accepts besides
the action's own traffic: the device session takes it while the action runs, and when `id` is the
running action that has not reported a final state, it queues `DeviceEvent::ActionCancelRequested
{ id }`. The application stops the work and reports `Cancelled` (or another final state, if it
cannot stop); a cancel for another id, or after the final report, is ignored. The device sends no
separate answer to the cancel, so the host learns the outcome from the action's final result. The
node maps the companion action `<name>.cancel` to this message (`docs/actions.md`, "Link devices"). A request for the running id is a repeat and is ignored. The
device frees itself once it has sent a final result, and only then accepts the next request. The
request's name and payload are copied into the session's buffer, which the first report overwrites
(so read them before reporting). Newer reports replace unsent ones, and a final result is never
replaced. A device handler should always end with a final report: a device that never reports stays
busy, and the node times the action out at its deadline.

Action results, like `Status`, are not acknowledged or retransmitted. A final result lost on the
link is reported by the node as `TimedOut` when the deadline passes, so give actions a deadline.
The device buffers one request: the buffer and the busy refusal cost one receive-sized buffer and
eight bytes of control buffer, only with the `actions` feature (`scripts/mcu-size.sh`,
`uart-actions`). The action catalog adds a second receive-sized buffer (see "Action catalog").

**Host side.** `HostSession::send_action(id, name, payload)` (and `HostBus::send_action`) queues a
request for a connected device and fails with `NotConnected`, `TooLarge` (the negotiated frame
size) or `Congested` rather than queueing unbounded work. Results arrive as
`HostEvent::ActionResult { device_name, id, state, payload }`, only from the device the request
went to and only within its session. A device lost or replaced mid-action is reported by
`DeviceLost` or `DeviceConnected`; the node fails the actions in flight on it.

**Action catalog.** Feature `actions` only (so the minimal device build does not carry it).
`DeviceSession::declare_actions(&[&str])` encodes the names the device handles into one receive-sized
buffer. From the session's `Welcome` on, the device sends them as `ActionCatalog` (kind `0x33`) and
retransmits until the host acknowledges that transmission, with the snapshot's backoff. The
acknowledgement must match the catalog's latest transmission exactly, so it cannot clear the
provider snapshot's acknowledgement. The host reports the list once per session as
`HostEvent::ActionCatalog { device_name, names }` and acknowledges every transmission. An empty list
withdraws the declaration, and a list too large for the largest frame is refused
(`PublishError::TooLarge`). A list that does not fit the negotiated frame size of a session is not
sent in that session.

The node then checks names on its side. For a device that declared a list, an action whose name is
not in it is `Rejected` before anything is sent (``link device `<device>` on <link> does not declare
action `<name>` ``). A device that declared nothing gets every name, as before, and the device still
refuses the names it does not handle (`Rejected`, reason `unknown action` in the reference
simulator). `orion-node` lists the declared names per device in the link status
(`LinkStatusSnapshot::actions`, `orionctl get links` prints one `link_actions` line per device).

Cost: the `actions` feature adds one receive-sized buffer for the catalog (`RX` bytes) and about 24
bytes of state; the minimal `uart` and `can` builds are unchanged (`scripts/mcu-size.sh`).

### Session lifecycle

1. The device sends `Hello` at its first `poll`, then retries with exponential backoff (default
   250 ms doubling to 4 s) until it hears `Welcome`. Its `max_frame` is `min(RX, TX)` of its
   const-generic buffers.
2. The host checks the allowlist, roles, and `max_frame`, then answers `Welcome` (negotiated
   `max_frame = min(device, host)`, host default 4096) followed at once by the current `Leases`.
   A repeated `Hello` from the same device before it has sent any session traffic repeats the
   `Welcome` within the same session (the first one was lost). A `Hello` after session traffic,
   or from a different device name, starts a new session (`session_id` changes); the gateway sees
   `DeviceConnected` again and replaces what it knew about the device.
3. On `Welcome` the device sends its latest provider snapshot (if any; it is re-sent on **every**
   new session, so a device never has to republish after a reconnect) and starts pinging every
   `heartbeat_ms`. A duplicate `Welcome` (same `session_id`) is ignored; a different `session_id`
   replaces the session.
4. After a `Reject` the device stays silent for the reject backoff (default 5 s doubling to 60 s),
   then starts again at step 1.

Neither side sends a frame larger than the negotiated `max_frame`. A snapshot that no longer fits
after negotiation is dropped and reported to the device application (`StateTooLarge`); a lease set
that does not fit is not sent and is reported to the gateway (`LeasesTooLarge`).

### Reliability

- Every frame carries a fresh sequence number. Each side accepts only frames whose `seq` is newer
  than the last one it accepted in the session (wrapping, within half the sequence space); older or
  equal ones are dropped as duplicates or stale. `Hello`, `Welcome`, and `Reject` are exempt
  because they open or close sessions. A new session resets the window.
- `ProviderState` is retransmitted by the device until acknowledged, with exponential backoff
  (default 200 ms, measured from the end of each transmission) capped at the heartbeat interval.
  Each retransmission uses a new `seq`. The ack rule is the same for every reliable stream (the
  snapshot, the action catalog `0x33`, and `DeviceInfo` `0x20`): an `Ack { seq }` acknowledges the
  stream only when `seq` is the sequence number of that stream's latest transmission. Sequence
  numbers are unique per frame, so an ack for an earlier transmission of the same stream, or for
  another stream's frame, never matches; the next retransmission is acknowledged in turn, since
  the host acknowledges every new frame it accepts. Because snapshots are full state, only the
  newest pending one is kept; publishing a new snapshot while the previous one is still being
  sent aborts that transmission (the receiver drops the partial frame).
- The host acknowledges every `ProviderState` it receives but reports a snapshot to the gateway
  only when it differs from the last one in the session, so retransmissions after a lost `Ack` are
  invisible to the gateway.
- The host sends `Leases` right after `Welcome`, whenever the set changes, and after every `Pong`,
  so a lost `Leases` message is repaired within one heartbeat. The device reports a lease set only
  when it differs from the last one it reported in the session.
- Liveness: either side considers the session lost after `missed_heartbeats` (default 3) heartbeat
  intervals without any valid frame from the other. The device then reconnects (step 1); the host
  reports `DeviceLost` so the gateway marks the device's resources unavailable.
- Sessions on byte streams send a `0x00` before every frame, so line noise or an aborted frame
  never corrupts the next one.

### Trust

A configured link is a physical, local connection and is treated like a local IPC client. The
gateway can restrict which `device_name`s a link accepts. Devices never receive cluster
credentials.

## Device bring-up

A port provides three things:

1. a way to write bytes or CAN frames,
2. a way to feed received bytes or CAN frames into the session, and
3. a monotonic millisecond timestamp passed to `poll` (any start value; it may wrap).

The minimal device path (`orion-link` feature `device`) needs no allocator, no heap, and no other
crate: the session has fixed const-generic buffers, the provider snapshot is described with
borrowed `orion_link::wire` views, and lease sets are read as borrowed views straight from the
session. The views encode to exactly the bytes postcard produces for the `orion-control-plane`
records (`crates/link/tests/wire.rs` checks every message against the recorded fixture, and
`tests/wire_property.rs` against postcard for random records), so hosts cannot tell the two
paths apart.

```rust
use orion_link::Stream;
use orion_link::device::{DeviceConfig, DeviceEvent, StreamDevice};
use orion_link::wire::{Health, ProviderView, ResourceView};

// `const`: the snapshot lives in flash. The gateway owns `node_id`; any placeholder works.
const PROVIDER: ProviderView<'static> =
    ProviderView::new("provider.imu-board", "unassigned").with_resource_types(&["imu.sample_source"]);
const RESOURCES: [ResourceView<'static>; 1] =
    [ResourceView::new("imu-board.imu-0", "imu.sample_source", "provider.imu-board")
        .with_health(Health::Healthy)];

// 128-byte receive and transmit buffers: about 0.9 KB of RAM in total, no heap.
let mut session = StreamDevice::<128, 128>::new(DeviceConfig::provider("imu-board"), Stream);
session.publish_provider_state(&PROVIDER, &RESOURCES)?;
let mut tx = [0u8; 32];
loop {
    session.receive(uart.read_available()); // clock-free; may also run in the RX interrupt
    session.poll(now_ms());
    while let Some(event) = session.next_event() {
        if event == DeviceEvent::LeasesChanged {
            for lease in session.leases() {
                handle(lease.resource_id, lease.holder_workload_id);
            }
        }
    }
    loop {
        let n = session.transmit(&mut tx); // resumable: any FIFO or DMA chunk size
        if n == 0 {
            break;
        }
        uart.write_all(&tx[..n]);
    }
}
```

Snapshots and status batches are encoded into the session's buffers when published, so views
built at run time (from sensor state, a serial number, ...) may be dropped right after the call.
`publish_status(&[StatusView])` works the same way. Events are small `Copy` values; the data that
goes with them is read from the session (`leases()`, `node_id()`, `session_id()`).

Firmware that already has an allocator can enable `alloc` and pass the full records instead
(`publish_provider_state(&ProviderRecord, &[ResourceRecord])`, `publish_status(&[StatusEntry])`,
`lease_records() -> Vec<LeaseRecord>`); it is the same session and the same bytes on the wire.

CAN ports use `CanDevice::<RX, TX>::new(config, Packet::CLASSIC)` (or `Packet::FD`), feed the data
of frames carrying the link's host→device identifier to `receive_segment`, and send
`transmit_segment(&mut buf)` / `next_segment()` (or `peek_segment()` + `commit_segment()` when
the controller can be busy) with the device→host identifier.

Footprint (the `examples/mcu-template` staticlib with its C entry points, `RX = TX = 128`,
release, LTO, `--gc-sections`): about 7.8 KB of flash and 0.7 KB of static RAM on Cortex-M0+
(`thumbv6m-none-eabi`) for UART, 8.1 KB for CAN; 7.7 KB on Cortex-M4F; 9.7 KB on RV32IMC. No
`core::fmt`, no panic machinery, and no atomics are linked, so cores without compare-and-swap work
as well. `scripts/mcu-size.sh` reports every target, and CI enforces a size budget.

With feature `actions` (device side of protocol 1 actions, see "Actions") the minimal UART build
grows by about 0.6 KB of flash and 160 bytes of RAM on Cortex-M0+ (`uart-actions` in
`scripts/mcu-size.sh`); the default build is unchanged.

`examples/mcu-template` is a complete, chip-agnostic starting point (`embedded-io` UART and
`embedded-can` ports and C entry points, no allocator by default), and
`crates/link/examples/sim_device.rs` prints a full session timeline on the host.

## Host side

`orion_link::host` (feature `std`) is the gateway's half, also sans-IO: `HostSession<Stream>` for a
serial link, `HostSession<Packet>` for a point-to-point CAN link, and `HostBus` for many devices on
one CAN bus. `HostBus` derives each device's identifiers as `CanLinkIds::for_address(base,
address)` and creates a session when the first frame from an address in its configured range
arrives. The gateway feeds received bytes or frames, calls `poll(now_ms)`, drains events
(`DeviceConnected`, `ProviderState`, `Status`, `DeviceLost`, `DeviceRejected`, `LeasesTooLarge`), sets lease
sets with `set_leases`, and writes what `transmit()` / `next_segment()` / `next_frame()` return.
The node's gateway (next section) is the production driver.

## Gateway

`orion-node` serves links with the opt-in `link-gateway` feature (Linux only; it adds no dependency
beyond `orion-link` itself: serial ports use termios and SocketCAN uses `PF_CAN` raw sockets
directly through `libc` and tokio's `AsyncFd`). It also works in the IPC-only build
(`--no-default-features --features link-gateway`). Links are configured with `ORION_NODE_LINKS`, a
`;`-separated list:

```text
ORION_NODE_LINKS='serial:/dev/ttyAMA0?baud=115200&allow=imu-board,motor-a;can:can0?device_base=0x600&host_base=0x680&addresses=1-16&fd=false&extended=false'
```

Every key (`allow`, `heartbeat_ms`, `missed_heartbeats`, `max_frame`; serial `baud`; CAN
`device_base`, `host_base`, `addresses`, `fd`, `extended`) is documented in `docs/node-env.md`
("Link Gateway"). A serial link serves one device (`HostSession<Stream>`); a CAN link serves every
device address in its range on one interface (`HostBus`), with a kernel receive filter per
device-to-host identifier. A port or interface that is missing or fails is reopened every second,
so USB adapters can be unplugged and replugged.

A Unix link (`unix:/run/orion/link.sock?max_connections=32`) is a socket the node listens on, for
software on the same machine: each connection is one session (`HostSession<Stream>`, COBS framed as
on a serial port), so the existing provider kinds work over it unchanged. The caller's `SO_PEERCRED`
identity is judged at connect by the node's local policy, frames are read within a per-connection
budget (backpressure, never a dropped frame or a lost session), and output is bounded per connection (`docs/node-env.md`, "Link Gateway"). A device name
belongs to one connection at a time. Release, attach and firmware transfers do not apply to it.
Frames are limited to `HOST_MAX_FRAME` (4096) like the other links: a larger limit for local
sockets comes with the kinds that need it.

A Unix link session may also take the client role (revision 1, see "Revisions"): it queries and
watches the node's status and runs, queries and watches actions, as a local control-plane client
named `link:<link>/<device name>` with the caller's identity, so the control sockets'
authorization and rate limit apply to each request. From revision 2 a Unix link session also
carries streams: a provider session publishes streams of its own provider, under the same local
address, and a client session subscribes to any stream of the node.

Bridging, kept generic (nothing about the device's resource types is assumed):

- **Provider path.** `ProviderState` goes through the same code path as a `ProviderState` from a
  local IPC client: the provider record is registered in desired state, the resources are applied to
  observed state, the change is persisted, and a reconcile is requested. Validation, persistence,
  reconcile triggering, and observability are therefore identical. The gateway overwrites
  `ProviderRecord::node_id` with the local node id.
- **Ownership.** A provider id belongs to one publisher. A device snapshot is ignored (logged,
  counted as `snapshot_rejects`, lease set cleared) if its provider id is published by a local IPC
  client, by another device (on any link), or belongs to another node, if a resource names a
  different provider, or if a resource id already belongs to another provider. Conversely, a local
  IPC client cannot publish a provider that a device owns. A device that is not on the link's
  `allow` list never gets a session (`Reject { UnknownDevice }`).
- **Leases.** A device's lease set is the set `WatchProviderLeases` reports for its provider (leases
  on desired resources of the provider) plus leases on the resources the device reported. It is
  recomputed after every desired-state commit (and at least once a second) and handed to
  `set_leases`, which sends it only when it changed (and after every `Pong`). A reconnecting device
  is seeded with its leases on `DeviceConnected`, before its first snapshot.
- **Device loss.** On `DeviceLost` (missed heartbeats, a replaced device, or a port error followed
  by silence) the device's resources are re-applied with `availability = Unavailable` and
  `health = Unknown`. The provider record, the resource records, the leases, and the mutation
  history stay, so a reconnecting device (which always resends its snapshot) restores them in place.
- **Restart.** The providers link devices own, with their last accepted provider and resource
  records and the link and device names, are persisted in `<state dir>/link-devices.rkyv` whenever
  that set changes (a new provider, a provider switch, a removed device; not connects or losses). A
  crash (no shutdown) therefore leaves them recorded. When the node starts, before its IPC servers accept
  clients and before any link task runs, each persisted provider whose link is still configured is seeded as owned by its device,
  disconnected, and its resources are marked unavailable (`Unavailable`, `Unknown`). Until the device
  reconnects, another device or a local IPC client cannot publish that provider id. The reconnecting
  device's snapshot makes the resources available again. A persisted provider whose link is no longer
  in `ORION_NODE_LINKS` is forgotten: its resources stay unavailable and the id is free again. A
  persisted provider that is no longer a local provider in desired state is dropped without being
  recreated. Without a state directory nothing is persisted. The file is written atomically and has
  its own format version, so the snapshot format is unchanged. `orion-node` restores before it starts
  the IPC servers (the link tasks still start with the gateway), so no local client can publish a
  persisted id before the restore. A restore that finds an id already published (only possible when a
  library caller starts IPC first) skips that provider and logs it.
- **Shutdown.** On node shutdown every link task stops, marks its connected devices lost, and closes
  its port or socket before the reconcile loop and IPC servers stop.
- **Status.** Device `Status` batches go to the node's volatile status lane under the device's
  provider subject (see "Status" above).
- **Observability.** `NodeApp::link_status()` returns per-link counters
  (`frames_rx`/`frames_tx`, bytes, CRC and framing errors, transport drops, decode errors, sessions,
  device timeouts, hello and snapshot rejects, status batches and rejects, I/O errors, last error,
  connected devices), and the
  gateway logs device connects, publishes, losses, rejections, I/O errors (once per distinct
  error), and a counter summary when each link closes. The same counters are in the
  observability snapshot's `links` section (control protocol 1): `orionctl get links`
  (`-o json|yaml|toml|metrics`) and the `orion_link_*` Prometheus families (see
  `docs/observability.md`). The devices' providers and resources are visible with
  `orionctl get providers` / `orionctl get resources` like any other.

### Try it without hardware

`crates/node/examples/link_device_sim.rs` runs the device side (`StreamDevice`, as on an MCU) on
the master end of a pseudo-terminal and prints the slave path:

```sh
cargo build -p orion-node --no-default-features --features link-gateway \
  --bin orion-node --example link_device_sim
cargo build -p orionctl

# terminal 1: the simulated device (prints "link_device_sim: pty /dev/pts/N")
target/debug/examples/link_device_sim --name imu-board

# terminal 2: the node, IPC only (Unix socket paths must stay under 108 bytes)
ORION_NODE_ID=node-a ORION_NODE_HTTP_ADDR=off \
  ORION_NODE_IPC_SOCKET=/tmp/orion-a.sock ORION_NODE_IPC_STREAM_SOCKET=/tmp/orion-a-stream.sock \
  ORION_NODE_LINKS='serial:/dev/pts/N?allow=imu-board&heartbeat_ms=500' \
  target/debug/orion-node

# terminal 3
target/debug/orionctl get resources --socket /tmp/orion-a.sock
# resource id=imu-board.imu-0 type=imu.sample_source provider=provider.imu-board ... health=healthy availability=available
```

Stop the simulator and the resource turns `health=unknown availability=unavailable` after
`missed_heartbeats` heartbeats. `link_device_sim --port <path>` drives an existing serial port
(115200 8N1) instead of a new pty, for example a USB-serial adapter cabled to the gateway's port,
so the device can be stopped and restarted on a fixed path to see its resources restored.

## Firmware transfer

Feature `xfer` of `orion-link` (kinds `0x20`-`0x28`, additive: `LINK_PROTOCOL_VERSION` stays 1).
The bodies are postcard, in the order below; `u8` and `bool` are one byte, `u16` and `u32` are
varints, strings and byte strings are length-prefixed. The wire fixture is
`crates/link/tests/fixtures/firmware_encodings.txt`, checked by `tests/firmware_encoding.rs`
against the codec and against postcard through serde.

| Kind | Name | Body |
| --- | --- | --- |
| `0x20` | `DeviceInfo` | `fw_version: str`, `active_slot: u8`, `slots: u8`, `trial: bool`, `xfer_max_chunk: u16`, `xfer_window: u8`, `bootloader: str`, `rollback: option<(version: str, reason: u8, note: str)>` |
| `0x21` | `XferBegin` | `xfer_id: u32`, `component: str`, `version: str`, `total_len: u32`, `chunk_len: u16`, `image_crc32c: u32`, `target_slot: u8` |
| `0x22` | `XferChunk` | `xfer_id: u32`, `offset: u32`, `crc32c: u32` (of `data`), `data: bytes` |
| `0x23` | `XferAck` | `xfer_id: u32`, `next_offset: u32`, `status: u8` |
| `0x24` | `XferEnd` | `xfer_id: u32` |
| `0x25` | `XferResult` | `xfer_id: u32`, `status: u8` |
| `0x26` | `XferAbort` | `xfer_id: u32` (0 if not about a transfer), `reason: u8` |
| `0x27` | `Activate` | `xfer_id: u32`, `mode: u8` (0 trial, 1 handoff) |
| `0x28` | `Confirm` | `slot: u8` |

Status codes (`u8`): 0 ok, 1 bad chunk CRC, 2 bad offset, 3 bad digest, 4 flash, 5 no space,
6 busy, 7 unsupported, 8 unknown transfer, 9 bad slot. Unknown codes decode as `Other`.

Rollback report (the optional last field of `DeviceInfo`, `None` is a `0` tag): after the device's
bootloader reverts a trial, it reports the rolled-back `version` (at most 64 bytes), a `reason` and a
short `note` (at most 32 bytes, empty if none). Reason codes (`u8`): 1 not confirmed (the trial was
not confirmed before the device rebooted, a reset or the watchdog), 2 boot failure (the image did not
boot), 3 verify failed (the image failed verification at boot), 4 requested (the application asked
for the rollback), 0 or any other code: other. The report stays in `DeviceInfo` until the next trial
starts; a longer string is a decode error.

- **Device.** `DeviceInfo` is sent after every `Welcome` and after every firmware change, and is
  resent until the host's `Ack` for one of its transmissions. A device whose firmware sink reports
  zero slots never sends it. The sink's `last_trial()` supplies the rollback report (none by
  default, so a sink whose bootloader reverts silently reports nothing). `XferBegin` is answered with an `XferAck` whose `next_offset` is what
  the sink already holds, which is the resume point. A chunk is accepted only at the next offset
  (a chunk that repeats stored bytes is acknowledged without a write, one that overlaps writes only
  its new tail, one past the next offset is `BadOffset`). Its CRC-32C is checked before it is
  written, so a corrupted chunk is refused with `BadChunkCrc` and the host resends from
  `next_offset`. `XferEnd` verifies the whole image once; a repeated `End` gets the same verdict.
  `Activate` needs a verified image (otherwise `XferAbort` with `BadDigest`).
- **Host.** `HostSession::send_xfer` queues a request; `HostEvent::DeviceInfo` and
  `HostEvent::Xfer` carry the replies. A rollback report for the transfer's version ends the
  transfer as `RolledBack { reason }` (a report for the running version is stale and ignored). `host::xfer::FirmwareTransfer` is the sans-IO driver (see
  `docs/update-recovery.md`, "MCU firmware over the link").
- **Digest.** Length and CRC-32C of the whole image, not SHA-256 (see `docs/update-recovery.md`).
- **Chunk size.** The host's chunk is at most the device's `xfer_max_chunk` (the receive buffer
  minus `max_chunk_for_frame`, and the sink's own limit) and the negotiated frame size, so a chunk
  always fits one frame and one CAN FD message.

The node's gateway drives this transfer on a serial link and on a CAN bus: the `link.firmware.update`
action (`docs/actions.md`, "Link firmware") runs one committed blob on the device's session, resumes
from the device's `next_offset` after a reconnect, and sends `Confirm` once the device reports the
target version. On a CAN bus the chunk is also bounded by the segment budget (32 frames, so 199 bytes
on classic CAN and at most 256 on CAN FD), and the other devices on the bus are not addressed
(`docs/update-recovery.md`, "CAN links").

## Versioning

`LINK_PROTOCOL_VERSION` is independent of `CONTROL_PROTOCOL_VERSION`, so the node's IPC protocol can
evolve without reflashing devices. `crates/link/tests/link_encoding.rs` compares complete frames of
canonical messages with a recorded fixture, so any change to the wire format (frame layout, kind
numbers, body fields, or the postcard encoding of the shared records) fails until
`LINK_PROTOCOL_VERSION` is bumped and the fixture is regenerated with
`ORION_UPDATE_LINK_ENCODINGS=1`. Additive changes (new kinds, appended body fields) need no bump:
the `Status` body was added this way (the `status` fixture entry is appended, earlier entries are
unchanged, and `LINK_PROTOCOL_VERSION` stays 1). The action kinds (`0x30`-`0x33`) and the
`alloc`-only bodies were added the same way; the fixture entries `action_cancel`,
`action_result.cancelled` and `action_catalog` are appended. Hosts and devices that predate a kind ignore it.

The device's hand-written codec (`orion_link::wire`) is held to the same fixture:
`crates/link/tests/wire.rs` encodes every device → host message from views and compares the frames
byte for byte, and decodes every host → device message from it; `tests/wire_property.rs` compares
the views with postcard for random records and lease sets. A wire change therefore has to update
the records, the views, and the fixture together.

The link protocol is also Orion's stable message interface for software built outside the image.
Its rules for extending the protocol, and the plan for the client role and the Unix listener, are in
[design/stable-interface.md](design/stable-interface.md).

The bodies that carry Orion records (`ProviderState`, `Leases`, `Status`, action payloads) are
encoded through `orion-link`'s own body types, which mirror the records field by field in wire
order. A record that changes does not change the wire: the conversion stops compiling until the
change is decided for the link.

### Revisions

`Hello` and `Welcome` end with an optional `revision: u16`, the highest interface revision the
sender speaks (`LINK_INTERFACE_REVISION`). It is left off when it is 0, so a revision-0 body is the
body of version 1 before revisions existed, and a missing field reads as 0. A session uses the lower
of both sides' revisions (`HostSession::revision`), and a side sends a kind only when that revision
includes it. Revisions do not change `LINK_PROTOCOL_VERSION`.

| Revision | Adds |
|---|---|
| 0 | The protocol as of version 1, before revisions. |
| 1 | The client role (`roles` bit 2) and kinds `0x40`, `0x41`, `0x50`-`0x52`, `0x60`-`0x63`: status query and watch, and running, querying and watching actions. Served on Unix links. The record and filter bodies are in [design/stable-interface.md](design/stable-interface.md) ("Step 3 wire"). |
| 2 | Streams, kinds `0x70`-`0x76`: providers declare and publish, clients subscribe. Served on Unix links. The bodies are in [design/stable-interface.md](design/stable-interface.md) ("Step 4 wire"). |
