# Orion Link Protocol

The link protocol connects microcontrollers and other constrained devices to an `orion-node` over
low-level links such as UART, RS-485, USB-CDC, classic CAN, and CAN FD. A device takes part as an
ordinary Orion provider (and optionally executor): it publishes typed `ProviderRecord` /
`ResourceRecord` state and receives its leases, exactly like an IPC client. The node-side gateway
bridges link sessions into the node.

Goals, in priority order:

1. **Any MCU, minimal bring-up.** No chip, HAL, RTOS, or async runtime is assumed. A port supplies
   bytes in, bytes out, and a millisecond clock.
2. **Minimal footprint.** The framing layer needs no allocator. Only typed messages use `alloc`.
3. **Robust on lossy links.** Every frame is CRC-checked. Every state message is an idempotent full
   snapshot, so loss is repaired by retransmission, never by replaying deltas.
4. **Same model as the rest of Orion.** Devices use the shared `orion-core` / `orion-control-plane`
   types (built `no_std` + `alloc`). The gateway maps them onto the node's normal provider path.

## Crates

| Crate | `std`? | Role |
| --- | --- | --- |
| `orion-link` | `no_std` (framing: no `alloc`; messages and device session: `alloc`; host session: `std`) | Framing codecs, link messages, the sans-IO device session, and the sans-IO host session. |
| `orion-node` (`link-gateway` feature) | std | Serves configured serial and SocketCAN links and bridges each device session into the node as a provider. |

Optional `orion-link` features add adapters for common trait ecosystems without changing the core:
`embedded-io` (blocking byte streams), `embedded-io-async`, and `embedded-can` (CAN frames). The core
API is sans-IO: the session never performs I/O and never reads a clock itself.

## Layering

```
 typed messages (postcard, LINK_PROTOCOL_VERSION)      ← orion-link::message
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
| `0x01` | `Hello` | device → host | `device_name: String`, `roles: u8` (bit 0 provider, bit 1 executor), `max_frame: u32` | Opens or reopens a session. The protocol version is the frame header's `version`; the body carries no separate copy. |
| `0x02` | `Welcome` | host → device | `node_id`, `session_id: u32`, `heartbeat_ms: u32`, `max_frame: u32` | `max_frame` is the negotiated minimum of both sides. |
| `0x03` | `Reject` | host → device | `reason: u8` | See the reject reasons below. |
| `0x04` | `Ping` | device → host (either direction is answered) | `now_ms: u64` | Sent by the device every heartbeat. |
| `0x05` | `Pong` | answer to `Ping` | `now_ms: u64` | Echoes the ping's `now_ms`, so the pinging side can measure the round trip. |
| `0x06` | `Ack` | host → device | `seq: u16` | Acknowledges the `ProviderState` frame sent with `seq`. |
| `0x10` | `ProviderState` | device → host | `ProviderRecord`, `Vec<ResourceRecord>` | Full snapshot; idempotent. The gateway owns `ProviderRecord::node_id`. |
| `0x11` | `Leases` | host → device | `Vec<LeaseRecord>` | Full set for this device's provider. |
| `0x12` | `ExecutorState` | device → host | | Reserved for the executor role. |
| `0x13` | `Workloads` | host → device | | Reserved for the executor role. |
| `0x14` | `Status` | device → host | `Vec<StatusEntry>`; each `key: String`, `value: TypedConfigValue`, `ttl_ms: u32` | Volatile status values (see "Status" below). Fire-and-forget: never acknowledged or retransmitted. Added in version 1 as an additive kind. |

Reject reasons (one byte; unknown codes are reported as `Other(code)`):

| Code | Reason | Device reaction |
| --- | --- | --- |
| 1 | `VersionMismatch` | Back off and retry. |
| 2 | `UnknownDevice` (empty name, or not on the link's allowlist) | Back off and retry. |
| 3 | `UnsupportedRoles` (no role the host serves; v1 hosts serve `provider`) | Back off and retry. |
| 4 | `FrameTooSmall` (`max_frame` below the host minimum, default 32) | Back off and retry. |
| 5 | `NoSession` (session traffic from a device the host has no session for, for example after a host restart) | Reconnect immediately, without backoff and without a `Rejected` event. |

Rules for decoders:

- Unknown and reserved kinds are ignored by both sessions (after duplicate suppression they still
  count as liveness), so new kinds can be added without a version bump.
- Trailing bytes after a body are ignored, so a later version can append fields compatibly.
- `Hello` and `Reject` (kind numbers and bodies) are frozen across versions. A host answers a
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

- **Device.** `DeviceSession::publish_status(&[StatusEntry])` keeps only the newest unsent batch
  (it is cloned; `publish_status` fails with `TooLarge` if the frame cannot fit). The batch is
  sent once connected, after any due `Pong`, `Ping`, and provider snapshot, and at most every
  `DeviceConfig::status_min_interval_ms` (default 100 ms); publishing faster, or while
  disconnected, replaces the pending batch (`DeviceStats::status_replaced`). Batches are not
  re-sent after a reconnect: publish again, or rely on the next periodic publish. Each batch
  should contain every key that must stay current.
- **Host.** `HostSession` reports `HostEvent::Status { device_name, entries }` for every `Status`
  frame of the current session (sequence-number duplicate suppression applies) and never acks
  it. Status from a device without a session is answered with `Reject { NoSession }` like any
  other session traffic.
- **Gateway.** Status is accepted only after the device's provider snapshot was accepted on that
  link; otherwise it is dropped and counted in `LinkStatus::status_rejects`. Accepted batches
  count in `status_batches`. TTL `0` means the node maximum (`ORION_NODE_STATUS_MAX_TTL_MS`),
  which also caps longer TTLs; node caps on keys, values, and entries per publisher apply (see
  `docs/node-env.md`), and a batch over a cap is refused whole.

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
  Each retransmission uses a new `seq`; an `Ack` for any transmission of the current snapshot
  counts, an `Ack` for an older snapshot does not. Because snapshots are full state, only the
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
3. a monotonic millisecond timestamp passed to `poll`.

```rust
use orion_link::Stream;
use orion_link::device::{DeviceConfig, DeviceEvent, StreamDevice};

// 256-byte receive and transmit buffers; the only heap use is message bodies.
let mut session = StreamDevice::<256, 256>::new(DeviceConfig::provider("imu-board"), Stream);
session.publish_provider_state(&provider_record, &resources)?;
let mut tx = [0u8; 32];
loop {
    session.receive(uart.read_available()); // clock-free; may also run in the RX interrupt
    session.poll(now_ms());
    while let Some(event) = session.next_event() {
        if let DeviceEvent::Leases(leases) = event {
            handle(leases);
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

CAN ports use `CanDevice::<RX, TX>::new(config, Packet::CLASSIC)` (or `Packet::FD`), feed the data
of frames carrying the link's host→device identifier to `receive_segment`, and send
`next_segment()` (or `peek_segment()` + `commit_segment()` when the controller can be busy) with
the device→host identifier.

`examples/mcu-template` is a complete, chip-agnostic starting point (`embedded-io` UART and
`embedded-can` ports, a replaceable heap, and C entry points), and
`crates/link/examples/sim_device.rs` prints a full session timeline on the host. A heap of a few
KiB is enough for typical record sets.

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
- **Shutdown.** On node shutdown every link task stops, marks its connected devices lost, and closes
  its port or socket before the reconcile loop and IPC servers stop.
- **Status.** Device `Status` batches go to the node's volatile status lane under the device's
  provider subject (see "Status" above).
- **Observability.** `NodeApp::link_status()` returns per-link counters
  (`frames_rx`/`frames_tx`, bytes, CRC and framing errors, transport drops, decode errors, sessions,
  device timeouts, hello and snapshot rejects, status batches and rejects, I/O errors, last error,
  connected devices), and the
  gateway logs device connects, publishes, losses, rejections, I/O errors (once per distinct
  error), and a counter summary when each link closes. The devices' providers and resources are
  visible with `orionctl get providers` / `orionctl get resources` like any other.

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

## Versioning

`LINK_PROTOCOL_VERSION` is independent of `CONTROL_PROTOCOL_VERSION`, so the node's IPC protocol can
evolve without reflashing devices. `crates/link/tests/link_encoding.rs` compares complete frames of
canonical messages with a recorded fixture, so any change to the wire format (frame layout, kind
numbers, body fields, or the postcard encoding of the shared records) fails until
`LINK_PROTOCOL_VERSION` is bumped and the fixture is regenerated with
`ORION_UPDATE_LINK_ENCODINGS=1`. Additive changes (new kinds, appended body fields) need no bump:
the `Status` body was added this way (the `status` fixture entry is appended, earlier entries are
unchanged, and `LINK_PROTOCOL_VERSION` stays 1). Hosts and devices that predate it ignore the kind.
