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
| `orion-link` | `no_std` (framing: no `alloc`; messages/session: `alloc`) | Framing codecs, link messages, sans-IO device session, and the std host session (`std` feature). |
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

## Messages

| Kind | Direction | Body | Notes |
| --- | --- | --- | --- |
| `Hello` | device → host | `link_version`, `device_name`, `roles` (provider/executor), `max_frame` | Opens or reopens a session. |
| `Welcome` | host → device | `node_id`, `session_id`, `heartbeat_ms`, `max_frame` | Negotiated `max_frame` is the minimum of both sides. |
| `Reject` | host → device | `reason` (version mismatch, unknown device, ...) | Device backs off and retries. |
| `ProviderState` | device → host | `ProviderRecord`, `Vec<ResourceRecord>` | Full snapshot; idempotent. |
| `Ack` | host → device | `seq` | Acknowledges a state message. |
| `Leases` | host → device | `Vec<LeaseRecord>` | Full set for this device's provider; sent on change and on heartbeat. |
| `Ping` / `Pong` | both | `now_ms` | Liveness. The host considers a device gone after missed heartbeats and marks its resources unavailable. |
| `ExecutorState`, `Workloads` | | | Reserved for the executor role. |
| `Status` | device → host | | Reserved for the volatile status lane (see TODO.md "Decided model"). |

### Reliability

- State messages (`ProviderState`) are retransmitted by the device until acknowledged, with
  exponential backoff capped at the heartbeat interval. Because they are full snapshots, only the
  newest pending one is kept.
- The host resends `Leases` whenever they change and piggybacks the current set after each `Pong`,
  so a lost `Leases` message is repaired within one heartbeat.
- Duplicate `seq` values within a session are ignored.

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
let mut session = DeviceSession::<512>::new(DeviceConfig::provider("imu-board", provider_record));
loop {
    while let Some(byte) = uart.try_read() {
        if let Some(event) = session.receive_stream_byte(byte) {
            handle(event); // e.g. LinkEvent::Leases(leases)
        }
    }
    session.publish_provider_state(&resources_if_changed);
    while let Some(bytes) = session.poll_stream(now_ms()) {
        uart.write_all(bytes);
    }
}
```

A small heap (a few KiB, for example via `embedded-alloc`) is enough for typical record sets.

## Versioning

`LINK_PROTOCOL_VERSION` is independent of `CONTROL_PROTOCOL_VERSION`, so the node's IPC protocol can
evolve without reflashing devices. A layout fingerprint test over the postcard encodings of canonical
messages forces a version bump when the link wire format changes.
