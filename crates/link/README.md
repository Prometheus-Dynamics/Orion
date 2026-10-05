# orion-link

The Orion link protocol, which connects microcontrollers to an `orion-node` over UART, RS-485,
USB-CDC, classic CAN, and CAN FD. See [`docs/link-protocol.md`](../../docs/link-protocol.md).

No chip, HAL, RTOS, or async runtime is assumed. Everything is sans-IO: the sessions never perform
I/O and never read a clock, so a port only moves bytes (or CAN frames) and passes a millisecond
timestamp.

## What Lives Here

Always available (`no_std`, no allocator, no required dependencies, panic-free on any input):

- `frame`: message frames `[version u8][kind u8][seq u16 LE][payload][crc32c u32 LE]`, encoded into
  caller buffers (or in place) and decoded into borrowed views. `LINK_PROTOCOL_VERSION`.
- `stream`: COBS encoding with `0x00` delimiters. `StreamEncoder` yields bytes one at a time with
  no second buffer; `StreamDecoder<N>` is fed bytes or slices, yields validated frames,
  resynchronizes on `0x00`, and counts dropped packets.
- `packet`: segmentation into classic CAN (7 payload bytes per frame) and CAN FD (up to 63) with the
  start/end/counter header byte. Segments always have a valid CAN FD length, so no padding is ever
  needed. `Reassembler<N>` discards on missing or reordered segments, ignores CAN-level duplicates,
  and checks the frame CRC. `CanLinkIds` holds a link's identifier pair.
- CRC-32C with a 16-entry nibble table (64 bytes), or a 256-entry table with `crc-table`.

With `device` (the minimal device path: still no allocator and no dependencies, and no
`core::fmt`, serde, or postcard):

- `wire`: a hand-written postcard-compatible codec (`Writer`, `Reader`, `Encode`: varints, zigzag,
  length-prefixed strings and sequences, `Option`, enum variant indices) and borrowed views of
  every body a device sends or receives. Device → host: `HelloView`, `ProviderView` +
  `ResourceView` (with `CapabilityView`, `Ownership`, `Health`, `Availability`, `LeaseState`, and
  `ResourceStateView` / `ActionResultView` / `ConfigField` / `Value` for resource state), and
  `StatusView`. They encode to exactly the postcard bytes of the corresponding
  `orion-control-plane` records, and they are `const`, so a fixed snapshot can live in flash.
  Host → device: `WelcomeView`, `decode_reject` / `decode_ack` / `decode_u64`, and `Leases`, an
  iterator of `LeaseView`s (strings borrowed from the receive buffer) over a payload validated
  up front. `str_from_utf8` is a compact UTF-8 validator; `kind`, `Roles`, `RejectReason`.
- `device`: `DeviceSession<T, RX, TX, N>` with aliases `StreamDevice<RX, TX>` (COBS) and
  `CanDevice<RX, TX>` (CAN / CAN FD). `RX` and `TX` bound received and sent frames; `N` holds the
  device name (`&'static str` by default, any `AsRef<str>`). Memory is fixed: `2 * RX + 2 * TX`
  plus about 350 bytes on 32-bit targets: the receive decoder; a copy of the current lease set
  (which also holds the outgoing `Hello` outside a session); the encoded latest snapshot for
  retransmission; the newest status batch; and an 18-byte ping/pong buffer, the node id
  (`NODE_ID_CAPACITY` = 32 bytes), a 4-slot event queue, counters, and timers. Time is kept as a
  wrapping 32-bit millisecond clock internally, so 64-bit arithmetic stays out of small cores.

  | Call | Purpose |
  | --- | --- |
  | `new(DeviceConfig::provider("name"), Stream)` / `new(config, Packet::CLASSIC)` | Create (always inlined, so it can be built in place in a static). |
  | `receive(&[u8])` / `receive_segment(&[u8])` / `receive_can_frame(&ids, &frame)` | Feed received data. Clock-free. |
  | `poll(now_ms)` | Timers: `Hello` backoff, pings, retransmission, host-loss detection. |
  | `next_event()` | `Connected { session_id }`, `LeasesChanged`, `StateAcked`, `Disconnected`, `Rejected(reason)`, `StateTooLarge`; small `Copy` values. |
  | `leases()` / `node_id()` / `session_id()` | The current lease set (`LeaseView`s), host node, and session. |
  | `transmit(&mut [u8]) -> usize` / `transmit_segment(&mut [u8]) -> usize` / `next_segment()` / `peek_segment()` + `commit_segment()` | Pull data to send. Resumable. |
  | `publish_provider_state(&provider, &resources)` | Replace the snapshot (newest wins; retransmitted until acked; re-sent on reconnect). |
  | `publish_status(&entries)` | Volatile status values (fire-and-forget, newest batch wins, rate-limited). |

With `alloc` (still `no_std`; implies `device` and adds a global-allocator requirement):

- `message`: `Message` (`Hello`, `Welcome`, `Reject`, `ProviderState`, `Ack`, `Leases`, `Ping`,
  `Pong`, `Status`, `Unknown(kind)`), `Message::encode` / `Message::decode` (postcard + serde over
  the shared records), borrowed `encode_provider_state` / `encode_leases` / `encode_status`, and
  re-exports of the Orion record and id types. This is what the host side uses.
- The same `DeviceSession` additionally accepts the full records in `publish_provider_state`
  (`ProviderRecord`, `ResourceRecord`) and `publish_status` (`StatusEntry`), encoded through
  postcard itself, and decodes the lease set into `LeaseRecord`s with `lease_records()`.

With `std` (implies `alloc`):

- `host`: `HostSession<Stream>` / `HostSession<Packet>` (one device link) and `HostBus` (many
  devices on one CAN bus, demultiplexed by `CanLinkIds` address, sessions created on first
  contact). Same sans-IO shape: `receive*`, `poll(now_ms)`, `next_event()` (`DeviceConnected`,
  `ProviderState`, `Status`, `DeviceLost`, `DeviceRejected`, `LeasesTooLarge`), `set_leases`,
  and `transmit()` / `next_segment()` / `next_frame()`. `HostConfig` holds the node id,
  heartbeat, frame limit, and an optional device-name allowlist.
- `Display` and `std::error::Error` impls (the device path has none, so it links no formatting
  code).

## Choosing the Device Path

| | Minimal (`device`) | Full records (`alloc`) |
| --- | --- | --- |
| Describes state with | `ProviderView` / `ResourceView` / `StatusView` (borrowed, `const`-able) | also `ProviderRecord` / `ResourceRecord` / `StatusEntry` |
| Reads leases as | `leases()`: `LeaseView`s borrowing the session | also `lease_records()`: `Vec<LeaseRecord>` |
| Allocator | none | required (the template's example heap is 8 KiB) |
| Dependencies | none | `orion-core`, `orion-control-plane`, `serde`, `postcard` |
| Template flash (thumbv7em, UART) | 7.9 KB | 18.8 KB + 8 KiB heap |
| Wire bytes | identical | identical |

Both are the same session type and the same state machine; `alloc` only adds conveniences for
firmware that already has an allocator and wants to share record-building code with the host. A
device can mix them (for example views for the snapshot, `lease_records()` for leases).

There is one `DeviceSession` rather than two: the minimal session replaced the earlier
alloc-only one because it does strictly more with less (identical wire behaviour, no heap at all,
about a third of the flash), and the record conveniences sit on top of it. Compared with the
earlier alloc-only API, events no longer carry data (`DeviceEvent::LeasesChanged` replaces
`Leases(Vec<LeaseRecord>)`, read the set with `leases()` / `lease_records()`; `Connected` no longer
carries the node id, read it with `node_id()`), `DeviceConfig` is generic over the name type, and
`PublishError::Encode` is gone because encoding into a sized buffer cannot fail.

## Features

| Feature | Effect |
| --- | --- |
| *(default)* | Framing only: no allocator, no dependencies. |
| `device` | `wire` and `device`: the minimal device path. No allocator, no dependencies. |
| `alloc` | `message` and the record conveniences on the device session; implies `device`; pulls `orion-core` and `orion-control-plane` (`default-features = false`), `postcard` (`alloc`), and `serde`. |
| `std` | `host`, `Display`, and `std::error::Error` impls; implies `alloc` and enables `std` on those dependencies. |
| `crc-table` | 1 KiB CRC table instead of 64 bytes, for roughly twice the CRC throughput. |
| `embedded-io` | `io::{write_stream, read_frame, read_frame_buffered}` over `embedded-io` 0.6. |
| `embedded-io-async` | The same over `embedded-io-async` 0.6. |
| `embedded-can` | `Segment::to_can_frame`, `Reassembler::push_can_frame`, `embedded_can::Id` helpers on `CanLinkIds`, `CanDevice::{receive_can_frame, peek_can_frame}`, and `HostBus::receive_can_frame` / `BusFrame::to_can_frame`. |

## Getting Started

- `cargo run -p orion-link --example sim_device --features std` prints a device ↔ host timeline
  over a simulated 115200-baud serial line (handshake, snapshot, leases, cable pulled and
  replugged); the device publishes `wire` views.
- [`examples/mcu-template`](../../examples/mcu-template) is a standalone, chip-agnostic firmware
  starting point: `UartPort` (any `embedded-io` UART), `CanPort` (any `embedded-can` controller),
  and C entry points for C firmware, all without an allocator by default.

## Footprint

`scripts/mcu-size.sh` links the MCU template's staticlib (C entry points, device session, wire
codec, framing; release, `opt-level = "z"`, LTO) with `--gc-sections`. Minimal device path,
`RX = TX = 128`, no heap:

| Target | Link | Flash | Static RAM | of which compiler-builtins `mem*` |
| --- | --- | --- | --- | --- |
| `thumbv6m-none-eabi` (Cortex-M0/M0+) | UART | 8,048 B | 908 B | 204 B |
| `thumbv6m-none-eabi` | CAN | 9,180 B | 932 B | 1,000 B |
| `thumbv7em-none-eabihf` (Cortex-M4F) | UART | 7,856 B | 908 B | 290 B |
| `thumbv7em-none-eabihf` | CAN | 9,078 B | 932 B | 1,252 B |
| `riscv32imc-unknown-none-elf` | UART | 9,860 B | 908 B | 140 B |
| `riscv32imc-unknown-none-elf` | CAN | 10,528 B | 932 B | 566 B |
| `riscv32imac-unknown-none-elf` | UART | 9,860 B | 908 B | 140 B |
| `riscv32imac-unknown-none-elf` | CAN | 10,528 B | 932 B | 566 B |

The full-record variant (`--features global-heap`: records, postcard/serde, the 8 KiB example
heap) is 18.8 KB flash on Cortex-M4F and 21.4 KB on `riscv32imac`, plus 9.1 KB RAM. Before the
minimal path existed, the template needed 26.9 KB flash and 1.2 KB RAM plus an 8 KiB heap on
Cortex-M4F (31.1 KB on `riscv32imac`) and did not build for cores without compare-and-swap.

Flash of the minimal UART build on Cortex-M0+, by component: device session state machine 3.1 KB,
wire codec 2.1 KB, framing (COBS, CRC-32C, frame checks) 1.5 KB, template and C API 1.1 KB,
compiler-builtins 0.2 KB. The binary contains no `core::fmt`, no panic machinery, no `memcpy`
(UART), and no 64-bit multiply. CI (`link-no-std`) fails if the minimal UART build exceeds its
per-target budget in `scripts/mcu-size.sh` (about 17% above these numbers);
`MCU_SIZE_BREAKDOWN=1` prints the per-component split for every build.

## Tests

`tests/session/` drives devices and hosts against each other with a deterministic virtual clock
over noisy, chunked byte streams and lossy, duplicating classic CAN and CAN FD buses (handshake,
retransmission, newest-snapshot-wins, lease repair within one heartbeat, host loss and restart,
device loss, version mismatch, allowlist, unknown kinds, several devices on one bus), including a
device publishing `wire` views end to end (`views_e2e.rs`). The scripted device tests also run
without `std`, and `tests/device_views.rs` runs the session without `alloc` (wrapping clocks,
`Hello`/lease buffer sharing, CAN segments into caller buffers). `tests/wire.rs` encodes every
device body from views and compares it byte for byte with the recorded fixture, and decodes every
host body from it; `tests/wire_property.rs` compares views with postcard + serde for random records
and lease sets; `tests/wire_utf8.rs` checks the compact UTF-8 validator against `core`.
`tests/link_encoding.rs` pins the wire format against `tests/fixtures/link_encodings.txt`; refresh
it only together with a `LINK_PROTOCOL_VERSION` bump
(`ORION_UPDATE_LINK_ENCODINGS=1 cargo test -p orion-link --features alloc --test link_encoding`).

## Node Gateway

The `orion-node` `link-gateway` feature (Linux) owns the serial ports and SocketCAN sockets
configured with `ORION_NODE_LINKS`, drives `HostSession` / `HostBus`, and maps their events onto
the node's provider path. See the "Gateway" section of
[`docs/link-protocol.md`](../../docs/link-protocol.md). To try it without hardware, run the
simulated device on a pseudo-terminal and point a node at it:

```sh
cargo build -p orion-node --no-default-features --features link-gateway --bin orion-node --example link_device_sim
target/debug/examples/link_device_sim            # prints "link_device_sim: pty /dev/pts/N"
ORION_NODE_HTTP_ADDR=off ORION_NODE_IPC_SOCKET=/tmp/orion-a.sock \
  ORION_NODE_IPC_STREAM_SOCKET=/tmp/orion-a-stream.sock \
  ORION_NODE_LINKS='serial:/dev/pts/N?allow=imu-board' target/debug/orion-node
orionctl get resources --socket /tmp/orion-a.sock   # shows imu-board.imu-0
```

## What Does Not Live Here (yet)

- The executor role (`ExecutorState`, `Workloads`); its kind numbers are reserved.
