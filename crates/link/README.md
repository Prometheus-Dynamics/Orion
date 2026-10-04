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

With `alloc` (still `no_std`; a global allocator is needed for message bodies only):

- `message`: `Message` (`Hello`, `Welcome`, `Reject`, `ProviderState`, `Ack`, `Leases`, `Ping`,
  `Pong`, `Unknown(kind)`), stable `kind` numbers (with `EXECUTOR_STATE`, `WORKLOADS`, `STATUS`
  reserved), `Message::encode` (postcard written straight into the frame's payload area),
  `Message::decode`, borrowed `encode_provider_state` / `encode_leases`, and re-exports of the
  Orion record and id types a device needs.
- `device`: `DeviceSession<T, RX, TX>` with aliases `StreamDevice<RX, TX>` (COBS) and
  `CanDevice<RX, TX>` (CAN / CAN FD). `RX` and `TX` are the receive and transmit frame buffers;
  memory is fixed (`RX + 2 * TX` plus a few hundred bytes of state; the second `TX` buffer holds the
  encoded latest snapshot for retransmission).

  | Call | Purpose |
  | --- | --- |
  | `new(DeviceConfig::provider("name"), Stream)` / `new(config, Packet::CLASSIC)` | Create. |
  | `receive(&[u8])` / `receive_segment(&[u8])` / `receive_can_frame(&ids, &frame)` | Feed received data. Clock-free. |
  | `poll(now_ms)` | Timers: `Hello` backoff, pings, retransmission, host-loss detection. |
  | `next_event()` | `Connected`, `Leases`, `StateAcked`, `Disconnected`, `Rejected`, `StateTooLarge`. |
  | `transmit(&mut [u8]) -> usize` / `next_segment()` / `peek_segment()` + `commit_segment()` | Pull data to send. Resumable. |
  | `publish_provider_state(&provider, &resources)` | Replace the snapshot (newest wins; retransmitted until acked; re-sent on reconnect). |

With `std` (implies `alloc`):

- `host`: `HostSession<Stream>` / `HostSession<Packet>` (one device link) and `HostBus` (many
  devices on one CAN bus, demultiplexed by `CanLinkIds` address, sessions created on first
  contact). Same sans-IO shape: `receive*`, `poll(now_ms)`, `next_event()` (`DeviceConnected`,
  `ProviderState`, `DeviceLost`, `DeviceRejected`, `LeasesTooLarge`), `set_leases`, and
  `transmit()` / `next_segment()` / `next_frame()`. `HostConfig` holds the node id, heartbeat,
  frame limit, and an optional device-name allowlist.
- `std::error::Error` impls.

## Features

| Feature | Effect |
| --- | --- |
| *(default)* | Framing only: no allocator, no dependencies. |
| `alloc` | `message` and `device`; pulls `orion-core` and `orion-control-plane` (`default-features = false`), `postcard` (`alloc`), and `serde`. |
| `std` | `host` and `std::error::Error` impls; implies `alloc` and enables `std` on those dependencies. |
| `crc-table` | 1 KiB CRC table instead of 64 bytes, for roughly twice the CRC throughput. |
| `embedded-io` | `io::{write_stream, read_frame, read_frame_buffered}` over `embedded-io` 0.6. |
| `embedded-io-async` | The same over `embedded-io-async` 0.6. |
| `embedded-can` | `Segment::to_can_frame`, `Reassembler::push_can_frame`, `embedded_can::Id` helpers on `CanLinkIds`, `CanDevice::{receive_can_frame, peek_can_frame}`, and `HostBus::receive_can_frame` / `BusFrame::to_can_frame`. |

## Getting Started

- `cargo run -p orion-link --example sim_device --features std` prints a device ↔ host timeline
  over a simulated 115200-baud serial line (handshake, snapshot, leases, cable pulled and
  replugged).
- [`examples/mcu-template`](../../examples/mcu-template) is a standalone, chip-agnostic firmware
  starting point: `UartPort` (any `embedded-io` UART), `CanPort` (any `embedded-can` controller), a
  replaceable heap, and C entry points for C firmware.

## Footprint

`scripts/mcu-size.sh` links the MCU template's staticlib (C entry points, device session,
postcard, framing, the record types it touches, and the example heap; release, `opt-level = "z"`,
LTO) with `--gc-sections`:

| Target | Link | Flash | RAM (static) | of which heap |
| --- | --- | --- | --- | --- |
| `thumbv7em-none-eabihf` | UART | 26.0 KB | 9.3 KB | 8 KiB |
| `thumbv7em-none-eabihf` | CAN | 26.9 KB | 9.4 KB | 8 KiB |
| `riscv32imac-unknown-none-elf` | UART | 29.9 KB | 9.3 KB | 8 KiB |
| `riscv32imac-unknown-none-elf` | CAN | 30.6 KB | 9.4 KB | 8 KiB |

With `RX = TX = 256` the session itself is about 1.1 KB of RAM. On Cortex-M4F the flash splits
roughly into postcard/serde and record (de)serializers 7 KB, `core`/`alloc`/compiler builtins
7 KB (memcpy, `str` validation, panic formatting), device session 3.5 KB, framing 1.3 KB, template
and heap 2 KB, and constants/glue. The framing layers alone (default features) need no heap.

## Tests

`tests/session/` drives devices and hosts against each other with a deterministic virtual clock
over noisy, chunked byte streams and lossy, duplicating classic CAN and CAN FD buses (handshake,
retransmission, newest-snapshot-wins, lease repair within one heartbeat, host loss and restart,
device loss, version mismatch, allowlist, unknown kinds, several devices on one bus). The scripted
device tests also run without `std`. `tests/link_encoding.rs` pins the wire format against
`tests/fixtures/link_encodings.txt`; refresh it only together with a `LINK_PROTOCOL_VERSION` bump
(`ORION_UPDATE_LINK_ENCODINGS=1 cargo test -p orion-link --features alloc --test link_encoding`).

## What Does Not Live Here (yet)

- The node gateway (`orion-node` `link-gateway` feature) that owns serial ports and SocketCAN
  sockets, drives `HostSession` / `HostBus`, and maps their events onto the node's provider path.
- The executor role (`ExecutorState`, `Workloads`) and the volatile `Status` lane; their kind
  numbers are reserved.
