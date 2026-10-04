# orion-link

Framing layers of the Orion link protocol, which connects microcontrollers to an `orion-node` over
UART, RS-485, USB-CDC, classic CAN, and CAN FD. See [`docs/link-protocol.md`](../../docs/link-protocol.md).

The crate is `#![no_std]`, has no required dependencies, never allocates, and does not panic on any
input. Buffers are caller-provided or const-generic.

## What Lives Here

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

## Features

| Feature | Effect |
| --- | --- |
| `std` | `std::error::Error` impls. |
| `crc-table` | 1 KiB CRC table instead of 64 bytes, for roughly twice the CRC throughput. |
| `embedded-io` | `io::{write_stream, read_frame, read_frame_buffered}` over `embedded-io` 0.6. |
| `embedded-io-async` | The same over `embedded-io-async` 0.6. |
| `embedded-can` | `Segment::to_can_frame`, `Reassembler::push_can_frame`, and `embedded_can::Id` helpers on `CanLinkIds`. |

## What Does Not Live Here (yet)

- typed link messages (postcard), the device and host sessions, and the node gateway. These
  follow once the shared Orion model crates build as `no_std` + `alloc`.
