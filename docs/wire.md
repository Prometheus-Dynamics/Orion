# The wire

Every carrier sends the same frames. There is one protocol, version 1.

```text
[version u8][kind u8][seq u16 LE][payload][crc32c u32 LE]
```

The CRC is CRC-32C (Castagnoli) over everything before it. It uses the CPU's CRC instructions where
they exist. A receiver reports a wrong version rather than dropping the frame, so it can answer.

## Carriers

| Carrier | Framing |
| --- | --- |
| Same board | `[u32 LE length][frame]` on a Unix socket, with file descriptors attached to a frame |
| Network | The same prefix, inside the signed session |
| Browser | One frame per WebSocket message |
| UART, RS-485, USB-CDC | COBS, `0x00` delimited. A receiver resynchronizes at the next `0x00` |
| CAN, CAN FD | Segments, each with a header byte: bit 7 start, bit 6 end, bits 5..0 counter |

Details of COBS and segmentation:

- Canonical COBS. A sender may put a `0x00` before each frame, so line noise can't damage the next
  one. Empty packets are ignored.
- CAN segments are never padded. Every segment is a valid CAN FD length, so the received length is
  exact. A missing or out-of-order segment discards that message. A byte-identical repeat of the
  last segment is ignored.
- A frame has no maximum on the wire. Receivers bound it with their buffers, and a socket peer's
  claimed length is checked before anything is buffered.

## Kinds

The whole table is in `crates/core/wire/src/kind.rs`. A number is never reused, and a build fails if
one is repeated or sits outside its range. A receiver ignores kinds it doesn't know.

| Range | Concept |
| --- | --- |
| `0x00` to `0x0F` | Session |
| `0x10` to `0x1F` | Records |
| `0x20` to `0x2F` | Files |
| `0x30` to `0x3F` | Actions |
| `0x40` to `0x4F` | Topics |
| `0x50` to `0x5F` | Workloads and executors |
| `0x60` to `0x6F` | Peers and trust |
| `0x70` to `0x7F` | Time |

## Bodies

Bodies are postcard, with their fields in a fixed order. Postcard is not self-describing, so these
rules are what keep old and new builds talking:

1. **Append only.** A new capability is a new kind, or a field at the end of a body. Nothing changes
   in place and nothing is removed.
2. **Short is fine.** A body shorter than the reader's type leaves the appended fields at their
   defaults. The fields a body started with are required.
3. **Long is fine.** A reader ignores trailing bytes it doesn't understand.
4. **Enums that may grow are open.** They travel as a number, and a value the reader doesn't know
   arrives as `Unknown(n)` rather than failing the body. Enums with data inside are closed: a new
   variant is appended and sent only when the session's revision includes it.
5. **Lists carry lengths.** A list is a total byte length, then each element with its own length in
   front. An element can grow by the same rules, and a reader can skip one it can't decode.
6. **The version stays 1.** A bump would stop every deployed peer, so it is reserved for a change
   to the framing itself. Additions use revisions.

## Revisions

`Hello` and `Welcome` each carry the highest interface revision their side speaks. A session uses
the lower of the two, and a side sends a kind only when that revision includes it. `Hello` and
`Reject` keep their existing fields first, so a version mismatch can always be reported.

Every kind in the table belongs to revision 1 so far. The recorded bytes in
`crates/testing/wire-vectors` pin every kind, so any change to a body fails a test.
