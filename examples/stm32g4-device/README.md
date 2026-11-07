# STM32G4 device example

A whole firmware for an STM32G431 that speaks the Orion link and publishes:

- **one record:** the provider `provider.g4-demo` with the resource `g4-demo.mcu`;
- **one stream:** `g4-demo.loop`, one `u32` series with the main-loop iterations per 10 ms, sampled
  only while someone subscribes;
- **status:** `stack.high_water_bytes` (measured by stack painting) and `stack.size_bytes`.

It has no HAL and no peripheral access crate. The registers it touches are listed in
[`src/reg.rs`](src/reg.rs). The drivers are polled, and the clock is the 16 MHz HSI the chip starts
on. So its size is the Orion device tier plus about 300 bytes of driver, not a HAL.

| Feature | Link |
| --- | --- |
| `link-can` (default) | FDCAN1 on PA11 (RX) and PA12 (TX): CAN FD, 500 kbit/s nominal, 2 Mbit/s data. Link ids 0x101 (device to host) and 0x181 (host to device). |
| `link-uart` | USART2 on PA2 (TX) and PA3 (RX) at 115200 baud: the ST-LINK virtual COM port of a Nucleo-G431. |
| `small-mem` (default) | Byte-loop `mem*` routines in place of compiler-builtins' unrolled ones: 1,128 bytes less flash. |
| `hw-crc` | The link's CRC-32C on the chip's CRC unit (`orion-link` feature `crc-extern`). Not yet verified on silicon. |

## Build and flash

```bash
cargo build --release
probe-rs run --chip STM32G431KBTx target/thumbv7em-none-eabihf/release/orion-stm32g4-device
```

On the host side, the node's CAN link gateway serves it (`docs/link.md`). With `link-uart`, use a
serial link on the board's virtual COM port.

## Size (tracked per commit)

`scripts/device-size.sh` builds every variant and checks it against
[`size-baseline.txt`](size-baseline.txt). A commit that grows the firmware past the tolerance
(64 bytes of flash, 32 of RAM) fails until it updates the baseline, so every growth shows up as a
line in the diff. CI runs it in the `link` job.

| Variant | Flash | Static RAM | Stack bound | RAM in total |
| --- | ---: | ---: | ---: | ---: |
| FDCAN (`link-can`, `small-mem`) | 9,228 | 4 | 1,984 | 1,988 |
| UART (`link-uart`, `small-mem`) | 8,700 | 4 | 1,904 | 1,908 |
| FDCAN with the CRC unit (`hw-crc`) | 9,172 | 4 | 1,984 | 1,988 |
| FDCAN with compiler-builtins `mem*` | 10,356 | 4 | 1,984 | 1,988 |

All sizes are in bytes:
- **Flash** is `.vector_table` + `.text` + `.rodata` + `.data`.
- **Static RAM** is 4 bytes because the session and its buffers live in `main`'s frame, not in
  `.bss`. The stack bound carries the RAM: 1,160 bytes of it are the session with 128-byte frame
  buffers.
- **The stack bound** is `main` plus the deepest exception handler with its FPU frame. It comes
  from `-Z emit-stack-sizes` and the call graph
  ([`scripts/stack-depth.py`](../../scripts/stack-depth.py)), with the codec's `dyn Encode` calls
  followed to every implementation. The firmware's painted high-water mark
  (`stack.high_water_bytes`) is the measured counterpart on a board.

### Where the flash goes (FDCAN, ranked)

From `DEVICE_SIZE_BREAKDOWN=1`, which charges each instruction to the source line it came from,
so inlined code counts where it was written.

| Component | Bytes | What it is |
| --- | ---: | --- |
| Device session | 2,794 | Hello and Welcome, snapshot retransmission, leases, status, the event queue |
| Wire codec | 1,922 | Encoding the provider and status views; decoding Welcome, Leases and acks |
| `core` inlined into the above | 1,788 | Checked arithmetic, `Option` and `Result`, slice bounds: the session's and codec's own logic, inlined from `core` |
| CAN segments and reassembly | 564 | Splitting frames into CAN FD segments and joining them |
| Application | 550 | `main`, the clock, stack painting and the `mem*` loops |
| Streams codec | 374 | The stream declaration, sample batches and demand. The session's stream state is in "Device session". |
| FDCAN driver | 330 | |
| Frame and CRC-32C | 238 | Frame header and CRC (64 of it the nibble table) |
| Vector table, reset, the rest | 266 | |
| `core::fmt` and panics | 12 | No formatting is linked. A panic resets the chip. |

### Cuts

| Cut | Flash | RAM | State |
| --- | ---: | ---: | --- |
| One interrupt vector instead of `cortex-m-rt`'s 240 (the firmware is polled) | −956 | 0 | done |
| Byte-loop `mem*` (`small-mem`) | −1,128 | 0 | done |
| Write segments straight into the driver buffer (`transmit_segment`, no `Segment` copies) | −88 | −248 stack | done |
| The CRC unit for the link's CRC-32C (`hw-crc`, the `orion-link` feature `crc-extern`) | −56 | 0 | done; untested on silicon |
| Frame buffers sized to this device (96 / 64 bytes instead of 128) | −52 / −124 | −192 / −384 | measured; only valid while the snapshot, the stream declaration and the lease set fit |

**Today:** 9,228 bytes of flash and 1,988 of RAM over FDCAN with one record and one stream.
Before the cuts it was 11,612 and 2,236.

**The floor without protocol changes:** about 9.1 KB of flash and 1.6 KB of RAM, with 64-byte
buffers. The rest is the session and codec themselves, about 4.7 KB with their `core` inlines.
Going below that means dropping features rather than overhead:
- the stream costs about 1.2 KB of flash and 184 bytes of RAM;
- a device with no leases skips the lease decoding.
