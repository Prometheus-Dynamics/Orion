# STM32G4 device example

A whole firmware for an STM32G431 on Orion's wire. It declares two topics:

- **`loop`**, field `iterations`: main-loop turns per 100 ms, published only while someone
  listens. It is a direct reading of how much CPU the link leaves.
- **`stack`**, fields `high_water` and `size` (bytes), once a second: the deepest the stack has
  grown, measured by painting (`src/stack.rs`), and what it may use.

It has no HAL and no peripheral access crate. The registers it touches are listed in
[`src/reg.rs`](src/reg.rs), the drivers are polled, and the clock is the 16 MHz HSI the chip starts
on. Its size is Orion's device crates (`orion-device-core`, `orion-device-topics`) plus a few
hundred bytes of driver, not a HAL.

| Feature | What it does |
| --- | --- |
| `link-can` (default) | FDCAN1 on PA11 (RX) and PA12 (TX): CAN FD, 500 kbit/s nominal, 2 Mbit/s data. Address 1 on a bus whose base identifiers are 0x100 and 0x180, so it sends on 0x101 and is sent on 0x181. |
| `link-uart` | USART2 on PA2 (TX) and PA3 (RX) at 115200 baud: the ST-LINK virtual COM port of a Nucleo-G431. |
| `small-mem` (default) | Byte-loop `mem*` routines in place of compiler-builtins' unrolled ones: 1,128 bytes less flash. |
| `hw-crc` | The frame CRC-32C on the chip's CRC unit (`orion-wire` feature `crc-extern`). Not yet verified on silicon. |

## Build and flash

```bash
cargo build --release
probe-rs run --chip STM32G431KBTx target/thumbv7em-none-eabihf/release/orion-stm32g4-device
```

The comm core serves it over its CAN link with the bus `can0=can0:0x100:0x180:1:fd`, and it
appears as the device `can0.1`. With `link-uart`, give the comm core a serial link on the board's
virtual COM port.

## Size (held per commit)

`scripts/device-size.sh` builds every variant and checks it against
[`size-baseline.txt`](size-baseline.txt). A change that grows the firmware past the tolerance (64
bytes of flash, 32 of RAM or stack) fails until it updates the baseline
(`DEVICE_SIZE_UPDATE=1 scripts/device-size.sh`), so every growth is a line in the diff.

| Variant | Flash | Static RAM | Stack bound | RAM in total |
| --- | ---: | ---: | ---: | ---: |
| FDCAN (`link-can`, `small-mem`) | 7,040 | 4 | 1,128 | 1,132 |
| UART (`link-uart`, `small-mem`) | 6,368 | 4 | 1,056 | 1,060 |
| FDCAN with the CRC unit (`hw-crc`) | 6,996 | 4 | 1,112 | 1,116 |
| FDCAN with compiler-builtins `mem*` | 8,168 | 4 | 1,128 | 1,132 |

All sizes are in bytes:

- **Flash** is `.vector_table` + `.text` + `.rodata` + `.data`.
- **Static RAM** is 4 bytes, the millisecond counter: the device and its buffers live in `main`'s
  frame, so the stack bound carries the RAM.
- **The stack bound** is `main` plus the deepest exception handler with its FPU frame, from
  `-Z emit-stack-sizes` and the call graph ([`scripts/stack-depth.py`](../../scripts/stack-depth.py))
  on a pinned nightly. The `stack` topic's `high_water` is the measured counterpart on a board.

The same firmware on the previous device tier was 9,228 bytes of flash and 1,988 of RAM.
