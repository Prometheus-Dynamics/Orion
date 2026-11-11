# Orion MCU template

A starting point for a microcontroller on Orion's wire, over a UART, RS-485, USB serial,
classic CAN or CAN FD. Nothing here names a chip, a HAL, an RTOS or an async runtime: the
session moves bytes and reads a millisecond counter, and a port supplies both.

The device is a program like any other on the board it connects to. It says hello with the
`device` role, declares topics, registers actions and asks for its firmware, and on the board it
is a resource: any client finds it, reads its topics and runs its actions (`docs/comm.md`,
"Devices as resources"). The layers are the crates in `crates/device`; this crate wires them to
hardware, and gives C firmware an API.

This crate is not a member of the Orion workspace: copy the directory into the firmware's
repository and edit it.

| File | What it is |
| --- | --- |
| `src/uart.rs` | `UartPort<U>`: a device on any `embedded-io` byte stream. |
| `src/can.rs` | `CanPort<C>`: a device on any `embedded-can` controller. |
| `src/lib.rs` | `RX` and `TX`, the buffer sizes, and the `App` trait a Rust firmware's layers implement. |
| `src/ffi.rs` | The C API, over one static: no allocator. Declared in `include/orion_mcu.h`. |
| `tests/` | The ports and the C API against a host played by the test, the header against the library, firmware into slots over C flash hooks. |

## Features

| Feature | What it does |
| --- | --- |
| `standalone` *(default)* | A `#[panic_handler]` on bare-metal targets, which a static library needs. |
| `ffi-uart` *(default)* | The C API over a byte stream. |
| `ffi-can` | The C API over CAN instead. |
| `ffi-slots` | The C API also takes new firmware into two slots, over flash hooks the port defines, and gives the bootloader `orion_boot`. |

## Rust firmware

Put the layers in something that implements `App`, and call `service` from the main loop:

```rust
static TOPICS: [TopicSpec; 1] = [TopicSpec::latest(
    "imu-board.yaw",
    &[Field::new("yaw", FieldType::Float, "deg")],
)];
static ACTIONS: [ActionSpec; 1] = [ActionSpec::on_resource("zero", "imu-board", "Sets yaw to zero")];

struct Imu { topics: Topics<1>, actions: Actions<1> }

impl App<TX> for Imu {
    fn handle(&mut self, link: &mut Link<TX>, frame: &FrameView<'_>) {
        self.topics.handle(frame);
        if let Heard::Run { call, .. } = self.actions.handle(frame) {
            let _ = self.actions.finish(link, call, Outcome::Succeeded, &[], "");
        }
    }
    fn poll(&mut self, link: &mut Link<TX>, now_ms: u32) {
        self.topics.poll(link, now_ms);
        self.actions.poll(link, now_ms);
        if self.topics.subscribers(0) > 0 {
            let _ = self.topics.publish_f64(link, 0, 0, time_ns(), read_yaw());
        }
    }
}

let mut port = UartPort::new(uart, "imu-board");
let mut imu = Imu { topics: Topics::new(&TOPICS), actions: Actions::new(&ACTIONS) };
loop {
    port.service(millis(), &mut imu)?;
}
```

On CAN, `CanPort::new(can, to_host, from_host, SegmentMtu::CLASSIC, "imu-board")`, where the two
identifiers are the bus's two base identifiers plus the device's address, as the board's CAN link
is set up (`crates/comm/link-can`). Set the controller's acceptance filter to `from_host`.

Rust firmware with its own panic handler depends on this crate with `default-features = false`
and drops `staticlib` from `crate-type`.

## C firmware

Link `liborion_mcu_template.a`, define `ORION_MCU_UART` or `ORION_MCU_CAN` (and `ORION_MCU_SLOTS`
for `ffi-slots`) to match it, and include `include/orion_mcu.h`:

```c
orion_init("imu-board");
int32_t yaw = orion_topic("imu-board.yaw", ORION_FIELD_FLOAT, "deg");
int32_t zero = orion_action("zero", "Sets yaw to zero", ORION_ADMITS_LOCAL | ORION_ADMITS_OPERATOR);
for (;;) {
    orion_rx(rx_bytes, rx_len);                  /* what the UART received */
    uint32_t events = orion_poll(millis());
    if (events & ORION_EVENT_RUN) {
        uint64_t call; uint8_t args[64]; size_t len;
        if (orion_run(&call, args, sizeof args, &len) == zero) {
            yaw_offset = read_yaw();
            orion_finish(call, ORION_OUTCOME_SUCCEEDED, NULL, 0, NULL);
        }
    }
    if (orion_subscribers(yaw) > 0) {
        orion_publish(yaw, time_ns(), read_yaw() - yaw_offset);
    }
    uint8_t out[64];
    size_t n;
    while ((n = orion_tx(out, sizeof out)) > 0) {
        uart_write(out, n);
    }
}
```

- The first `orion_poll` fixes the tables: up to 8 topics and 8 actions, added before it.
- Every string the tables are given stays where it is for as long as the firmware runs: a string
  literal. Nothing is copied.
- Call from one main loop, not from an interrupt. A call made while another is under way does
  nothing and returns its error value; there is no lock, so the API builds for cores without
  compare-and-swap (Cortex-M0+, RV32 without `A`).
- A sample the link has no room for is refused (`ORION_ERR_BUSY`), never queued: the next one is
  the newest.
- One run waits at a time. Another that comes while one waits is rejected at once.
- `orion_api_version()` against the header's `ORION_MCU_API_MAJOR` and `ORION_MCU_API_MINOR` at
  start-up; `orion_interface_revision()` is the wire revision the device says it speaks.

On CAN, `orion_can_rx` takes the data of each frame with the device's `from_host` identifier and
`orion_can_tx` gives the data of the next frame to send with its `to_host` identifier.

### New firmware (`ffi-slots`)

The port defines six hooks over its flash (`orion_port_*` in the header): a slot's capacity, and
erase, write and read of two slots and of two copies of a 96-byte boot record. Then
`orion_firmware("devices/imu-board/firmware/")` before the first `orion_poll`, and the board
hands the device every file sent under that prefix: put an image on the board once and run its
`files.send` action (`docs/node.md`), or push it straight to the destination.

The image goes to the slot that is not running, in chunks the device's receive buffer holds,
one at a time. A commit reads it back and checks its SHA-256; only an image that checks out is
put on trial, and `ORION_EVENT_FIRMWARE` says so. Reset when ready. The bootloader calls
`orion_boot(fault)` after every reset for the slot to start; the trial runs once, and
`orion_firmware_confirm()` keeps it, or the next reset goes back.

## Building

```sh
rustup target add thumbv6m-none-eabi thumbv7em-none-eabihf riscv32imc-unknown-none-elf
cargo build --release --target thumbv7em-none-eabihf
cargo build --release --target thumbv6m-none-eabi --no-default-features --features standalone,ffi-can
cargo build --release --target riscv32imc-unknown-none-elf --features ffi-slots

cargo test
cargo test --no-default-features --features standalone,ffi-can
cargo test --features ffi-slots --test slots
```

`scripts/mcu-template.sh` runs all of it, and the gate runs that.

What the C API costs, linked with `--gc-sections` and every entry point kept, with 256-byte
buffers and room for 8 topics and 8 actions (bytes):

| Target | UART flash / RAM | CAN flash / RAM | UART with slots flash / RAM |
| --- | --- | --- | --- |
| thumbv6m-none-eabi | 17,510 / 2,048 | 17,798 / 2,080 | 24,142 / 2,256 |
| thumbv7em-none-eabihf | 16,576 / 2,048 | 16,872 / 2,080 | 22,660 / 2,256 |
| riscv32imc-unknown-none-elf | 18,826 / 2,048 | 19,090 / 2,080 | 26,944 / 2,256 |

No formatting code is linked. Smaller buffers or tables take RAM down; a Rust firmware that
uses the crates directly links only what it calls (`scripts/device-size.sh` for the layers one
at a time).
