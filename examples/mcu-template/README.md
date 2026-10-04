# Orion MCU template

A starting point for connecting **any** microcontroller to an `orion-node` over UART, RS-485,
USB-CDC, classic CAN, or CAN FD, using the [link protocol](../../docs/link-protocol.md) from
[`orion-link`](../../crates/link). Nothing here names a chip, HAL, RTOS, or async runtime: the
device session is sans-IO, so a port only moves bytes and reads a millisecond counter.

This crate is standalone (not a workspace member) so you can copy the directory into your firmware
repository and edit it.

| File | What it shows |
| --- | --- |
| `src/uart.rs` | `UartPort<U>`: a `StreamDevice` wired to any `embedded_io::{Read, ReadReady, Write}` byte stream. |
| `src/can.rs` | `CanPort<C>`: a `CanDevice` wired to any `embedded_can::nb::Can` controller. |
| `src/heap.rs` | `FreeListHeap<N>`: a tiny first-fit `GlobalAlloc` over a static array. Replaceable. |
| `src/ffi.rs` | C entry points (`orion_init`, `orion_rx`/`orion_tx` or `orion_can_rx`/`orion_can_tx`, `orion_poll`, `orion_publish`) for C firmware. |
| `src/lib.rs` | Record helpers, buffer sizes, and the `#[panic_handler]` / `#[global_allocator]` used only by the standalone staticlib. |
| `tests/ports.rs` | Host tests: both ports against the real `orion-link` host session and CAN bus. |

## Port Orion to a new MCU in 6 steps

1. **Pick the transport.** A UART-like byte stream (UART, RS-485 with one device per link,
   USB-CDC) uses `UartPort`; a CAN controller uses `CanPort` with `Packet::CLASSIC`, `Packet::FD`,
   or a custom `SegmentMtu`.

2. **Provide the byte or frame driver.** If your HAL implements `embedded-io` 0.6
   (`Read + ReadReady + Write`) or `embedded-can` 0.4 (`nb::Can`), you are done. Otherwise write a
   ten-line adapter around your driver's "bytes available / read / write" or "receive / transmit
   frame" calls. Interrupt-driven receive works too: push bytes into a ring buffer in the ISR and
   implement `Read` over it, or call the session's clock-free `receive` directly.

3. **Provide a millisecond clock.** Any monotonic `u64` millisecond counter (SysTick, a hardware
   timer, an RTOS tick) passed to `service(now_ms)`. Only differences matter; it may start at 0.

4. **Provide an allocator.** Message bodies (the node id, decoded lease sets) and the device name
   use the heap; framing never does. A few KiB is plenty for typical record sets. Use
   `heap::FreeListHeap` (the `global-heap` feature installs an 8 KiB one), `embedded-alloc`, or
   your RTOS heap. Size `RX`/`TX` in `src/lib.rs` so your largest lease set and provider snapshot
   fit (each is header + postcard body + CRC; a provider with one simple resource is about 120 bytes).

5. **Describe what the device provides.** Build a `ProviderRecord` and its `ResourceRecord`s
   (`provider_record` / `resource_record` are minimal helpers) and call `publish(...)` whenever
   they change. The session sends the newest snapshot, retransmits it until the host acknowledges
   it, and re-sends it automatically after every reconnect.

6. **Run the loop.**

   ```rust
   let mut port = UartPort::new(uart, "imu-board");
   port.publish(&provider, &resources)?;
   loop {
       port.service(millis())?;
       while let Some(event) = port.next_event() {
           match event {
               DeviceEvent::Leases(leases) => apply(leases), // start/stop work
               DeviceEvent::Disconnected => stop_all(),        // leases are stale
               _ => {}
           }
       }
       // ... your application ...
   }
   ```

   For CAN, pick the link identifiers (`CanLinkIds::for_address(base, address)`, the same base and
   address range as the gateway's bus configuration) and set your controller's acceptance filter to
   the host→device identifier.

C firmware skips steps 2 and 6: link `liborion_mcu_template.a` and call `orion_init`, then
`orion_rx(bytes)` for received bytes, `orion_poll(now_ms)` every loop (it returns
`ORION_EVENT_*` bits), and `orion_tx(buf, cap)` until it returns 0, writing the bytes to the UART.

## Building

```sh
# Standalone staticlib (default features: example heap + panic handler + UART C API).
rustup target add thumbv7em-none-eabihf riscv32imac-unknown-none-elf
cargo build --release --target thumbv7em-none-eabihf
cargo build --release --target riscv32imac-unknown-none-elf --no-default-features --features standalone,ffi-can

# Host tests (ports against the real host session).
cargo test
```

Any target with a Rust `core` + `alloc` works the same way; the two above are what CI builds.
When the code becomes part of a Rust firmware that has its own `#[global_allocator]` and
`#[panic_handler]`, disable the default features and drop `staticlib` from `crate-type` (a
staticlib is a final artifact and needs both).

`../../scripts/mcu-size.sh` links the staticlib with `--gc-sections` and reports flash/RAM per
target and transport; see `crates/link/README.md` for current numbers.

## Notes

- `FreeListHeap` and the C API's global session use a spin flag, so call them from one main loop.
  If interrupt handlers allocate, use a critical-section based allocator instead. Targets without
  atomic compare-and-swap (for example `thumbv6m`) need such an allocator and a different global
  for the C API.
- The device announces `min(RX, TX)` as its maximum frame; the host never sends anything larger.
- Timing defaults (`DeviceConfig::provider`) suit links from 9600 baud to CAN FD; raise
  `state_retry_min_ms` on very slow links so retransmissions do not start before the first
  attempt could have been acknowledged.
