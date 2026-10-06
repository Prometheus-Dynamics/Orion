# Orion MCU template

A starting point for connecting **any** microcontroller to an `orion-node` over UART, RS-485,
USB-CDC, classic CAN, or CAN FD, using the [link protocol](../../docs/link-protocol.md) from
[`orion-link`](../../crates/link). Nothing here names a chip, HAL, RTOS, or async runtime: the
device session is sans-IO, so a port only moves bytes and reads a millisecond counter.

The default build uses the **minimal device path**: no allocator, no heap, no serde. The session
has fixed buffers, and the provider snapshot is described with borrowed `orion_link::wire` views
that encode to exactly the bytes of the full Orion records. It fits Cortex-M0+ class parts with
16 KiB of flash and 4 KiB of RAM (about 8 KB of flash and 0.9 KB of static RAM with 128-byte
buffers; see `crates/link/README.md` for the per-target table).

This crate is standalone (not a workspace member) so you can copy the directory into your firmware
repository and edit it.

| File | What it shows |
| --- | --- |
| `src/uart.rs` | `UartPort<U>`: a `StreamDevice` wired to any `embedded_io::{Read, ReadReady, Write}` byte stream. |
| `src/can.rs` | `CanPort<C>`: a `CanDevice` wired to any `embedded_can::nb::Can` controller. |
| `src/ffi.rs` | C entry points (`orion_init`, `orion_rx`/`orion_tx` or `orion_can_rx`/`orion_can_tx`, `orion_poll`, `orion_publish`, `orion_lease_count`) for C firmware, over one zero-initialized static. |
| `src/lib.rs` | Buffer sizes, `provider_view` / `resource_view` helpers (and `provider_record` / `resource_record` with `alloc`), and the `#[panic_handler]` / `#[global_allocator]` used only by the standalone staticlib. |
| `src/heap.rs` | `FreeListHeap<N>` (feature `heap`): a tiny first-fit `GlobalAlloc` for the `alloc` variant. Replaceable. |
| `tests/` | Host tests: both ports and the C API against the real `orion-link` host session and CAN bus. |

## Features

| Feature | Effect |
| --- | --- |
| `standalone` *(default)* | `#[panic_handler]` on bare-metal targets (a staticlib needs one). |
| `ffi-uart` *(default)* / `ffi-can` | C entry points over a byte stream / over CAN. |
| `alloc` | The full-record variant: the ports also accept `ProviderRecord` / `ResourceRecord` / `StatusEntry`, the C API publishes records, `provider_record` / `resource_record` exist. Needs a global allocator. |
| `heap` / `global-heap` | The example heap / install an 8 KiB one (implies `alloc`). Uses compare-and-swap, so not for Cortex-M0/M0+ or RV32 without `A`. |

## Port Orion to a new MCU in 6 steps

1. **Pick the transport.** A UART-like byte stream (UART, RS-485 with one device per link,
   USB-CDC) uses `UartPort`; a CAN controller uses `CanPort` with `Packet::CLASSIC`, `Packet::FD`,
   or a custom `SegmentMtu`.

2. **Provide the byte or frame driver.** If your HAL implements `embedded-io` 0.7
   (`Read + ReadReady + Write`) or `embedded-can` 0.4 (`nb::Can`), you are done. Otherwise write a
   ten-line adapter around your driver's "bytes available / read / write" or "receive / transmit
   frame" calls. Interrupt-driven receive works too: push bytes into a ring buffer in the ISR and
   implement `Read` over it, or call the session's clock-free `receive` directly.

3. **Provide a millisecond clock.** Any monotonic `u64` millisecond counter (SysTick, a hardware
   timer, an RTOS tick) passed to `service(now_ms)`. Only differences matter; it may start at any
   value and may wrap.

4. **Size the buffers.** `RX` and `TX` in `src/lib.rs` (default 128 bytes each) bound the largest
   lease set and provider snapshot (header + body + CRC; a provider with one simple resource is
   about 100 bytes, one lease about 40). The session needs `2 * RX` (decoder, lease set) +
   `2 * TX` (snapshot, status batch) + about 350 bytes. No allocator is needed.

5. **Describe what the device provides.** Build a `ProviderView` and its `ResourceView`s; they are
   `const`, so a fixed snapshot can live in flash:

   ```rust
   use orion_link::wire::{Health, ProviderView, ResourceView};

   const PROVIDER: ProviderView<'static> =
       ProviderView::new("provider.imu-board", "unassigned").with_resource_types(&["imu.sample_source"]);
   const RESOURCES: [ResourceView<'static>; 1] =
       [ResourceView::new("imu-board.imu-0", "imu.sample_source", "provider.imu-board")
           .with_health(Health::Healthy)];
   ```

   Call `publish(&PROVIDER, &RESOURCES)` whenever they change (views built at run time work the
   same; they are encoded at once and may be dropped afterwards). The session sends the newest
   snapshot, retransmits it until the host acknowledges it, and re-sends it automatically after
   every reconnect.

6. **Run the loop.**

   ```rust
   let mut port = UartPort::new(uart, "imu-board");
   port.publish(&PROVIDER, &RESOURCES)?;
   loop {
       port.service(millis())?;
       while let Some(event) = port.next_event() {
           match event {
               DeviceEvent::LeasesChanged => {
                   for lease in port.session().leases() {
                       apply(lease.resource_id, lease.holder_workload_id); // start/stop work
                   }
               }
               DeviceEvent::Disconnected => stop_all(), // the lease set is cleared
               _ => {}
           }
       }
       // ... your application ...
   }
   ```

   For CAN, pick the link identifiers (`CanLinkIds::for_address(base, address)`, the same base and
   address range as the gateway's bus configuration) and set your controller's acceptance filter to
   the host→device identifier.

C firmware skips steps 2, 5, and 6: link `liborion_mcu_template.a` and call `orion_init(name)`,
`orion_publish(resource_id, resource_type, healthy)`, then `orion_rx(bytes)` for received bytes,
`orion_poll(now_ms)` every loop (it returns `ORION_EVENT_*` bits), and `orion_tx(buf, cap)` until
it returns 0, writing the bytes to the UART (`orion_can_rx` / `orion_can_tx` with `ffi-can`).

## Building

```sh
# Standalone staticlib (default features: panic handler + UART C API, no allocator).
rustup target add thumbv6m-none-eabi thumbv7em-none-eabihf riscv32imc-unknown-none-elf
cargo build --release --target thumbv6m-none-eabi
cargo build --release --target riscv32imc-unknown-none-elf --no-default-features --features standalone,ffi-can

# The full-record variant with the example heap (cores with compare-and-swap).
cargo build --release --target thumbv7em-none-eabihf --features global-heap

# Host tests (ports and the C API against the real host session).
cargo test
```

Any target with a Rust `core` works the same way; CI builds the four targets above plus
`riscv32imac-unknown-none-elf`. When the code becomes part of a Rust firmware that has its own
`#[panic_handler]`, disable the default features and drop `staticlib` from `crate-type` (a
staticlib is a final artifact and needs one).

`../../scripts/mcu-size.sh` links the staticlib with `--gc-sections` and reports flash/RAM per
target and transport (CI fails if the minimal UART build outgrows its budget); see
`crates/link/README.md` for current numbers.

## Notes

- Nothing here needs atomic compare-and-swap: the C API's global uses only atomic loads and stores
  as a re-entry guard, so it builds for Cortex-M0/M0+ and RV32 without `A`. It is not a lock: call
  the C API from one main loop, not from interrupt handlers.
- The session never allocates, never panics, and links no formatting code (`core::fmt`); keep it
  that way in your glue (avoid `format!`, `unwrap`, and slice indexing that can panic) to stay
  small.
- `DeviceSession::new` is always inlined, so `slot.write(DeviceSession::new(..))` into a static
  `MaybeUninit` (as `src/ffi.rs` does) builds the session in place instead of on the stack.
- The device announces `min(RX, TX)` as its maximum frame; the host never sends anything larger.
- Timing defaults (`DeviceConfig::provider`) suit links from 9600 baud to CAN FD; raise
  `state_retry_min_ms` on very slow links so retransmissions do not start before the first
  attempt could have been acknowledged.
