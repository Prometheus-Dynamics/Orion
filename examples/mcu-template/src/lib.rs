//! A starting point for a microcontroller on Orion's wire.
//!
//! Nothing here names a chip, a HAL, an RTOS or an async runtime. A port brings:
//!
//! - a byte stream that implements [`embedded_io::Read`], [`embedded_io::ReadReady`] and
//!   [`embedded_io::Write`], which most HALs' UARTs do, **or** a CAN controller that
//!   implements [`embedded_can::nb::Can`];
//! - a millisecond counter, which may start anywhere and wrap.
//!
//! Rust firmware puts its layers (topics, actions, firmware slots, from `crates/device`) in
//! something that implements [`App`] and calls [`UartPort::service`] or [`CanPort::service`]
//! from its main loop. C firmware links the static library and calls the `orion_*` functions
//! of [`ffi`], declared in `include/orion_mcu.h`.
//!
//! No allocator, no heap, no formatting code. Copy this crate into the firmware's repository
//! and edit it: it is a start, not a dependency.

#![cfg_attr(target_os = "none", no_std)]

pub mod can;
#[cfg(any(feature = "ffi-uart", feature = "ffi-can"))]
pub mod ffi;
pub mod uart;

pub use can::{CanPort, CanPortError};
pub use orion_device_core::{FrameView, Link};
pub use uart::UartPort;

/// The longest frame the device takes. A chunk of new firmware is this less 31 bytes.
pub const RX: usize = 256;
/// The longest frame the device sends: the longest topic declaration or action
/// registration must fit.
pub const TX: usize = 256;

/// What runs above the session: the firmware's layers. A port hands it every frame the
/// session does not take, and polls it after the session, with the same counter.
///
/// ```ignore
/// struct Imu {
///     topics: Topics<1>,
///     actions: Actions<1>,
/// }
///
/// impl App<{ TX }> for Imu {
///     fn handle(&mut self, link: &mut Link<{ TX }>, frame: &FrameView<'_>) {
///         self.topics.handle(frame);
///         if let Heard::Run { call, .. } = self.actions.handle(frame) {
///             let _ = self.actions.finish(link, call, Outcome::Succeeded, &[], "");
///         }
///     }
///     fn poll(&mut self, link: &mut Link<{ TX }>, now_ms: u32) {
///         self.topics.poll(link, now_ms);
///         self.actions.poll(link, now_ms);
///     }
/// }
/// ```
pub trait App<const TX: usize> {
    /// A frame from the host that is not the session's own.
    fn handle(&mut self, link: &mut Link<TX>, frame: &FrameView<'_>);
    /// Time passed: declare, register, publish.
    fn poll(&mut self, link: &mut Link<TX>, now_ms: u32);
}

/// No layers: a device that only says hello.
impl<const TX: usize> App<TX> for () {
    fn handle(&mut self, _: &mut Link<TX>, _: &FrameView<'_>) {}
    fn poll(&mut self, _: &mut Link<TX>, _: u32) {}
}

#[cfg(all(feature = "standalone", target_os = "none"))]
#[panic_handler]
fn panic(_: &core::panic::PanicInfo<'_>) -> ! {
    // Replace with the board's reset or fault report.
    loop {
        core::hint::spin_loop();
    }
}
