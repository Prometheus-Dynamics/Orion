//! What a firmware pays for the device side of the wire.
//!
//! This is the smallest firmware that is still one: a device with 128-byte buffers and the
//! entry points a port calls from its main loop and its receive interrupt. It is built for
//! each microcontroller target and linked with only what those entry points reach, and
//! `scripts/device-size.sh` holds the result to a budget. Nothing runs it.

#![cfg_attr(target_os = "none", no_std)]

use core::cell::UnsafeCell;

use orion_device_core::DeviceConfig;
#[cfg(not(feature = "can"))]
use orion_device_core::StreamDevice;
#[cfg(feature = "can")]
use orion_device_core::{CanDevice, SegmentMtu};

#[cfg(not(feature = "can"))]
type Device = StreamDevice<128, 128>;
#[cfg(feature = "can")]
type Device = CanDevice<128, 128>;

/// The one device, in static memory: what a firmware without a heap has.
struct Shared(UnsafeCell<Device>);

// SAFETY: a port calls the entry points from one context at a time, as their contract says.
unsafe impl Sync for Shared {}

#[cfg(not(feature = "can"))]
static DEVICE: Shared = Shared(UnsafeCell::new(Device::new(DeviceConfig::new("probe"))));
#[cfg(feature = "can")]
static DEVICE: Shared = Shared(UnsafeCell::new(Device::new(
    DeviceConfig::new("probe"),
    SegmentMtu::CLASSIC,
)));

/// The device. The port must not call two entry points at once.
fn device() -> &'static mut Device {
    // SAFETY: see `Shared`: one caller at a time, and the reference does not outlive the call.
    unsafe { &mut *DEVICE.0.get() }
}

/// Timers. Returns the session's state: 0 connecting, 1 rejected, 2 connected.
#[unsafe(no_mangle)]
pub extern "C" fn orion_probe_poll(now_ms: u32) -> u32 {
    let device = device();
    device.link.poll(now_ms);
    while device.link.next_event().is_some() {}
    device.link.state() as u32
}

/// Feeds `len` bytes from the port at `bytes`.
///
/// # Safety
///
/// `bytes` must point at `len` readable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn orion_probe_receive(bytes: *const u8, len: usize) {
    // SAFETY: the caller's contract.
    let bytes = unsafe { core::slice::from_raw_parts(bytes, len) };
    #[cfg(not(feature = "can"))]
    device().receive(bytes, |_, _| {});
    #[cfg(feature = "can")]
    device().receive_segment(bytes, |_, _| {});
}

/// Writes up to `len` bytes to send at `out` and returns how many.
///
/// # Safety
///
/// `out` must point at `len` writable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn orion_probe_transmit(out: *mut u8, len: usize) -> usize {
    // SAFETY: the caller's contract.
    let out = unsafe { core::slice::from_raw_parts_mut(out, len) };
    #[cfg(not(feature = "can"))]
    return device().transmit(out);
    #[cfg(feature = "can")]
    return device().next_segment(out).unwrap_or(0);
}

#[cfg(target_os = "none")]
#[panic_handler]
fn panic(_: &core::panic::PanicInfo<'_>) -> ! {
    loop {}
}
