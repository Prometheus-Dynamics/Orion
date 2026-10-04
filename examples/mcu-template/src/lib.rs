//! Template for connecting any microcontroller to Orion over UART or CAN.
//!
//! Nothing here names a chip, HAL, RTOS, or async runtime. A port supplies:
//!
//! - a byte stream implementing [`embedded_io::Read`] + [`embedded_io::ReadReady`] +
//!   [`embedded_io::Write`] (most HALs' UARTs do), **or** a CAN controller implementing
//!   [`embedded_can::nb::Can`];
//! - a monotonic millisecond counter (a SysTick/timer interrupt incrementing a `u64` is enough);
//! - a global allocator (a few KiB; [`heap::FreeListHeap`] is a small replaceable example).
//!
//! Then it calls [`UartPort::service`] or [`CanPort::service`] from the main loop. C firmware links
//! the `staticlib` and calls the `orion_*` functions in [`ffi`] instead.
//!
//! Copy this crate into your firmware repository and edit it; it is a starting point, not a
//! dependency.

#![no_std]
#![deny(unsafe_op_in_unsafe_fn)]

extern crate alloc;

pub mod can;
#[cfg(any(feature = "ffi-uart", feature = "ffi-can"))]
pub mod ffi;
#[cfg(feature = "heap")]
pub mod heap;
pub mod uart;

pub use can::CanPort;
pub use uart::UartPort;

use orion_link::message::{
    HealthState, NodeId, ProviderId, ProviderRecord, ResourceId, ResourceRecord, ResourceType,
};

/// Receive buffer: the largest frame the device accepts (lease sets must fit).
pub const RX: usize = 256;
/// Transmit buffer: the largest frame the device sends (its provider snapshot must fit).
pub const TX: usize = 256;

/// `provider.<device_name>` without `format!` (which would link the formatting machinery).
fn provider_id(device_name: &str) -> Option<ProviderId> {
    let mut id = alloc::string::String::with_capacity(device_name.len() + 9);
    id.push_str("provider.");
    id.push_str(device_name);
    ProviderId::try_new(id).ok()
}

/// The provider record for a device. The gateway assigns the real node id, so a placeholder is
/// fine. Returns `None` if the name is empty.
pub fn provider_record(device_name: &str, resource_type: &str) -> Option<ProviderRecord> {
    let provider_id = provider_id(device_name)?;
    let mut record = ProviderRecord::builder(provider_id, NodeId::try_new("unassigned").ok()?);
    if let Ok(resource_type) = ResourceType::try_new(resource_type) {
        record = record.resource_type(resource_type);
    }
    Some(record.build())
}

/// A single resource offered by `device_name`. Returns `None` on empty identifiers.
pub fn resource_record(
    device_name: &str,
    resource_id: &str,
    resource_type: &str,
    healthy: bool,
) -> Option<ResourceRecord> {
    let provider_id = provider_id(device_name)?;
    let record = ResourceRecord::builder(
        ResourceId::try_new(resource_id).ok()?,
        ResourceType::try_new(resource_type).ok()?,
        provider_id,
    )
    .health(if healthy {
        HealthState::Healthy
    } else {
        HealthState::Degraded
    })
    .build();
    Some(record)
}

#[cfg(all(feature = "panic-handler", target_os = "none"))]
#[panic_handler]
fn panic(_info: &core::panic::PanicInfo<'_>) -> ! {
    // Replace with your board's reset or fault reporting.
    loop {
        core::hint::spin_loop();
    }
}

/// The example heap as the global allocator in standalone builds. 8 KiB covers the session's
/// small allocations (device name, node id, decoded lease sets) with room to spare.
#[cfg(all(feature = "global-heap", target_os = "none"))]
#[global_allocator]
static HEAP: heap::FreeListHeap<8192> = heap::FreeListHeap::new();
