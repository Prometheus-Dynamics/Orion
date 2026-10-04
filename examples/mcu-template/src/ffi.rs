//! C entry points, for firmware written in C against any vendor SDK.
//!
//! Link `liborion_mcu_template.a` and call, from one main loop (not from interrupts):
//!
//! ```c
//! bool orion_init(const uint8_t *device_name, size_t len);
//! int32_t orion_publish(const uint8_t *resource_id, size_t id_len,
//!                       const uint8_t *resource_type, size_t type_len, bool healthy);
//! void orion_rx(const uint8_t *data, size_t len);     // ffi-uart: received UART bytes
//! size_t orion_tx(uint8_t *out, size_t cap);          // ffi-uart: bytes to write to the UART
//! void orion_can_rx(const uint8_t *data, size_t len); // ffi-can: data of a host->device frame
//! size_t orion_can_tx(uint8_t *out64);                // ffi-can: next device->host frame data
//! uint32_t orion_poll(uint64_t now_ms);               // returns ORION_EVENT_* bits
//! uint32_t orion_lease_count(void);                   // leases held after the last lease event
//! ```
//!
//! With `ffi-can`, the CAN identifiers are the caller's business: send `orion_can_tx` data with
//! the link's device→host id and pass only frames with its host→device id to `orion_can_rx`.

use alloc::string::String;
use core::cell::UnsafeCell;
use core::sync::atomic::{AtomicBool, Ordering};

use orion_link::device::{DeviceConfig, DeviceEvent};

use crate::{RX, TX, provider_record, resource_record};

/// `orion_poll` bit: a session was established.
pub const ORION_EVENT_CONNECTED: u32 = 1 << 0;
/// `orion_poll` bit: the lease set changed (see `orion_lease_count`).
pub const ORION_EVENT_LEASES: u32 = 1 << 1;
/// `orion_poll` bit: the session ended; the device is reconnecting.
pub const ORION_EVENT_DISCONNECTED: u32 = 1 << 2;
/// `orion_poll` bit: the host rejected the device; it retries after a backoff.
pub const ORION_EVENT_REJECTED: u32 = 1 << 3;
/// `orion_poll` bit: the host acknowledged the latest snapshot.
pub const ORION_EVENT_STATE_ACKED: u32 = 1 << 4;

#[cfg(feature = "ffi-can")]
type Session = orion_link::device::CanDevice<RX, TX>;
#[cfg(all(feature = "ffi-uart", not(feature = "ffi-can")))]
type Session = orion_link::device::StreamDevice<RX, TX>;

struct Port {
    session: Session,
    device_name: String,
    lease_count: u32,
}

/// A global slot guarded by a busy flag, so re-entrant calls fail instead of aliasing.
struct Global(UnsafeCell<Option<Port>>, AtomicBool);

// SAFETY: access to the cell is serialized by the busy flag.
unsafe impl Sync for Global {}

static PORT: Global = Global(UnsafeCell::new(None), AtomicBool::new(false));

impl Global {
    fn with<R>(&self, f: impl FnOnce(&mut Option<Port>) -> R) -> Option<R> {
        if self
            .1
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            return None;
        }
        // SAFETY: the busy flag gives this call exclusive access.
        let result = f(unsafe { &mut *self.0.get() });
        self.1.store(false, Ordering::Release);
        Some(result)
    }
}

/// # Safety
/// `ptr` must be valid for `len` bytes (or `len` must be 0).
unsafe fn bytes<'a>(ptr: *const u8, len: usize) -> &'a [u8] {
    if ptr.is_null() || len == 0 {
        &[]
    } else {
        // SAFETY: guaranteed by the caller.
        unsafe { core::slice::from_raw_parts(ptr, len) }
    }
}

/// Creates the session. Returns `false` if the name is not UTF-8 or empty, or on re-entry.
///
/// # Safety
/// `device_name` must be valid for `len` bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn orion_init(device_name: *const u8, len: usize) -> bool {
    // SAFETY: forwarded caller guarantee.
    let Ok(name) = core::str::from_utf8(unsafe { bytes(device_name, len) }) else {
        return false;
    };
    if name.trim().is_empty() {
        return false;
    }
    #[cfg(feature = "ffi-can")]
    let transport = orion_link::Packet::CLASSIC;
    #[cfg(all(feature = "ffi-uart", not(feature = "ffi-can")))]
    let transport = orion_link::Stream;
    PORT.with(|slot| {
        *slot = Some(Port {
            session: Session::new(DeviceConfig::provider(name), transport),
            device_name: String::from(name),
            lease_count: 0,
        });
    })
    .is_some()
}

/// Publishes a snapshot with one resource. Returns 0 on success, -1 if not initialized or on
/// re-entry, -2 for invalid strings, -3 if it does not fit the transmit buffer.
///
/// # Safety
/// The pointers must be valid for their lengths.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn orion_publish(
    resource_id: *const u8,
    id_len: usize,
    resource_type: *const u8,
    type_len: usize,
    healthy: bool,
) -> i32 {
    // SAFETY: forwarded caller guarantees.
    let (id, kind) = unsafe { (bytes(resource_id, id_len), bytes(resource_type, type_len)) };
    let (Ok(id), Ok(kind)) = (core::str::from_utf8(id), core::str::from_utf8(kind)) else {
        return -2;
    };
    PORT.with(|slot| {
        let Some(port) = slot else {
            return -1;
        };
        let (Some(provider), Some(resource)) = (
            provider_record(&port.device_name, kind),
            resource_record(&port.device_name, id, kind, healthy),
        ) else {
            return -2;
        };
        match port
            .session
            .publish_provider_state(&provider, core::slice::from_ref(&resource))
        {
            Ok(()) => 0,
            Err(_) => -3,
        }
    })
    .unwrap_or(-1)
}

/// Advances the session clock and returns the `ORION_EVENT_*` bits of the events since the last
/// call.
#[unsafe(no_mangle)]
pub extern "C" fn orion_poll(now_ms: u64) -> u32 {
    PORT.with(|slot| {
        let Some(port) = slot else {
            return 0;
        };
        port.session.poll(now_ms);
        let mut bits = 0;
        while let Some(event) = port.session.next_event() {
            bits |= match event {
                DeviceEvent::Connected { .. } => ORION_EVENT_CONNECTED,
                DeviceEvent::Leases(leases) => {
                    port.lease_count = u32::try_from(leases.len()).unwrap_or(u32::MAX);
                    ORION_EVENT_LEASES
                }
                DeviceEvent::Disconnected => {
                    port.lease_count = 0;
                    ORION_EVENT_DISCONNECTED
                }
                DeviceEvent::Rejected(_) => ORION_EVENT_REJECTED,
                DeviceEvent::StateAcked => ORION_EVENT_STATE_ACKED,
                _ => 0,
            };
        }
        bits
    })
    .unwrap_or(0)
}

/// Leases currently held (after the last `ORION_EVENT_LEASES`).
#[unsafe(no_mangle)]
pub extern "C" fn orion_lease_count() -> u32 {
    PORT.with(|slot| slot.as_ref().map_or(0, |port| port.lease_count))
        .unwrap_or(0)
}

/// Feeds bytes received from the UART.
///
/// # Safety
/// `data` must be valid for `len` bytes.
#[cfg(all(feature = "ffi-uart", not(feature = "ffi-can")))]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn orion_rx(data: *const u8, len: usize) {
    // SAFETY: forwarded caller guarantee.
    let data = unsafe { bytes(data, len) };
    let _ = PORT.with(|slot| {
        if let Some(port) = slot {
            port.session.receive(data);
        }
    });
}

/// Writes up to `cap` bytes to send into `out`; returns how many. Call until it returns 0.
///
/// # Safety
/// `out` must be valid for writes of `cap` bytes.
#[cfg(all(feature = "ffi-uart", not(feature = "ffi-can")))]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn orion_tx(out: *mut u8, cap: usize) -> usize {
    if out.is_null() || cap == 0 {
        return 0;
    }
    // SAFETY: guaranteed by the caller.
    let out = unsafe { core::slice::from_raw_parts_mut(out, cap) };
    PORT.with(|slot| slot.as_mut().map_or(0, |port| port.session.transmit(out)))
        .unwrap_or(0)
}

/// Feeds the data of one received host→device CAN frame.
///
/// # Safety
/// `data` must be valid for `len` bytes.
#[cfg(feature = "ffi-can")]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn orion_can_rx(data: *const u8, len: usize) {
    // SAFETY: forwarded caller guarantee.
    let data = unsafe { bytes(data, len) };
    let _ = PORT.with(|slot| {
        if let Some(port) = slot {
            port.session.receive_segment(data);
        }
    });
}

/// Writes the data of the next device→host CAN frame into `out` (64 bytes) and returns its
/// length, or 0 when idle.
///
/// # Safety
/// `out` must be valid for writes of 64 bytes.
#[cfg(feature = "ffi-can")]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn orion_can_tx(out: *mut u8) -> usize {
    if out.is_null() {
        return 0;
    }
    // SAFETY: guaranteed by the caller.
    let out = unsafe { core::slice::from_raw_parts_mut(out, 64) };
    PORT.with(|slot| {
        let Some(segment) = slot.as_mut().and_then(|port| port.session.next_segment()) else {
            return 0;
        };
        let data = segment.as_bytes();
        out.get_mut(..data.len()).map_or(0, |dst| {
            dst.copy_from_slice(data);
            data.len()
        })
    })
    .unwrap_or(0)
}
