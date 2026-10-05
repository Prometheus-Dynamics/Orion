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
//! uint32_t orion_lease_count(void);                   // leases currently held
//! ```
//!
//! No allocator is used: the session and the provider id `provider.<name>` (whose suffix is the
//! device name) live in one zero-initialized static.
//! With `ffi-can`, the CAN identifiers are the caller's business: send `orion_can_tx` data with
//! the link's device→host id and pass only frames with its host→device id to `orion_can_rx`.

use core::cell::UnsafeCell;
use core::mem::MaybeUninit;
use core::sync::atomic::{AtomicBool, Ordering, compiler_fence};

use orion_link::device::{DeviceConfig, DeviceEvent};
use orion_link::wire::str_from_utf8;

use crate::{RX, TX};
#[cfg(not(feature = "alloc"))]
use crate::{provider_view, resource_view};

/// Longest device name `orion_init` accepts.
pub const NAME_CAPACITY: usize = 32;

const PROVIDER_PREFIX: &[u8] = b"provider.";
const PREFIX_LEN: usize = PROVIDER_PREFIX.len();

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
type Session = orion_link::device::CanDevice<RX, TX, StoredName>;
#[cfg(all(feature = "ffi-uart", not(feature = "ffi-can")))]
type Session = orion_link::device::StreamDevice<RX, TX, StoredName>;

/// The session's device name: a view of the name stored in [`PORT`], so the session holds no
/// copy of it.
#[derive(Debug, Clone, Copy)]
pub struct StoredName;

impl AsRef<str> for StoredName {
    fn as_ref(&self) -> &str {
        PORT.provider_id().get(PREFIX_LEN..).unwrap_or_default()
    }
}

/// The global session with a busy flag, so a re-entrant call (for example from a callback) fails
/// instead of aliasing. Only atomic loads and stores are used, so this also builds for cores
/// without compare-and-swap (Cortex-M0/M0+, RV32 without the A extension). It is not a lock: do
/// not call the C API from interrupt handlers.
///
/// The session is `MaybeUninit` so the static is zero-initialized (`.bss`): an `Option<Session>`
/// would put a full initialized copy of the session into `.data`, which costs flash.
struct Global {
    slot: UnsafeCell<MaybeUninit<Session>>,
    /// `provider.<device name>`; written only by `orion_init`.
    name: UnsafeCell<[u8; PREFIX_LEN + NAME_CAPACITY]>,
    name_len: UnsafeCell<usize>,
    ready: AtomicBool,
    busy: AtomicBool,
}

// SAFETY: access to the cell is serialized by the busy flag under the single-main-loop contract
// documented above.
unsafe impl Sync for Global {}

static PORT: Global = Global {
    slot: UnsafeCell::new(MaybeUninit::uninit()),
    name: UnsafeCell::new([0; PREFIX_LEN + NAME_CAPACITY]),
    name_len: UnsafeCell::new(0),
    ready: AtomicBool::new(false),
    busy: AtomicBool::new(false),
};

impl Global {
    /// Runs `f` with exclusive access to the slot; `None` on re-entry.
    fn lock<R>(&self, f: impl FnOnce(&mut MaybeUninit<Session>) -> R) -> Option<R> {
        if self.busy.load(Ordering::Relaxed) {
            return None;
        }
        self.busy.store(true, Ordering::Relaxed);
        compiler_fence(Ordering::Acquire);
        // SAFETY: the busy flag gives this call exclusive access (single main loop).
        let result = f(unsafe { &mut *self.slot.get() });
        compiler_fence(Ordering::Release);
        self.busy.store(false, Ordering::Relaxed);
        Some(result)
    }

    /// `provider.<device name>` (empty before `orion_init`).
    fn provider_id(&self) -> &str {
        // SAFETY: the name is written only by `orion_init`, under the busy flag, and no string
        // borrowed from it outlives a C API call.
        let (name, len) = unsafe { (&*self.name.get(), *self.name_len.get()) };
        name.get(..len)
            .and_then(|bytes| str_from_utf8(bytes).ok())
            .unwrap_or_default()
    }

    /// Stores `provider.<name>`. Call only under the busy flag.
    fn set_name(&self, name: &str) {
        // SAFETY: the caller holds the busy flag; nothing borrows the name between calls.
        let (buf, len) = unsafe { (&mut *self.name.get(), &mut *self.name_len.get()) };
        for (dst, src) in buf
            .iter_mut()
            .zip(PROVIDER_PREFIX.iter().chain(name.as_bytes()))
        {
            *dst = *src;
        }
        *len = PREFIX_LEN + name.len();
    }

    /// Runs `f` on the session; `None` before `orion_init` or on re-entry.
    fn with<R>(&self, f: impl FnOnce(&mut Session) -> R) -> Option<R> {
        self.lock(|slot| {
            // SAFETY: `ready` is only set after the slot was written by `orion_init`.
            self.ready
                .load(Ordering::Relaxed)
                .then(|| f(unsafe { slot.assume_init_mut() }))
        })
        .flatten()
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

/// Creates the session. Returns `false` if the name is not UTF-8, empty, longer than
/// [`NAME_CAPACITY`] bytes, or on re-entry.
///
/// # Safety
/// `device_name` must be valid for `len` bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn orion_init(device_name: *const u8, len: usize) -> bool {
    // SAFETY: forwarded caller guarantee.
    let Ok(name) = str_from_utf8(unsafe { bytes(device_name, len) }) else {
        return false;
    };
    if name.is_empty() || name.len() > NAME_CAPACITY {
        return false;
    }
    #[cfg(feature = "ffi-can")]
    let transport = orion_link::Packet::CLASSIC;
    #[cfg(all(feature = "ffi-uart", not(feature = "ffi-can")))]
    let transport = orion_link::Stream;
    PORT.lock(|slot| {
        PORT.set_name(name);
        // The session owns nothing that needs dropping, so a previous one is simply overwritten.
        // `new` is inlined, so the session is built in place rather than on the stack.
        slot.write(Session::new(DeviceConfig::provider(StoredName), transport));
        PORT.ready.store(true, Ordering::Relaxed);
    })
    .is_some()
}

/// Publishes a snapshot with one resource. Returns 0 on success, -1 if not initialized or on
/// re-entry, -2 for invalid strings, -3 if it does not fit the transmit buffer.
///
/// The strings are encoded at once and need not outlive the call.
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
    let (Ok(id), Ok(kind)) = (str_from_utf8(id), str_from_utf8(kind)) else {
        return -2;
    };
    if id.is_empty() || kind.is_empty() {
        return -2;
    }
    PORT.with(|session| {
        let provider_id = PORT.provider_id();
        // The minimal path: borrowed views, encoded at once, no allocation.
        #[cfg(not(feature = "alloc"))]
        let published = {
            let types = [kind];
            let provider = provider_view(provider_id, &types);
            let resource = resource_view(provider_id, id, kind, healthy);
            session.publish_provider_state(&provider, &[resource])
        };
        // The `alloc` variant publishes the full records (postcard + serde, heap).
        #[cfg(feature = "alloc")]
        let published = {
            let name = provider_id.get(PREFIX_LEN..).unwrap_or_default();
            let (Some(provider), Some(resource)) = (
                crate::provider_record(name, kind),
                crate::resource_record(name, id, kind, healthy),
            ) else {
                return -2;
            };
            session.publish_provider_state(&provider, core::slice::from_ref(&resource))
        };
        match published {
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
    PORT.with(|session| {
        session.poll(now_ms);
        let mut bits = 0;
        while let Some(event) = session.next_event() {
            bits |= match event {
                DeviceEvent::Connected { .. } => ORION_EVENT_CONNECTED,
                DeviceEvent::LeasesChanged => ORION_EVENT_LEASES,
                DeviceEvent::Disconnected => ORION_EVENT_DISCONNECTED,
                DeviceEvent::Rejected(_) => ORION_EVENT_REJECTED,
                DeviceEvent::StateAcked => ORION_EVENT_STATE_ACKED,
                _ => 0,
            };
        }
        bits
    })
    .unwrap_or(0)
}

/// Leases currently held (0 while disconnected).
#[unsafe(no_mangle)]
pub extern "C" fn orion_lease_count() -> u32 {
    PORT.with(|session| u32::try_from(session.leases().len()).unwrap_or(u32::MAX))
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
    let _ = PORT.with(|session| session.receive(data));
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
    PORT.with(|session| session.transmit(out)).unwrap_or(0)
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
    let _ = PORT.with(|session| session.receive_segment(data));
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
    PORT.with(|session| session.transmit_segment(out))
        .unwrap_or(0)
}
