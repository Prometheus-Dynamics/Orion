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

/// What the firmware holds: the device and the layers it was built with.
struct Probe {
    device: Device,
    #[cfg(feature = "topics")]
    topics: orion_device_topics::Topics<1>,
    #[cfg(feature = "actions")]
    actions: orion_device_actions::Actions<1>,
    /// The run to answer, if one came.
    #[cfg(feature = "actions")]
    run: Option<u64>,
    #[cfg(feature = "firmware")]
    firmware: Option<orion_device_firmware::Firmware<flash::Mapped>>,
}

/// Flash as a firmware's port has it, mapped at fixed addresses: enough to measure.
#[cfg(feature = "firmware")]
mod flash {
    use orion_device_firmware::{FlashError, RECORD_LEN, SlotFlash};

    const SLOT: [usize; 2] = [0x0801_0000, 0x0804_0000];
    const RECORD: [usize; 2] = [0x0807_C000, 0x0807_E000];
    const CAPACITY: u32 = 0x3_0000;

    /// The port's flash.
    #[derive(Debug)]
    pub struct Mapped;

    fn at(base: &[usize; 2], which: u8, offset: u32) -> Result<*mut u8, FlashError> {
        let base = base.get(usize::from(which)).ok_or(FlashError)?;
        Ok((base + offset as usize) as *mut u8)
    }

    impl SlotFlash for Mapped {
        fn slot_capacity(&self) -> u32 {
            CAPACITY
        }

        fn erase_slot(&mut self, slot: u8) -> Result<(), FlashError> {
            let to = at(&SLOT, slot, 0)?;
            // SAFETY: the slot's region is this firmware's, as the port lays it out.
            unsafe { core::ptr::write_volatile(to, 0xFF) };
            Ok(())
        }

        fn write_slot(&mut self, slot: u8, offset: u32, data: &[u8]) -> Result<(), FlashError> {
            let to = at(&SLOT, slot, offset)?;
            for (n, byte) in data.iter().enumerate() {
                // SAFETY: as in `erase_slot`.
                unsafe { core::ptr::write_volatile(to.wrapping_add(n), *byte) };
            }
            Ok(())
        }

        fn read_slot(&self, slot: u8, offset: u32, out: &mut [u8]) -> Result<(), FlashError> {
            let from = at(&SLOT, slot, offset)?;
            for (n, byte) in out.iter_mut().enumerate() {
                // SAFETY: as in `erase_slot`.
                *byte = unsafe { core::ptr::read_volatile(from.wrapping_add(n)) };
            }
            Ok(())
        }

        fn read_record(&self, copy: u8, out: &mut [u8; RECORD_LEN]) -> Result<(), FlashError> {
            let from = at(&RECORD, copy, 0)?;
            for (n, byte) in out.iter_mut().enumerate() {
                // SAFETY: as in `erase_slot`.
                *byte = unsafe { core::ptr::read_volatile(from.wrapping_add(n)) };
            }
            Ok(())
        }

        fn write_record(&mut self, copy: u8, data: &[u8; RECORD_LEN]) -> Result<(), FlashError> {
            let to = at(&RECORD, copy, 0)?;
            for (n, byte) in data.iter().enumerate() {
                // SAFETY: as in `erase_slot`.
                unsafe { core::ptr::write_volatile(to.wrapping_add(n), *byte) };
            }
            Ok(())
        }
    }
}

#[cfg(feature = "actions")]
static ACTIONS: [orion_device_actions::ActionSpec; 1] =
    [orion_device_actions::ActionSpec::on_resource(
        "zero",
        "probe",
        "Sets yaw to zero",
    )];

#[cfg(feature = "topics")]
static TOPICS: [orion_device_topics::TopicSpec; 1] = [orion_device_topics::TopicSpec::latest(
    "yaw",
    &[orion_device_topics::Field::new(
        "yaw",
        orion_device_topics::FieldType::Float,
        "deg",
    )],
)];

#[cfg(not(feature = "can"))]
const DEVICE: Device = Device::new(DeviceConfig::new("probe"));
#[cfg(feature = "can")]
const DEVICE: Device = Device::new(DeviceConfig::new("probe"), SegmentMtu::CLASSIC);

/// The one firmware, in static memory: what a firmware without a heap has.
struct Shared(UnsafeCell<Probe>);

// SAFETY: a port calls the entry points from one context at a time, as their contract says.
unsafe impl Sync for Shared {}

static PROBE: Shared = Shared(UnsafeCell::new(Probe {
    device: DEVICE,
    #[cfg(feature = "topics")]
    topics: orion_device_topics::Topics::new(&TOPICS),
    #[cfg(feature = "actions")]
    actions: orion_device_actions::Actions::new(&ACTIONS),
    #[cfg(feature = "actions")]
    run: None,
    #[cfg(feature = "firmware")]
    firmware: None,
}));

/// The firmware. The port must not call two entry points at once.
fn probe() -> &'static mut Probe {
    // SAFETY: see `Shared`: one caller at a time, and the reference does not outlive the call.
    unsafe { &mut *PROBE.0.get() }
}

/// Timers. Returns the session's state: 0 connecting, 1 rejected, 2 connected.
#[unsafe(no_mangle)]
pub extern "C" fn orion_probe_poll(now_ms: u32) -> u32 {
    let probe = probe();
    probe.device.link.poll(now_ms);
    while probe.device.link.next_event().is_some() {}
    #[cfg(feature = "topics")]
    probe.topics.poll(&mut probe.device.link, now_ms);
    #[cfg(feature = "actions")]
    {
        probe.actions.poll(&mut probe.device.link, now_ms);
        if let Some(call) = probe.run
            && probe
                .actions
                .finish(
                    &mut probe.device.link,
                    call,
                    orion_device_actions::Outcome::Succeeded,
                    &[],
                    "",
                )
                .is_ok()
        {
            probe.run = None;
        }
    }
    // The record is read from flash, so the firmware layer starts on the first call.
    #[cfg(feature = "firmware")]
    probe
        .firmware
        .get_or_insert_with(|| {
            orion_device_firmware::Firmware::new("devices/probe/firmware", flash::Mapped)
        })
        .poll(&mut probe.device.link, now_ms);
    probe.device.link.state() as u32
}

/// What the bootloader calls after a reset: the slot to start, or 255 for none.
#[cfg(feature = "firmware")]
#[unsafe(no_mangle)]
pub extern "C" fn orion_probe_boot(fault: bool) -> u8 {
    let reason = if fault {
        orion_device_firmware::ResetReason::Fault
    } else {
        orion_device_firmware::ResetReason::Normal
    };
    orion_device_firmware::boot(&mut flash::Mapped, reason).unwrap_or(0xFF)
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
    let probe = probe();
    #[cfg(feature = "topics")]
    let topics = &mut probe.topics;
    #[cfg(feature = "actions")]
    let (actions, run) = (&mut probe.actions, &mut probe.run);
    #[cfg(feature = "firmware")]
    let firmware = &mut probe.firmware;
    let up = |_: &mut _, _frame: &orion_device_core::FrameView<'_>| {
        #[cfg(feature = "topics")]
        let _ = topics.handle(_frame);
        #[cfg(feature = "actions")]
        if let orion_device_actions::Heard::Run { call, .. } = actions.handle(_frame) {
            *run = Some(call);
        }
        #[cfg(feature = "firmware")]
        if let Some(firmware) = firmware.as_mut() {
            let _ = firmware.handle(_frame);
        }
    };
    #[cfg(not(feature = "can"))]
    probe.device.receive(bytes, up);
    #[cfg(feature = "can")]
    probe.device.receive_segment(bytes, up);
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
    return probe().device.transmit(out);
    #[cfg(feature = "can")]
    return probe().device.next_segment(out).unwrap_or(0);
}

/// Publishes one sample of the probe's topic. Returns whether it was taken.
#[cfg(feature = "topics")]
#[unsafe(no_mangle)]
pub extern "C" fn orion_probe_publish(time_ns: u64, value: f64) -> bool {
    let probe = probe();
    probe
        .topics
        .publish_f64(&mut probe.device.link, 0, 0, time_ns, value)
        .is_ok()
}

#[cfg(target_os = "none")]
#[panic_handler]
fn panic(_: &core::panic::PanicInfo<'_>) -> ! {
    loop {}
}
