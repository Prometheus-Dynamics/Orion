//! The C API, for firmware written in C against any vendor SDK.
//!
//! Link `liborion_mcu_template.a`, include `include/orion_mcu.h`, and call from one main loop,
//! never from an interrupt:
//!
//! 1. `orion_init("imu-board")`, then the device's topics (`orion_topic`), actions
//!    (`orion_action`) and, with `ffi-slots`, where its firmware goes (`orion_firmware`).
//! 2. Every pass: hand over what arrived (`orion_rx`, or `orion_can_rx` for each CAN frame),
//!    call `orion_poll(now_ms)` and look at the `ORION_EVENT_*` bits it returns, publish
//!    (`orion_publish`), answer runs (`orion_run`, `orion_finish`), and send what
//!    `orion_tx` (`orion_can_tx`) hands out until it says 0.
//!
//! The first `orion_poll` fixes the tables: topics and actions are added before it. Every
//! string passed (names, units, descriptions, the prefix) must stay where it is for as long
//! as the firmware runs: a string literal. Nothing is copied, so the tables cost no RAM
//! beyond a pointer and a length each.
//!
//! Everything lives in static memory: no allocator, no heap. A call made while another is
//! under way (from an interrupt, say) does nothing and returns its error value.

use core::cell::UnsafeCell;
use core::ffi::{CStr, c_char};
use core::mem::MaybeUninit;
use core::sync::atomic::{AtomicBool, AtomicU8, Ordering, compiler_fence};

use orion_device_actions::{ActionSpec, Actions, Admits, Heard as Run, Outcome, Policy, Target};
use orion_device_core::{DeviceConfig, Event, NAME_MAX, State as Session};
use orion_device_topics::{Field, FieldType, PublishError, TopicSpec, Topics};

use crate::{RX, TX};

/// Breaking changes to this API. Moved only at an Orion release.
pub const ORION_MCU_API_MAJOR: u32 = 1;
/// Additions to this API.
pub const ORION_MCU_API_MINOR: u32 = 0;
/// What [`orion_api_version`] returns: the major in the high 16 bits, the minor in the low.
pub const ORION_MCU_API_VERSION: u32 = (ORION_MCU_API_MAJOR << 16) | ORION_MCU_API_MINOR;

/// The most topics a device declares.
pub const ORION_MAX_TOPICS: usize = 8;
/// The most actions a device registers.
pub const ORION_MAX_ACTIONS: usize = 8;

/// `orion_poll`: the host took the device. Topics and actions are said again by themselves.
pub const ORION_EVENT_CONNECTED: u32 = 1 << 0;
/// `orion_poll`: the session ended; hello is being said again.
pub const ORION_EVENT_DISCONNECTED: u32 = 1 << 1;
/// `orion_poll`: the host said no; hello is said again after a while.
pub const ORION_EVENT_REJECTED: u32 = 1 << 2;
/// `orion_poll`: a run is waiting: take it with `orion_run`.
pub const ORION_EVENT_RUN: u32 = 1 << 3;
/// `orion_poll`: a run is to stop: `orion_cancelled` says which.
pub const ORION_EVENT_CANCEL: u32 = 1 << 4;
/// `orion_poll`: how many listen to a topic changed: see `orion_subscribers`.
pub const ORION_EVENT_DEMAND: u32 = 1 << 5;
/// `orion_poll`: new firmware checked out and is on trial: reset when ready to start it.
pub const ORION_EVENT_FIRMWARE: u32 = 1 << 6;

/// A field of a number with a fraction, 8 bytes.
pub const ORION_FIELD_FLOAT: u8 = 3;
/// A field of a whole number, 8 bytes.
pub const ORION_FIELD_INT: u8 = 2;
/// A field of true or false, 1 byte.
pub const ORION_FIELD_BOOL: u8 = 1;

/// `orion_action`: programs on the board may run it.
pub const ORION_ADMITS_LOCAL: u8 = 1 << 0;
/// `orion_action`: operators may run it, once granted.
pub const ORION_ADMITS_OPERATOR: u8 = 1 << 1;
/// `orion_action`: other nodes may run it.
pub const ORION_ADMITS_PEER: u8 = 1 << 2;

/// `orion_finish`: it did what it was asked.
pub const ORION_OUTCOME_SUCCEEDED: u8 = 1;
/// `orion_finish`: it tried, and it did not work.
pub const ORION_OUTCOME_FAILED: u8 = 2;
/// `orion_finish`: it would not, for the arguments or the state the device is in.
pub const ORION_OUTCOME_REJECTED: u8 = 3;

/// Not now: before `orion_init`, after the first `orion_poll` for a table, or during another
/// call.
pub const ORION_ERR_STATE: i32 = -1;
/// A string that is not one, a name too long, a value out of range.
pub const ORION_ERR_ARG: i32 = -2;
/// The table is full.
pub const ORION_ERR_FULL: i32 = -3;
/// No such topic, or the host has not taken it in this session.
pub const ORION_ERR_CLOSED: i32 = -4;
/// The link is sending something else: call again after `orion_tx`.
pub const ORION_ERR_BUSY: i32 = -5;
/// No session, or a frame too long for the link.
pub const ORION_ERR_LINK: i32 = -6;

#[cfg(feature = "ffi-can")]
type Device = orion_device_core::CanDevice<RX, TX>;
#[cfg(not(feature = "ffi-can"))]
type Device = orion_device_core::StreamDevice<RX, TX>;

#[cfg(feature = "ffi-slots")]
type Firmware = orion_device_firmware::Firmware<slots::Hooks>;

/// What the firmware declares: written before the first `orion_poll`, only read after.
struct Tables {
    name: &'static str,
    topics: [TopicSpec; ORION_MAX_TOPICS],
    fields: [[Field; 1]; ORION_MAX_TOPICS],
    topic_count: usize,
    actions: [ActionSpec; ORION_MAX_ACTIONS],
    action_count: usize,
    #[cfg(feature = "ffi-slots")]
    prefix: &'static str,
}

const NO_FIELD: [Field; 1] = [Field::new("", FieldType::Float, "")];
const NO_TOPIC: TopicSpec = TopicSpec::latest("", &[]);
const NO_ACTION: ActionSpec = ActionSpec::on_resource("", "", "");

/// A run handed over and not yet taken.
#[derive(Clone, Copy)]
struct Waiting {
    action: usize,
    call: u64,
    len: usize,
}

/// The session and the layers, once the tables are fixed.
struct Running {
    device: Device,
    layers: Layers,
}

/// What runs above the session.
struct Layers {
    topics: Topics<ORION_MAX_TOPICS>,
    actions: Actions<ORION_MAX_ACTIONS>,
    run: Option<Waiting>,
    args: [u8; RX],
    cancelled: u64,
    events: u32,
    #[cfg(feature = "ffi-slots")]
    firmware: Option<Firmware>,
}

/// Nothing yet.
const NOTHING: u8 = 0;
/// `orion_init` was called: tables may be added to.
const DECLARING: u8 = 1;
/// The first `orion_poll` fixed the tables and started the session.
const RUNNING: u8 = 2;

/// The one device. Only atomic loads and stores guard it, so it builds for cores without
/// compare-and-swap (Cortex-M0+, RV32 without `A`). That is a guard against a call from an
/// interrupt, not a lock: the API is for one main loop.
struct Port {
    tables: UnsafeCell<Tables>,
    running: UnsafeCell<MaybeUninit<Running>>,
    phase: AtomicU8,
    busy: AtomicBool,
}

// SAFETY: every access goes through `Port::with`, which the busy flag keeps to one at a time,
// under the one-main-loop contract above.
unsafe impl Sync for Port {}

static PORT: Port = Port {
    tables: UnsafeCell::new(Tables {
        name: "",
        topics: [NO_TOPIC; ORION_MAX_TOPICS],
        fields: [NO_FIELD; ORION_MAX_TOPICS],
        topic_count: 0,
        actions: [NO_ACTION; ORION_MAX_ACTIONS],
        action_count: 0,
        #[cfg(feature = "ffi-slots")]
        prefix: "",
    }),
    // Zeroed, in `.bss`: a whole session in `.data` would cost its size in flash too.
    running: UnsafeCell::new(MaybeUninit::uninit()),
    phase: AtomicU8::new(NOTHING),
    busy: AtomicBool::new(false),
};

impl Port {
    /// Runs `f` alone, or returns `None` if another call is under way.
    fn alone<R>(&self, f: impl FnOnce() -> R) -> Option<R> {
        if self.busy.load(Ordering::Relaxed) {
            return None;
        }
        self.busy.store(true, Ordering::Relaxed);
        compiler_fence(Ordering::Acquire);
        let result = f();
        compiler_fence(Ordering::Release);
        self.busy.store(false, Ordering::Relaxed);
        Some(result)
    }

    /// The tables, to add to: only while declaring.
    fn declaring<R>(&self, f: impl FnOnce(&mut Tables) -> R) -> Option<R> {
        self.alone(|| {
            (self.phase.load(Ordering::Relaxed) == DECLARING).then(|| {
                // SAFETY: alone, and nothing borrows the tables while declaring.
                f(unsafe { &mut *self.tables.get() })
            })
        })
        .flatten()
    }

    /// The session, once running: fixes the tables and starts it on the first call.
    fn running<R>(&self, f: impl FnOnce(&mut Running) -> R) -> Option<R> {
        self.alone(|| {
            match self.phase.load(Ordering::Relaxed) {
                DECLARING => {
                    // SAFETY: alone. From here on the tables are only read, and the
                    // references made here into them are the only ones there are.
                    let tables: &'static Tables = unsafe { &*self.tables.get() };
                    let running = Running {
                        device: device(tables.name),
                        layers: Layers {
                            topics: Topics::from_slice(
                                tables.topics.get(..tables.topic_count).unwrap_or_default(),
                            ),
                            actions: Actions::from_slice(
                                tables
                                    .actions
                                    .get(..tables.action_count)
                                    .unwrap_or_default(),
                            ),
                            run: None,
                            args: [0; RX],
                            cancelled: 0,
                            events: 0,
                            #[cfg(feature = "ffi-slots")]
                            firmware: (!tables.prefix.is_empty()).then(|| {
                                orion_device_firmware::Firmware::new(tables.prefix, slots::Hooks)
                            }),
                        },
                    };
                    // SAFETY: alone; the slot holds nothing that needs dropping.
                    unsafe { (*self.running.get()).write(running) };
                    self.phase.store(RUNNING, Ordering::Relaxed);
                }
                RUNNING => {}
                _ => return None,
            }
            // SAFETY: alone, and written above or by an earlier call.
            Some(f(unsafe { (*self.running.get()).assume_init_mut() }))
        })
        .flatten()
    }
}

#[cfg(feature = "ffi-can")]
fn device(name: &'static str) -> Device {
    Device::new(
        DeviceConfig::new(name),
        orion_device_core::SegmentMtu::CLASSIC,
    )
}

#[cfg(not(feature = "ffi-can"))]
fn device(name: &'static str) -> Device {
    Device::new(DeviceConfig::new(name))
}

/// A table's index as C is given it. The tables are far smaller than `i32`.
fn number(at: usize) -> i32 {
    i32::try_from(at).unwrap_or(ORION_ERR_FULL)
}

/// A string the caller keeps for as long as the firmware runs. `None` if it is not UTF-8 or
/// longer than `max`; `Some("")` for a null pointer.
///
/// # Safety
///
/// `text` is null or a NUL-terminated string that is never changed or freed.
unsafe fn kept(text: *const c_char, max: usize) -> Option<&'static str> {
    if text.is_null() {
        return Some("");
    }
    // SAFETY: the caller's promise.
    let text = unsafe { CStr::from_ptr(text) }.to_str().ok()?;
    (text.len() <= max).then_some(text)
}

/// A string for the length of the call. `""` for a null pointer or one that is not UTF-8.
///
/// # Safety
///
/// `text` is null or a NUL-terminated string.
unsafe fn borrowed<'a>(text: *const c_char) -> &'a str {
    if text.is_null() {
        return "";
    }
    // SAFETY: the caller's promise.
    unsafe { CStr::from_ptr(text) }.to_str().unwrap_or_default()
}

/// # Safety
///
/// `data` is null, or valid for `len` bytes.
unsafe fn bytes<'a>(data: *const u8, len: usize) -> &'a [u8] {
    if data.is_null() || len == 0 {
        &[]
    } else {
        // SAFETY: the caller's promise.
        unsafe { core::slice::from_raw_parts(data, len) }
    }
}

/// Starts over as the device `name`, with no topics and no actions. Returns false if the name
/// is empty, longer than 32 bytes, or not UTF-8.
///
/// # Safety
///
/// `name` is a NUL-terminated string that stays as it is for as long as the firmware runs.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn orion_init(name: *const c_char) -> bool {
    // SAFETY: the caller's promise.
    let Some(name) = (unsafe { kept(name, NAME_MAX) }).filter(|name| !name.is_empty()) else {
        return false;
    };
    PORT.alone(|| {
        // SAFETY: alone. A running session is left behind: nothing in it needs dropping,
        // and nothing refers to the tables once the phase says so.
        let tables = unsafe { &mut *PORT.tables.get() };
        PORT.phase.store(DECLARING, Ordering::Relaxed);
        tables.name = name;
        tables.topic_count = 0;
        tables.action_count = 0;
        #[cfg(feature = "ffi-slots")]
        {
            tables.prefix = "";
        }
    })
    .is_some()
}

/// Adds a topic of one field, `type` one of `ORION_FIELD_*`, `unit` such as "deg" or null.
/// Returns its number, which `orion_publish` takes, or `ORION_ERR_*`.
///
/// # Safety
///
/// `name` and `unit` are null or NUL-terminated strings that stay as they are for as long as
/// the firmware runs.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn orion_topic(name: *const c_char, r#type: u8, unit: *const c_char) -> i32 {
    // SAFETY: the caller's promise.
    let (Some(name), Some(unit)) = (unsafe { kept(name, 63) }, unsafe { kept(unit, 63) }) else {
        return ORION_ERR_ARG;
    };
    let ty = match r#type {
        ORION_FIELD_FLOAT => FieldType::Float,
        ORION_FIELD_INT => FieldType::Int,
        ORION_FIELD_BOOL => FieldType::Bool,
        _ => return ORION_ERR_ARG,
    };
    if name.is_empty() {
        return ORION_ERR_ARG;
    }
    PORT.declaring(|tables| {
        let at = tables.topic_count;
        let Some(field) = tables.fields.get_mut(at) else {
            return ORION_ERR_FULL;
        };
        *field = [Field::new(name, ty, unit)];
        // SAFETY: the field lives in a static and, once the tables are fixed, is never
        // written again.
        let field: &'static [Field; 1] = unsafe { &*core::ptr::from_ref(field) };
        if let Some(topic) = tables.topics.get_mut(at) {
            *topic = TopicSpec::latest(name, field);
        }
        tables.topic_count += 1;
        number(at)
    })
    .unwrap_or(ORION_ERR_STATE)
}

/// Adds an action on the device itself, run by whoever `admits` names (`ORION_ADMITS_*`).
/// Returns its number, which `orion_run` gives back, or `ORION_ERR_*`.
///
/// # Safety
///
/// `name` and `description` are null or NUL-terminated strings that stay as they are for as
/// long as the firmware runs.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn orion_action(
    name: *const c_char,
    description: *const c_char,
    admits: u8,
) -> i32 {
    // SAFETY: the caller's promise.
    let (Some(name), Some(description)) =
        (unsafe { kept(name, 63) }, unsafe { kept(description, 200) })
    else {
        return ORION_ERR_ARG;
    };
    let mut who = Admits::NOBODY;
    for (bit, class) in [
        (ORION_ADMITS_LOCAL, Admits::LOCAL),
        (ORION_ADMITS_OPERATOR, Admits::OPERATOR),
        (ORION_ADMITS_PEER, Admits::PEER),
    ] {
        if admits & bit != 0 {
            who = who.and(class);
        }
    }
    if name.is_empty() || who == Admits::NOBODY {
        return ORION_ERR_ARG;
    }
    PORT.declaring(|tables| {
        let at = tables.action_count;
        let device = tables.name;
        let Some(action) = tables.actions.get_mut(at) else {
            return ORION_ERR_FULL;
        };
        *action = ActionSpec {
            name,
            target: Target::Resource(device),
            policy: Policy::new(who),
            description,
        };
        tables.action_count += 1;
        number(at)
    })
    .unwrap_or(ORION_ERR_STATE)
}

/// Moves the session on to `now_ms`, a counter in milliseconds that may wrap, and returns
/// the `ORION_EVENT_*` bits of what happened since the last call. The first call fixes the
/// tables.
#[unsafe(no_mangle)]
pub extern "C" fn orion_poll(now_ms: u32) -> u32 {
    PORT.running(|Running { device, layers }| {
        let link = &mut device.link;
        link.poll(now_ms);
        let mut events = core::mem::take(&mut layers.events);
        while let Some(event) = link.next_event() {
            events |= match event {
                Event::Connected { .. } => ORION_EVENT_CONNECTED,
                Event::Disconnected => ORION_EVENT_DISCONNECTED,
                Event::Rejected(_) => ORION_EVENT_REJECTED,
            };
        }
        layers.topics.poll(link, now_ms);
        layers.actions.poll(link, now_ms);
        #[cfg(feature = "ffi-slots")]
        if let Some(firmware) = &mut layers.firmware {
            firmware.poll(link, now_ms);
        }
        events
    })
    .unwrap_or(0)
}

/// Where the session is: 0 saying hello, 1 turned away for now, 2 in a session.
#[unsafe(no_mangle)]
pub extern "C" fn orion_state() -> u32 {
    PORT.running(|running| match running.device.link.state() {
        Session::Connecting => 0,
        Session::Rejected => 1,
        Session::Connected => 2,
    })
    .unwrap_or(0)
}

/// Hands a frame that arrived to the layers.
fn heard(
    running: &mut Layers,
    link: &mut orion_device_core::Link<TX>,
    frame: &orion_device_core::FrameView<'_>,
) {
    use orion_device_topics::Heard as Topic;
    if let Topic::Demand { .. } = running.topics.handle(frame) {
        running.events |= ORION_EVENT_DEMAND;
    }
    match running.actions.handle(frame) {
        Run::Run {
            action, call, args, ..
        } => {
            if running.run.is_some() || args.len() > running.args.len() {
                // One run at a time: the host is told now rather than at the deadline.
                let _ = running
                    .actions
                    .finish(link, call, Outcome::Rejected, &[], "busy");
            } else {
                // A copy that cannot panic: a length check would bring formatting code in.
                for (to, from) in running.args.iter_mut().zip(args) {
                    *to = *from;
                }
                running.run = Some(Waiting {
                    action,
                    call,
                    len: args.len(),
                });
                running.events |= ORION_EVENT_RUN;
            }
        }
        Run::Cancel { call, .. } => {
            if running.run.is_some_and(|run| run.call == call) {
                running.run = None;
            }
            running.cancelled = call;
            running.events |= ORION_EVENT_CANCEL;
        }
        _ => {}
    }
    #[cfg(feature = "ffi-slots")]
    if let Some(firmware) = &mut running.firmware
        && let orion_device_firmware::Heard::Committed { .. } = firmware.handle(frame)
    {
        running.events |= ORION_EVENT_FIRMWARE;
    }
}

/// Hands over `len` bytes that arrived on the UART.
///
/// # Safety
///
/// `data` is valid for `len` bytes.
#[cfg(not(feature = "ffi-can"))]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn orion_rx(data: *const u8, len: usize) {
    // SAFETY: the caller's promise.
    let data = unsafe { bytes(data, len) };
    let _ = PORT.running(|Running { device, layers }| {
        device.receive(data, |link, frame| heard(layers, link, frame));
    });
}

/// Writes up to `cap` bytes for the UART into `out` and says how many. Call again until it
/// says 0.
///
/// # Safety
///
/// `out` is valid for writing `cap` bytes.
#[cfg(not(feature = "ffi-can"))]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn orion_tx(out: *mut u8, cap: usize) -> usize {
    if out.is_null() || cap == 0 {
        return 0;
    }
    // SAFETY: the caller's promise.
    let out = unsafe { core::slice::from_raw_parts_mut(out, cap) };
    PORT.running(|running| running.device.transmit(out))
        .unwrap_or(0)
}

/// Hands over the data of one CAN frame sent to this device.
///
/// # Safety
///
/// `data` is valid for `len` bytes.
#[cfg(feature = "ffi-can")]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn orion_can_rx(data: *const u8, len: usize) {
    // SAFETY: the caller's promise.
    let data = unsafe { bytes(data, len) };
    let _ = PORT.running(|Running { device, layers }| {
        device.receive_segment(data, |link, frame| heard(layers, link, frame));
    });
}

/// Writes the data of the next CAN frame to send into `out` and says how long it is: 0 when
/// there is nothing. Send it with the device's identifier.
///
/// # Safety
///
/// `out` is valid for writing 64 bytes.
#[cfg(feature = "ffi-can")]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn orion_can_tx(out: *mut u8) -> usize {
    if out.is_null() {
        return 0;
    }
    // SAFETY: the caller's promise.
    let out = unsafe { core::slice::from_raw_parts_mut(out, 64) };
    PORT.running(|running| running.device.next_segment(out).unwrap_or(0))
        .unwrap_or(0)
}

fn published(result: Result<u16, PublishError>) -> i32 {
    match result {
        Ok(_) => 0,
        Err(PublishError::NoSuchTopic | PublishError::NotOpen) => ORION_ERR_CLOSED,
        Err(PublishError::Link(orion_device_core::SendError::Busy)) => ORION_ERR_BUSY,
        Err(PublishError::Link(_)) => ORION_ERR_LINK,
    }
}

/// Publishes `value` on a float topic at `time_ns`. 0, or `ORION_ERR_*`: a sample the link
/// has no room for is not queued, so the next one is the newest.
#[unsafe(no_mangle)]
pub extern "C" fn orion_publish(topic: u32, time_ns: u64, value: f64) -> i32 {
    PORT.running(|running| {
        published(running.layers.topics.publish_f64(
            &mut running.device.link,
            topic as usize,
            0,
            time_ns,
            value,
        ))
    })
    .unwrap_or(ORION_ERR_STATE)
}

/// Publishes `value` on an int topic. As `orion_publish`.
#[unsafe(no_mangle)]
pub extern "C" fn orion_publish_int(topic: u32, time_ns: u64, value: i64) -> i32 {
    PORT.running(|running| {
        published(running.layers.topics.publish_i64(
            &mut running.device.link,
            topic as usize,
            0,
            time_ns,
            value,
        ))
    })
    .unwrap_or(ORION_ERR_STATE)
}

/// Publishes `value` on a bool topic. As `orion_publish`.
#[unsafe(no_mangle)]
pub extern "C" fn orion_publish_bool(topic: u32, time_ns: u64, value: bool) -> i32 {
    PORT.running(|running| {
        published(running.layers.topics.publish_bool(
            &mut running.device.link,
            topic as usize,
            0,
            time_ns,
            value,
        ))
    })
    .unwrap_or(ORION_ERR_STATE)
}

/// How many listen to `topic`. A device can stop measuring what nobody reads.
#[unsafe(no_mangle)]
pub extern "C" fn orion_subscribers(topic: u32) -> u32 {
    PORT.running(|running| running.layers.topics.subscribers(topic as usize))
        .unwrap_or(0)
}

/// Takes the run that is waiting: its number into `call`, up to `cap` bytes of its arguments
/// into `args` and their whole length into `len`. Returns the action's number, or -1 if
/// none waits. Answer it with `orion_finish`.
///
/// # Safety
///
/// `call` and `len` are valid for writing; `args` is null or valid for writing `cap` bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn orion_run(
    call: *mut u64,
    args: *mut u8,
    cap: usize,
    len: *mut usize,
) -> i32 {
    if call.is_null() || len.is_null() {
        return ORION_ERR_ARG;
    }
    PORT.running(|running| {
        let Some(run) = running.layers.run.take() else {
            return -1;
        };
        let held = running.layers.args.get(..run.len).unwrap_or_default();
        // SAFETY: the caller's promise.
        unsafe {
            *call = run.call;
            *len = run.len;
            if !args.is_null() {
                let n = held.len().min(cap);
                core::ptr::copy_nonoverlapping(held.as_ptr(), args, n);
            }
        }
        number(run.action)
    })
    .unwrap_or(ORION_ERR_STATE)
}

/// The run the host asked to stop, or 0. The host ends it as cancelled; a result sent after
/// is dropped.
#[unsafe(no_mangle)]
pub extern "C" fn orion_cancelled() -> u64 {
    PORT.running(|running| core::mem::take(&mut running.layers.cancelled))
        .unwrap_or(0)
}

fn sent(result: Result<u16, orion_device_core::SendError>) -> i32 {
    match result {
        Ok(_) => 0,
        Err(orion_device_core::SendError::Busy) => ORION_ERR_BUSY,
        Err(_) => ORION_ERR_LINK,
    }
}

/// Says how run `call` is doing: `percent` done (255 for unknown) and what it is doing.
/// 0, or `ORION_ERR_*`.
///
/// # Safety
///
/// `message` is null or a NUL-terminated string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn orion_progress(call: u64, percent: u8, message: *const c_char) -> i32 {
    // SAFETY: the caller's promise.
    let message = unsafe { borrowed(message) };
    PORT.running(|running| {
        sent(
            running
                .layers
                .actions
                .progress(&mut running.device.link, call, percent, message),
        )
    })
    .unwrap_or(ORION_ERR_STATE)
}

/// Ends run `call` with `outcome` (`ORION_OUTCOME_*`), what it returned, and why if it did not
/// succeed. 0, or `ORION_ERR_*`: on `ORION_ERR_BUSY` nothing was sent, so call again.
///
/// # Safety
///
/// `output` is null or valid for `len` bytes; `detail` is null or a NUL-terminated string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn orion_finish(
    call: u64,
    outcome: u8,
    output: *const u8,
    len: usize,
    detail: *const c_char,
) -> i32 {
    let outcome = match outcome {
        ORION_OUTCOME_SUCCEEDED => Outcome::Succeeded,
        ORION_OUTCOME_FAILED => Outcome::Failed,
        ORION_OUTCOME_REJECTED => Outcome::Rejected,
        _ => return ORION_ERR_ARG,
    };
    // SAFETY: the caller's promises.
    let (output, detail) = unsafe { (bytes(output, len), borrowed(detail)) };
    PORT.running(|running| {
        sent(
            running
                .layers
                .actions
                .finish(&mut running.device.link, call, outcome, output, detail),
        )
    })
    .unwrap_or(ORION_ERR_STATE)
}

/// This library's API version, [`ORION_MCU_API_VERSION`] as it was built. Compare the major
/// with the header's, and that the minor is at least the header's, before anything else.
#[unsafe(no_mangle)]
pub extern "C" fn orion_api_version() -> u32 {
    ORION_MCU_API_VERSION
}

/// The revision of the wire this device says it speaks in hello.
#[unsafe(no_mangle)]
pub extern "C" fn orion_interface_revision() -> u16 {
    orion_device_core::INTERFACE_REVISION
}

#[cfg(feature = "ffi-slots")]
mod slots;
#[cfg(feature = "ffi-slots")]
pub use slots::*;
