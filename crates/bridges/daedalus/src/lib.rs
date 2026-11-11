//! Orion's topics as Daedalus nodes: a plugin library `daedalusd` loads from its plugin
//! directory.
//!
//! | Node | Inputs | Outputs |
//! | --- | --- | --- |
//! | `orion.topic.read` | `topic: string` | `value: f64`, the newest sample |
//! | `orion.topic.write` | `topic: string`, `value: f64` | `published: u64`, how many so far |
//! | `orion.frame.read` | `topic: string` | `frame: bytes`, the newest frame, in place |
//!
//! A node runs each time its inputs arrive, and every input must be connected: Daedalus gives a
//! plugin's step a cell only for a port with an edge. `topic` names the topic, and for
//! `orion.topic.read` the field after a colon: `imu:1` is field 1 of `imu`, `imu` is field 0.
//! A reader with nothing new leaves its output empty. The plugin connects to the comm core on
//! the first step that needs it, at `ORION_ADDRESS` or the board's own socket, and again after
//! the connection is lost.
//!
//! A frame is not copied. Its buffer is handed to Daedalus where it is in shared memory, held
//! out of the publisher's hands, and given back when Daedalus drops the last value of it, on
//! whichever thread that is. The owner of the frame's type reads the bytes where they are and
//! turns them into its own type.

use std::cell::RefCell;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use daedalus_abi::{
    BytesC, HANDOVER_PENDING, OWNER_HANDED, Slot, SlotState, Slots, StepCx, StepEnd,
};
use daedalus_loader::export::{NodeDef, PluginDef, PortDef};
use orion::buffers::{BufferSubscriber, HoldLimits, NextFrame};
use orion::topics::{FieldId, FrameTime, Publisher, Subscriber, TopicDecl};
use orion::{Address, Orion, fields};

/// The plugin's one backend: its nodes run on the step's own thread.
const BACKEND: &str = "orion.cpu";
const SOURCE: &str = "orion-daedalus";

/// A frame handed to Daedalus: its held buffers, which go back to the publisher when this is
/// dropped. `Send`, because Daedalus drops it on whichever thread lets go last.
type Frame = Box<dyn Send>;

/// How many frames the plugin may hold at once, across everything Daedalus keeps.
const MAX_HELD: u32 = 4;

/// What one thread has: its connection and the topics it has opened. A step thread connects
/// once; a backend that moved steps between threads would have a connection on each.
struct Bridge {
    orion: Orion,
    readers: Vec<(Box<str>, Subscriber)>,
    writers: Vec<(Box<str>, Publisher, u64)>,
    frames: Vec<(Box<str>, BufferSubscriber)>,
}

thread_local! {
    static BRIDGE: RefCell<Option<Bridge>> = const { RefCell::new(None) };
}

impl Bridge {
    fn connect() -> Result<Self, String> {
        let address = match std::env::var("ORION_ADDRESS") {
            Ok(text) => text.parse::<Address>().map_err(|e| e.to_string())?,
            Err(_) => Address::default(),
        };
        let orion = Orion::connect(&address, "daedalus").map_err(|e| e.to_string())?;
        Ok(Self {
            orion,
            readers: Vec::new(),
            writers: Vec::new(),
            frames: Vec::new(),
        })
    }
}

/// Runs `step` with this thread's connection, connecting first if there is none. A step that
/// fails drops the connection, so the next one starts over.
fn with_bridge(cx: &mut StepCx, step: impl FnOnce(&mut Bridge) -> Result<(), String>) -> StepEnd {
    BRIDGE.with_borrow_mut(|slot| {
        if slot.is_none() {
            match Bridge::connect() {
                Ok(bridge) => *slot = Some(bridge),
                Err(error) => {
                    cx.fault(None, &format!("orion: {error}"));
                    return StepEnd::Fault;
                }
            }
        }
        let Some(bridge) = slot.as_mut() else {
            return StepEnd::Fault;
        };
        // What the comm core said since the last step: subscriptions answered, readers come.
        let pumped = bridge.orion.pump(Duration::ZERO).map_err(|e| e.to_string());
        match pumped.and_then(|_| step(bridge)) {
            Ok(()) => StepEnd::Done,
            Err(error) => {
                *slot = None;
                cx.fault(None, &format!("orion: {error}"));
                StepEnd::Fault
            }
        }
    })
}

/// The text in input cell `index`.
fn text(slots: &mut Slots, index: usize) -> Option<&str> {
    let cell = *slots.cell(index)?;
    if cell.state != SlotState::Full as u32 || cell.data.is_null() {
        return None;
    }
    // SAFETY: a full cell's `data` and `len` describe its bytes for as long as the step runs.
    let bytes = unsafe { std::slice::from_raw_parts(cell.data, cell.len) };
    std::str::from_utf8(bytes).ok()
}

/// The plain value of `N` bytes in input cell `index`.
fn plain<const N: usize>(slots: &mut Slots, index: usize) -> Option<[u8; N]> {
    let cell = *slots.cell(index)?;
    if cell.state != SlotState::Full as u32 || cell.data.is_null() || cell.len != N {
        return None;
    }
    // SAFETY: as in `text`.
    let bytes = unsafe { std::slice::from_raw_parts(cell.data, cell.len) };
    bytes.try_into().ok()
}

/// What an inline output's bytes are while the step runs: the runner copies them out when the
/// step returns. Each node has one plain output, so one is enough.
struct Out(std::cell::UnsafeCell<[u8; 16]>);

thread_local! {
    static OUT: Out = const { Out(std::cell::UnsafeCell::new([0; 16])) };
}

/// The owner word of a value whose bytes are in place: `daedalus-value`'s `OwnerKind::Inline`.
const OWNER_INLINE: u32 = 1;

/// Writes `value` into output cell `index` as an inline value.
fn put<const N: usize>(slots: &mut Slots, index: usize, value: [u8; N]) -> bool {
    let Some(out) = slots.cell(index) else {
        return false;
    };
    OUT.with(|buf| {
        let at = buf.0.get().cast::<u8>();
        // SAFETY: the buffer is this thread's and 16 bytes long, and `N` is at most 8. Nothing
        // else writes it until the runner has copied it after this step.
        unsafe { std::ptr::copy_nonoverlapping(value.as_ptr(), at, N) };
        out.data = at;
    });
    out.len = N;
    out.cap = N;
    out.owner = OWNER_INLINE;
    out.state = SlotState::Full as u32;
    true
}

/// A sample's payload as a number: a float, an integer, or a flag.
fn number(payload: &[u8]) -> Option<f64> {
    match payload.len() {
        8 => Some(f64::from_le_bytes(payload.try_into().ok()?)),
        1 => Some(f64::from(payload[0])),
        _ => None,
    }
}

fn now() -> FrameTime {
    FrameTime::from_nanos(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |since| since.as_nanos() as u64),
    )
}

/// The newest sample of a field of a topic, as a number.
fn read(slots: &mut Slots, cx: &mut StepCx) -> StepEnd {
    let Some(named) = text(slots, 0) else {
        cx.fault(None, "orion.topic.read: no topic");
        return StepEnd::Fault;
    };
    let (topic, field) = match named.rsplit_once(':') {
        Some((topic, field)) => match field.parse::<u16>() {
            Ok(field) => (Box::<str>::from(topic), field),
            Err(_) => {
                cx.fault(
                    None,
                    "orion.topic.read: the field after `:` is not a number",
                );
                return StepEnd::Fault;
            }
        },
        None => (Box::<str>::from(named), 0),
    };
    let mut value = None;
    let end = with_bridge(cx, |bridge| {
        let index = match bridge.readers.iter().position(|(name, _)| *name == topic) {
            Some(index) => index,
            None => {
                let subscriber = bridge.orion.subscribe(&topic).map_err(|e| e.to_string())?;
                bridge.readers.push((topic.clone(), subscriber));
                bridge.readers.len() - 1
            }
        };
        let (_, subscriber) = &mut bridge.readers[index];
        value = subscriber
            .latest(FieldId(field))
            .and_then(|sample| number(sample.payload));
        Ok(())
    });
    if let (StepEnd::Done, Some(value)) = (&end, value)
        && !put(slots, 1, value.to_ne_bytes())
    {
        return StepEnd::Fault;
    }
    end
}

/// Publishes `value` on `topic`, a topic of one float field `value` this plugin declares.
fn write(slots: &mut Slots, cx: &mut StepCx) -> StepEnd {
    let Some(topic) = text(slots, 0).map(Box::<str>::from) else {
        cx.fault(None, "orion.topic.write: no topic");
        return StepEnd::Fault;
    };
    let Some(value) = plain::<8>(slots, 1).map(f64::from_ne_bytes) else {
        // Nothing to publish this tick.
        return StepEnd::Done;
    };
    let mut published = 0;
    let end = with_bridge(cx, |bridge| {
        let index = match bridge.writers.iter().position(|(name, ..)| *name == topic) {
            Some(index) => index,
            None => {
                let publisher = bridge
                    .orion
                    .declare(&TopicDecl {
                        name: &topic,
                        fields: &fields![value: Float ""],
                        history: 64,
                        expiry_ms: 0,
                        max_payload: 8,
                    })
                    .map_err(|e| e.to_string())?;
                bridge.writers.push((topic.clone(), publisher, 0));
                bridge.writers.len() - 1
            }
        };
        let (_, publisher, count) = &mut bridge.writers[index];
        publisher
            .publish(FieldId(0), now(), &value.to_le_bytes())
            .map_err(|e| e.to_string())?;
        *count += 1;
        published = *count;
        Ok(())
    });
    if matches!(end, StepEnd::Done) && !put(slots, 2, published.to_ne_bytes()) {
        return StepEnd::Fault;
    }
    end
}

/// Called once, on any thread, when Daedalus drops the last value of a frame.
///
/// # Safety
///
/// `data` is the `Box<Frame>` that `frame` made, given here once.
unsafe extern "C" fn release_frame(data: *mut ()) {
    // SAFETY: see above.
    drop(unsafe { Box::from_raw(data.cast::<Frame>()) });
}

/// The newest frame of `topic`, handed over where it is.
fn frame(slots: &mut Slots, cx: &mut StepCx) -> StepEnd {
    let Some(topic) = text(slots, 0).map(Box::<str>::from) else {
        cx.fault(None, "orion.frame.read: no topic");
        return StepEnd::Fault;
    };
    let mut newest = None;
    let end = with_bridge(cx, |bridge| {
        let index = match bridge.frames.iter().position(|(name, _)| *name == topic) {
            Some(index) => index,
            None => {
                let limits = HoldLimits {
                    max_held: MAX_HELD,
                    ..HoldLimits::default()
                };
                let subscriber = bridge
                    .orion
                    .subscribe_buffers(&topic, &[], limits)
                    .map_err(|e| e.to_string())?;
                bridge.frames.push((topic.clone(), subscriber));
                bridge.frames.len() - 1
            }
        };
        let (_, subscriber) = &mut bridge.frames[index];
        // Only the newest frame is wanted: an older one is let go as soon as a newer is seen.
        loop {
            match subscriber.next() {
                NextFrame::Frame(frame) => newest = Some(frame.held),
                NextFrame::Skipped(_) | NextFrame::PoolChanged { .. } => {}
                NextFrame::Empty | NextFrame::Limited => break,
                NextFrame::CutOff => return Err(format!("{topic}: cut off by the publisher")),
            }
        }
        Ok(())
    });
    let Some(held) = newest else {
        return end;
    };
    let Some((ptr, len)) = held.bytes(0).map(|bytes| (bytes.as_ptr(), bytes.len())) else {
        return end;
    };
    let Some(out) = slots.cell(1) else {
        return StepEnd::Fault;
    };
    let held: Frame = Box::new(held);
    let data = Box::into_raw(Box::new(held)).cast::<()>();
    cx.handover[0] = BytesC {
        ptr,
        len,
        data,
        release: Some(release_frame),
    };
    *out = Slot {
        state: SlotState::Full as u32,
        owner: OWNER_HANDED,
        reserved: HANDOVER_PENDING,
        data: ptr.cast_mut(),
        len,
        ..Slot::EMPTY
    };
    end
}

const fn port(name: &'static str, key: &'static str) -> PortDef {
    PortDef {
        name,
        key,
        optional: false,
    }
}

static READ_IN: [PortDef; 1] = [port("topic", "string")];
static READ_OUT: [PortDef; 1] = [port("value", "f64")];
static WRITE_IN: [PortDef; 2] = [port("topic", "string"), port("value", "f64")];
static WRITE_OUT: [PortDef; 1] = [port("published", "u64")];
static FRAME_IN: [PortDef; 1] = [port("topic", "string")];
static FRAME_OUT: [PortDef; 1] = [port("frame", "bytes")];

/// The plugin's nodes, in the order of the table above. Public for the tests that run them
/// without a host.
pub static NODES: [NodeDef; 3] = [
    NodeDef {
        id: "orion.topic.read",
        source: SOURCE,
        backend: BACKEND,
        text: "read",
        inputs: &READ_IN,
        outputs: &READ_OUT,
        run: read,
    },
    NodeDef {
        id: "orion.topic.write",
        source: SOURCE,
        backend: BACKEND,
        text: "write",
        inputs: &WRITE_IN,
        outputs: &WRITE_OUT,
        run: write,
    },
    NodeDef {
        id: "orion.frame.read",
        source: SOURCE,
        backend: BACKEND,
        text: "frame",
        inputs: &FRAME_IN,
        outputs: &FRAME_OUT,
        run: frame,
    },
];

daedalus_loader::export_plugin!(PluginDef {
    name: SOURCE,
    backends: &[BACKEND],
    nodes: &NODES,
    codecs: &[],
    produce: None,
});
