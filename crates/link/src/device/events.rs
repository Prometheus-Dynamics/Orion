//! Fixed-capacity event queue for the device session.

use crate::wire::RejectReason;

/// Events queued between [`super::DeviceSession::next_event`] calls.
pub const EVENT_CAPACITY: usize = 4;

/// Something the application should react to. Events are small `Copy` values; data that comes
/// with them (the lease set, the node id) is read from the session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum DeviceEvent {
    /// The host accepted the device (see [`super::DeviceSession::node_id`]). The latest
    /// published snapshot (if any) is being sent.
    Connected {
        /// The new session.
        session_id: u32,
    },
    /// The full lease set for this device's provider changed (also delivered once after every
    /// connect); read it with [`super::DeviceSession::leases`]. Repeated identical sets are not
    /// reported again.
    LeasesChanged,
    /// The host acknowledged the latest published snapshot.
    StateAcked,
    /// The session ended (host silent for too long, host restarted, or rejected). The device is
    /// reconnecting; the lease set is cleared.
    Disconnected,
    /// The host refused the device; it retries after a backoff.
    Rejected(RejectReason),
    /// The latest snapshot no longer fits the negotiated frame size and was dropped. Publish a
    /// smaller one.
    StateTooLarge {
        /// Encoded frame length of the snapshot.
        frame_len: usize,
        /// Negotiated maximum frame length.
        max_frame: usize,
    },
}

// Queued events are stored as plain integers (a tag and two words each). Integer arrays let a
// whole session be zero-filled in place; an array of the (padded) enum is built on the stack and
// copied instead, which links `memcpy` into firmware that otherwise needs none.
const CONNECTED: u8 = 1;
const LEASES_CHANGED: u8 = 2;
const STATE_ACKED: u8 = 3;
const DISCONNECTED: u8 = 4;
const REJECTED: u8 = 5;
const STATE_TOO_LARGE: u8 = 6;

fn pack(event: DeviceEvent) -> (u8, usize, usize) {
    match event {
        DeviceEvent::Connected { session_id } => (CONNECTED, session_id as usize, 0),
        DeviceEvent::LeasesChanged => (LEASES_CHANGED, 0, 0),
        DeviceEvent::StateAcked => (STATE_ACKED, 0, 0),
        DeviceEvent::Disconnected => (DISCONNECTED, 0, 0),
        DeviceEvent::Rejected(reason) => (REJECTED, usize::from(reason.code()), 0),
        DeviceEvent::StateTooLarge {
            frame_len,
            max_frame,
        } => (STATE_TOO_LARGE, frame_len, max_frame),
    }
}

fn unpack(tag: u8, a: usize, b: usize) -> Option<DeviceEvent> {
    Some(match tag {
        CONNECTED => DeviceEvent::Connected {
            session_id: a as u32,
        },
        LEASES_CHANGED => DeviceEvent::LeasesChanged,
        STATE_ACKED => DeviceEvent::StateAcked,
        DISCONNECTED => DeviceEvent::Disconnected,
        REJECTED => DeviceEvent::Rejected(RejectReason::from_code(a as u8)),
        STATE_TOO_LARGE => DeviceEvent::StateTooLarge {
            frame_len: a,
            max_frame: b,
        },
        _ => return None,
    })
}

/// Ring buffer of [`EVENT_CAPACITY`] events. When full, the oldest event is dropped. Repeated
/// lease changes and acks queued back to back collapse into one.
#[derive(Debug)]
pub(crate) struct EventQueue {
    tags: [u8; EVENT_CAPACITY],
    a: [usize; EVENT_CAPACITY],
    b: [usize; EVENT_CAPACITY],
    head: usize,
    len: usize,
}

impl EventQueue {
    #[inline(always)]
    pub(crate) const fn new() -> Self {
        Self {
            tags: [0; EVENT_CAPACITY],
            a: [0; EVENT_CAPACITY],
            b: [0; EVENT_CAPACITY],
            head: 0,
            len: 0,
        }
    }

    fn index(&self, offset: usize) -> usize {
        self.head.wrapping_add(offset) % EVENT_CAPACITY
    }

    /// Queues `event`; returns `false` if an older event had to be dropped.
    pub(crate) fn push(&mut self, event: DeviceEvent) -> bool {
        let (tag, a, b) = pack(event);
        if let Some(last_offset) = self.len.checked_sub(1)
            && self.tags.get(self.index(last_offset)) == Some(&tag)
            && matches!(tag, LEASES_CHANGED | STATE_ACKED)
        {
            return true;
        }
        let mut kept = true;
        if self.len == EVENT_CAPACITY {
            let _ = self.pop();
            kept = false;
        }
        let tail = self.index(self.len);
        if let (Some(t), Some(x), Some(y)) = (
            self.tags.get_mut(tail),
            self.a.get_mut(tail),
            self.b.get_mut(tail),
        ) {
            (*t, *x, *y) = (tag, a, b);
            self.len += 1;
        }
        kept
    }

    pub(crate) fn pop(&mut self) -> Option<DeviceEvent> {
        if self.len == 0 {
            return None;
        }
        let head = self.head;
        self.head = self.index(1);
        self.len -= 1;
        unpack(
            *self.tags.get(head)?,
            *self.a.get(head)?,
            *self.b.get(head)?,
        )
    }
}
