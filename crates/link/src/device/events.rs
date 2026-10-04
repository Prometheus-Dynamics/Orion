//! Fixed-capacity event queue for the device session.

use alloc::vec::Vec;

use crate::message::{LeaseRecord, NodeId, RejectReason};

/// Events queued between [`super::DeviceSession::next_event`] calls.
pub const EVENT_CAPACITY: usize = 4;

/// Something the application should react to.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum DeviceEvent {
    /// The host accepted the device. The latest published snapshot (if any) is being sent.
    Connected {
        /// The node the device is attached to.
        node_id: NodeId,
        /// The new session.
        session_id: u32,
    },
    /// The full lease set for this device's provider changed (also delivered once after every
    /// connect). Repeated identical sets are not reported again.
    Leases(Vec<LeaseRecord>),
    /// The host acknowledged the latest published snapshot.
    StateAcked,
    /// The session ended (host silent for too long, host restarted, or rejected). The device is
    /// reconnecting; leases from the old session are stale.
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

/// Ring buffer of [`EVENT_CAPACITY`] events. When full, the oldest event is dropped. A lease set
/// replaces a lease set queued directly before it, and repeated acks collapse.
#[derive(Debug)]
pub(crate) struct EventQueue {
    slots: [Option<DeviceEvent>; EVENT_CAPACITY],
    head: usize,
    len: usize,
}

impl EventQueue {
    pub(crate) const fn new() -> Self {
        Self {
            slots: [None, None, None, None],
            head: 0,
            len: 0,
        }
    }

    fn index(&self, offset: usize) -> usize {
        self.head.wrapping_add(offset) % EVENT_CAPACITY
    }

    /// Queues `event`; returns `false` if an older event had to be dropped.
    pub(crate) fn push(&mut self, event: DeviceEvent) -> bool {
        if let Some(last_offset) = self.len.checked_sub(1) {
            let last = self.index(last_offset);
            if let Some(slot) = self.slots.get_mut(last) {
                let collapse = matches!(
                    (&*slot, &event),
                    (Some(DeviceEvent::Leases(_)), DeviceEvent::Leases(_))
                        | (Some(DeviceEvent::StateAcked), DeviceEvent::StateAcked)
                );
                if collapse {
                    *slot = Some(event);
                    return true;
                }
            }
        }
        let mut kept = true;
        if self.len == EVENT_CAPACITY {
            let _ = self.pop();
            kept = false;
        }
        let tail = self.index(self.len);
        if let Some(slot) = self.slots.get_mut(tail) {
            *slot = Some(event);
            self.len += 1;
        }
        kept
    }

    pub(crate) fn pop(&mut self) -> Option<DeviceEvent> {
        if self.len == 0 {
            return None;
        }
        let event = self.slots.get_mut(self.head).and_then(Option::take);
        self.head = self.index(1);
        self.len -= 1;
        event
    }
}
