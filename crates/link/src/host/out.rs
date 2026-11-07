//! The host's output queue: frames back to back in one byte ring, so queuing and sending a frame
//! allocates nothing once the ring has grown to the session's peak, and each frame costs only its
//! own bytes.

use std::collections::VecDeque;
use std::vec::Vec;

#[derive(Debug, Default)]
pub(crate) struct OutQueue {
    bytes: VecDeque<u8>,
    lens: VecDeque<usize>,
}

/// A position to roll back to: frames queued after it are removed by [`OutQueue::rollback`].
#[derive(Clone, Copy)]
pub(crate) struct Mark {
    bytes: usize,
    frames: usize,
}

impl OutQueue {
    /// Frames queued.
    pub(crate) fn len(&self) -> usize {
        self.lens.len()
    }

    pub(crate) fn push_back(&mut self, frame: &[u8]) {
        self.bytes.extend(frame);
        self.lens.push_back(frame.len());
    }

    /// Moves the oldest frame into `out` (replacing its contents). `false` when empty.
    pub(crate) fn pop_into(&mut self, out: &mut Vec<u8>) -> bool {
        let Some(len) = self.lens.pop_front() else {
            return false;
        };
        out.clear();
        out.extend(self.bytes.drain(..len));
        true
    }

    pub(crate) fn clear(&mut self) {
        self.bytes.clear();
        self.lens.clear();
    }

    pub(crate) fn mark(&self) -> Mark {
        Mark {
            bytes: self.bytes.len(),
            frames: self.lens.len(),
        }
    }

    pub(crate) fn rollback(&mut self, mark: Mark) {
        self.bytes.truncate(mark.bytes);
        self.lens.truncate(mark.frames);
    }
}

#[cfg(test)]
impl OutQueue {
    pub(crate) fn mark_bytes(&self) -> usize {
        self.bytes.len()
    }
}
