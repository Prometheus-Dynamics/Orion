//! What waits to be sent to one session. Bounded, and reused: it stops allocating once it has
//! grown to what the session needs.

use std::collections::VecDeque;
use std::os::fd::OwnedFd;

/// Frames queued for one session, oldest first.
#[derive(Debug, Default)]
pub(crate) struct Outbox {
    bytes: Vec<u8>,
    /// Where the oldest unsent frame starts in `bytes`.
    head: usize,
    /// For each queued frame: its length and how many descriptors go with it.
    frames: VecDeque<(u32, u8)>,
    fds: VecDeque<OwnedFd>,
}

impl Outbox {
    /// Bytes waiting.
    pub(crate) fn pending(&self) -> usize {
        self.bytes.len() - self.head
    }

    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.frames.is_empty()
    }

    /// Queues `frame`, unless that would put more than `limit` bytes in the outbox.
    pub(crate) fn push(
        &mut self,
        frame: &[u8],
        fds: impl IntoIterator<Item = OwnedFd>,
        limit: usize,
    ) -> bool {
        if self.pending() + frame.len() > limit {
            return false;
        }
        if self.head > 0 && self.head == self.bytes.len() {
            self.bytes.clear();
            self.head = 0;
        } else if self.head >= limit {
            // Sent bytes are taking more room than the outbox is allowed: move the rest down.
            self.bytes.drain(..self.head);
            self.head = 0;
        }
        self.bytes.extend_from_slice(frame);
        let before = self.fds.len();
        self.fds.extend(fds);
        self.frames
            .push_back((frame.len() as u32, (self.fds.len() - before) as u8));
        true
    }

    /// Moves the oldest frame into `frame` and its descriptors into `fds`. `false` if there is
    /// none.
    pub(crate) fn pop(&mut self, frame: &mut Vec<u8>, fds: &mut Vec<OwnedFd>) -> bool {
        let Some((len, count)) = self.frames.pop_front() else {
            return false;
        };
        let end = self.head + len as usize;
        frame.clear();
        frame.extend_from_slice(&self.bytes[self.head..end]);
        self.head = end;
        fds.clear();
        fds.extend(self.fds.drain(..usize::from(count)));
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frames_come_out_in_order_and_the_bound_holds() {
        let mut outbox = Outbox::default();
        assert!(outbox.push(&[1; 10], [], 32));
        assert!(outbox.push(&[2; 20], [], 32));
        assert!(!outbox.push(&[3; 3], [], 32), "33 bytes would wait");
        assert_eq!(outbox.pending(), 30);
        let (mut frame, mut fds) = (Vec::new(), Vec::new());
        assert!(outbox.pop(&mut frame, &mut fds));
        assert_eq!(frame, [1; 10]);
        assert!(outbox.push(&[3; 3], [], 32));
        assert!(outbox.pop(&mut frame, &mut fds));
        assert_eq!(frame, [2; 20]);
        assert!(outbox.pop(&mut frame, &mut fds));
        assert_eq!(frame, [3; 3]);
        assert!(!outbox.pop(&mut frame, &mut fds) && outbox.is_empty());
    }

    #[test]
    fn the_buffer_does_not_grow_without_bound_under_a_steady_flow() {
        let mut outbox = Outbox::default();
        let (mut frame, mut fds) = (Vec::new(), Vec::new());
        // Never empty, so the buffer is never simply reset.
        assert!(outbox.push(&[0; 100], [], 1000));
        for round in 0..10_000_u32 {
            assert!(outbox.push(&[round as u8; 100], [], 1000));
            assert!(outbox.pop(&mut frame, &mut fds));
        }
        assert!(
            outbox.bytes.capacity() <= 4096,
            "{}",
            outbox.bytes.capacity()
        );
    }

    #[test]
    fn descriptors_stay_with_their_frame() {
        let file = || OwnedFd::from(std::fs::File::open("/proc/self/exe").expect("file"));
        let mut outbox = Outbox::default();
        assert!(outbox.push(b"none", [], 100));
        assert!(outbox.push(b"two", [file(), file()], 100));
        assert!(outbox.push(b"one", [file()], 100));
        let (mut frame, mut fds) = (Vec::new(), Vec::new());
        for (bytes, count) in [(&b"none"[..], 0), (b"two", 2), (b"one", 1)] {
            assert!(outbox.pop(&mut frame, &mut fds));
            assert_eq!((frame.as_slice(), fds.len()), (bytes, count));
        }
    }
}
