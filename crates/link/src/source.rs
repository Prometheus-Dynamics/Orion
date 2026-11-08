//! A frame's bytes viewed either as one encoded slice or as `header ‖ payload ‖ crc` parts, so the
//! transport encoders can emit a message without first assembling it in a second buffer.

use crate::crc::Crc32c;
use crate::frame::{CRC_LEN, FrameHeader, HEADER_LEN};

#[derive(Debug, Clone, Copy)]
pub(crate) struct FrameSource<'a> {
    head: [u8; HEADER_LEN],
    head_len: usize,
    body: &'a [u8],
    tail: [u8; CRC_LEN],
    tail_len: usize,
}

impl<'a> FrameSource<'a> {
    /// Bytes that are already a complete frame (or any opaque byte string).
    pub(crate) const fn raw(bytes: &'a [u8]) -> Self {
        Self {
            head: [0; HEADER_LEN],
            head_len: 0,
            body: bytes,
            tail: [0; CRC_LEN],
            tail_len: 0,
        }
    }

    /// A frame built on the fly from its header and payload; the CRC is computed here.
    pub(crate) fn message(header: FrameHeader, payload: &'a [u8]) -> Self {
        let head = header.to_bytes();
        let mut crc = Crc32c::new();
        crc.update(&head);
        crc.update(payload);
        Self {
            head,
            head_len: HEADER_LEN,
            body: payload,
            tail: crc.finish().to_le_bytes(),
            tail_len: CRC_LEN,
        }
    }

    pub(crate) const fn len(&self) -> usize {
        self.head_len
            .saturating_add(self.body.len())
            .saturating_add(self.tail_len)
    }

    /// The three parts in order, each as one slice.
    #[cfg(feature = "std")]
    fn parts(&self) -> [&[u8]; 3] {
        [
            self.head.get(..self.head_len).unwrap_or_default(),
            self.body,
            self.tail.get(..self.tail_len).unwrap_or_default(),
        ]
    }

    /// The contiguous bytes from `index` to the end of the part holding it (empty past the end).
    #[cfg(feature = "std")]
    pub(crate) fn run_at(&self, mut index: usize) -> &[u8] {
        for part in self.parts() {
            if index < part.len() {
                return part.get(index..).unwrap_or_default();
            }
            index -= part.len();
        }
        &[]
    }

    /// Copies the bytes from `index` into `out` as far as both reach; returns how many.
    #[cfg(feature = "std")]
    pub(crate) fn copy_to(&self, mut index: usize, out: &mut [u8]) -> usize {
        let mut written = 0;
        while written < out.len() {
            let run = self.run_at(index);
            if run.is_empty() {
                break;
            }
            let n = run.len().min(out.len() - written);
            if let (Some(to), Some(from)) = (out.get_mut(written..written + n), run.get(..n)) {
                to.copy_from_slice(from);
            }
            written += n;
            index += n;
        }
        written
    }

    /// From `index`, the number of nonzero bytes (at most `max`) and whether a zero follows them.
    #[cfg(feature = "std")]
    pub(crate) fn nonzero_run(&self, mut index: usize, max: usize) -> (usize, bool) {
        let mut run = 0;
        while run < max {
            let part = self.run_at(index);
            if part.is_empty() {
                return (run, false);
            }
            let part = part.get(..part.len().min(max - run)).unwrap_or_default();
            if let Some(zero) = part.iter().position(|&byte| byte == 0) {
                return (run + zero, true);
            }
            run += part.len();
            index += part.len();
        }
        (run, false)
    }

    pub(crate) fn get(&self, index: usize) -> Option<u8> {
        if index < self.head_len {
            return self.head.get(index).copied();
        }
        let index = index - self.head_len;
        if index < self.body.len() {
            return self.body.get(index).copied();
        }
        let index = index - self.body.len();
        if index < self.tail_len {
            self.tail.get(index).copied()
        } else {
            None
        }
    }
}
