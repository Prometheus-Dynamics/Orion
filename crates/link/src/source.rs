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
