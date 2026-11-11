//! Reading and writing the fields of a body by hand.
//!
//! Bodies on the wire are postcard: numbers as varints, text and bytes with their length
//! first, fields one after another. A general codec for that costs a microcontroller more
//! flash than the whole session, so the device side writes each field out with these, and
//! tests hold the result to the same bytes the general codec gives.
//!
//! Neither can fail half way in a way that matters: a [`Writer`] that ran out of room
//! says so at the end, and a [`Reader`] that ran out of bytes gives `None` from then on.

/// Writes fields into a buffer.
#[derive(Debug)]
pub struct Writer<'a> {
    buf: &'a mut [u8],
    at: usize,
    full: bool,
}

impl<'a> Writer<'a> {
    /// A writer at the start of `buf`.
    pub const fn new(buf: &'a mut [u8]) -> Self {
        Self {
            buf,
            at: 0,
            full: false,
        }
    }

    /// One byte: a `u8`, or a `bool`.
    pub fn u8(&mut self, value: u8) -> &mut Self {
        match self.buf.get_mut(self.at) {
            Some(slot) => {
                *slot = value;
                self.at += 1;
            }
            None => self.full = true,
        }
        self
    }

    /// A `u16`, `u32` or `u64`: seven bits a byte, the low ones first.
    pub fn varint(&mut self, mut value: u64) -> &mut Self {
        while value >= 0x80 {
            self.u8(value as u8 | 0x80);
            value >>= 7;
        }
        self.u8(value as u8)
    }

    /// An `i16`, `i32` or `i64`: zigzag, then as a varint.
    pub fn signed(&mut self, value: i64) -> &mut Self {
        self.varint(((value << 1) ^ (value >> 63)) as u64)
    }

    /// Bytes as they are, with nothing before them: the fixed-width types.
    pub fn raw(&mut self, bytes: &[u8]) -> &mut Self {
        match self.buf.get_mut(self.at..self.at.wrapping_add(bytes.len())) {
            Some(room) => {
                // The same length by construction, written without a check that could fail.
                for (to, from) in room.iter_mut().zip(bytes) {
                    *to = *from;
                }
                self.at += bytes.len();
            }
            None => self.full = true,
        }
        self
    }

    /// Bytes or text: their length, then themselves.
    pub fn bytes(&mut self, bytes: &[u8]) -> &mut Self {
        self.varint(bytes.len() as u64).raw(bytes)
    }

    /// How many bytes were written, or `None` if they did not all fit.
    #[must_use]
    pub const fn finish(&self) -> Option<usize> {
        if self.full { None } else { Some(self.at) }
    }
}

/// Reads fields from a payload.
#[derive(Clone, Copy, Debug)]
pub struct Reader<'a> {
    bytes: &'a [u8],
}

impl<'a> Reader<'a> {
    /// A reader at the start of `bytes`.
    #[must_use]
    pub const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes }
    }

    /// One byte.
    pub fn u8(&mut self) -> Option<u8> {
        let (first, rest) = self.bytes.split_first()?;
        self.bytes = rest;
        Some(*first)
    }

    /// A varint of up to 64 bits.
    pub fn varint(&mut self) -> Option<u64> {
        let mut value = 0_u64;
        let mut shift = 0_u32;
        loop {
            let byte = self.u8()?;
            // The tenth byte may only carry the one bit that is left.
            if shift == 63 && byte > 1 {
                return None;
            }
            value |= u64::from(byte & 0x7F) << shift;
            if byte & 0x80 == 0 {
                return Some(value);
            }
            shift += 7;
            if shift > 63 {
                return None;
            }
        }
    }

    /// A varint that must fit a `u32`.
    pub fn u32(&mut self) -> Option<u32> {
        u32::try_from(self.varint()?).ok()
    }

    /// A varint that must fit a `u16`.
    pub fn u16(&mut self) -> Option<u16> {
        u16::try_from(self.varint()?).ok()
    }

    /// A zigzag varint.
    pub fn signed(&mut self) -> Option<i64> {
        let value = self.varint()?;
        Some((value >> 1) as i64 ^ -((value & 1) as i64))
    }

    /// So many bytes as they are.
    pub fn raw(&mut self, len: usize) -> Option<&'a [u8]> {
        let (front, rest) = self.bytes.split_at_checked(len)?;
        self.bytes = rest;
        Some(front)
    }

    /// Bytes or text: their length, then themselves. Text is not checked to be UTF-8 here:
    /// a device that shows it to nobody has no need to.
    pub fn bytes(&mut self) -> Option<&'a [u8]> {
        let len = usize::try_from(self.varint()?).ok()?;
        self.raw(len)
    }

    /// What has not been read. A newer host may have added fields at the end.
    #[must_use]
    pub const fn rest(&self) -> &'a [u8] {
        self.bytes
    }
}
