//! The postcard subset the link bodies use, written by hand: varints, zigzag, length-prefixed
//! strings and sequences, `Option` tags, and enum variant indices.
//!
//! [`Writer`] never fails: it counts every byte and stores the ones that fit, so the same code
//! path computes an encoded length (with an empty buffer) and writes it. [`Reader`] returns
//! [`WireError::Decode`] for anything malformed and never panics.

use super::WireError;

/// Writes postcard-encoded values into a byte buffer. See [`Encode`].
#[derive(Debug)]
pub struct Writer<'a> {
    buf: &'a mut [u8],
    len: usize,
}

impl<'a> Writer<'a> {
    /// A writer filling `buf` from the start.
    pub fn new(buf: &'a mut [u8]) -> Self {
        Self { buf, len: 0 }
    }

    /// Bytes written so far, including those that did not fit.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.len
    }

    /// Whether nothing was written.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Whether everything written so far fit into the buffer.
    #[must_use]
    pub fn fits(&self) -> bool {
        self.len <= self.buf.len()
    }

    /// One raw byte (`u8`, `bool`, and the frozen one-byte bodies).
    // Kept out of line: it is called from every encoder, and a call is smaller than the inlined
    // bounds check and store on small cores.
    #[inline(never)]
    pub fn byte(&mut self, byte: u8) {
        if let Some(slot) = self.buf.get_mut(self.len) {
            *slot = byte;
        }
        self.len = self.len.saturating_add(1);
    }

    /// Raw bytes without a length prefix.
    pub fn raw(&mut self, bytes: &[u8]) {
        for &byte in bytes {
            self.byte(byte);
        }
    }

    /// An unsigned LEB128 varint (postcard's `u16`, `u32`, `u64`, `usize`, lengths, and enum
    /// variant indices).
    pub fn varint(&mut self, mut value: u64) {
        while value >= 0x80 {
            self.byte((value as u8) | 0x80);
            value >>= 7;
        }
        self.byte(value as u8);
    }

    /// A zigzag varint (postcard's signed integers).
    pub fn zigzag(&mut self, value: i64) {
        self.varint(((value << 1) ^ (value >> 63)) as u64);
    }

    /// A length prefix (strings, byte strings, sequences, maps).
    pub fn len_prefix(&mut self, len: usize) {
        self.varint(len as u64);
    }

    /// A length-prefixed byte string (`String`, `&str`, `Vec<u8>`).
    pub fn bytes(&mut self, bytes: &[u8]) {
        self.len_prefix(bytes.len());
        self.raw(bytes);
    }
}

/// A value with a postcard encoding.
///
/// Implemented for the primitives the link bodies use, slices (length-prefixed sequences),
/// `Option`, references, and the device views in [`crate::wire`].
pub trait Encode {
    /// Appends the encoding of `self`.
    fn encode(&self, w: &mut Writer<'_>);
}

impl<T: Encode + ?Sized> Encode for &T {
    fn encode(&self, w: &mut Writer<'_>) {
        (**self).encode(w);
    }
}

impl Encode for u8 {
    fn encode(&self, w: &mut Writer<'_>) {
        w.byte(*self);
    }
}

impl Encode for bool {
    fn encode(&self, w: &mut Writer<'_>) {
        w.byte(u8::from(*self));
    }
}

impl Encode for u16 {
    fn encode(&self, w: &mut Writer<'_>) {
        w.varint(u64::from(*self));
    }
}

impl Encode for u32 {
    fn encode(&self, w: &mut Writer<'_>) {
        w.varint(u64::from(*self));
    }
}

impl Encode for u64 {
    fn encode(&self, w: &mut Writer<'_>) {
        w.varint(*self);
    }
}

impl Encode for i64 {
    fn encode(&self, w: &mut Writer<'_>) {
        w.zigzag(*self);
    }
}

impl Encode for str {
    fn encode(&self, w: &mut Writer<'_>) {
        w.bytes(self.as_bytes());
    }
}

impl<T: Encode> Encode for [T] {
    fn encode(&self, w: &mut Writer<'_>) {
        w.len_prefix(self.len());
        for item in self {
            item.encode(w);
        }
    }
}

impl<T: Encode> Encode for Option<T> {
    fn encode(&self, w: &mut Writer<'_>) {
        match self {
            None => w.byte(0),
            Some(value) => {
                w.byte(1);
                value.encode(w);
            }
        }
    }
}

/// Encoded length of `value`, computed without a buffer.
pub fn encoded_len<T: Encode + ?Sized>(value: &T) -> usize {
    let mut w = Writer::new(&mut []);
    value.encode(&mut w);
    w.len()
}

/// A `u64` varint kept verbatim (validated, at most 10 bytes), so a `Pong` can echo a `Ping`
/// without 64-bit arithmetic. Empty until [`RawVarint::read`] succeeds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RawVarint {
    bytes: [u8; 10],
    len: u8,
}

impl RawVarint {
    /// No varint.
    pub const EMPTY: Self = Self {
        bytes: [0; 10],
        len: 0,
    };

    /// Reads one varint as postcard's `u64` accepts it into `self` (in place, so a caller can
    /// keep it in a session without moving it). On error `self` is left empty.
    ///
    /// # Errors
    ///
    /// [`WireError::Decode`] if truncated or longer than a `u64`.
    pub fn read(&mut self, r: &mut Reader<'_>) -> Result<(), WireError> {
        self.len = 0;
        let mut len = 0u8;
        for slot in &mut self.bytes {
            let byte = r.byte()?;
            *slot = byte;
            len += 1;
            if byte & 0x80 == 0 {
                if len == 10 && byte > 1 {
                    return Err(WireError::Decode);
                }
                self.len = len;
                return Ok(());
            }
        }
        Err(WireError::Decode)
    }

    /// The varint's bytes (empty if none was read).
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        self.bytes.get(..usize::from(self.len)).unwrap_or_default()
    }

    /// Whether no varint is held.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Forgets the varint.
    pub fn clear(&mut self) {
        self.len = 0;
    }
}

impl Encode for RawVarint {
    fn encode(&self, w: &mut Writer<'_>) {
        w.raw(self.as_bytes());
    }
}

/// Validates UTF-8 (the same rules as `core::str::from_utf8`: no overlong forms, no surrogates,
/// nothing above U+10FFFF) with a few dozen instructions instead of the fast but large `core`
/// validator, and returns the string.
///
/// # Errors
///
/// [`WireError::Decode`] if `bytes` is not UTF-8.
pub fn str_from_utf8(bytes: &[u8]) -> Result<&str, WireError> {
    if !is_utf8(bytes) {
        return Err(WireError::Decode);
    }
    // SAFETY: `is_utf8` accepted exactly the well-formed UTF-8 sequences (checked against
    // `core::str::from_utf8` exhaustively for 1- to 3-byte sequences in `tests/wire_utf8.rs`).
    Ok(unsafe { core::str::from_utf8_unchecked(bytes) })
}

fn is_utf8(bytes: &[u8]) -> bool {
    let mut rest = bytes.iter();
    while let Some(&lead) = rest.next() {
        if lead < 0x80 {
            continue;
        }
        // Continuation bytes, and the smallest scalar that needs this many bytes.
        let (extra, min) = if lead < 0xE0 {
            (1, 0x80)
        } else if lead < 0xF0 {
            (2, 0x800)
        } else {
            (3, 0x1_0000)
        };
        // Strips the length marker; a stray continuation byte (`10xxxxxx`) or an `F8..FF` lead
        // keeps a high bit set and is rejected by the range checks below.
        let mut scalar = u32::from(lead) & (0x7F >> extra);
        if !(0xC0..=0xF7).contains(&lead) {
            return false;
        }
        for _ in 0..extra {
            match rest.next() {
                Some(&next) if next & 0xC0 == 0x80 => {
                    scalar = (scalar << 6) | u32::from(next & 0x3F);
                }
                _ => return false,
            }
        }
        // Overlong forms, values past U+10FFFF, and UTF-16 surrogates (D800..DFFF).
        if scalar < min || scalar > 0x10_FFFF || scalar >> 11 == 0x1B {
            return false;
        }
    }
    true
}

/// Reads postcard-encoded values from a payload, borrowing strings from it.
#[derive(Debug, Clone)]
pub struct Reader<'a> {
    data: &'a [u8],
}

impl<'a> Reader<'a> {
    /// A reader over `data`.
    #[must_use]
    pub const fn new(data: &'a [u8]) -> Self {
        Self { data }
    }

    /// The unread bytes (trailing bytes after a body are allowed and ignored).
    #[must_use]
    pub const fn rest(&self) -> &'a [u8] {
        self.data
    }

    /// One raw byte.
    ///
    /// # Errors
    ///
    /// [`WireError::Decode`] at the end of the data.
    pub fn byte(&mut self) -> Result<u8, WireError> {
        let (&first, rest) = self.data.split_first().ok_or(WireError::Decode)?;
        self.data = rest;
        Ok(first)
    }

    /// `n` raw bytes.
    ///
    /// # Errors
    ///
    /// [`WireError::Decode`] if fewer remain.
    pub fn take(&mut self, n: usize) -> Result<&'a [u8], WireError> {
        let (head, rest) = self.data.split_at_checked(n).ok_or(WireError::Decode)?;
        self.data = rest;
        Ok(head)
    }

    /// An unsigned varint of at most 64 bits.
    ///
    /// # Errors
    ///
    /// [`WireError::Decode`] if truncated or longer than 10 bytes.
    pub fn varint(&mut self) -> Result<u64, WireError> {
        let mut value = 0u64;
        let mut shift = 0u32;
        loop {
            let byte = self.byte()?;
            let low = u64::from(byte & 0x7F);
            if shift == 63 && low > 1 {
                return Err(WireError::Decode);
            }
            value |= low << shift;
            if byte & 0x80 == 0 {
                return Ok(value);
            }
            shift += 7;
            if shift > 63 {
                return Err(WireError::Decode);
            }
        }
    }

    /// A varint that must fit a `u32` (also lengths and enum variant indices). Uses 32-bit
    /// arithmetic only, which is much smaller than the `u64` path on cores without 64-bit shifts.
    ///
    /// # Errors
    ///
    /// [`WireError::Decode`] if malformed or out of range.
    pub fn varint_u32(&mut self) -> Result<u32, WireError> {
        let mut value = 0u32;
        let mut shift = 0u32;
        loop {
            let byte = self.byte()?;
            let low = u32::from(byte & 0x7F);
            if shift == 28 && low > 0x0F {
                return Err(WireError::Decode);
            }
            value |= low << shift;
            if byte & 0x80 == 0 {
                return Ok(value);
            }
            shift += 7;
            if shift > 28 {
                return Err(WireError::Decode);
            }
        }
    }

    /// A varint that must fit a `u16`.
    ///
    /// # Errors
    ///
    /// [`WireError::Decode`] if malformed or out of range.
    pub fn varint_u16(&mut self) -> Result<u16, WireError> {
        u16::try_from(self.varint_u32()?).map_err(|_| WireError::Decode)
    }

    /// A length-prefixed byte string.
    ///
    /// # Errors
    ///
    /// [`WireError::Decode`] if truncated.
    pub fn bytes(&mut self) -> Result<&'a [u8], WireError> {
        let len = self.varint_u32()?;
        self.take(len as usize)
    }

    /// A length-prefixed UTF-8 string.
    ///
    /// # Errors
    ///
    /// [`WireError::Decode`] if truncated or not UTF-8.
    pub fn str(&mut self) -> Result<&'a str, WireError> {
        str_from_utf8(self.bytes()?)
    }

    /// An `Option` tag: `false` for `None`, `true` for `Some`.
    ///
    /// # Errors
    ///
    /// [`WireError::Decode`] for a tag other than 0 or 1.
    pub fn option(&mut self) -> Result<bool, WireError> {
        match self.byte()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(WireError::Decode),
        }
    }

    /// An `Option<&str>`.
    ///
    /// # Errors
    ///
    /// [`WireError::Decode`] if malformed.
    pub fn option_str(&mut self) -> Result<Option<&'a str>, WireError> {
        if self.option()? {
            self.str().map(Some)
        } else {
            Ok(None)
        }
    }
}
