//! CRC-32C (Castagnoli), reflected, init `0xFFFF_FFFF`, final XOR `0xFFFF_FFFF`.
//!
//! The default implementation uses a 16-entry nibble table (64 bytes of flash). The `crc-table`
//! feature switches to the classic 256-entry byte table (1 KiB) for roughly twice the throughput.

// Every table index below is masked to the table size, so indexing cannot go out of bounds.
#![allow(clippy::indexing_slicing)]

/// Reflected CRC-32C polynomial.
const POLY: u32 = 0x82F6_3B78;

const fn step_bits(mut crc: u32, bits: u32) -> u32 {
    let mut i = 0;
    while i < bits {
        crc = if crc & 1 != 0 {
            (crc >> 1) ^ POLY
        } else {
            crc >> 1
        };
        i += 1;
    }
    crc
}

#[cfg(not(feature = "crc-table"))]
const NIBBLE_TABLE: [u32; 16] = {
    let mut table = [0u32; 16];
    let mut i = 0;
    while i < 16 {
        table[i] = step_bits(i as u32, 4);
        i += 1;
    }
    table
};

#[cfg(feature = "crc-table")]
const BYTE_TABLE: [u32; 256] = {
    let mut table = [0u32; 256];
    let mut i = 0;
    while i < 256 {
        table[i] = step_bits(i as u32, 8);
        i += 1;
    }
    table
};

#[cfg(not(feature = "crc-table"))]
#[inline]
fn update_byte(crc: u32, byte: u8) -> u32 {
    let crc = crc ^ u32::from(byte);
    // `& 0xF` keeps the index in bounds; the compiler elides the check.
    let crc = (crc >> 4) ^ NIBBLE_TABLE[(crc & 0xF) as usize];
    (crc >> 4) ^ NIBBLE_TABLE[(crc & 0xF) as usize]
}

#[cfg(feature = "crc-table")]
#[inline]
fn update_byte(crc: u32, byte: u8) -> u32 {
    (crc >> 8) ^ BYTE_TABLE[((crc ^ u32::from(byte)) & 0xFF) as usize]
}

/// Incremental CRC-32C state.
///
/// ```
/// let mut crc = orion_link::Crc32c::new();
/// crc.update(b"1234");
/// crc.update(b"56789");
/// assert_eq!(crc.finish(), 0xE306_9283);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Crc32c {
    state: u32,
}

impl Crc32c {
    /// Fresh state.
    #[must_use]
    pub const fn new() -> Self {
        Self { state: 0xFFFF_FFFF }
    }

    /// Feeds `bytes` into the checksum.
    pub fn update(&mut self, bytes: &[u8]) {
        for &byte in bytes {
            self.state = update_byte(self.state, byte);
        }
    }

    /// Feeds a single byte into the checksum.
    pub fn update_byte(&mut self, byte: u8) {
        self.state = update_byte(self.state, byte);
    }

    /// Final checksum value. The state is not consumed, so more bytes may follow.
    #[must_use]
    pub const fn finish(&self) -> u32 {
        self.state ^ 0xFFFF_FFFF
    }
}

impl Default for Crc32c {
    fn default() -> Self {
        Self::new()
    }
}

/// One-shot CRC-32C of `bytes`.
#[must_use]
pub fn crc32c(bytes: &[u8]) -> u32 {
    let mut crc = Crc32c::new();
    crc.update(bytes);
    crc.finish()
}
