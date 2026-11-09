//! CRC-32C (Castagnoli), reflected, init `0xFFFF_FFFF`, final XOR `0xFFFF_FFFF`.
//!
//! The default implementation uses a 16-entry nibble table (64 bytes of flash). The `crc-table`
//! feature switches to the classic 256-entry byte table (1 KiB) for roughly twice the throughput.

#![allow(unsafe_code)]
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

/// Software CRC-32C of `bytes`, continuing from the reflected register `state`.
fn update_software(mut state: u32, bytes: &[u8]) -> u32 {
    for &byte in bytes {
        state = update_byte(state, byte);
    }
    state
}

#[cfg(all(feature = "crc-extern", target_os = "none"))]
unsafe extern "Rust" {
    /// The firmware's CRC-32C routine (feature `crc-extern`, bare-metal targets only; on hosted
    /// targets the feature does nothing, so `--all-features` builds link): continues the reflected CRC-32C
    /// register `state` (initially `0xFFFF_FFFF`, no final XOR) over `bytes` and returns the new
    /// register. Defined by the firmware with `#[unsafe(no_mangle)]`, for example over the STM32
    /// CRC unit with polynomial `0x1EDC_6F41`, byte-reversed input and reversed output, `INIT`
    /// set to `state.reverse_bits()`.
    pub fn orion_wire_crc32c_update(state: u32, bytes: &[u8]) -> u32;
}

#[inline]
fn update_slice(state: u32, bytes: &[u8]) -> u32 {
    #[cfg(all(feature = "crc-extern", target_os = "none"))]
    {
        // SAFETY: the firmware defines the routine with this signature (feature contract).
        return unsafe { orion_wire_crc32c_update(state, bytes) };
    }
    #[cfg(all(
        not(all(feature = "crc-extern", target_os = "none")),
        target_arch = "aarch64",
        target_feature = "crc"
    ))]
    {
        // SAFETY: the target is built with the `crc` feature.
        return unsafe { hw::aarch64(state, bytes) };
    }
    #[cfg(all(
        not(all(feature = "crc-extern", target_os = "none")),
        target_arch = "x86_64",
        target_feature = "sse4.2"
    ))]
    {
        // SAFETY: the target is built with SSE4.2.
        return unsafe { hw::x86_64(state, bytes) };
    }
    #[allow(unreachable_code)]
    {
        #[cfg(all(
            feature = "std",
            not(all(feature = "crc-extern", target_os = "none")),
            target_arch = "aarch64",
            not(target_feature = "crc")
        ))]
        if std::arch::is_aarch64_feature_detected!("crc") {
            // SAFETY: the CPU reported the `crc` feature.
            return unsafe { hw::aarch64(state, bytes) };
        }
        #[cfg(all(
            feature = "std",
            not(all(feature = "crc-extern", target_os = "none")),
            target_arch = "x86_64",
            not(target_feature = "sse4.2")
        ))]
        if std::arch::is_x86_feature_detected!("sse4.2") {
            // SAFETY: the CPU reported SSE4.2.
            return unsafe { hw::x86_64(state, bytes) };
        }
        update_software(state, bytes)
    }
}

/// The CRC-32C instructions: eight bytes per instruction, then the tail byte by byte. Both use
/// the reflected Castagnoli polynomial, the same register the software path keeps.
#[cfg(all(
    not(all(feature = "crc-extern", target_os = "none")),
    any(target_arch = "aarch64", target_arch = "x86_64")
))]
#[allow(dead_code)]
mod hw {
    #[cfg(target_arch = "aarch64")]
    #[target_feature(enable = "crc")]
    pub unsafe fn aarch64(mut state: u32, bytes: &[u8]) -> u32 {
        use core::arch::aarch64::{__crc32cb, __crc32cd};
        let (words, tail) = bytes.as_chunks::<8>();
        for word in words {
            state = __crc32cd(state, u64::from_le_bytes(*word));
        }
        for &byte in tail {
            state = __crc32cb(state, byte);
        }
        state
    }

    #[cfg(target_arch = "x86_64")]
    #[target_feature(enable = "sse4.2")]
    pub unsafe fn x86_64(state: u32, bytes: &[u8]) -> u32 {
        use core::arch::x86_64::{_mm_crc32_u8, _mm_crc32_u64};
        let mut wide = u64::from(state);
        let (words, tail) = bytes.as_chunks::<8>();
        for word in words {
            wide = _mm_crc32_u64(wide, u64::from_le_bytes(*word));
        }
        // The 64-bit form keeps the CRC in the low 32 bits.
        let mut state = wide as u32;
        for &byte in tail {
            state = _mm_crc32_u8(state, byte);
        }
        state
    }
}

/// Incremental CRC-32C state.
///
/// ```
/// let mut crc = orion_wire::Crc32c::new();
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
    ///
    /// Uses the CPU's CRC-32C instructions where it has them: ARMv8 `CRC32CB/CRC32CX` (built for a
    /// target with the `crc` feature, or detected at run time with `std`), x86-64 SSE4.2 `crc32`
    /// (likewise). With the `crc-extern` feature a bare-metal firmware supplies the routine
    /// instead, for example over a CRC peripheral (`orion_wire_crc32c_update`).
    pub fn update(&mut self, bytes: &[u8]) {
        self.state = update_slice(self.state, bytes);
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

#[cfg(test)]
mod tests {
    use super::*;

    /// The hardware paths (when this machine has them) agree with the software one at every
    /// length and alignment, including split updates.
    #[test]
    fn every_path_matches_the_software_crc() {
        let data: [u8; 300] = core::array::from_fn(|i| (i as u8).wrapping_mul(31).wrapping_add(7));
        for start in 0..9 {
            for len in [0, 1, 7, 8, 9, 15, 16, 17, 63, 64, 65, 200, 291] {
                let bytes = &data[start..start + len];
                let want = update_software(0xFFFF_FFFF, bytes);
                assert_eq!(
                    update_slice(0xFFFF_FFFF, bytes),
                    want,
                    "start {start} len {len}"
                );
                let (a, b) = bytes.split_at(len / 3);
                let mut crc = Crc32c::new();
                crc.update(a);
                crc.update(b);
                assert_eq!(crc.finish(), want ^ 0xFFFF_FFFF);
            }
        }
        assert_eq!(crc32c(b"123456789"), 0xE306_9283);
    }
}
