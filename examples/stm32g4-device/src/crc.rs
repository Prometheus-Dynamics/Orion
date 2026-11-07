//! The link's CRC-32C on the STM32G4 CRC unit (feature `hw-crc`, `orion-link/crc-extern`), in
//! place of the software nibble table.
//!
//! The unit computes a non-reflected CRC; with byte-reversed input and reversed output it yields
//! the reflected CRC-32C register the link keeps, so a call continues from `state` by loading
//! `INIT` with `state` bit-reversed. Not verified on silicon yet: check `stack.*` status arriving at
//! the gateway (every frame carries the CRC) before relying on it.

use crate::reg::{self, rcc};

const BASE: u32 = 0x4002_3000;
const DR: u32 = BASE;
const CR: u32 = BASE + 0x08;
const INIT: u32 = BASE + 0x10;
const POL: u32 = BASE + 0x14;
/// CRC-32C (Castagnoli), non-reflected form.
const CASTAGNOLI: u32 = 0x1EDC_6F41;
/// RESET, 32-bit polynomial, input reversed by byte, output reversed.
const CR_RUN: u32 = 1 | (0b01 << 5) | (1 << 7);

/// Clocks the CRC unit and sets the polynomial. Call before the session sends anything.
pub fn init() {
    reg::set(rcc::AHB1ENR, 1 << 12);
    reg::write(POL, CASTAGNOLI);
}

#[unsafe(no_mangle)]
fn orion_link_crc32c_update(state: u32, bytes: &[u8]) -> u32 {
    reg::write(INIT, state.reverse_bits());
    reg::write(CR, CR_RUN);
    for &byte in bytes {
        // SAFETY: DR accepts 8-bit writes; each one feeds one byte.
        unsafe { core::ptr::write_volatile(DR as *mut u8, byte) };
    }
    reg::read(DR)
}
