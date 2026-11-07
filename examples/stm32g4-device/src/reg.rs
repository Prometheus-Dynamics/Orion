//! Volatile register access by address: the example uses no peripheral access crate, so every
//! register it touches is named here and nothing else is linked.

/// Reads the 32-bit register at `addr`.
#[inline(always)]
pub fn read(addr: u32) -> u32 {
    // SAFETY: every caller passes the address of a memory-mapped register (or of message RAM) of
    // the STM32G4, which is always readable as a 32-bit word.
    unsafe { core::ptr::read_volatile(addr as *const u32) }
}

/// Writes the 32-bit register at `addr`.
#[inline(always)]
pub fn write(addr: u32, value: u32) {
    // SAFETY: as for `read`; the example owns every peripheral it writes.
    unsafe { core::ptr::write_volatile(addr as *mut u32, value) }
}

/// Sets `bits` in the register at `addr`.
#[inline(always)]
pub fn set(addr: u32, bits: u32) {
    write(addr, read(addr) | bits);
}

/// Replaces the field `mask << shift` of the register at `addr` with `value`.
#[inline(always)]
pub fn field(addr: u32, shift: u32, mask: u32, value: u32) {
    write(
        addr,
        (read(addr) & !(mask << shift)) | ((value & mask) << shift),
    );
}

/// Reset and clock control.
pub mod rcc {
    const BASE: u32 = 0x4002_1000;
    #[cfg(feature = "hw-crc")]
    pub const AHB1ENR: u32 = BASE + 0x48;
    pub const AHB2ENR: u32 = BASE + 0x4C;
    pub const APB1ENR1: u32 = BASE + 0x58;
    #[cfg(feature = "link-can")]
    pub const CCIPR: u32 = BASE + 0x88;
}

/// GPIO port A.
pub mod gpioa {
    const BASE: u32 = 0x4800_0000;
    pub const MODER: u32 = BASE;
    pub const AFRL: u32 = BASE + 0x20;
    pub const AFRH: u32 = BASE + 0x24;

    /// Puts `pin` in alternate function `af`.
    pub fn alternate(pin: u32, af: u32) {
        super::field(MODER, pin * 2, 0b11, 0b10);
        let (reg, slot) = if pin < 8 {
            (AFRL, pin)
        } else {
            (AFRH, pin - 8)
        };
        super::field(reg, slot * 4, 0xF, af);
    }
}

/// The Cortex-M SysTick timer.
pub mod systick {
    pub const CSR: u32 = 0xE000_E010;
    pub const RVR: u32 = 0xE000_E014;
    pub const CVR: u32 = 0xE000_E018;
}
