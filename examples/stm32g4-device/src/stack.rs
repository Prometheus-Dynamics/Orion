//! Stack high-water mark by painting: at start the free stack is filled with a pattern, and the
//! deepest word that no longer holds it is how far the stack has ever grown.
//!
//! `cortex-m-rt` places the stack at the top of RAM, growing down towards the end of `.bss`
//! (`__sheap`). The firmware reports the mark as a status value, so the number tracked per
//! commit is measured on the board rather than estimated.

const PATTERN: u32 = 0xA5A5_A5A5;
/// Words left unpainted below the current stack pointer when painting, for `paint`'s own frame.
const GUARD: u32 = 16;

unsafe extern "C" {
    static mut __sheap: u32;
    static _stack_start: u32;
}

fn bottom() -> u32 {
    (&raw const __sheap) as u32
}

fn top() -> u32 {
    (&raw const _stack_start) as u32
}

/// Paints the free stack. Call first thing in `main`.
#[inline(never)]
pub fn paint() {
    let sp: u32;
    // SAFETY: reads the stack pointer only.
    unsafe { core::arch::asm!("mov {}, sp", out(reg) sp) };
    let mut addr = bottom();
    let end = sp - GUARD * 4;
    while addr < end {
        // SAFETY: `[__sheap, sp - GUARD)` is RAM that neither statics nor live frames use.
        unsafe { core::ptr::write_volatile(addr as *mut u32, PATTERN) };
        addr += 4;
    }
}

/// The deepest the stack has grown since `paint`, in bytes.
pub fn high_water() -> u32 {
    let mut addr = bottom();
    let top = top();
    while addr < top {
        // SAFETY: as in `paint`; reading a word of the stack area is always valid.
        if unsafe { core::ptr::read_volatile(addr as *const u32) } != PATTERN {
            break;
        }
        addr += 4;
    }
    top - addr
}

/// Bytes of RAM between the end of `.bss` and the top of RAM (what the stack may use).
pub fn size() -> u32 {
    top() - bottom()
}
