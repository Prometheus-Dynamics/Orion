//! Byte-loop `memcpy`, `memmove`, `memset` and `memcmp` in place of compiler-builtins' unrolled
//! ones (feature `small-mem`). The device tier copies at most a frame (128 bytes) at a time, so
//! speed does not matter here and about 1 KiB of flash does. Volatile accesses keep LLVM from
//! turning the loops back into calls to themselves.

use core::ffi::c_void;
use core::ptr::{read_volatile, write_volatile};

#[unsafe(no_mangle)]
unsafe extern "C" fn memcpy(dest: *mut c_void, src: *const c_void, n: usize) -> *mut c_void {
    let (d, s) = (dest.cast::<u8>(), src.cast::<u8>());
    for i in 0..n {
        // SAFETY: the caller guarantees `n` valid bytes at both pointers.
        unsafe { write_volatile(d.add(i), read_volatile(s.add(i))) };
    }
    dest
}

#[unsafe(no_mangle)]
unsafe extern "C" fn memmove(dest: *mut c_void, src: *const c_void, n: usize) -> *mut c_void {
    let (d, s) = (dest.cast::<u8>(), src.cast::<u8>());
    if (dest as usize) <= (src as usize) {
        // SAFETY: as for `memcpy`; copying forwards is correct when `dest` is below `src`.
        return unsafe { memcpy(dest.cast(), src.cast(), n) };
    }
    for i in (0..n).rev() {
        // SAFETY: the caller guarantees `n` valid bytes at both pointers.
        unsafe { write_volatile(d.add(i), read_volatile(s.add(i))) };
    }
    dest
}

#[unsafe(no_mangle)]
unsafe extern "C" fn memset(dest: *mut c_void, value: i32, n: usize) -> *mut c_void {
    let d = dest.cast::<u8>();
    for i in 0..n {
        // SAFETY: the caller guarantees `n` writable bytes at `dest`.
        unsafe { write_volatile(d.add(i), value as u8) };
    }
    dest
}

#[unsafe(no_mangle)]
unsafe extern "C" fn memcmp(a: *const c_void, b: *const c_void, n: usize) -> i32 {
    let (a, b) = (a.cast::<u8>(), b.cast::<u8>());
    for i in 0..n {
        // SAFETY: the caller guarantees `n` readable bytes at both pointers.
        let (x, y) = unsafe { (read_volatile(a.add(i)), read_volatile(b.add(i))) };
        if x != y {
            return i32::from(x) - i32::from(y);
        }
    }
    0
}

// The ARM EABI entry points LLVM calls directly on Cortex-M. compiler-builtins defines them with
// their own unrolled loops, so they are replaced too. Note the argument order of `memclr`/`memset`.

#[unsafe(no_mangle)]
unsafe extern "C" fn __aeabi_memcpy(dest: *mut u8, src: *const u8, n: usize) {
    // SAFETY: the same contract as `memcpy`.
    unsafe { memcpy(dest.cast(), src.cast(), n) };
}

#[unsafe(no_mangle)]
unsafe extern "C" fn __aeabi_memcpy4(dest: *mut u8, src: *const u8, n: usize) {
    // SAFETY: the same contract as `memcpy`.
    unsafe { memcpy(dest.cast(), src.cast(), n) };
}

#[unsafe(no_mangle)]
unsafe extern "C" fn __aeabi_memcpy8(dest: *mut u8, src: *const u8, n: usize) {
    // SAFETY: the same contract as `memcpy`.
    unsafe { memcpy(dest.cast(), src.cast(), n) };
}

#[unsafe(no_mangle)]
unsafe extern "C" fn __aeabi_memmove(dest: *mut u8, src: *const u8, n: usize) {
    // SAFETY: the same contract as `memmove`.
    unsafe { memmove(dest.cast(), src.cast(), n) };
}

#[unsafe(no_mangle)]
unsafe extern "C" fn __aeabi_memset(dest: *mut u8, n: usize, value: i32) {
    // SAFETY: the same contract as `memset`.
    unsafe { memset(dest.cast(), value, n) };
}

#[unsafe(no_mangle)]
unsafe extern "C" fn __aeabi_memclr(dest: *mut u8, n: usize) {
    // SAFETY: the same contract as `memset`.
    unsafe { memset(dest.cast(), 0, n) };
}

#[unsafe(no_mangle)]
unsafe extern "C" fn __aeabi_memclr4(dest: *mut u8, n: usize) {
    // SAFETY: the same contract as `memset`.
    unsafe { memset(dest.cast(), 0, n) };
}

#[unsafe(no_mangle)]
unsafe extern "C" fn __aeabi_memclr8(dest: *mut u8, n: usize) {
    // SAFETY: the same contract as `memset`.
    unsafe { memset(dest.cast(), 0, n) };
}
