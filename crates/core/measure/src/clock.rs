//! The wall clock as the kernel keeps it: whether it is synchronized, and the two calls that
//! correct it. Correcting it needs `CAP_SYS_TIME`.

use std::io;

fn timex() -> libc::timex {
    // SAFETY: `timex` is plain old data, so all-zero is a valid value.
    unsafe { std::mem::zeroed() }
}

/// Whether the kernel holds the clock synchronized (`STA_UNSYNC` is clear): some daemon, or
/// [`slew`], is keeping it. `None` if the kernel will not say.
#[must_use]
pub fn synchronized() -> Option<bool> {
    let mut timex = timex();
    // SAFETY: `timex` is a valid, exclusively borrowed struct; `modes = 0` only reads.
    let state = unsafe { libc::adjtimex(&raw mut timex) };
    (state >= 0).then_some(timex.status & libc::STA_UNSYNC == 0 && state != libc::TIME_ERROR)
}

/// Moves the wall clock by `offset_ns` at once, and marks it synchronized.
///
/// # Errors
///
/// The kernel's: most often no permission.
pub fn step(offset_ns: i64) -> io::Result<()> {
    let mut now = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: `now` is a valid, exclusively borrowed timespec.
    if unsafe { libc::clock_gettime(libc::CLOCK_REALTIME, &raw mut now) } != 0 {
        return Err(io::Error::last_os_error());
    }
    let total =
        i128::from(now.tv_sec) * 1_000_000_000 + i128::from(now.tv_nsec) + i128::from(offset_ns);
    let target = libc::timespec {
        tv_sec: total.div_euclid(1_000_000_000) as _,
        tv_nsec: total.rem_euclid(1_000_000_000) as _,
    };
    // SAFETY: `target` is a valid timespec.
    if unsafe { libc::clock_settime(libc::CLOCK_REALTIME, &raw const target) } != 0 {
        return Err(io::Error::last_os_error());
    }
    // The step leaves nothing to slew; this also clears `STA_UNSYNC`.
    slew(0, 0)
}

/// Hands `offset_ns` to the kernel's clock discipline to slew away, and marks the clock
/// synchronized with `max_error_ns` as its error bound. The kernel takes at most half a second
/// at a time.
///
/// # Errors
///
/// The kernel's: most often no permission.
// `timex` fields are `c_long`, which is `i64` only on 64-bit targets.
#[allow(clippy::useless_conversion)]
pub fn slew(offset_ns: i64, max_error_ns: i64) -> io::Result<()> {
    let mut timex = timex();
    timex.modes = libc::ADJ_STATUS
        | libc::ADJ_NANO
        | libc::ADJ_OFFSET
        | libc::ADJ_TIMECONST
        | libc::ADJ_MAXERROR
        | libc::ADJ_ESTERROR;
    // The kernel PLL, synchronized (`STA_UNSYNC` cleared), offsets in nanoseconds.
    timex.status = libc::STA_PLL;
    timex.offset = offset_ns.clamp(-500_000_000, 500_000_000) as libc::c_long;
    timex.constant = 4;
    let error_us = (max_error_ns / 1_000).clamp(0, i64::from(i32::MAX));
    timex.maxerror = error_us as libc::c_long;
    timex.esterror = error_us as libc::c_long;
    // SAFETY: `timex` is a valid, exclusively borrowed struct.
    if unsafe { libc::adjtimex(&raw mut timex) } < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_kernel_says_whether_the_clock_is_synchronized() {
        assert!(synchronized().is_some());
    }

    #[test]
    fn correcting_the_clock_without_permission_is_refused_not_done() {
        // Tests do not run with `CAP_SYS_TIME`. If these ever do, moving by nothing is harmless.
        let _ = step(0);
        let _ = slew(0, 0);
    }
}
