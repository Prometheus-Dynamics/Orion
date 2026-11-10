//! The eventfd system calls. The only `unsafe` in this crate.

use std::io;
use std::os::fd::{AsRawFd, BorrowedFd, FromRawFd, OwnedFd};
use std::time::Duration;

/// A non-blocking eventfd.
pub(crate) fn create() -> io::Result<OwnedFd> {
    // SAFETY: valid flags; the descriptor is new and owned by nothing else.
    let raw = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) };
    if raw < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: as above.
    Ok(unsafe { OwnedFd::from_raw_fd(raw) })
}

/// Adds one to the eventfd, making it readable. A full counter or a closed reader is not the
/// publisher's problem: the result is ignored on purpose.
pub(crate) fn ring(fd: BorrowedFd<'_>) {
    let one = 1_u64.to_ne_bytes();
    // SAFETY: `fd` is open and `one` is eight readable bytes.
    let _ = unsafe { libc::write(fd.as_raw_fd(), one.as_ptr().cast(), one.len()) };
}

/// Empties the eventfd so it stops reading as ready. It is non-blocking, so an empty one
/// returns at once.
pub(crate) fn drain(fd: BorrowedFd<'_>) {
    let mut count = [0_u8; 8];
    // SAFETY: `fd` is open and `count` is eight writable bytes.
    let _ = unsafe { libc::read(fd.as_raw_fd(), count.as_mut_ptr().cast(), count.len()) };
}

/// Blocks until the eventfd is readable or `timeout` passes. `true` if it is readable.
pub(crate) fn wait(fd: BorrowedFd<'_>, timeout: Option<Duration>) -> io::Result<bool> {
    let mut poll = libc::pollfd {
        fd: fd.as_raw_fd(),
        events: libc::POLLIN,
        revents: 0,
    };
    // Rounded up, so a short timeout doesn't become a busy loop.
    let millis = timeout.map_or(-1, |timeout| {
        libc::c_int::try_from(timeout.as_nanos().div_ceil(1_000_000)).unwrap_or(libc::c_int::MAX)
    });
    loop {
        // SAFETY: `poll` is one valid `pollfd`.
        let ready = unsafe { libc::poll(&raw mut poll, 1, millis) };
        if ready >= 0 {
            return Ok(ready > 0);
        }
        let err = io::Error::last_os_error();
        if err.kind() != io::ErrorKind::Interrupted {
            return Err(err);
        }
    }
}
