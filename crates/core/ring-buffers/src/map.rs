//! Mapping a pool's descriptors. The only `unsafe` in this crate.

use std::io;
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, OwnedFd};
use std::ptr::NonNull;

use orion_ring::RingError;

/// One descriptor of a pool, mapped whole.
#[derive(Debug)]
pub(crate) struct Mapped {
    base: NonNull<u8>,
    len: usize,
    fd: OwnedFd,
}

// SAFETY: the mapping is plain memory with no thread affinity, and the descriptor is `Send +
// Sync`. What may be done through it is decided by the methods below.
unsafe impl Send for Mapped {}
// SAFETY: as above.
unsafe impl Sync for Mapped {}

impl Mapped {
    /// Maps the first `len` bytes of `fd`, for writing too if `writable`.
    ///
    /// Refuses a descriptor that could shrink under the mapping: a regular or shared-memory
    /// file must be sealed against shrinking. A dma-buf is neither and cannot be resized.
    pub(crate) fn new(fd: OwnedFd, len: usize, writable: bool) -> Result<Self, RingError> {
        // SAFETY: plain old data; all zeroes is a valid `stat`.
        let mut stat: libc::stat = unsafe { std::mem::zeroed() };
        // SAFETY: `fd` is open and `stat` is a valid place to write one `stat`.
        if unsafe { libc::fstat(fd.as_raw_fd(), &raw mut stat) } != 0 {
            return Err(io::Error::last_os_error().into());
        }
        if stat.st_mode & libc::S_IFMT == libc::S_IFREG {
            // SAFETY: `fd` is open; `F_GET_SEALS` takes no argument.
            let seals = unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_GET_SEALS) };
            if seals < 0 || seals & libc::F_SEAL_SHRINK == 0 {
                return Err(RingError::Unsealed);
            }
            if u64::try_from(stat.st_size).unwrap_or(0) < len as u64 {
                return Err(RingError::Malformed(
                    "a buffer lies past the end of its descriptor",
                ));
            }
        }
        if len == 0 {
            return Err(RingError::Malformed("an empty buffer descriptor"));
        }
        let protection = if writable {
            libc::PROT_READ | libc::PROT_WRITE
        } else {
            libc::PROT_READ
        };
        // SAFETY: a fresh shared mapping of an open descriptor, placed by the kernel. It
        // aliases no Rust object.
        let base = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                len,
                protection,
                libc::MAP_SHARED,
                fd.as_raw_fd(),
                0,
            )
        };
        if base == libc::MAP_FAILED {
            return Err(io::Error::last_os_error().into());
        }
        let base = NonNull::new(base.cast::<u8>())
            .ok_or(RingError::Malformed("mapped at address zero"))?;
        Ok(Self { base, len, fd })
    }

    pub(crate) fn fd(&self) -> BorrowedFd<'_> {
        self.fd.as_fd()
    }

    /// `len` bytes at `offset`, to read. `None` if that isn't inside the mapping.
    ///
    /// The bytes belong to the publisher, which reuses the buffer for a later frame. The caller
    /// only hands this out while the frame is held (see `Held`), which is what keeps an honest
    /// publisher from writing here. A publisher that writes anyway gives its own subscribers a
    /// torn picture and nothing worse: the memory stays mapped and in bounds.
    pub(crate) fn bytes(&self, offset: usize, len: usize) -> Option<&[u8]> {
        let end = offset.checked_add(len)?;
        if end > self.len {
            return None;
        }
        // SAFETY: `offset + len` is inside the mapping, which lives as long as `self` and
        // cannot shrink (checked in `new`). `u8` has no alignment or validity requirement.
        Some(unsafe { std::slice::from_raw_parts(self.base.as_ptr().add(offset), len) })
    }

    /// `len` bytes at `offset`, to write: the publisher filling a buffer it has taken back.
    pub(crate) fn bytes_mut(&mut self, offset: usize, len: usize) -> Option<&mut [u8]> {
        let end = offset.checked_add(len)?;
        if end > self.len {
            return None;
        }
        // SAFETY: in bounds as above, and `&mut self` makes this the only slice into the
        // mapping from this process.
        Some(unsafe { std::slice::from_raw_parts_mut(self.base.as_ptr().add(offset), len) })
    }
}

impl Drop for Mapped {
    fn drop(&mut self) {
        // SAFETY: this is the mapping made in `new`, unmapped once; every slice handed out
        // borrowed `self`.
        unsafe { libc::munmap(self.base.as_ptr().cast(), self.len) };
    }
}
