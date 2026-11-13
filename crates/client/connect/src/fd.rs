//! Descriptors that travel with frames. They travel only on the board's own socket, which is
//! Linux's. Elsewhere a frame never has any, and the types say so: there is no value of them.

#[cfg(unix)]
pub use std::os::fd::{BorrowedFd, OwnedFd};

#[cfg(not(unix))]
mod none {
    use std::convert::Infallible;
    use std::marker::PhantomData;

    /// A descriptor lent with a frame. There is none on this system.
    #[derive(Clone, Copy, Debug)]
    pub struct BorrowedFd<'a>(Infallible, PhantomData<&'a ()>);

    /// A descriptor that came with a frame. There is none on this system.
    #[derive(Debug)]
    pub struct OwnedFd(Infallible);
}

#[cfg(not(unix))]
pub use none::{BorrowedFd, OwnedFd};

#[cfg(target_os = "linux")]
pub(crate) use shm_fdpass::FdSet;

/// The descriptors that came with a frame: none, on this system.
#[cfg(not(target_os = "linux"))]
#[derive(Debug, Default)]
pub(crate) struct FdSet {
    _none: (),
}

#[cfg(not(target_os = "linux"))]
impl FdSet {
    pub(crate) const fn take(&mut self, _index: usize) -> Option<OwnedFd> {
        None
    }

    pub(crate) const fn len(&self) -> usize {
        0
    }
}
