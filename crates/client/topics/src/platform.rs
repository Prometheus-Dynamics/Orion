//! What differs between Linux, where a program may be on the board and share its rings, and
//! the systems where it may not: descriptors, which exist only on unix and travel only on
//! Linux, and how a subscriber sleeps.

use std::time::Duration;

use orion_connect::BorrowedFd;

use crate::{Subscriber, TopicError};

/// A second handle on a kept descriptor.
#[cfg(unix)]
pub(crate) fn copy(fd: &orion_connect::OwnedFd) -> Option<orion_connect::OwnedFd> {
    fd.try_clone().ok()
}

/// A second handle on a kept descriptor: there is none here.
#[cfg(not(unix))]
pub(crate) fn copy(_fd: &orion_connect::OwnedFd) -> Option<orion_connect::OwnedFd> {
    unreachable!("no descriptor exists here")
}

/// Lends a kept descriptor.
#[cfg(unix)]
pub(crate) fn lend(fd: Option<&orion_connect::OwnedFd>) -> Option<BorrowedFd<'_>> {
    fd.map(std::os::fd::AsFd::as_fd)
}

/// Lends a kept descriptor: there is none here.
#[cfg(not(unix))]
pub(crate) fn lend(fd: Option<&orion_connect::OwnedFd>) -> Option<BorrowedFd<'_>> {
    fd.map(|_| unreachable!("no descriptor exists here"))
}

/// Keeps a lent descriptor, to lend again after a reconnect.
#[cfg(unix)]
pub(crate) fn keep(fd: BorrowedFd<'_>) -> Option<orion_connect::OwnedFd> {
    fd.try_clone_to_owned().ok()
}

/// Keeps a lent descriptor: there is none here.
#[cfg(not(unix))]
pub(crate) fn keep(_fd: BorrowedFd<'_>) -> Option<orion_connect::OwnedFd> {
    unreachable!("no descriptor exists here")
}

impl Subscriber {
    /// Arms the doorbell before a sleep, or disarms it after.
    #[cfg(target_os = "linux")]
    pub(crate) fn arm(&self, arm: bool) {
        if let Some(doorbell) = &mut self.state.borrow_mut().doorbell {
            if arm {
                doorbell.arm();
            } else {
                doorbell.disarm();
            }
        }
    }

    /// Off the board there is no doorbell: a sample arrives as a message.
    #[cfg(not(target_os = "linux"))]
    #[allow(clippy::unused_self)]
    pub(crate) const fn arm(&self, _arm: bool) {}

    /// Sleeps on the doorbell and the session, for up to `sleep`.
    #[cfg(target_os = "linux")]
    #[allow(clippy::unnecessary_wraps)]
    pub(crate) fn sleep(&self, sleep: Duration) -> Result<(), TopicError> {
        use rustix::event::{PollFd, PollFlags, Timespec, poll};
        let timeout = Timespec {
            tv_sec: sleep.as_secs() as i64,
            tv_nsec: i64::from(sleep.subsec_nanos()),
        };
        let state = self.state.borrow();
        self.client.session(|session| {
            let session_fd = PollFd::from_borrowed_fd(session.as_fd(), PollFlags::IN);
            let _ = match &state.doorbell {
                Some(doorbell) => poll(
                    &mut [session_fd, PollFd::new(doorbell, PollFlags::IN)],
                    Some(&timeout),
                ),
                None => poll(&mut [session_fd], Some(&timeout)),
            };
        });
        Ok(())
    }

    /// Sleeps on the session for up to `sleep`, taking what comes: samples are messages here.
    #[cfg(not(target_os = "linux"))]
    pub(crate) fn sleep(&self, sleep: Duration) -> Result<(), TopicError> {
        self.client.pump(sleep)?;
        Ok(())
    }
}
