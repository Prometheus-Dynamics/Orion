//! Doorbells: how a subscriber sleeps and a publisher wakes it.
//!
//! A subscriber has one doorbell for everything it subscribes to: an eventfd it can poll, and a
//! page of shared memory that only it writes, saying whether it is asleep. A publisher holds a
//! [`Bell`] for each subscriber: the eventfd, and the page mapped read-only.
//!
//! After a publish, the publisher looks at the page. Awake: nothing more, so a publish to a
//! subscriber that is keeping up stays a memory write. Asleep: one `write` to the eventfd, once
//! per sleep however many samples follow.
//!
//! No wake-up is lost. The subscriber says it is going to sleep, *then* looks for samples once
//! more, and only then sleeps. The publisher publishes, *then* looks at the page. Whichever
//! order those happen in, either the subscriber sees the sample or the publisher sees it asleep
//! ([`protocol`]).
//!
//! A subscriber's page is its own. A publisher cannot write it, and a subscriber that fills it
//! with nonsense only spoils its own wake-ups. A publisher never waits on a doorbell: ringing
//! one that is full, or whose owner is gone, does nothing.

#![cfg(target_os = "linux")]

#[allow(unsafe_code)]
mod eventfd;
pub mod protocol;

use std::io;
use std::os::fd::{AsFd, BorrowedFd, OwnedFd};
use std::time::Duration;

use orion_ring::{ReadMap, RingError, WriteMap, read_only, sealed_memfd};

/// Why [`Doorbell::wait`] returned.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Wake {
    /// There was already something to read. No system call was made.
    Ready,
    /// A publisher rang. Rarely there is nothing new behind it (a ring left over from a sleep
    /// that was called off), so read and wait again.
    Rung,
    /// Nothing happened within the time allowed.
    TimedOut,
}

/// A subscriber's doorbell: what it sleeps on.
#[derive(Debug)]
pub struct Doorbell {
    eventfd: OwnedFd,
    page: WriteMap,
    sleeps: u64,
}

impl Doorbell {
    /// Makes a doorbell.
    ///
    /// # Errors
    ///
    /// If the eventfd or the page can't be made.
    pub fn create() -> Result<Self, RingError> {
        Self::attach(eventfd::create()?, sealed_memfd(8)?)
    }

    /// Takes a doorbell made elsewhere: its eventfd and its page, writable.
    ///
    /// # Errors
    ///
    /// If the page can't be mapped for writing.
    pub fn attach(eventfd: OwnedFd, page: OwnedFd) -> Result<Self, RingError> {
        Ok(Self {
            eventfd,
            page: WriteMap::new(page)?,
            sleeps: 0,
        })
    }

    /// The publisher's side of this doorbell.
    ///
    /// # Errors
    ///
    /// If the descriptors can't be duplicated.
    pub fn bell(&self) -> Result<Bell, RingError> {
        let (eventfd, page) = self.bell_fds()?;
        Bell::attach(eventfd, page)
    }

    /// The two descriptors to pass to a publisher: the eventfd, and the page, read-only.
    ///
    /// # Errors
    ///
    /// If the descriptors can't be duplicated.
    pub fn bell_fds(&self) -> Result<(OwnedFd, OwnedFd), RingError> {
        Ok((self.eventfd.try_clone()?, read_only(self.page.fd())?))
    }

    /// Says "I am about to sleep". The caller must then look for samples once more, and sleep
    /// (poll the doorbell) only if there are none. For a caller with its own event loop;
    /// [`Doorbell::wait`] does all of it.
    pub fn arm(&mut self) {
        self.sleeps = self.sleeps.wrapping_add(1);
        protocol::arm(&self.page, self.sleeps);
    }

    /// Says "I am awake", and takes any ring out of the eventfd so the next poll blocks.
    pub fn disarm(&mut self) {
        protocol::disarm(&self.page, self.sleeps);
        eventfd::drain(self.eventfd.as_fd());
    }

    /// Sleeps until a publisher rings, unless `ready` says there is something to read. `ready`
    /// is what makes this safe against a sample that arrives just before the sleep: it is asked
    /// again after the doorbell is armed.
    ///
    /// # Errors
    ///
    /// If `poll` fails.
    pub fn wait(
        &mut self,
        timeout: Option<Duration>,
        mut ready: impl FnMut() -> bool,
    ) -> io::Result<Wake> {
        if ready() {
            return Ok(Wake::Ready);
        }
        self.arm();
        if ready() {
            // Awake again without having slept, and without a system call. If a publisher saw
            // the armed page in between and rang, the next wait returns `Rung` once for nothing,
            // which costs less than draining here every time.
            protocol::disarm(&self.page, self.sleeps);
            return Ok(Wake::Ready);
        }
        let rung = eventfd::wait(self.eventfd.as_fd(), timeout);
        self.disarm();
        Ok(if rung? { Wake::Rung } else { Wake::TimedOut })
    }
}

/// The doorbell is pollable: it reads as ready when a publisher has rung.
impl AsFd for Doorbell {
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.eventfd.as_fd()
    }
}

/// A publisher's handle on one subscriber's doorbell.
#[derive(Debug)]
pub struct Bell {
    eventfd: OwnedFd,
    page: ReadMap,
    rung: u64,
}

impl Bell {
    /// Takes the publisher's side of a doorbell: its eventfd and its page, read-only.
    ///
    /// # Errors
    ///
    /// If the page can't be mapped.
    pub fn attach(eventfd: OwnedFd, page: OwnedFd) -> Result<Self, RingError> {
        Ok(Self {
            eventfd,
            page: ReadMap::new(page)?,
            rung: 0,
        })
    }

    /// Whether the subscriber is asleep right now. For tools and benches; a publisher just
    /// calls [`Bell::ring`].
    #[must_use]
    pub fn asleep(&self) -> bool {
        use orion_ring::Mem;
        self.page
            .load(protocol::STATE_WORD, std::sync::atomic::Ordering::Acquire)
            & 1
            == 1
    }

    /// Call after publishing. Wakes the subscriber if it is asleep and hasn't been rung for this
    /// sleep yet, and says whether it did. With the subscriber awake this is a fence and one
    /// memory read: no system call.
    pub fn ring(&mut self) -> bool {
        if !protocol::wants_ring(&self.page, &mut self.rung) {
            return false;
        }
        eventfd::ring(self.eventfd.as_fd());
        true
    }
}
