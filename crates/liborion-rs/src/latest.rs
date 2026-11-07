//! A client of an fd latest-value channel.

use std::os::fd::{FromRawFd, OwnedFd};
use std::path::Path;
use std::time::Duration;

use crate::sys::{self, Handle, OrionLatestFrame, Status};
use crate::{Error, c_string, check, check_version, error};

/// What a latest-value reply is (`ORION_LATEST_*`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum FrameStatus {
    /// A frame, with its payload and descriptors.
    Frame,
    /// Nothing published yet.
    Empty,
    /// The newest frame is older than the channel's maximum age.
    Stale,
    /// No newer frame within the wait.
    Timeout,
    /// The channel has its maximum of clients.
    Busy,
    /// A status this wrapper does not know (a newer library).
    Other(u32),
}

impl FrameStatus {
    fn of(status: u32) -> Self {
        match status {
            sys::ORION_LATEST_FRAME => Self::Frame,
            sys::ORION_LATEST_EMPTY => Self::Empty,
            sys::ORION_LATEST_STALE => Self::Stale,
            sys::ORION_LATEST_TIMEOUT => Self::Timeout,
            sys::ORION_LATEST_BUSY => Self::Busy,
            other => Self::Other(other),
        }
    }
}

/// One reply. The descriptors are the caller's (closed on drop).
#[derive(Debug)]
pub struct Frame {
    pub status: FrameStatus,
    /// The frame's sequence, or the newest known one for [`FrameStatus::Timeout`].
    pub sequence: u64,
    pub published_at_unix_nanos: u64,
    pub payload: Vec<u8>,
    pub fds: Vec<OwnedFd>,
}

/// A latest-value connection. Closed on drop.
#[derive(Debug)]
pub struct Latest {
    handle: Handle,
}

impl Latest {
    /// Connects to the channel at `path`. Checks the library's version first.
    pub fn connect(path: &Path) -> Result<Self, Error> {
        check_version()?;
        let path = c_string(&path.to_string_lossy(), "path")?;
        let mut handle = Handle::NULL;
        // SAFETY: a valid string and handle.
        check(unsafe { sys::orion_latest_connect(path.as_ptr(), &mut handle) })?;
        Ok(Self { handle })
    }

    /// The newest frame, at once.
    pub fn get(&mut self) -> Result<Frame, Error> {
        self.reply(None)
    }

    /// The first frame newer than `after_sequence`, waiting up to `wait` (the channel caps it).
    pub fn next(&mut self, after_sequence: u64, wait: Duration) -> Result<Frame, Error> {
        let ms = u32::try_from(wait.as_millis()).unwrap_or(u32::MAX);
        self.reply(Some((after_sequence, ms)))
    }

    fn reply(&mut self, after: Option<(u64, u32)>) -> Result<Frame, Error> {
        let mut payload = Vec::new();
        loop {
            let mut frame = OrionLatestFrame {
                struct_size: size_of::<OrionLatestFrame>() as u32,
                ..OrionLatestFrame::default()
            };
            let (buf, cap) = if payload.is_empty() {
                (core::ptr::null_mut(), 0)
            } else {
                (payload.as_mut_ptr(), payload.len())
            };
            // SAFETY: a valid frame and a buffer of `cap` bytes (or none).
            let status = unsafe {
                match after {
                    None => sys::orion_latest_get(self.handle, &mut frame, buf, cap),
                    Some((sequence, ms)) => {
                        sys::orion_latest_next(self.handle, sequence, ms, &mut frame, buf, cap)
                    }
                }
            };
            match status {
                Status::OK => {
                    payload.truncate(usize::try_from(frame.payload_len).unwrap_or(usize::MAX));
                    let count = (frame.fd_count as usize).min(sys::ORION_LATEST_MAX_FDS);
                    let fds = frame.fds[..count]
                        .iter()
                        // SAFETY: the library handed these descriptors over; each is owned once.
                        .map(|&fd| unsafe { OwnedFd::from_raw_fd(fd) })
                        .collect();
                    return Ok(Frame {
                        status: FrameStatus::of(frame.status),
                        sequence: frame.sequence,
                        published_at_unix_nanos: frame.published_at_unix_nanos,
                        payload,
                        fds,
                    });
                }
                // The reply is kept; the next call (get or next alike) returns it.
                Status::BUFFER_TOO_SMALL => {
                    let len = usize::try_from(frame.payload_len).unwrap_or(usize::MAX);
                    payload.resize(len, 0);
                }
                other => return Err(error(other)),
            }
        }
    }
}

impl Drop for Latest {
    fn drop(&mut self) {
        // SAFETY: this handle, closed once.
        let _ = unsafe { sys::orion_close(self.handle) };
    }
}
