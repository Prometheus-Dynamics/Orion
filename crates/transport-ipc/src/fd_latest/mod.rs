//! Latest-value fd channel built on [`UnixFdFrame`].
//!
//! A producer publishes frames (an opaque payload plus file descriptors, e.g. one dmabuf or memfd
//! per plane) into a [`UnixFdLatestServer`]; only the most recent frame is retained. Any number of
//! consumers connect with [`UnixFdLatestClient`] over a Unix socket and ask for the current frame or
//! wait for one newer than a sequence they already hold. Each reply carries freshly dup'd
//! descriptors, so consumers own their copies independently of later publishes.
//!
//! Connections are persistent: a client can issue any number of requests over one socket.

mod client;
mod protocol;
mod server;

#[cfg(test)]
mod tests;

use std::time::{Duration, SystemTime};

use orion_transport_common::DEFAULT_TRANSPORT_IO_TIMEOUT;

use crate::{DEFAULT_UNIX_FD_FRAME_MAX_FDS, DEFAULT_UNIX_FD_FRAME_MAX_PAYLOAD_BYTES, UnixFdFrame};

pub use client::UnixFdLatestClient;
pub use server::{UnixFdLatestPublisher, UnixFdLatestServer};

pub const DEFAULT_UNIX_FD_LATEST_MAX_CLIENTS: usize = 64;
pub const DEFAULT_UNIX_FD_LATEST_MAX_WAIT: Duration = Duration::from_secs(30);

/// Server-side bounds for a [`UnixFdLatestServer`].
///
/// `max_payload_bytes` and `max_fds` bound what the producer may publish (and therefore what each
/// reply carries); the small protocol header is accounted for separately.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UnixFdLatestConfig {
    max_clients: usize,
    max_age: Option<Duration>,
    max_payload_bytes: usize,
    max_fds: usize,
    max_wait: Duration,
    io_timeout: Duration,
    idle_timeout: Option<Duration>,
}

impl Default for UnixFdLatestConfig {
    fn default() -> Self {
        Self {
            max_clients: DEFAULT_UNIX_FD_LATEST_MAX_CLIENTS,
            max_age: None,
            max_payload_bytes: DEFAULT_UNIX_FD_FRAME_MAX_PAYLOAD_BYTES,
            max_fds: DEFAULT_UNIX_FD_FRAME_MAX_FDS,
            max_wait: DEFAULT_UNIX_FD_LATEST_MAX_WAIT,
            io_timeout: DEFAULT_TRANSPORT_IO_TIMEOUT,
            idle_timeout: None,
        }
    }
}

impl UnixFdLatestConfig {
    pub fn new() -> Self {
        Self::default()
    }

    /// Maximum concurrently connected clients; extra connections receive a busy reply and are
    /// closed immediately.
    pub fn with_max_clients(mut self, max_clients: usize) -> Self {
        self.max_clients = max_clients.max(1);
        self
    }

    /// Frames older than `max_age` are reported as stale instead of being handed out.
    pub fn with_max_age(mut self, max_age: Duration) -> Self {
        self.max_age = Some(max_age);
        self
    }

    pub fn with_max_payload_bytes(mut self, max_payload_bytes: usize) -> Self {
        self.max_payload_bytes =
            max_payload_bytes.clamp(1, u32::MAX as usize - protocol::RESPONSE_HEADER_BYTES);
        self
    }

    pub fn with_max_fds(mut self, max_fds: usize) -> Self {
        self.max_fds = max_fds;
        self
    }

    /// Upper bound applied to the wait a client requests in `next_after`.
    pub fn with_max_wait(mut self, max_wait: Duration) -> Self {
        self.max_wait = max_wait;
        self
    }

    /// Timeout for writing a single reply to a client.
    pub fn with_io_timeout(mut self, io_timeout: Duration) -> Self {
        self.io_timeout = io_timeout.max(Duration::from_millis(1));
        self
    }

    /// Close connections that send no request for `idle_timeout`. Disabled by default.
    pub fn with_idle_timeout(mut self, idle_timeout: Duration) -> Self {
        self.idle_timeout = Some(idle_timeout.max(Duration::from_millis(1)));
        self
    }

    pub fn max_clients(&self) -> usize {
        self.max_clients
    }

    pub fn max_age(&self) -> Option<Duration> {
        self.max_age
    }

    pub fn max_payload_bytes(&self) -> usize {
        self.max_payload_bytes
    }

    pub fn max_fds(&self) -> usize {
        self.max_fds
    }

    pub fn max_wait(&self) -> Duration {
        self.max_wait
    }

    pub fn io_timeout(&self) -> Duration {
        self.io_timeout
    }

    pub fn idle_timeout(&self) -> Option<Duration> {
        self.idle_timeout
    }
}

/// A frame received from a [`UnixFdLatestServer`], with descriptors owned by the receiver.
#[derive(Debug)]
pub struct UnixFdLatestFrame {
    /// Monotonically increasing publish sequence, starting at 1.
    pub sequence: u64,
    /// Producer wall-clock time at publish.
    pub published_at: SystemTime,
    /// Opaque producer payload plus dup'd descriptors.
    pub frame: UnixFdFrame,
}

/// Outcome of a latest-value request.
#[derive(Debug)]
pub enum UnixFdLatestReply {
    /// The current (or newly published) frame.
    Frame(UnixFdLatestFrame),
    /// Nothing is currently published.
    Empty,
    /// A frame exists but is older than the server's `max_age`; it is not handed out.
    Stale {
        sequence: u64,
        published_at: SystemTime,
    },
    /// No frame newer than the requested sequence was published before the wait elapsed.
    Timeout { latest_sequence: Option<u64> },
}

impl UnixFdLatestReply {
    pub fn into_frame(self) -> Option<UnixFdLatestFrame> {
        match self {
            Self::Frame(frame) => Some(frame),
            _ => None,
        }
    }

    pub fn sequence(&self) -> Option<u64> {
        match self {
            Self::Frame(frame) => Some(frame.sequence),
            Self::Stale { sequence, .. } => Some(*sequence),
            Self::Timeout { latest_sequence } => *latest_sequence,
            Self::Empty => None,
        }
    }
}
