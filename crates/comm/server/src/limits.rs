//! The bounds every session lives inside.

use orion_config::{ConfigError, Source};

/// What a session may use and how long it may take.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Limits {
    /// `comm.max_sessions`: how many sessions there can be at once.
    pub max_sessions: u32,
    /// `comm.max_frame`: the largest frame, in bytes, in either direction.
    pub max_frame: u32,
    /// `comm.outbox_bytes`: how much may wait to be sent to one session.
    pub outbox_bytes: u32,
    /// `comm.heartbeat_ms`: how often a session is asked to ping. A session not heard from
    /// for three of these is closed.
    pub heartbeat_ms: u32,
    /// `comm.hello_ms`: how long a new connection has to say hello.
    pub hello_ms: u32,
    /// `comm.write_ms`: how long one frame may take to be taken by the other side.
    pub write_ms: u32,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_sessions: 256,
            max_frame: 64 * 1024,
            outbox_bytes: 256 * 1024,
            heartbeat_ms: 2000,
            hello_ms: 5000,
            write_ms: 5000,
        }
    }
}

impl Limits {
    /// Takes the limits out of `source`.
    ///
    /// # Errors
    ///
    /// A value that doesn't parse, or one that can't work.
    pub fn from_source(source: &mut Source) -> Result<Self, ConfigError> {
        let default = Self::default();
        let limits = Self {
            max_sessions: source.get("comm.max_sessions", default.max_sessions)?,
            max_frame: source.get("comm.max_frame", default.max_frame)?,
            outbox_bytes: source.get("comm.outbox_bytes", default.outbox_bytes)?,
            heartbeat_ms: source.get("comm.heartbeat_ms", default.heartbeat_ms)?,
            hello_ms: source.get("comm.hello_ms", default.hello_ms)?,
            write_ms: source.get("comm.write_ms", default.write_ms)?,
        };
        let bad = |key: &str, message: &str| ConfigError {
            key: key.into(),
            message: message.into(),
        };
        if limits.max_sessions == 0 || limits.max_sessions > 65_535 {
            return Err(bad("comm.max_sessions", "must be 1 to 65535"));
        }
        if limits.max_frame < 256 {
            return Err(bad("comm.max_frame", "must be at least 256"));
        }
        if limits.outbox_bytes < limits.max_frame.saturating_add(4) {
            return Err(bad("comm.outbox_bytes", "must hold at least one frame"));
        }
        if limits.heartbeat_ms == 0 || limits.hello_ms == 0 || limits.write_ms == 0 {
            return Err(bad("comm", "deadlines must be above zero"));
        }
        Ok(limits)
    }

    /// How long a session may go unheard before it is closed.
    #[must_use]
    pub const fn idle_ms(&self) -> u64 {
        self.heartbeat_ms as u64 * 3
    }
}
