//! The bounds every session lives inside.

use orion_config::{ConfigError, Source};

/// No idle time is shorter than this, however short the heartbeat.
pub const IDLE_FLOOR_MS: u64 = 1000;

/// What a session may use and how long it may take.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Limits {
    /// `comm.max_sessions`: how many sessions there can be at once.
    pub max_sessions: u32,
    /// `comm.max_frame`: the largest frame, in bytes, in either direction.
    pub max_frame: u32,
    /// `comm.outbox_bytes`: how much may wait to be sent to one session.
    pub outbox_bytes: u32,
    /// `comm.heartbeat_ms`: how often a session is asked to ping.
    pub heartbeat_ms: u32,
    /// `comm.idle_ms`: how long a session that is not a program on this board may go unheard
    /// before it is closed, and the comm core before a client gives up on it. In settings, zero
    /// is three heartbeats or [`IDLE_FLOOR_MS`], whichever is longer, and anything else must be
    /// at least that.
    pub idle_ms: u32,
    /// `comm.hello_ms`: how long a new connection has to say hello.
    pub hello_ms: u32,
    /// `comm.write_ms`: how long one frame may take to be taken by the other side.
    pub write_ms: u32,
    /// Not a setting: the additive level the comm core names in its `Welcome`. This build's
    /// own; set lower only to stand in for an older node, in a test. Never above this build's.
    pub level: u16,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_sessions: 256,
            max_frame: 64 * 1024,
            outbox_bytes: 256 * 1024,
            heartbeat_ms: 2000,
            idle_ms: 0,
            hello_ms: 5000,
            write_ms: 5000,
            level: orion_wire::INTERFACE_LEVEL,
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
            idle_ms: source.get("comm.idle_ms", default.idle_ms)?,
            hello_ms: source.get("comm.hello_ms", default.hello_ms)?,
            write_ms: source.get("comm.write_ms", default.write_ms)?,
            level: default.level,
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
        if limits.idle_ms != 0 && u64::from(limits.idle_ms) < limits.idle_floor() {
            // A client pings twice a heartbeat. Less than three heartbeats leaves no room for
            // one late ping, and under a second a busy board's scheduler alone can use up.
            return Err(bad(
                "comm.idle_ms",
                &format!(
                    "must be zero or at least {} (three heartbeats, and no less than \
                     {IDLE_FLOOR_MS} ms)",
                    limits.idle_floor()
                ),
            ));
        }
        // With none said and heartbeats this short, three of them would be under the floor.
        let mut limits = limits;
        if limits.idle_ms == 0 && limits.idle_ms() < IDLE_FLOOR_MS {
            limits.idle_ms = IDLE_FLOOR_MS as u32;
        }
        Ok(limits)
    }

    /// How long a session may go unheard before it is closed: `comm.idle_ms`, or three
    /// heartbeats. Limits read from settings are never under [`Limits::idle_floor`]; ones a
    /// test builds itself are what it says.
    #[must_use]
    pub const fn idle_ms(&self) -> u64 {
        if self.idle_ms == 0 {
            self.heartbeat_ms as u64 * 3
        } else {
            self.idle_ms as u64
        }
    }

    /// The shortest idle time these heartbeats allow: three of them, and never under
    /// [`IDLE_FLOOR_MS`].
    #[must_use]
    pub const fn idle_floor(&self) -> u64 {
        let three = self.heartbeat_ms as u64 * 3;
        if three < IDLE_FLOOR_MS {
            IDLE_FLOOR_MS
        } else {
            three
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limits(text: &str) -> Result<Limits, ConfigError> {
        let mut source = Source::parse(text, []).expect("settings");
        Limits::from_source(&mut source)
    }

    #[test]
    fn the_idle_time_is_never_under_three_heartbeats_or_a_second() {
        // None said: three heartbeats, or the floor when those are shorter.
        assert_eq!(limits("").expect("defaults").idle_ms(), 6000);
        let short = limits("comm.heartbeat_ms = 100\n").expect("short heartbeats");
        assert_eq!(short.idle_ms(), IDLE_FLOOR_MS);
        // Said: refused under three heartbeats, with the number it must reach.
        let refused = limits("comm.idle_ms = 5999\n").expect_err("under three heartbeats");
        assert_eq!(refused.key, "comm.idle_ms");
        assert!(
            refused.message.contains("at least 6000"),
            "{}",
            refused.message
        );
        assert_eq!(
            limits("comm.idle_ms = 6000\n").expect("three").idle_ms(),
            6000
        );
        // And under a second, however short the heartbeats.
        let text = "comm.heartbeat_ms = 100\ncomm.idle_ms = 999\n";
        let refused = limits(text).expect_err("under a second");
        assert!(
            refused.message.contains("at least 1000"),
            "{}",
            refused.message
        );
        let text = "comm.heartbeat_ms = 100\ncomm.idle_ms = 1000\n";
        assert_eq!(limits(text).expect("a second").idle_ms(), 1000);
    }
}
