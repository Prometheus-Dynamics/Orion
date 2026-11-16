//! Keeping a session alive, and knowing when it is not.
//!
//! The comm core ends a session it has not heard from for its idle time, so a client pings
//! twice a heartbeat: one ping lost or late still leaves two inside the shortest idle time
//! there is, three heartbeats. A ping is only sent while the program pumps the session (waits
//! on it, or calls into it): a program that stops doing that goes quiet, and a comm core on the
//! other end of a network or a link then takes it for gone.
//!
//! The other way round, a client takes a comm core it has not heard from for the idle time as
//! gone, except over the board's own socket. That socket says so the moment the comm core's end
//! is closed, so silence there is a comm core that is busy or stopped, not one that is gone,
//! and the session is kept. Neither side of a local session ends it for being quiet.

use std::time::{Duration, Instant};

use orion_msg_session::Ping;

use crate::{ClientError, Session};

/// How long a wait for an answer may go on: [`crate::Options::reply`], or
/// [`Session::set_reply`] for every wait after it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Reply {
    /// Each answer within [`crate::Options::timeout`], however long the node takes to send
    /// it. A busy node or a starved machine can make a healthy answer late.
    #[default]
    Timeout,
    /// Each answer within this long, a bound of its own: [`crate::Options::timeout`] then
    /// bounds connecting and saying hello alone.
    Within(Duration),
    /// For as long as the node is heard from. The wait ends when nothing at all has come from
    /// it for its idle time, the window the node declared in its welcome (`comm.idle_ms`,
    /// three heartbeats unless set): that is [`ClientError::Lost`]. Any frame renews the
    /// window, and the answers to the pings this client sends every half heartbeat while it
    /// waits are frames, so a node that is running is never silent that long. With a
    /// `ceiling`, the wait also ends that long after it began, as a timed-out
    /// [`ClientError::Io`]: the session is fine, the answer was slow.
    Alive {
        /// The longest one wait may take, if any.
        ceiling: Option<Duration>,
    },
}

/// A wait that ran out of time, with the session still good.
pub(crate) fn timed_out() -> ClientError {
    ClientError::Io(std::io::ErrorKind::TimedOut.into())
}

impl Session {
    /// How long since anything at all, a ping's answer included, came from the node.
    #[must_use]
    pub fn silent_for(&self) -> Duration {
        self.last_heard.elapsed()
    }

    /// How every wait for an answer from now on is bounded, until this is called again:
    /// [`Session::answer`] and everything that waits on one. Set it back after a request
    /// that needed a longer ceiling. Kept when the session connects again.
    pub fn set_reply(&mut self, reply: Reply) {
        self.options.reply = reply;
    }

    /// How waits for an answer are bounded now.
    #[must_use]
    pub const fn reply(&self) -> Reply {
        self.options.reply
    }

    /// How much longer a wait for an answer that began at `since` may go on, as
    /// [`Session::reply`] says; never zero.
    ///
    /// # Errors
    ///
    /// [`ClientError::Lost`] if the node has been silent for its idle time ([`Reply::Alive`]),
    /// and a timed-out [`ClientError::Io`] if the wait has had its time.
    pub fn reply_left(&self, since: Instant) -> Result<Duration, ClientError> {
        let now = Instant::now();
        let (limit, silent) = match self.options.reply {
            Reply::Timeout => (Some(self.options.timeout), None),
            Reply::Within(bound) => (Some(bound), None),
            Reply::Alive { ceiling } => (ceiling, Some(self.last_heard + self.welcomed.idle)),
        };
        let mut left = Duration::MAX;
        if let Some(silent) = silent {
            left = silent.saturating_duration_since(now);
            if left.is_zero() {
                return Err(ClientError::Lost);
            }
        }
        if let Some(limit) = limit {
            left = left.min((since + limit).saturating_duration_since(now));
            if left.is_zero() {
                return Err(timed_out());
            }
        }
        Ok(left)
    }

    /// When the comm core will have been silent too long, if silence ends this session.
    fn silent_at(&self) -> Option<Instant> {
        (!self.transport.is_local()).then(|| self.last_heard + self.welcomed.idle)
    }

    /// When to stop waiting for a frame: at `deadline`, when a ping is due, or when the comm
    /// core has been silent too long, whichever is first.
    pub(crate) fn wake_at(&self, deadline: Instant) -> Instant {
        let until = deadline.min(self.next_ping);
        self.silent_at().map_or(until, |silent| until.min(silent))
    }

    /// Sends a ping if one is due, and fails if the comm core has been silent too long.
    pub(crate) fn keep_alive(&mut self, now: Instant) -> Result<(), ClientError> {
        if self.silent_at().is_some_and(|silent| now >= silent) {
            return Err(ClientError::Lost);
        }
        if now >= self.next_ping {
            self.next_ping = now + self.welcomed.heartbeat / 2;
            let now_ms = self.started.elapsed().as_millis() as u64;
            self.send(&Ping { now_ms })?;
        }
        Ok(())
    }
}
