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

use std::time::Instant;

use orion_msg_session::Ping;

use crate::{ClientError, Session};

impl Session {
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
