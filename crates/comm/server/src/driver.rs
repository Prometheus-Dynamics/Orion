//! The tasks that move frames between connections and the core.

use std::cell::RefCell;
use std::io;
use std::os::fd::OwnedFd;
use std::rc::Rc;
use std::time::Duration;

use orion_check::{Grants, Security};
use orion_msg_session::{Reject, RejectReason};
use orion_supervise::ACCESS_RETRY_MS;
use orion_types::NodeName;
use orion_wire::encode_body;
use shm_fdpass::FdSet;
use tokio::sync::Notify;
use tokio::time::{Instant, sleep, timeout, timeout_at};

use crate::server::{DROPS_KEPT, DroppedSession, Flow, Server, Stats};
use crate::sessions::{Dropped, Popped, SessionId, Sessions};
use crate::{Carrier, Connection, FrameRead, FrameWrite, Limits, Service};

/// How many frames a session's reader handles before it lets the others run.
const TURN: u32 = 32;

/// The most frames read from a session whose program closed its end: what it sent before it
/// closed, and no more.
const LAST_WORDS: u32 = 1024;

/// The comm core's server. Cloning shares it. Everything it does happens on the thread, and
/// inside the `LocalSet`, it was made on.
#[derive(Clone)]
pub struct Comm {
    core: Rc<RefCell<Server>>,
    started: Instant,
}

/// A [`Comm`] that does not keep it alive.
#[derive(Clone)]
pub struct WeakComm {
    core: std::rc::Weak<RefCell<Server>>,
    started: Instant,
}

impl WeakComm {
    /// The server, if it is still there.
    #[must_use]
    pub fn upgrade(&self) -> Option<Comm> {
        Some(Comm {
            core: self.core.upgrade()?,
            started: self.started,
        })
    }
}

impl std::fmt::Debug for Comm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Comm")
    }
}

impl Comm {
    /// A server for `node`. With `security` on, `grants` says what each operator may do; `None`
    /// lets every admitted operator do everything.
    #[must_use]
    pub fn new(
        node: NodeName,
        security: Security,
        grants: Option<Rc<dyn Grants>>,
        limits: Limits,
    ) -> Self {
        Self {
            core: Rc::new(RefCell::new(Server::new(node, security, grants, limits))),
            started: Instant::now(),
        }
    }

    /// Adds a service.
    ///
    /// # Panics
    ///
    /// If it answers a kind another service already answers, or one of the server's own. That
    /// is a mistake in how the program is assembled, found the first time it starts.
    pub fn add_service(&self, service: impl Service + 'static) {
        assert!(
            self.core.borrow_mut().add(Box::new(service)),
            "two services answer the same kind"
        );
    }

    /// Milliseconds on the server's clock: since it was made.
    #[must_use]
    pub fn now_ms(&self) -> u64 {
        self.now().as_millis() as u64
    }

    /// The server's clock, finer than [`Comm::now_ms`].
    fn now(&self) -> Duration {
        self.started.elapsed()
    }

    /// A handle that does not keep the server alive: for a service's own tasks, since the
    /// server holds the service.
    #[must_use]
    pub fn downgrade(&self) -> WeakComm {
        WeakComm {
            core: Rc::downgrade(&self.core),
            started: self.started,
        }
    }

    /// Runs `f` with every session, for a service's own task to send from.
    pub fn with<R>(&self, f: impl FnOnce(&mut Sessions, u64) -> R) -> R {
        let now_ms = self.now_ms();
        f(&mut self.core.borrow_mut().sessions, now_ms)
    }

    /// The last sessions the comm core ended, oldest first, with why. A session its own
    /// program closed is not one.
    #[must_use]
    pub fn drops(&self) -> Vec<DroppedSession> {
        self.core.borrow().drops.iter().cloned().collect()
    }

    /// What the server has seen so far.
    #[must_use]
    pub fn stats(&self) -> Stats {
        self.core.borrow().stats
    }

    /// Accepts connections from `carrier` for as long as the program runs. A failed accept is
    /// tried again after a short, fixed wait: a carrier is a way in, and ways in are never
    /// backed off.
    pub async fn serve(&self, mut carrier: impl Carrier) {
        loop {
            match carrier.accept().await {
                Ok(connection) => {
                    self.attach(connection);
                }
                Err(error) => {
                    tracing::warn!(%error, "accepting a connection failed");
                    sleep(Duration::from_millis(u64::from(ACCESS_RETRY_MS))).await;
                }
            }
        }
    }

    /// Serves one connection, in its own task: one the program made itself, such as a link or
    /// an outbound peer.
    pub fn attach(&self, connection: impl Connection + 'static) {
        let comm = self.clone();
        tokio::task::spawn_local(async move { comm.run(connection).await });
    }

    async fn run(self, connection: impl Connection) {
        let limits = self.core.borrow().sessions.limits;
        let opened = self.core.borrow_mut().open(&connection);
        let (mut read, mut write) = connection.split();
        let Some(session) = opened else {
            // Full. Say so, so the other side knows to try again, and let go.
            let mut frame = [0_u8; 32];
            let reject = Reject {
                reason: RejectReason::Busy,
                node_revision: 0,
                peer_revision: 0,
            };
            if let Ok(len) = encode_body(&reject, 0, &mut frame) {
                let write_ms = Duration::from_millis(u64::from(limits.write_ms));
                let _ = timeout(write_ms, write.send(&frame[..len], &[])).await;
            }
            return;
        };
        let (wake, stop) = {
            let core = self.core.borrow();
            let entry = core
                .sessions
                .entry(session)
                .expect("a session just opened is there");
            (entry.wake.clone(), entry.stop.clone())
        };
        let mut writer = tokio::task::spawn_local(self.clone().write(session, wake, write));

        let mut buf = vec![0_u8; limits.max_frame as usize];
        let mut fds = FdSet::default();
        // `None` while a local session is quiet past the idle time: it is waited for.
        let mut deadline = Some(Instant::now() + Duration::from_millis(u64::from(limits.hello_ms)));
        let mut heard = self.now();
        let mut turn = 0;
        loop {
            let received = tokio::select! {
                received = receive(deadline, read.recv(&mut buf, &mut fds)) => received,
                () = stop.notified() => {
                    if self.core.borrow().sessions.is_gone(session) {
                        self.last_words(session, &mut read, &mut buf).await;
                    }
                    break;
                }
            };
            let len = match received {
                Ok(Ok(Some(len))) => len,
                // The other side closed: its own doing, and nothing to say.
                Ok(Ok(None)) => break,
                Ok(Err(error)) => {
                    let mut core = self.core.borrow_mut();
                    core.sessions
                        .dropped(session, Dropped::ReadFailed(error.kind()));
                    break;
                }
                Err(_) => {
                    if self.quiet(session, heard) {
                        deadline = None;
                        continue;
                    }
                    break;
                }
            };
            let now = self.now();
            let now_ms = now.as_millis() as u64;
            self.heard(session, now_ms);
            heard = now;
            let flow = self
                .core
                .borrow_mut()
                .receive(session, &buf[..len], &mut fds, now_ms);
            // Descriptors the service did not take are closed here.
            fds = FdSet::default();
            if flow == Flow::Close {
                break;
            }
            deadline = Some(Instant::now() + Duration::from_millis(limits.idle_ms()));
            turn += 1;
            if turn == TURN {
                turn = 0;
                tokio::task::yield_now().await;
            }
        }
        // Let what is queued (a reject, a last answer) go out, within the write deadline.
        self.core.borrow_mut().sessions.close(session);
        self.say_dropped(session);
        let flush = Duration::from_millis(u64::from(limits.write_ms));
        if timeout(flush, &mut writer).await.is_err() {
            writer.abort();
        }
        self.core.borrow_mut().close(session);
    }

    /// The deadline passed with nothing heard from `session` since `heard`. Whether it is
    /// kept: a local session that has said hello is, and is now seen as quiet. Any other is
    /// dropped, and why is noted.
    fn quiet(&self, session: SessionId, heard: Duration) -> bool {
        let quiet_ms = quiet_for(heard, self.now());
        let heard_ms = heard.as_millis() as u64;
        let mut core = self.core.borrow_mut();
        let limits = core.sessions.limits;
        let Some(entry) = core.sessions.entry_mut(session) else {
            return false;
        };
        let why = if !entry.info.open {
            Dropped::NoHello {
                limit_ms: u64::from(limits.hello_ms),
            }
        } else if entry.info.local {
            // Its socket says when every holder has closed it, so quiet is not gone: its
            // program stopped pumping the session, or is stopped itself. Kept, and seen.
            entry.info.quiet_since = Some(heard_ms);
            tracing::warn!(
                "session {} ({}) has been quiet for {quiet_ms} ms, past the idle time of {} ms: \
                 it is kept, since its socket is open, but its program is not pumping it",
                entry.info.name,
                entry.info.principal,
                limits.idle_ms()
            );
            core.quiet(session, quiet_ms.max(1));
            return true;
        } else {
            Dropped::Quiet {
                quiet_ms,
                limit_ms: limits.idle_ms(),
            }
        };
        entry.why.get_or_insert(why);
        core.stats.dropped += 1;
        false
    }

    /// A frame came from `session` at `now_ms`: if it was quiet, it no longer is.
    fn heard(&self, session: SessionId, now_ms: u64) {
        let mut core = self.core.borrow_mut();
        let Some(entry) = core.sessions.entry_mut(session) else {
            return;
        };
        if let Some(since) = entry.info.quiet_since.take() {
            tracing::info!(
                "session {} ({}) is heard again after {} ms",
                entry.info.name,
                entry.info.principal,
                now_ms.saturating_sub(since)
            );
            core.quiet(session, 0);
        }
    }

    /// Says in the log why the comm core ended `session`, if it did, and keeps it to be asked
    /// for ([`Comm::drops`]).
    fn say_dropped(&self, session: SessionId) {
        let at_ms = self.now_ms();
        let mut core = self.core.borrow_mut();
        let Some(entry) = core.sessions.entry(session) else {
            return;
        };
        let Some(why) = entry.why.clone() else {
            return;
        };
        let dropped = DroppedSession {
            name: entry.info.name.clone(),
            principal: entry.info.principal.to_string(),
            at_ms,
            why,
        };
        let name = if entry.info.open {
            dropped.name.as_str()
        } else {
            "(no hello)"
        };
        tracing::warn!(
            "session {name} ({}) dropped: {}",
            dropped.principal,
            dropped.why
        );
        if core.drops.len() == DROPS_KEPT {
            core.drops.pop_front();
        }
        core.drops.push_back(dropped);
    }

    /// Handles what `session`'s program sent before it closed its end, as far as it is already
    /// here: a client that says why it stops and goes is heard, not taken as gone without a
    /// word. Up to [`LAST_WORDS`] frames, none waited for.
    async fn last_words(&self, session: SessionId, read: &mut impl FrameRead, buf: &mut [u8]) {
        for _ in 0..LAST_WORDS {
            let mut fds = FdSet::default();
            let Ok(Ok(Some(len))) = timeout(Duration::ZERO, read.recv(buf, &mut fds)).await else {
                return;
            };
            let now_ms = self.now_ms();
            let flow = self
                .core
                .borrow_mut()
                .receive(session, &buf[..len], &mut fds, now_ms);
            if flow == Flow::Close {
                return;
            }
        }
    }

    async fn write(self, session: SessionId, wake: Rc<Notify>, mut write: impl FrameWrite) {
        let write_ms =
            Duration::from_millis(u64::from(self.core.borrow().sessions.limits.write_ms));
        let mut frame = Vec::new();
        let mut fds: Vec<OwnedFd> = Vec::new();
        loop {
            let popped = self
                .core
                .borrow_mut()
                .sessions
                .pop(session, &mut frame, &mut fds);
            match popped {
                Popped::Frame => {
                    let sent = timeout(write_ms, write.send(&frame, &fds)).await;
                    fds.clear();
                    let why = match sent {
                        Ok(Ok(())) => continue,
                        // It is not taking what it is sent.
                        Err(_) => Dropped::WriteStalled {
                            kind: frame_kind(&frame),
                            waiting: self.core.borrow().sessions.pending(session),
                            limit_ms: write_ms.as_millis() as u64,
                        },
                        Ok(Err(error)) => Dropped::WriteFailed {
                            kind: frame_kind(&frame),
                            error: error.kind(),
                        },
                    };
                    let mut core = self.core.borrow_mut();
                    core.stats.dropped += 1;
                    if matches!(
                        why,
                        Dropped::WriteFailed {
                            error: io::ErrorKind::BrokenPipe | io::ErrorKind::ConnectionReset,
                            ..
                        }
                    ) {
                        // Its program closed its end: what it said before it went is still
                        // read.
                        core.sessions.gone(session, why);
                        return;
                    }
                    // Stop the reader too.
                    core.sessions.end(session, Some(why));
                    return;
                }
                Popped::Empty => wake.notified().await,
                Popped::Closed => return,
            }
        }
    }
}

/// How long a session heard last at `heard` has been quiet at `now`, both on the server's
/// clock, in the whole milliseconds the services are told: rounded down, so none is told it
/// was quiet longer than it was. Each rounded first, the two could make it up to 1 ms more,
/// and a lease taken from a quiet holder ([`crate::Service::quiet`]) would be taken early.
fn quiet_for(heard: Duration, now: Duration) -> u64 {
    now.saturating_sub(heard).as_millis() as u64
}

/// The kind of an encoded frame, for saying which one was not taken.
fn frame_kind(frame: &[u8]) -> u8 {
    orion_wire::frame::decode(frame).map_or(0, |view| view.kind())
}

/// `read`, given up at `deadline` if there is one.
async fn receive<T>(
    deadline: Option<Instant>,
    read: impl Future<Output = T>,
) -> Result<T, tokio::time::error::Elapsed> {
    match deadline {
        Some(deadline) => timeout_at(deadline, read).await,
        None => Ok(read.await),
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::quiet_for;

    /// A service is never told a session was quiet for longer than it was, even when the
    /// times sit either side of a millisecond's edge: what a lease taken from a quiet holder
    /// counts on.
    #[test]
    fn quiet_is_never_said_longer_than_it_was() {
        let ms = |micros: u64| Duration::from_micros(micros);
        for (heard, now) in [
            (ms(10_900), ms(1_510_000)),
            (ms(999), ms(1_000)),
            (ms(10_000), ms(1_510_000)),
            (ms(10_100), ms(1_510_099)),
        ] {
            let said = quiet_for(heard, now);
            assert!(
                Duration::from_millis(said) <= now - heard,
                "{said} ms said for {:?}",
                now - heard
            );
        }
        // And no shorter than the whole milliseconds that passed.
        assert_eq!(quiet_for(ms(10_900), ms(1_510_000)), 1_499);
    }
}
