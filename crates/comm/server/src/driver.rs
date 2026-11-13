//! The tasks that move frames between connections and the core.

use std::cell::RefCell;
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

use crate::server::{Flow, Server, Stats};
use crate::sessions::{Popped, SessionId, Sessions};
use crate::{Carrier, Connection, FrameRead, FrameWrite, Limits, Service};

/// How many frames a session's reader handles before it lets the others run.
const TURN: u32 = 32;

/// The comm core's server. Cloning shares it. Everything it does happens on the thread, and
/// inside the `LocalSet`, it was made on.
#[derive(Clone)]
pub struct Comm {
    core: Rc<RefCell<Server>>,
    started: Instant,
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
        self.started.elapsed().as_millis() as u64
    }

    /// Runs `f` with every session, for a service's own task to send from.
    pub fn with<R>(&self, f: impl FnOnce(&mut Sessions, u64) -> R) -> R {
        let now_ms = self.now_ms();
        f(&mut self.core.borrow_mut().sessions, now_ms)
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
        let mut deadline = Instant::now() + Duration::from_millis(u64::from(limits.hello_ms));
        let mut turn = 0;
        loop {
            let received = tokio::select! {
                received = timeout_at(deadline, read.recv(&mut buf, &mut fds)) => received,
                () = stop.notified() => break,
            };
            let Ok(Ok(Some(len))) = received else {
                // The deadline, an error, or the other side closed.
                if received.is_err() {
                    self.core.borrow_mut().stats.dropped += 1;
                }
                break;
            };
            let now_ms = self.now_ms();
            let flow = self
                .core
                .borrow_mut()
                .receive(session, &buf[..len], &mut fds, now_ms);
            // Descriptors the service did not take are closed here.
            fds = FdSet::default();
            if flow == Flow::Close {
                break;
            }
            deadline = Instant::now() + Duration::from_millis(limits.idle_ms());
            turn += 1;
            if turn == TURN {
                turn = 0;
                tokio::task::yield_now().await;
            }
        }
        // Let what is queued (a reject, a last answer) go out, within the write deadline.
        self.core.borrow_mut().sessions.close(session);
        let flush = Duration::from_millis(u64::from(limits.write_ms));
        if timeout(flush, &mut writer).await.is_err() {
            writer.abort();
        }
        self.core.borrow_mut().close(session);
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
                    if !matches!(sent, Ok(Ok(()))) {
                        // It is not taking what it is sent. Stop the reader too.
                        let mut core = self.core.borrow_mut();
                        core.stats.dropped += 1;
                        core.sessions.close(session);
                        return;
                    }
                }
                Popped::Empty => wake.notified().await,
                Popped::Closed => return,
            }
        }
    }
}
