//! The server's core: a frame comes in, the right thing happens, frames are queued. No I/O.

use std::rc::Rc;

use orion_check::{Check, Grants, Refused, Security};
use orion_fdpass::FdSet;
use orion_msg_session::{
    Hello, Ping, Pong, Refusal, Reject, RejectReason, Welcome, session_revision,
};
use orion_supervise::contain;
use orion_types::{Error, NodeName};
use orion_wire::frame;
use orion_wire::{Body, Decode, FrameView, INTERFACE_REVISION, kind};

use crate::service::{Cx, Incoming, Service};
use crate::sessions::{Delivery, SessionId, Sessions};
use crate::{Connection, Limits};

/// No service answers this kind.
const NO_ROUTE: u8 = u8::MAX;

/// Counts of what the server has seen, for its own state topic.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Stats {
    /// Sessions ever opened.
    pub opened: u64,
    /// Connections turned away: full, wrong version, a role not offered.
    pub rejected: u64,
    /// Requests handed to a service.
    pub requests: u64,
    /// Requests the check or a service refused.
    pub refused: u64,
    /// Frames of a kind no service answers, ignored.
    pub unknown: u64,
    /// Sessions closed for breaking the protocol or a deadline.
    pub dropped: u64,
    /// Requests that made a service panic.
    pub panics: u64,
}

/// What the reader does after a frame.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Flow {
    Continue,
    Close,
}

pub(crate) struct Server {
    node: NodeName,
    security: Security,
    grants: Option<Rc<dyn Grants>>,
    pub(crate) sessions: Sessions,
    services: Vec<Box<dyn Service>>,
    routes: [u8; 256],
    pub(crate) stats: Stats,
}

impl Server {
    pub(crate) fn new(
        node: NodeName,
        security: Security,
        grants: Option<Rc<dyn Grants>>,
        limits: Limits,
    ) -> Self {
        Self {
            node,
            security,
            grants,
            sessions: Sessions::new(limits),
            services: Vec::new(),
            routes: [NO_ROUTE; 256],
            stats: Stats::default(),
        }
    }

    /// Adds a service. `false`, with nothing added, if it answers a kind that is the server's
    /// own or another service's, or there is no room.
    pub(crate) fn add(&mut self, service: Box<dyn Service>) -> bool {
        let index = self.services.len();
        let own = [
            kind::HELLO,
            kind::WELCOME,
            kind::REJECT,
            kind::PING,
            kind::PONG,
        ];
        let taken = service
            .kinds()
            .iter()
            .any(|kind| own.contains(kind) || self.routes[usize::from(*kind)] != NO_ROUTE);
        if taken || index >= usize::from(NO_ROUTE) {
            return false;
        }
        for kind in service.kinds() {
            self.routes[usize::from(*kind)] = index as u8;
        }
        self.services.push(service);
        true
    }

    pub(crate) fn open(&mut self, connection: &impl Connection) -> Option<SessionId> {
        let id = self.sessions.open(
            connection.principal(),
            connection.offers(),
            connection.passes_descriptors(),
        );
        match id {
            Some(_) => self.stats.opened += 1,
            None => self.stats.rejected += 1,
        }
        id
    }

    /// Removes `session` and tells every service. Does nothing the second time.
    pub(crate) fn close(&mut self, session: SessionId) {
        if !self.sessions.remove(session) {
            return;
        }
        for service in &mut self.services {
            service.closed(session, &mut self.sessions);
        }
    }

    fn send<B: Body>(&mut self, session: SessionId, body: &B) {
        // A session that can't take the server's own answers is closed by `Delivery::Must`.
        let _ = self.sessions.send(session, body, Delivery::Must);
    }

    fn reject(&mut self, session: SessionId, reason: RejectReason) -> Flow {
        self.stats.rejected += 1;
        self.send(session, &Reject { reason });
        Flow::Close
    }

    fn refuse(&mut self, session: SessionId, request_seq: u16, error: Error) {
        self.stats.refused += 1;
        self.send(
            session,
            &Refusal {
                request_seq,
                code: error.code(),
                detail: error.as_str(),
            },
        );
    }

    /// One frame from `session`.
    pub(crate) fn receive(
        &mut self,
        session: SessionId,
        bytes: &[u8],
        fds: &mut FdSet,
        now_ms: u64,
    ) -> Flow {
        let Ok(frame) = frame::decode(bytes) else {
            // A carrier delivers whole frames, so a bad one is not line noise.
            self.stats.dropped += 1;
            return Flow::Close;
        };
        if !frame.header().is_current_version() {
            return self.reject(session, RejectReason::VersionMismatch);
        }
        let Some(entry) = self.sessions.entry(session) else {
            return Flow::Close;
        };
        if entry.closing {
            return Flow::Close;
        }
        if !entry.info.open {
            return self.hello(session, &frame);
        }
        match frame.kind() {
            kind::PING => {
                if let Ok(ping) = Ping::decode(frame.payload()) {
                    self.send(
                        session,
                        &Pong {
                            now_ms: ping.now_ms,
                        },
                    );
                }
                Flow::Continue
            }
            kind::PONG => Flow::Continue,
            // A second hello, or the server's own answers sent back at it.
            kind::HELLO | kind::WELCOME | kind::REJECT => {
                self.stats.dropped += 1;
                Flow::Close
            }
            _ => self.dispatch(session, frame, fds, now_ms),
        }
    }

    fn hello(&mut self, session: SessionId, frame: &FrameView<'_>) -> Flow {
        let Ok(hello) = orion_wire::decode_body::<Hello<'_>>(frame) else {
            self.stats.dropped += 1;
            return Flow::Close;
        };
        let limits = self.sessions.limits;
        let Some(entry) = self.sessions.entry_mut(session) else {
            return Flow::Close;
        };
        if !entry.offers.has(hello.roles) {
            return self.reject(session, RejectReason::RoleNotOffered);
        }
        let info = &mut entry.info;
        info.name.clear();
        info.name.extend(hello.name.chars().take(64));
        info.roles = hello.roles;
        info.revision = session_revision(INTERFACE_REVISION, hello.revision);
        // Never below what the server's own answers need.
        info.max_frame = hello.max_frame.clamp(256, limits.max_frame);
        info.open = true;
        let welcome = Welcome {
            node: self.node.clone(),
            session: session.0,
            heartbeat_ms: limits.heartbeat_ms,
            max_frame: info.max_frame,
            revision: INTERFACE_REVISION,
        };
        self.send(session, &welcome);
        Flow::Continue
    }

    fn dispatch(
        &mut self,
        session: SessionId,
        frame: FrameView<'_>,
        fds: &mut FdSet,
        now_ms: u64,
    ) -> Flow {
        let route = self.routes[usize::from(frame.kind())];
        let Some(service) = self.services.get_mut(usize::from(route)) else {
            // Unknown kinds are ignored, so an older comm core can talk to a newer client.
            self.stats.unknown += 1;
            return Flow::Continue;
        };
        let seq = frame.seq();
        let (what, policy) = match service.asks(&frame) {
            Ok(asks) => asks,
            Err(error) => {
                self.refuse(session, seq, error);
                return Flow::Continue;
            }
        };
        let Some(principal) = self
            .sessions
            .get(session)
            .map(|info| info.principal.clone())
        else {
            return Flow::Close;
        };
        let incoming = Incoming::new(frame, what, fds);
        let decided = match &self.grants {
            Some(grants) => {
                Check::with_grants(self.security, &**grants).decide(&principal, incoming, &policy)
            }
            None => Check::new(self.security).decide(&principal, incoming, &policy),
        };
        let checked = match decided {
            Ok(checked) => checked,
            Err(Refused::NotAdmitted | Refused::NotGranted) => {
                self.refuse(session, seq, Error::Denied);
                return Flow::Continue;
            }
        };
        self.stats.requests += 1;
        let sessions = &mut self.sessions;
        let outcome =
            contain(|| service.handle(checked, &mut Cx::new(sessions, session, seq, now_ms)));
        match outcome {
            Ok(Ok(())) => Flow::Continue,
            Ok(Err(error)) => {
                self.refuse(session, seq, error);
                Flow::Continue
            }
            Err(panic) => {
                // The request is poison for this session. The service keeps serving the rest.
                self.stats.panics += 1;
                tracing::error!(
                    session = session.0,
                    panic = panic.message(),
                    "a request made a service panic"
                );
                Flow::Close
            }
        }
    }
}
