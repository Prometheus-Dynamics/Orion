//! One session shared by every part of a client.
//!
//! A program has one session with its comm core, and several things going over it at once:
//! topics, records, actions. Each of those registers a [`Handler`] for the kinds of frame that
//! are its own. Whoever is waiting on the session, for whatever reason, [`Client::pump`]s it,
//! and each frame that arrives goes to the part it belongs to. So a part waiting for its
//! answer never loses another part's frame.
//!
//! A client is one thread's: its handles are not `Send`. A program that talks to the comm
//! core from several threads opens a session in each.

use crate::fd::BorrowedFd;
use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::time::{Duration, Instant};

use orion_msg_session::Refusal;
use orion_wire::{Body, kind};

use crate::{ClientError, Cx, Incoming, Session};

/// How many refusals are remembered until whoever sent the request asks.
const REFUSALS: usize = 8;

/// A part of the client that owns some kinds of frame.
pub trait Handler {
    /// The kinds of frame that are this handler's.
    fn kinds(&self) -> &'static [u8];

    /// Takes one frame. `cx` answers it and takes its descriptors.
    ///
    /// # Errors
    ///
    /// An error ends the pump that delivered the frame, and is what its caller sees.
    fn handle(&mut self, frame: &Incoming<'_>, cx: &mut Cx<'_>) -> Result<(), ClientError>;
}

struct Inner {
    session: RefCell<Session>,
    handlers: RefCell<Vec<Rc<RefCell<dyn Handler>>>>,
    refusals: RefCell<[Option<(u16, orion_types::Error)>; REFUSALS]>,
    next_refusal: Cell<usize>,
}

/// One session, shared. Cloning gives another handle on the same one.
#[derive(Clone)]
pub struct Client(Rc<Inner>);

impl std::fmt::Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0.session.try_borrow() {
            Ok(session) => write!(f, "Client({session:?})"),
            Err(_) => f.write_str("Client(in use)"),
        }
    }
}

impl Client {
    /// Shares `session`.
    #[must_use]
    pub fn new(session: Session) -> Self {
        Self(Rc::new(Inner {
            session: RefCell::new(session),
            handlers: RefCell::new(Vec::new()),
            refusals: RefCell::new([None; REFUSALS]),
            next_refusal: Cell::new(0),
        }))
    }

    /// Registers the part that owns some kinds of frame. Not from inside a handler.
    pub fn add(&self, handler: Rc<RefCell<dyn Handler>>) {
        self.0.handlers.borrow_mut().push(handler);
    }

    /// Sends `body` and returns the sequence number it went with. Not from inside a handler,
    /// which answers through its [`Cx`].
    ///
    /// # Errors
    ///
    /// As [`Session::send`].
    pub fn send<B: Body>(&self, body: &B) -> Result<u16, ClientError> {
        self.0.session.borrow_mut().send(body)
    }

    /// Sends `body` with descriptors attached.
    ///
    /// # Errors
    ///
    /// As [`Session::send_with_fds`].
    pub fn send_with_fds<B: Body>(
        &self,
        body: &B,
        fds: &[BorrowedFd<'_>],
    ) -> Result<u16, ClientError> {
        self.0.session.borrow_mut().send_with_fds(body, fds)
    }

    /// Sends a frame of `kind` whose payload is already encoded.
    ///
    /// # Errors
    ///
    /// As [`Session::send_frame`].
    pub fn send_frame(
        &self,
        kind: u8,
        payload: &[u8],
        fds: &[BorrowedFd<'_>],
    ) -> Result<u16, ClientError> {
        self.0.session.borrow_mut().send_frame(kind, payload, fds)
    }

    /// Waits up to `wait` for one frame and gives it to the part it belongs to. `true` if a
    /// frame came. A frame that is nobody's is dropped: the wire's rule for kinds this build
    /// does not know.
    ///
    /// # Errors
    ///
    /// As [`Session::next`], and whatever the handler returns.
    pub fn pump(&self, wait: Duration) -> Result<bool, ClientError> {
        let mut session = self.0.session.borrow_mut();
        let Some((frame, mut cx)) = session.next_with(wait)? else {
            return Ok(false);
        };
        if frame.kind() == kind::REFUSAL {
            let refusal: Refusal<'_> = frame.body()?;
            let at = self.0.next_refusal.get();
            self.0.refusals.borrow_mut()[at] = Some((
                refusal.request_seq,
                refusal.error().unwrap_or(orion_types::Error::Internal),
            ));
            self.0.next_refusal.set((at + 1) % REFUSALS);
            return Ok(true);
        }
        let handlers = self.0.handlers.borrow();
        for handler in handlers.iter() {
            let mut handler = handler.borrow_mut();
            if handler.kinds().contains(&frame.kind()) {
                handler.handle(&frame, &mut cx)?;
                break;
            }
        }
        Ok(true)
    }

    /// Gives every frame that is already here to its part, without waiting.
    ///
    /// # Errors
    ///
    /// As [`Client::pump`].
    pub fn drain(&self) -> Result<(), ClientError> {
        while self.pump(Duration::ZERO)? {}
        Ok(())
    }

    /// The refusal of the request sent as `seq`, if one came. It is given once.
    #[must_use]
    pub fn refusal(&self, seq: u16) -> Option<orion_types::Error> {
        let mut refusals = self.0.refusals.borrow_mut();
        let slot = refusals
            .iter_mut()
            .find(|slot| slot.is_some_and(|(refused, _)| refused == seq))?;
        slot.take().map(|(_, error)| error)
    }

    /// Pumps until `done` says the answer to the request sent as `seq` has arrived.
    ///
    /// # Errors
    ///
    /// [`ClientError::Refused`] if the comm core refused the request, a timed-out
    /// [`ClientError::Io`] if nothing came within the session's timeout, and as
    /// [`Client::pump`].
    pub fn answered(&self, seq: u16, mut done: impl FnMut() -> bool) -> Result<(), ClientError> {
        let deadline = Instant::now() + self.0.session.borrow().timeout();
        loop {
            if done() {
                return Ok(());
            }
            if let Some(error) = self.refusal(seq) {
                return Err(ClientError::Refused(error));
            }
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return Err(ClientError::Io(std::io::ErrorKind::TimedOut.into()));
            }
            self.pump(left)?;
        }
    }

    /// Does something with the session itself. Not from inside a handler.
    pub fn session<R>(&self, with: impl FnOnce(&mut Session) -> R) -> R {
        with(&mut self.0.session.borrow_mut())
    }
}
