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

pub(crate) struct Inner {
    session: RefCell<Session>,
    handlers: RefCell<Vec<Rc<RefCell<dyn Handler>>>>,
    refusals: RefCell<[Option<(u16, ClientError)>; REFUSALS]>,
    next_refusal: Cell<usize>,
    /// Whether [`Client::node_key`] waits for the node's answer to Identify, and the answer.
    pub(crate) asking: Cell<bool>,
    pub(crate) identity: RefCell<Option<Vec<u8>>>,
    /// Whether a handler added for kinds another has was warned of.
    warned: Cell<bool>,
}

/// One session, shared. Cloning gives another handle on the same one.
#[derive(Clone)]
pub struct Client(pub(crate) Rc<Inner>);

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
            refusals: RefCell::new(std::array::from_fn(|_| None)),
            next_refusal: Cell::new(0),
            asking: Cell::new(false),
            identity: RefCell::new(None),
            warned: Cell::new(false),
        }))
    }

    /// Registers the part that owns some kinds of frame. Not from inside a handler.
    ///
    /// **One handle for each part on a session.** A frame goes to the first part that has its
    /// kind, so a second handle for the same part (a second `Files`, `Records` or `Topics` on
    /// one client) hears nothing. The first time a part is added for kinds another already
    /// has, it is said once, on standard error, naming the kinds.
    pub fn add(&self, handler: Rc<RefCell<dyn Handler>>) {
        let mut handlers = self.0.handlers.borrow_mut();
        if let Some(said) = warning(&handlers, handler.borrow().kinds(), &self.0.warned) {
            // Written whole, without formatting: the C library carries this too.
            let _ = std::io::Write::write_all(&mut std::io::stderr(), &said);
        }
        handlers.push(handler);
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
                ClientError::refused(
                    refusal.error().unwrap_or(orion_types::Error::Internal),
                    refusal.detail,
                ),
            ));
            self.0.next_refusal.set((at + 1) % REFUSALS);
            return Ok(true);
        }
        if frame.kind() == kind::IDENTITY && self.0.asking.get() {
            self.0.identity.replace(Some(frame.payload().to_vec()));
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

    /// The refusal of the request sent as `seq`, if one came, as a
    /// [`ClientError::Refused`] with the node's words. It is given once.
    #[must_use]
    pub fn refusal(&self, seq: u16) -> Option<ClientError> {
        let mut refusals = self.0.refusals.borrow_mut();
        let slot = refusals
            .iter_mut()
            .find(|slot| slot.as_ref().is_some_and(|(refused, _)| *refused == seq))?;
        slot.take().map(|(_, error)| error)
    }

    /// Pumps until `done` says the answer to the request sent as `seq` has arrived.
    ///
    /// # Errors
    ///
    /// [`ClientError::Refused`] if the comm core refused the request, as
    /// [`Session::reply_left`] when the wait is over, and as [`Client::pump`].
    pub fn answered(&self, seq: u16, mut done: impl FnMut() -> bool) -> Result<(), ClientError> {
        let since = Instant::now();
        loop {
            if done() {
                return Ok(());
            }
            if let Some(refused) = self.refusal(seq) {
                return Err(refused);
            }
            let left = self.0.session.borrow().reply_left(since)?;
            self.pump(left)?;
        }
    }

    /// As [`Session::silent_for`].
    #[must_use]
    pub fn silent_for(&self) -> Duration {
        self.0.session.borrow().silent_for()
    }

    /// As [`Session::set_reply`]: how every later wait for an answer is bounded, the
    /// helpers' included, until this is called again.
    pub fn set_reply(&self, reply: crate::Reply) {
        self.0.session.borrow_mut().set_reply(reply);
    }

    /// As [`Session::reply`].
    #[must_use]
    pub fn reply(&self) -> crate::Reply {
        self.0.session.borrow().reply()
    }

    /// As [`Session::reply_left`].
    ///
    /// # Errors
    ///
    /// As [`Session::reply_left`].
    pub fn reply_left(&self, since: Instant) -> Result<Duration, ClientError> {
        self.0.session.borrow().reply_left(since)
    }

    /// Does something with the session itself. Not from inside a handler.
    pub fn session<R>(&self, with: impl FnOnce(&mut Session) -> R) -> R {
        with(&mut self.0.session.borrow_mut())
    }
}

/// What to say of a part added for `kinds` beside `handlers`: once for a session, as `warned`
/// keeps, and only if another part already has some of them. The kinds are named by number,
/// as `orion_wire::kind` lists them: their names would bring the wire's whole table of kinds
/// into every program, the C library's too.
fn warning(
    handlers: &[Rc<RefCell<dyn Handler>>],
    kinds: &[u8],
    warned: &Cell<bool>,
) -> Option<Vec<u8>> {
    let taken = shadowed(handlers, kinds);
    if taken.is_empty() || warned.replace(true) {
        return None;
    }
    let mut said = b"orion: a second handle on this session for kinds".to_vec();
    for kind in taken {
        let hex = |digit: u8| b"0123456789abcdef"[usize::from(digit)];
        said.extend_from_slice(&[b' ', b'0', b'x', hex(kind >> 4), hex(kind & 15)]);
    }
    said.extend_from_slice(
        b" will hear nothing: those frames go to the first. Keep one handle for each part on \
          a session.\n",
    );
    Some(said)
}

/// The kinds in `kinds` that a handler in `handlers` already has.
fn shadowed(handlers: &[Rc<RefCell<dyn Handler>>], kinds: &[u8]) -> Vec<u8> {
    kinds
        .iter()
        .copied()
        .filter(|kind| {
            handlers.iter().any(|known| {
                known
                    .try_borrow()
                    .is_ok_and(|known| known.kinds().contains(kind))
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Part(&'static [u8]);

    impl Handler for Part {
        fn kinds(&self) -> &'static [u8] {
            self.0
        }

        fn handle(&mut self, _: &Incoming<'_>, _: &mut Cx<'_>) -> Result<(), ClientError> {
            Ok(())
        }
    }

    fn part(kinds: &'static [u8]) -> Rc<RefCell<dyn Handler>> {
        Rc::new(RefCell::new(Part(kinds)))
    }

    #[test]
    fn a_part_added_for_kinds_another_has_is_told_by_their_numbers() {
        let first = [part(&[kind::FILE_STATUS, kind::FILE_SINK_READY])];
        assert_eq!(
            shadowed(&first, &[kind::FILE_SINK_READY, kind::FILE_BEGIN]),
            [kind::FILE_SINK_READY]
        );
        assert!(shadowed(&first, &[kind::PUT]).is_empty());
        assert!(shadowed(&[], &[kind::FILE_STATUS]).is_empty());

        // Said once for a session, naming the kinds, and only when some are taken.
        let warned = Cell::new(false);
        assert_eq!(warning(&first, &[kind::PUT], &warned), None);
        let said = warning(&first, &[kind::FILE_STATUS, kind::FILE_SINK_READY], &warned);
        let said = String::from_utf8(said.expect("said")).expect("text");
        let named = format!(
            "for kinds 0x{:02x} 0x{:02x} will hear nothing",
            kind::FILE_STATUS,
            kind::FILE_SINK_READY
        );
        assert!(said.contains(&named) && said.ends_with(".\n"), "{said}");
        assert_eq!(warning(&first, &[kind::FILE_STATUS], &warned), None);
    }
}
