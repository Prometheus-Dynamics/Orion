//! The table of sessions, and sending to them.

use std::os::fd::OwnedFd;
use std::rc::Rc;

use orion_check::Principal;
use orion_msg_session::Roles;
use orion_types::Error;
use orion_wire::Body;
use orion_wire::body::encode_fields;
use orion_wire::frame::{self, FrameHeader};
use tokio::sync::Notify;

use crate::Limits;
use crate::outbox::Outbox;

/// A session's number. It is never reused for another session while this process runs long
/// enough to matter: the slot is, the generation in the upper half is not.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SessionId(pub u32);

impl SessionId {
    const fn new(slot: usize, generation: u16) -> Self {
        Self(slot as u32 | (generation as u32) << 16)
    }

    const fn slot(self) -> usize {
        (self.0 & 0xFFFF) as usize
    }
}

/// What to do when a session's outbox has no room.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Delivery {
    /// The session cannot go on without this frame: an answer, a handle. With no room, the
    /// session is closed. It was not reading.
    Must,
    /// The frame is one of a stream and the next one supersedes it. With no room, it is
    /// dropped and counted.
    MayDrop,
}

/// Why the comm core ended a session. A session its own program closed has none.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Dropped {
    /// It did not say hello in time.
    NoHello {
        /// How long it had, in milliseconds.
        limit_ms: u64,
    },
    /// Nothing was heard from it for the idle time.
    Quiet {
        /// How long it was quiet, in milliseconds.
        quiet_ms: u64,
        /// How long it may be, in milliseconds.
        limit_ms: u64,
    },
    /// Its connection failed while reading.
    ReadFailed(std::io::ErrorKind),
    /// A frame from it could not be read, or its hello could not.
    Damaged,
    /// It sent a frame only the comm core sends, or said hello twice.
    OutOfTurn {
        /// The kind of the frame.
        kind: u8,
    },
    /// Its hello was turned away.
    Rejected(orion_msg_session::RejectReason),
    /// It was not reading: a frame it had to have did not fit its outbox.
    OutboxFull {
        /// The kind of the frame.
        kind: u8,
        /// The frame's size, in bytes.
        frame: usize,
        /// How many bytes were waiting for it.
        waiting: usize,
        /// How many may wait.
        limit: usize,
    },
    /// It did not take a frame within the write deadline, or its connection failed while
    /// writing.
    WriteStalled {
        /// The write deadline, in milliseconds.
        limit_ms: u64,
    },
    /// A request of its made a service panic.
    Panicked,
    /// A service ended it, and says why.
    Ended(&'static str),
}

impl std::fmt::Display for Dropped {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoHello { limit_ms } => write!(f, "no hello within {limit_ms} ms"),
            Self::Quiet { quiet_ms, limit_ms } => write!(
                f,
                "quiet for {quiet_ms} ms, and the idle time is {limit_ms} ms: is its program \
                 still pumping the session?"
            ),
            Self::ReadFailed(kind) => write!(f, "its connection failed: {kind}"),
            Self::Damaged => f.write_str("a frame from it could not be read"),
            Self::OutOfTurn { kind } => {
                write!(f, "it sent a frame of kind {kind:#04x} out of turn")
            }
            Self::Rejected(reason) => write!(f, "its hello was turned away: {reason:?}"),
            Self::OutboxFull {
                kind,
                frame,
                waiting,
                limit,
            } => write!(
                f,
                "it was not reading: a frame of kind {kind:#04x} and {frame} bytes did not fit, \
                 with {waiting} of {limit} bytes waiting"
            ),
            Self::WriteStalled { limit_ms } => {
                write!(f, "it did not take a frame within {limit_ms} ms")
            }
            Self::Panicked => f.write_str("a request of its made a service panic"),
            Self::Ended(why) => f.write_str(why),
        }
    }
}

/// What the server knows about a session.
#[derive(Clone, Debug)]
pub struct SessionInfo {
    /// Its number.
    pub id: SessionId,
    /// Who the carrier says it is.
    pub principal: Principal,
    /// The name it gave in its hello, for logs and status.
    pub name: String,
    /// The roles it asked for.
    pub roles: Roles,
    /// The additive level its hello named: 0 if it named none.
    pub level: u16,
    /// The largest frame it takes.
    pub max_frame: u32,
    /// Whether descriptors can be sent to it.
    pub passes_descriptors: bool,
    /// Whether it has said hello.
    pub open: bool,
    /// Frames dropped because its outbox was full.
    pub dropped: u64,
    /// Whether it is a program on this board over a socket that reports a hang-up: never
    /// ended for being quiet.
    pub local: bool,
    /// Since when, in milliseconds on the server's clock, nothing has been heard from it for
    /// longer than the idle time. Only a local session stays to be seen like this.
    pub quiet_since: Option<u64>,
}

#[derive(Debug)]
pub(crate) struct Entry {
    pub(crate) info: SessionInfo,
    pub(crate) offers: Roles,
    pub(crate) outbox: Outbox,
    out_seq: u16,
    pub(crate) closing: bool,
    /// Why the comm core is ending it: the first reason given.
    pub(crate) why: Option<Dropped>,
    /// Wakes the session's writer: something to send, or closing.
    pub(crate) wake: Rc<Notify>,
    /// Stops the session's reader.
    pub(crate) stop: Rc<Notify>,
}

/// What the writer finds when it looks for the next frame.
pub(crate) enum Popped {
    Frame,
    Empty,
    Closed,
}

/// Every session. A service reaches other sessions through this.
#[derive(Debug)]
pub struct Sessions {
    slots: Vec<Option<Entry>>,
    generations: Vec<u16>,
    live: u32,
    pub(crate) limits: Limits,
    scratch: Vec<u8>,
    payload: Vec<u8>,
}

impl Sessions {
    pub(crate) fn new(limits: Limits) -> Self {
        Self {
            slots: Vec::new(),
            generations: Vec::new(),
            live: 0,
            limits,
            scratch: vec![0; limits.max_frame as usize + 8],
            payload: Vec::new(),
        }
    }

    /// How many sessions there are.
    #[must_use]
    pub const fn len(&self) -> u32 {
        self.live
    }

    /// Whether there are none.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.live == 0
    }

    /// What is known about `session`, if it is still there.
    #[must_use]
    pub fn get(&self, session: SessionId) -> Option<&SessionInfo> {
        self.entry(session).map(|entry| &entry.info)
    }

    /// Every session that has said hello.
    pub fn iter(&self) -> impl Iterator<Item = &SessionInfo> {
        self.slots
            .iter()
            .flatten()
            .map(|entry| &entry.info)
            .filter(|info| info.open)
    }

    pub(crate) fn entry(&self, session: SessionId) -> Option<&Entry> {
        self.slots
            .get(session.slot())?
            .as_ref()
            .filter(|entry| entry.info.id == session)
    }

    pub(crate) fn entry_mut(&mut self, session: SessionId) -> Option<&mut Entry> {
        self.slots
            .get_mut(session.slot())?
            .as_mut()
            .filter(|entry| entry.info.id == session)
    }

    pub(crate) fn open(
        &mut self,
        principal: Principal,
        offers: Roles,
        passes_descriptors: bool,
        local: bool,
    ) -> Option<SessionId> {
        if self.live >= self.limits.max_sessions {
            return None;
        }
        let slot = match self.slots.iter().position(Option::is_none) {
            Some(slot) => slot,
            None => {
                self.slots.push(None);
                self.generations.push(0);
                self.slots.len() - 1
            }
        };
        self.generations[slot] = self.generations[slot].wrapping_add(1);
        let id = SessionId::new(slot, self.generations[slot]);
        self.slots[slot] = Some(Entry {
            info: SessionInfo {
                id,
                principal,
                name: String::new(),
                roles: Roles(0),
                level: 0,
                max_frame: self.limits.max_frame,
                passes_descriptors,
                open: false,
                dropped: 0,
                local,
                quiet_since: None,
            },
            offers,
            outbox: Outbox::default(),
            out_seq: 0,
            closing: false,
            why: None,
            wake: Rc::new(Notify::new()),
            stop: Rc::new(Notify::new()),
        });
        self.live += 1;
        Some(id)
    }

    pub(crate) fn remove(&mut self, session: SessionId) -> bool {
        if self.entry(session).is_none() {
            return false;
        }
        self.slots[session.slot()] = None;
        self.live -= 1;
        true
    }

    /// Sends `body` to `session`.
    ///
    /// # Errors
    ///
    /// - [`Error::NotFound`]: the session is gone.
    /// - [`Error::BufferTooSmall`]: the frame is larger than the session takes.
    /// - [`Error::Busy`]: its outbox is full. With [`Delivery::Must`] the session is now
    ///   closing.
    pub fn send<B: Body>(
        &mut self,
        session: SessionId,
        body: &B,
        delivery: Delivery,
    ) -> Result<(), Error> {
        self.send_with_fds(session, body, [], delivery)
    }

    /// Sends `body` to `session` with descriptors attached. They are closed here once sent, or
    /// if the frame can't be queued.
    ///
    /// # Errors
    ///
    /// As [`Sessions::send`], and [`Error::Unsupported`] if descriptors can't travel to that
    /// session.
    pub fn send_with_fds<B: Body>(
        &mut self,
        session: SessionId,
        body: &B,
        fds: impl IntoIterator<Item = OwnedFd>,
        delivery: Delivery,
    ) -> Result<(), Error> {
        let mut payload = std::mem::take(&mut self.payload);
        payload.resize(self.limits.max_frame as usize, 0);
        let sent = encode_fields(body, &mut payload)
            .and_then(|len| self.send_frame(session, B::KIND, &payload[..len], fds, delivery));
        self.payload = payload;
        sent
    }

    /// Sends a frame of `kind` whose payload is already encoded: a body passed on as it came.
    ///
    /// # Errors
    ///
    /// As [`Sessions::send_with_fds`].
    pub fn send_frame(
        &mut self,
        session: SessionId,
        kind: u8,
        payload: &[u8],
        fds: impl IntoIterator<Item = OwnedFd>,
        delivery: Delivery,
    ) -> Result<(), Error> {
        let limit = self.limits.outbox_bytes as usize;
        let Some(entry) = self
            .slots
            .get_mut(session.slot())
            .and_then(Option::as_mut)
            .filter(|entry| entry.info.id == session && !entry.closing)
        else {
            return Err(Error::NotFound);
        };
        let mut fds = fds.into_iter().peekable();
        if fds.peek().is_some() && !entry.info.passes_descriptors {
            return Err(Error::Unsupported);
        }
        let header = FrameHeader::new(kind, entry.out_seq);
        let len =
            frame::encode(header, payload, &mut self.scratch).map_err(|_| Error::BufferTooSmall)?;
        if len > entry.info.max_frame as usize {
            return Err(Error::BufferTooSmall);
        }
        if !entry.outbox.push(&self.scratch[..len], fds, limit) {
            match delivery {
                Delivery::MayDrop => entry.info.dropped += 1,
                Delivery::Must => {
                    entry.why.get_or_insert(Dropped::OutboxFull {
                        kind,
                        frame: len,
                        waiting: entry.outbox.pending(),
                        limit,
                    });
                    entry.closing = true;
                    entry.stop.notify_one();
                }
            }
            entry.wake.notify_one();
            return Err(Error::Busy);
        }
        entry.out_seq = entry.out_seq.wrapping_add(1);
        entry.wake.notify_one();
        Ok(())
    }

    /// Closes `session`: what is already queued for it is still sent, then its connection is
    /// dropped.
    pub fn close(&mut self, session: SessionId) {
        self.end(session, None);
    }

    /// Closes `session` as [`Sessions::close`] does, and has the log say `why`.
    pub fn close_because(&mut self, session: SessionId, why: &'static str) {
        self.end(session, Some(Dropped::Ended(why)));
    }

    /// Notes why `session` is being ended, unless a reason was given already.
    pub(crate) fn dropped(&mut self, session: SessionId, why: Dropped) {
        if let Some(entry) = self.entry_mut(session) {
            entry.why.get_or_insert(why);
        }
    }

    pub(crate) fn end(&mut self, session: SessionId, why: Option<Dropped>) {
        if let Some(entry) = self.entry_mut(session) {
            if let Some(why) = why {
                entry.why.get_or_insert(why);
            }
            entry.closing = true;
            entry.wake.notify_one();
            entry.stop.notify_one();
        }
    }

    /// How many bytes wait to be sent to `session`.
    #[must_use]
    pub fn pending(&self, session: SessionId) -> usize {
        self.entry(session)
            .map_or(0, |entry| entry.outbox.pending())
    }

    pub(crate) fn pop(
        &mut self,
        session: SessionId,
        frame: &mut Vec<u8>,
        fds: &mut Vec<OwnedFd>,
    ) -> Popped {
        let Some(entry) = self.entry_mut(session) else {
            return Popped::Closed;
        };
        if entry.outbox.pop(frame, fds) {
            Popped::Frame
        } else if entry.closing {
            Popped::Closed
        } else {
            Popped::Empty
        }
    }
}
