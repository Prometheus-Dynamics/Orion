//! What a service is, and what it is handed.

use std::os::fd::OwnedFd;

use orion_check::{Checked, Policy, Request, What};
use orion_types::Error;
use orion_wire::{Body, Decode, FrameView, decode_body};
use shm_fdpass::FdSet;

use crate::sessions::{Delivery, SessionId, SessionInfo, Sessions};

/// One request, as a service receives it: always inside a [`Checked`], which only the server
/// can make, after the check.
#[derive(Debug)]
pub struct Incoming<'a> {
    frame: FrameView<'a>,
    what: What<'a>,
    fds: &'a mut FdSet,
}

impl<'a> Incoming<'a> {
    pub(crate) const fn new(frame: FrameView<'a>, what: What<'a>, fds: &'a mut FdSet) -> Self {
        Self { frame, what, fds }
    }

    /// The frame.
    #[must_use]
    pub const fn frame(&self) -> &FrameView<'a> {
        &self.frame
    }

    /// The frame's kind.
    #[must_use]
    pub const fn kind(&self) -> u8 {
        self.frame.kind()
    }

    /// The body, as a `B`.
    ///
    /// # Errors
    ///
    /// [`Error::Malformed`] if the frame is another kind or doesn't decode.
    pub fn body<B: Body + Decode<'a>>(&self) -> Result<B, Error> {
        decode_body(&self.frame)
    }

    /// Takes descriptor `index` of those that came with the frame.
    pub fn take_fd(&mut self, index: usize) -> Option<OwnedFd> {
        self.fds.take(index)
    }

    /// How many descriptors came with the frame.
    #[must_use]
    pub const fn fds(&self) -> usize {
        self.fds.len()
    }
}

impl Request for Incoming<'_> {
    fn what(&self) -> What<'_> {
        self.what
    }
}

/// Something that answers a range of kinds.
///
/// Every method returns at once. A service that has slow work to do (a disk, another socket)
/// does it in a task of its own and answers from there through [`crate::Comm::with`].
pub trait Service {
    /// The kinds this service answers. No two services answer the same kind.
    fn kinds(&self) -> &'static [u8];

    /// What a frame asks for, and who may ask it. This is all the check is told, so it is
    /// where a service decides its policy. It runs before the check and must not act on the
    /// frame.
    ///
    /// # Errors
    ///
    /// [`Error::Malformed`] if the frame can't be read. The caller gets a refusal.
    fn asks<'a>(&self, frame: &FrameView<'a>) -> Result<(What<'a>, Policy), Error>;

    /// Handles one request that passed the check.
    ///
    /// # Errors
    ///
    /// Any [`Error`]: the caller gets a refusal with its code.
    fn handle(&mut self, request: Checked<Incoming<'_>>, cx: &mut Cx<'_>) -> Result<(), Error>;

    /// A session is gone. It is already out of `sessions`.
    fn closed(&mut self, session: SessionId, sessions: &mut Sessions) {
        let _ = (session, sessions);
    }
}

/// What a service can do while it handles a request.
#[derive(Debug)]
pub struct Cx<'a> {
    sessions: &'a mut Sessions,
    from: SessionId,
    request_seq: u16,
    now_ms: u64,
}

impl<'a> Cx<'a> {
    pub(crate) const fn new(
        sessions: &'a mut Sessions,
        from: SessionId,
        request_seq: u16,
        now_ms: u64,
    ) -> Self {
        Self {
            sessions,
            from,
            request_seq,
            now_ms,
        }
    }

    /// The session the request came from.
    #[must_use]
    pub const fn from(&self) -> SessionId {
        self.from
    }

    /// The `seq` of the request, which an answer names.
    #[must_use]
    pub const fn request_seq(&self) -> u16 {
        self.request_seq
    }

    /// Milliseconds on the server's clock.
    #[must_use]
    pub const fn now_ms(&self) -> u64 {
        self.now_ms
    }

    /// What is known about the caller.
    ///
    /// # Panics
    ///
    /// Never: the caller's session is there for as long as its request is handled.
    #[must_use]
    pub fn caller(&self) -> &SessionInfo {
        self.sessions
            .get(self.from)
            .expect("the caller's session outlives its request")
    }

    /// Answers the caller.
    ///
    /// # Errors
    ///
    /// As [`Sessions::send`]. An answer that can't be queued closes the caller.
    pub fn reply<B: Body>(&mut self, body: &B) -> Result<(), Error> {
        self.sessions.send(self.from, body, Delivery::Must)
    }

    /// Answers the caller with descriptors attached.
    ///
    /// # Errors
    ///
    /// As [`Sessions::send_with_fds`].
    pub fn reply_with_fds<B: Body>(
        &mut self,
        body: &B,
        fds: impl IntoIterator<Item = OwnedFd>,
    ) -> Result<(), Error> {
        self.sessions
            .send_with_fds(self.from, body, fds, Delivery::Must)
    }

    /// Every session, to send to others.
    pub const fn sessions(&mut self) -> &mut Sessions {
        self.sessions
    }
}
