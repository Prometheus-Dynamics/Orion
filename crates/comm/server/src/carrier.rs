//! What a carrier gives the server: connections that move whole frames.

use std::future::Future;
use std::io;
use std::os::fd::OwnedFd;

use orion_check::Principal;
use orion_fdpass::FdSet;
use orion_msg_session::Roles;

/// The reading half of a connection.
pub trait FrameRead {
    /// Reads one whole frame (header to CRC, without any framing the carrier adds) into `buf`
    /// and returns its length, with any descriptors that came with it in `fds`. `None` when the
    /// other side has closed.
    ///
    /// A frame longer than `buf` is an error: the carrier must refuse it without buffering it.
    /// The future may be dropped only when the connection is being closed.
    fn recv(
        &mut self,
        buf: &mut [u8],
        fds: &mut FdSet,
    ) -> impl Future<Output = io::Result<Option<usize>>>;
}

/// The writing half of a connection.
pub trait FrameWrite {
    /// Sends one whole frame, with `fds` attached if the carrier can pass descriptors.
    fn send(&mut self, frame: &[u8], fds: &[OwnedFd]) -> impl Future<Output = io::Result<()>>;
}

/// One connection, before it has said hello.
pub trait Connection {
    /// Its reading half.
    type Read: FrameRead + 'static;
    /// Its writing half.
    type Write: FrameWrite + 'static;

    /// Who the carrier knows the other end to be: socket credentials, a verified key, a link's
    /// device. `Anonymous` if it knows nothing.
    fn principal(&self) -> Principal;

    /// The roles a connection on this carrier may ask for.
    fn offers(&self) -> Roles;

    /// Whether descriptors can travel with frames.
    fn passes_descriptors(&self) -> bool;

    /// Splits into the two halves, which are driven by separate tasks.
    fn split(self) -> (Self::Read, Self::Write);
}

/// A source of connections.
pub trait Carrier {
    /// What it accepts.
    type Connection: Connection + 'static;

    /// Waits for the next connection. An error is retried: a carrier is a way in, and ways in
    /// never give up.
    fn accept(&mut self) -> impl Future<Output = io::Result<Self::Connection>>;
}
