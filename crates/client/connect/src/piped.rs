//! Bytes from a reader that blocks, brought to a session that does not.
//!
//! A link the application opened itself (a USB interface it claimed, say) comes as two halves:
//! a [`LinkRead`] and a [`LinkWrite`]. The read half goes to a thread of its own, which reads
//! for as long as it is let and passes what it read on. The session keeps the write half and
//! writes on the caller's thread when it sends, so a read never holds up a write.
//!
//! **Ending.** The session never waits for the reading thread. When the session is dropped, or
//! [`crate::Session::reconnect_over`] gives it new halves, it drops its write half: that is the
//! application's signal to make the read return, with an error (cancel the transfer, close
//! the handle). The thread ends when a read fails, or when one returns and nobody is listening
//! any more; what that read brought is thrown away.
//!
//! Off Linux, a TCP connection to a node goes the same way.

use std::io;
use std::sync::mpsc::{self, Receiver, TryRecvError};
use std::time::Duration;

/// The half of a link that brings bytes in. It runs on a thread of its own.
pub trait LinkRead: Send + 'static {
    /// Reads what has arrived into `into`, waiting for as long as it takes. `Ok(0)` is nothing
    /// yet (a zero-length packet, or a host that has not set the device up), not the end; an
    /// error is the end.
    ///
    /// # Errors
    ///
    /// When the link is gone, or the application is taking it down.
    fn read(&mut self, into: &mut [u8]) -> io::Result<usize>;
}

/// The half of a link that takes bytes out. Dropping it is the sign the link is let go.
pub trait LinkWrite: Send + 'static {
    /// Writes all of `bytes`.
    ///
    /// # Errors
    ///
    /// When the link is gone.
    fn write_all(&mut self, bytes: &[u8]) -> io::Result<()>;
}

/// What the reading thread passes on.
type Chunk = io::Result<Vec<u8>>;

/// The session's end of a reading thread.
pub(crate) struct Piped {
    from: Receiver<Chunk>,
    /// Read and not yet taken.
    held: Vec<u8>,
    at: usize,
    /// How the thread ended, said once.
    failed: Option<io::Error>,
    /// Rung once for every chunk, so the session has a descriptor to wait on.
    #[cfg(target_os = "linux")]
    bell: std::os::fd::OwnedFd,
}

impl Piped {
    /// Starts a thread that reads `read` for the session.
    pub(crate) fn start(mut read: impl LinkRead) -> io::Result<Self> {
        let (to, from) = mpsc::channel::<Chunk>();
        #[cfg(target_os = "linux")]
        let (bell, ring) = rustix::pipe::pipe_with(
            rustix::pipe::PipeFlags::CLOEXEC | rustix::pipe::PipeFlags::NONBLOCK,
        )?;
        std::thread::Builder::new()
            .name("orion-link-read".into())
            .spawn(move || {
                let mut chunk = vec![0_u8; 4096];
                loop {
                    let read = match read.read(&mut chunk) {
                        Ok(0) => continue,
                        Ok(len) => Ok(chunk[..len].to_vec()),
                        Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                        Err(error) => Err(error),
                    };
                    let end = read.is_err();
                    if to.send(read).is_err() {
                        return;
                    }
                    #[cfg(target_os = "linux")]
                    let _ = rustix::io::write(&ring, &[1]);
                    if end {
                        return;
                    }
                }
            })?;
        Ok(Self {
            from,
            held: Vec::new(),
            at: 0,
            failed: None,
            #[cfg(target_os = "linux")]
            bell,
        })
    }

    /// Takes what has arrived, without waiting: `WouldBlock` if nothing has, `Ok(0)` once the
    /// thread has ended.
    pub(crate) fn read(&mut self, into: &mut [u8]) -> io::Result<usize> {
        if self.at == self.held.len() {
            if let Some(error) = self.failed.take() {
                return Err(error);
            }
            // The bell first: a chunk sent after this rings it again.
            #[cfg(target_os = "linux")]
            {
                let mut drain = [0_u8; 64];
                while matches!(rustix::io::read(&self.bell, &mut drain), Ok(len) if len > 0) {}
            }
            match self.from.try_recv() {
                Ok(Ok(chunk)) => (self.held, self.at) = (chunk, 0),
                Ok(Err(error)) => return Err(error),
                Err(TryRecvError::Empty) => return Err(io::ErrorKind::WouldBlock.into()),
                Err(TryRecvError::Disconnected) => return Ok(0),
            }
        }
        let len = into.len().min(self.held.len() - self.at);
        into[..len].copy_from_slice(&self.held[self.at..self.at + len]);
        self.at += len;
        Ok(len)
    }

    /// Waits up to `wait` for something to read. `true` if there is (or the thread ended).
    pub(crate) fn wait(&mut self, wait: Duration) -> io::Result<bool> {
        if self.at < self.held.len() || self.failed.is_some() {
            return Ok(true);
        }
        #[cfg(target_os = "linux")]
        {
            use std::os::fd::AsFd;
            crate::transport::readable(self.bell.as_fd(), wait)
        }
        #[cfg(not(target_os = "linux"))]
        match self.from.recv_timeout(wait) {
            Ok(Ok(chunk)) => {
                (self.held, self.at) = (chunk, 0);
                Ok(true)
            }
            Ok(Err(error)) => {
                self.failed = Some(error);
                Ok(true)
            }
            Err(mpsc::RecvTimeoutError::Timeout) => Ok(false),
            Err(mpsc::RecvTimeoutError::Disconnected) => Ok(true),
        }
    }

    /// The descriptor that is readable when there is something to read.
    #[cfg(target_os = "linux")]
    pub(crate) fn fd(&self) -> std::os::fd::BorrowedFd<'_> {
        use std::os::fd::AsFd;
        self.bell.as_fd()
    }
}

/// A TCP connection's reading half.
#[cfg(not(target_os = "linux"))]
pub(crate) struct TcpRead(pub(crate) std::net::TcpStream);

#[cfg(not(target_os = "linux"))]
impl LinkRead for TcpRead {
    fn read(&mut self, into: &mut [u8]) -> io::Result<usize> {
        match io::Read::read(&mut self.0, into) {
            // The other end closed: the end, which a link read says with an error.
            Ok(0) => Err(io::ErrorKind::UnexpectedEof.into()),
            read => read,
        }
    }
}

/// A TCP connection's writing half. Dropping it shuts the connection, which ends the read.
#[cfg(not(target_os = "linux"))]
pub(crate) struct TcpWrite(pub(crate) std::net::TcpStream);

#[cfg(not(target_os = "linux"))]
impl LinkWrite for TcpWrite {
    fn write_all(&mut self, bytes: &[u8]) -> io::Result<()> {
        io::Write::write_all(&mut self.0, bytes)
    }
}

#[cfg(not(target_os = "linux"))]
impl Drop for TcpWrite {
    fn drop(&mut self) {
        let _ = self.0.shutdown(std::net::Shutdown::Both);
    }
}
