//! The same-board carrier.
//!
//! Every program on the board connects once to one Unix socket and keeps the connection. A
//! frame on it is `[u32 length][frame]`, sent in one `sendmsg`, and file descriptors travel
//! attached to a frame: that is how a ring, a doorbell or a pool of buffers is handed over.
//!
//! Who is on the other end comes from the kernel (`SO_PEERCRED`), not from anything the
//! program says: a user id and a group id, which the one check weighs against a policy.
//!
//! Binding is careful about what is already at the path. A socket nobody answers is a leftover
//! and is replaced. A socket somebody answers means another comm core is running, and this one
//! does not take its place. Anything that is not a socket is never touched.

#![cfg(target_os = "linux")]

use std::io;
use std::os::fd::{AsFd, OwnedFd};
use std::os::unix::fs::{FileTypeExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::rc::Rc;

use orion_fdpass::{HEADER_LEN, MAX_FDS, RecvError, SendError};
use orion_server::{Carrier, Connection, FdSet, FrameRead, FrameWrite, Principal, Roles};
use tokio::io::Interest;
use tokio::net::{UnixListener, UnixStream};

/// The roles any program on the board may ask for.
const LOCAL_ROLES: Roles = Roles::CLIENT.and(Roles::PROVIDER).and(Roles::EXECUTOR);

/// The carrier: a listening Unix socket.
#[derive(Debug)]
pub struct UnixCarrier {
    listener: UnixListener,
    path: PathBuf,
    node_uids: Rc<[u32]>,
}

impl UnixCarrier {
    /// Listens at `path`, replacing a stale socket there. `mode` sets the socket's permission
    /// bits, which is what decides who on the board can connect at all. `node_uids` are the
    /// users whose programs may be this board's node logic, besides root.
    ///
    /// Must be called inside the runtime the carrier will be served on.
    ///
    /// # Errors
    ///
    /// - `AddrInUse`: another program is listening at `path`.
    /// - `AlreadyExists`: something that is not a socket is at `path`. It is left alone.
    /// - Anything `bind` or `chmod` fails with.
    pub fn bind(path: &Path, mode: Option<u32>, node_uids: &[u32]) -> io::Result<Self> {
        clear_stale(path)?;
        let listener = UnixListener::bind(path)?;
        if let Some(mode) = mode {
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode))?;
        }
        Ok(Self {
            listener,
            path: path.to_owned(),
            node_uids: node_uids.into(),
        })
    }

    /// Where it listens.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }
}

/// Removes a socket at `path` that nobody is listening on.
fn clear_stale(path: &Path) -> io::Result<()> {
    let kind = match std::fs::symlink_metadata(path) {
        Ok(metadata) => metadata.file_type(),
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    };
    if !kind.is_socket() {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            format!("{} exists and is not a socket", path.display()),
        ));
    }
    // Whether it is stale is whether anyone answers. A blocking connect to a Unix socket
    // returns at once either way.
    match std::os::unix::net::UnixStream::connect(path) {
        Ok(_) => Err(io::Error::new(
            io::ErrorKind::AddrInUse,
            format!("another program is listening at {}", path.display()),
        )),
        Err(error) if error.kind() == io::ErrorKind::ConnectionRefused => {
            std::fs::remove_file(path)
        }
        Err(error) => Err(error),
    }
}

impl Carrier for UnixCarrier {
    type Connection = UnixConnection;

    async fn accept(&mut self) -> io::Result<UnixConnection> {
        let (stream, _) = self.listener.accept().await?;
        let credentials = stream.peer_cred()?;
        let (uid, gid) = (credentials.uid(), credentials.gid());
        let offers = if uid == 0 || self.node_uids.contains(&uid) {
            LOCAL_ROLES.and(Roles::NODE)
        } else {
            LOCAL_ROLES
        };
        Ok(UnixConnection {
            stream: Rc::new(stream),
            principal: Principal::Local { uid, gid },
            offers,
        })
    }
}

/// One connection on the socket: the server's end of it, or a client's.
#[derive(Debug)]
pub struct UnixConnection {
    stream: Rc<UnixStream>,
    principal: Principal,
    offers: Roles,
}

impl UnixConnection {
    /// Connects to the comm core at `path`, as a client.
    ///
    /// # Errors
    ///
    /// If nothing is listening there or the program may not connect.
    pub async fn connect(path: &Path) -> io::Result<Self> {
        Ok(Self {
            stream: Rc::new(UnixStream::connect(path).await?),
            principal: Principal::Anonymous,
            offers: Roles(0),
        })
    }

    /// Sends one frame from this end.
    ///
    /// # Errors
    ///
    /// If the connection is broken.
    pub async fn send(&self, frame: &[u8], fds: &[OwnedFd]) -> io::Result<()> {
        send(&self.stream, frame, fds).await
    }

    /// Receives one frame at this end. `None` when the other side has closed.
    ///
    /// # Errors
    ///
    /// If the connection is broken or the frame doesn't fit `buf`.
    pub async fn recv(&self, buf: &mut [u8], fds: &mut FdSet) -> io::Result<Option<usize>> {
        recv(&self.stream, buf, fds).await
    }
}

/// The reading half of a [`UnixConnection`].
#[derive(Debug)]
pub struct UnixRead(Rc<UnixStream>);

/// The writing half of a [`UnixConnection`].
#[derive(Debug)]
pub struct UnixWrite(Rc<UnixStream>);

impl Connection for UnixConnection {
    type Read = UnixRead;
    type Write = UnixWrite;

    fn principal(&self) -> Principal {
        self.principal.clone()
    }

    fn offers(&self) -> Roles {
        self.offers
    }

    fn passes_descriptors(&self) -> bool {
        true
    }

    fn split(self) -> (UnixRead, UnixWrite) {
        (UnixRead(self.stream.clone()), UnixWrite(self.stream))
    }
}

impl FrameRead for UnixRead {
    async fn recv(&mut self, buf: &mut [u8], fds: &mut FdSet) -> io::Result<Option<usize>> {
        recv(&self.0, buf, fds).await
    }
}

impl FrameWrite for UnixWrite {
    async fn send(&mut self, frame: &[u8], fds: &[OwnedFd]) -> io::Result<()> {
        send(&self.0, frame, fds).await
    }
}

async fn recv(stream: &UnixStream, buf: &mut [u8], fds: &mut FdSet) -> io::Result<Option<usize>> {
    loop {
        stream.readable().await?;
        // A frame that is only partly here stays queued and counts as "would block", which
        // makes tokio wait for more of it.
        let received = stream.try_io(Interest::READABLE, || {
            match orion_fdpass::recv(stream.as_fd(), buf, MAX_FDS, true) {
                Err(RecvError::Io(error)) if error.kind() == io::ErrorKind::WouldBlock => {
                    Err(error)
                }
                other => Ok(other),
            }
        });
        match received {
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
            Err(error) => return Err(error),
            Ok(Ok(None)) => return Ok(None),
            Ok(Ok(Some(received))) => {
                *fds = received.fds;
                return Ok(Some(received.len));
            }
            Ok(Err(error)) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("{error:?}"),
                ));
            }
        }
    }
}

async fn send(stream: &UnixStream, frame: &[u8], fds: &[OwnedFd]) -> io::Result<()> {
    if fds.len() > MAX_FDS {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "too many descriptors",
        ));
    }
    let mut borrowed = [stream.as_fd(); MAX_FDS];
    for (slot, fd) in borrowed.iter_mut().zip(fds) {
        *slot = fd.as_fd();
    }
    let borrowed = &borrowed[..fds.len()];
    let sent = loop {
        stream.writable().await?;
        let sent = stream.try_io(Interest::WRITABLE, || {
            match orion_fdpass::send(stream.as_fd(), &[frame], borrowed, true) {
                Err(SendError::Io(error)) if error.kind() == io::ErrorKind::WouldBlock => {
                    Err(error)
                }
                other => Ok(other),
            }
        });
        match sent {
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
            Err(error) => return Err(error),
            Ok(Ok(())) => return Ok(()),
            Ok(Err(SendError::ShortWrite { sent, .. })) => break sent,
            Ok(Err(SendError::Io(error))) => return Err(error),
            Ok(Err(SendError::TooLarge)) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "frame too large",
                ));
            }
        }
    };
    // The socket took the first part of the frame, descriptors included. The rest follows as
    // plain bytes, so a reader that is a little behind is not cut off for it.
    let header = (frame.len() as u32).to_le_bytes();
    let mut done = sent;
    while done < HEADER_LEN + frame.len() {
        stream.writable().await?;
        let rest = if done < HEADER_LEN {
            &header[done..]
        } else {
            &frame[done - HEADER_LEN..]
        };
        match stream.try_write(rest) {
            Ok(wrote) => done += wrote,
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
            Err(error) => return Err(error),
        }
    }
    Ok(())
}
