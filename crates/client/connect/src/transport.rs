//! The ways bytes reach a comm core, behind one pair of calls: send a frame, and take a frame
//! if a whole one is there.

use std::io::{self, Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::time::Duration;
#[cfg(target_os = "linux")]
use std::{
    fs::{File, OpenOptions},
    os::fd::{AsFd, BorrowedFd, OwnedFd},
    os::unix::fs::OpenOptionsExt,
    os::unix::net::UnixStream,
    path::Path,
    time::Instant,
};

use orion_identity::Identity;
use orion_msg_session::{LINK_SEALED_OVERHEAD, LinkSealed};
use orion_net_handshake::{
    Fresh, MODE_PLAIN, MODE_SIGNED, SERVER_HELLO_LEN, STATUS_OK, Seal, TAG_LEN, Transcript, hello,
    preamble,
};
use orion_wire::{Bytes, Decode, StreamDecoder, StreamEncoder, encode_body, frame, kind};
#[cfg(target_os = "linux")]
use rustix::event::{PollFd, PollFlags, Timespec, poll};
#[cfg(target_os = "linux")]
use rustix::fs::OFlags;
#[cfg(target_os = "linux")]
use rustix::termios::{ControlModes, InputModes, OptionalActions, QueueSelector, SpecialCodeIndex};
#[cfg(target_os = "linux")]
use shm_fdpass::{MAX_FDS, RecvError, SendError};

use crate::error::ClientError;
use crate::fd::{BorrowedFd as AnyBorrowedFd, FdSet};
use crate::piped::{LinkRead, LinkWrite, Piped};
#[cfg(not(target_os = "linux"))]
use crate::piped::{TcpRead, TcpWrite};

/// The largest frame a link carries.
const LINK_FRAME: usize = 8192;

/// How long a send waits for the other end to make room before the session counts as lost.
const SEND_WAIT: Duration = Duration::from_secs(5);

/// Writes all of `bytes` to a non-blocking descriptor, waiting for room when there is none.
#[cfg(target_os = "linux")]
fn write_all<W: Write + AsFd>(mut to: W, bytes: &[u8]) -> Result<(), ClientError> {
    let mut sent = 0;
    while sent < bytes.len() {
        match to.write(&bytes[sent..]) {
            Ok(0) => return Err(ClientError::Lost),
            Ok(wrote) => sent += wrote,
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                if !wait_for(to.as_fd(), PollFlags::OUT, SEND_WAIT)? {
                    return Err(ClientError::Lost);
                }
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

/// What a read found.
pub(crate) enum Step {
    /// A whole frame of this many bytes is in the buffer.
    Frame(usize),
    /// Nothing whole yet.
    Nothing,
    /// The other end is gone.
    Closed,
}

/// Where a transport's bytes come from and go to.
pub(crate) enum Io {
    /// A non-blocking descriptor, waited on with `poll`.
    #[cfg(target_os = "linux")]
    Fd(File),
    /// A reading thread, and the half that writes.
    Piped {
        read: Piped,
        write: Box<dyn LinkWrite>,
    },
}

impl Io {
    fn piped(read: impl LinkRead, write: impl LinkWrite) -> io::Result<Self> {
        Ok(Self::Piped {
            read: Piped::start(read)?,
            write: Box::new(write),
        })
    }

    /// What has arrived, without waiting: `WouldBlock` if nothing has, `Ok(0)` at the end.
    fn read(&mut self, into: &mut [u8]) -> io::Result<usize> {
        match self {
            #[cfg(target_os = "linux")]
            Self::Fd(file) => match file.read(into) {
                // A terminal whose other end is gone reads as an error, not the end.
                Err(error) if error.raw_os_error() == Some(5) => Ok(0),
                read => read,
            },
            Self::Piped { read, .. } => match read.read(into) {
                // A link's read that fails is its end.
                Err(error) if error.kind() != io::ErrorKind::WouldBlock => Ok(0),
                read => read,
            },
        }
    }

    fn write_all(&mut self, bytes: &[u8]) -> Result<(), ClientError> {
        match self {
            #[cfg(target_os = "linux")]
            Self::Fd(file) => write_all(&*file, bytes),
            Self::Piped { write, .. } => write.write_all(bytes).map_err(|_| ClientError::Lost),
        }
    }

    fn wait(&mut self, wait: Duration) -> io::Result<bool> {
        match self {
            #[cfg(target_os = "linux")]
            Self::Fd(file) => readable(file.as_fd(), wait),
            Self::Piped { read, .. } => read.wait(wait),
        }
    }

    #[cfg(target_os = "linux")]
    fn fd(&self) -> BorrowedFd<'_> {
        match self {
            Self::Fd(file) => file.as_fd(),
            Self::Piped { read, .. } => read.fd(),
        }
    }
}

pub(crate) enum Transport {
    #[cfg(target_os = "linux")]
    Local(UnixStream),
    Network {
        io: Io,
        seals: Option<(Seal, Seal)>,
        /// Bytes read and not yet a whole frame.
        partial: Vec<u8>,
    },
    Link {
        io: Io,
        decoder: Box<StreamDecoder<LINK_FRAME>>,
        chunk: Box<[u8; 1024]>,
        at: usize,
        len: usize,
        /// Once the link's session is signed: the board's seal, then this client's.
        seals: Option<(Seal, Seal)>,
        /// Room for one sealed frame, on a signed link.
        sealed: Vec<u8>,
    },
}

/// Waits until `fd` can be read, or `wait` has passed. `true` if it can.
#[cfg(target_os = "linux")]
pub(crate) fn readable(fd: BorrowedFd<'_>, wait: Duration) -> io::Result<bool> {
    wait_for(fd, PollFlags::IN, wait)
}

#[cfg(target_os = "linux")]
fn wait_for(fd: BorrowedFd<'_>, flags: PollFlags, wait: Duration) -> io::Result<bool> {
    let deadline = Instant::now() + wait;
    loop {
        let left = deadline.saturating_duration_since(Instant::now());
        let timeout = Timespec {
            tv_sec: left.as_secs() as i64,
            tv_nsec: i64::from(left.subsec_nanos()),
        };
        let mut fds = [PollFd::from_borrowed_fd(fd, flags)];
        match poll(&mut fds, Some(&timeout)) {
            Ok(ready) => return Ok(ready > 0),
            Err(rustix::io::Errno::INTR) => {}
            Err(error) => return Err(error.into()),
        }
    }
}

#[cfg(target_os = "linux")]
fn open_serial(path: &Path, baud: u32) -> io::Result<File> {
    let flags = OFlags::NOCTTY | OFlags::NONBLOCK | OFlags::CLOEXEC;
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .custom_flags(flags.bits() as i32)
        .open(path)?;
    let mut tio = rustix::termios::tcgetattr(&file)?;
    tio.make_raw();
    tio.control_modes.remove(
        ControlModes::CSIZE | ControlModes::CSTOPB | ControlModes::PARENB | ControlModes::CRTSCTS,
    );
    tio.control_modes
        .insert(ControlModes::CS8 | ControlModes::CLOCAL | ControlModes::CREAD);
    tio.input_modes
        .remove(InputModes::IXON | InputModes::IXOFF | InputModes::IXANY);
    tio.special_codes[SpecialCodeIndex::VMIN] = 1;
    tio.special_codes[SpecialCodeIndex::VTIME] = 0;
    tio.set_speed(baud).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("unsupported baud rate {baud}"),
        )
    })?;
    rustix::termios::tcsetattr(&file, OptionalActions::Now, &tio)?;
    let _ = rustix::termios::tcflush(&file, QueueSelector::IOFlush);
    Ok(file)
}

impl Transport {
    #[cfg(target_os = "linux")]
    pub(crate) fn local(path: &Path) -> Result<Self, ClientError> {
        let stream = UnixStream::connect(path)?;
        // Every descriptor of a session is non-blocking, so a runtime can wait on it too.
        // Sending waits for room itself.
        stream.set_nonblocking(true)?;
        Ok(Self::Local(stream))
    }

    #[cfg(target_os = "linux")]
    pub(crate) fn serial(path: &Path, baud: u32) -> Result<Self, ClientError> {
        Ok(Self::link(Io::Fd(open_serial(path, baud)?)))
    }

    /// A link over two halves the application opened.
    pub(crate) fn over(read: impl LinkRead, write: impl LinkWrite) -> Result<Self, ClientError> {
        Ok(Self::link(Io::piped(read, write)?))
    }

    /// A link over a byte stream.
    pub(crate) fn link(io: Io) -> Self {
        Self::Link {
            io,
            decoder: Box::new(StreamDecoder::new()),
            chunk: Box::new([0; 1024]),
            at: 0,
            len: 0,
            seals: None,
            sealed: Vec::new(),
        }
    }

    /// Seals every frame on this link from now on (`sign_link`).
    pub(crate) fn seal_link(&mut self, from_board: Seal, to_board: Seal) {
        if let Self::Link { seals, sealed, .. } = self {
            *seals = Some((from_board, to_board));
            *sealed = vec![0; LINK_FRAME];
        }
    }

    /// Connects to `host` and, with an identity, proves who this end is and checks who the
    /// other is.
    pub(crate) fn network(
        host: &str,
        expect: Option<&[u8; 32]>,
        identity: Option<&Identity>,
        timeout: Duration,
    ) -> Result<Self, ClientError> {
        let mut last = io::Error::new(io::ErrorKind::NotFound, "the host has no address");
        let mut connected = None;
        for addr in host.to_socket_addrs()? {
            match TcpStream::connect_timeout(&addr, timeout) {
                Ok(stream) => {
                    connected = Some(stream);
                    break;
                }
                Err(error) => last = error,
            }
        }
        let mut stream = connected.ok_or(last)?;
        stream.set_nodelay(true)?;
        // The handshake is a few fixed messages: a deadline on each is a deadline on it.
        stream.set_read_timeout(Some(timeout))?;
        stream.set_write_timeout(Some(timeout))?;
        stream.write_all(&preamble(if identity.is_some() {
            MODE_SIGNED
        } else {
            MODE_PLAIN
        }))?;
        let mut status = [0_u8; 1];
        stream.read_exact(&mut status)?;
        if status[0] != STATUS_OK {
            return Err(ClientError::Denied(
                "the node needs a signed session, and this client has no key",
            ));
        }
        let seals = match identity {
            None if expect.is_some() => {
                return Err(ClientError::Denied(
                    "a node's key can only be checked on a signed session",
                ));
            }
            None => None,
            Some(identity) => {
                let fresh = Fresh::new();
                let client = hello(&fresh, &identity.public());
                stream.write_all(&client)?;
                let mut answer = [0_u8; SERVER_HELLO_LEN];
                stream.read_exact(&mut answer)?;
                let mut server = [0_u8; 96];
                server.copy_from_slice(&answer[..96]);
                let transcript = Transcript { client, server };
                if !transcript.server_signed(&answer[96..]) {
                    return Err(ClientError::Denied(
                        "the node did not prove it holds its key",
                    ));
                }
                if let Some(asked) = expect.filter(|asked| **asked != transcript.server_key()) {
                    return Err(ClientError::OtherKey {
                        asked: *asked,
                        answered: transcript.server_key(),
                    });
                }
                let (to_server, to_client) = transcript.keys(&fresh, true).ok_or(
                    ClientError::Denied("the node sent a key exchange that gives no secret"),
                )?;
                stream.write_all(&transcript.sign_client(identity))?;
                stream.read_exact(&mut status)?;
                if status[0] != STATUS_OK {
                    return Err(ClientError::Denied("the node refused this client's key"));
                }
                Some((Seal::new(to_client), Seal::new(to_server)))
            }
        };
        stream.set_read_timeout(None)?;
        #[cfg(target_os = "linux")]
        let io = {
            stream.set_write_timeout(None)?;
            stream.set_nonblocking(true)?;
            Io::Fd(File::from(OwnedFd::from(stream)))
        };
        // A send that finds no room for this long counts the session lost.
        #[cfg(not(target_os = "linux"))]
        let io = {
            stream.set_write_timeout(Some(SEND_WAIT))?;
            Io::piped(TcpRead(stream.try_clone()?), TcpWrite(stream))?
        };
        Ok(Self::Network {
            io,
            seals,
            partial: Vec::with_capacity(4096),
        })
    }

    /// The descriptor that is readable when there is something to take.
    #[cfg(target_os = "linux")]
    pub(crate) fn fd(&self) -> BorrowedFd<'_> {
        match self {
            Self::Local(stream) => stream.as_fd(),
            Self::Network { io, .. } | Self::Link { io, .. } => io.fd(),
        }
    }

    /// Waits up to `wait` for something to take. `true` if there is.
    pub(crate) fn wait(&mut self, wait: Duration) -> io::Result<bool> {
        match self {
            #[cfg(target_os = "linux")]
            Self::Local(stream) => readable(stream.as_fd(), wait),
            Self::Network { io, .. } | Self::Link { io, .. } => io.wait(wait),
        }
    }

    pub(crate) const fn passes_descriptors(&self) -> bool {
        #[cfg(target_os = "linux")]
        return matches!(self, Self::Local(_));
        #[cfg(not(target_os = "linux"))]
        false
    }

    /// The largest frame this way of connecting carries, whatever was asked for.
    pub(crate) const fn frame_limit(&self) -> usize {
        match self {
            Self::Link { seals: None, .. } => LINK_FRAME,
            Self::Link { .. } => LINK_FRAME - LINK_SEALED_OVERHEAD,
            _ => usize::MAX,
        }
    }

    pub(crate) fn send(
        &mut self,
        frame: &[u8],
        fds: &[AnyBorrowedFd<'_>],
    ) -> Result<(), ClientError> {
        match self {
            #[cfg(target_os = "linux")]
            Self::Local(stream) => loop {
                match shm_fdpass::send(stream.as_fd(), &[frame], fds, true) {
                    Ok(()) => return Ok(()),
                    // The comm core has not read what it was sent: wait for room.
                    Err(SendError::Io(error)) if error.kind() == io::ErrorKind::WouldBlock => {
                        if !wait_for(stream.as_fd(), PollFlags::OUT, SEND_WAIT)? {
                            return Err(ClientError::Lost);
                        }
                    }
                    Err(SendError::Io(error)) => return Err(error.into()),
                    Err(SendError::TooLarge) => return Err(ClientError::TooLarge),
                    // The stream is out of step: only a new connection is any use.
                    Err(SendError::ShortWrite { .. }) => return Err(ClientError::Lost),
                }
            },
            _ if !fds.is_empty() => Err(ClientError::NoDescriptors),
            Self::Network { io, seals, .. } => {
                io.write_all(&(frame.len() as u32).to_le_bytes())?;
                io.write_all(frame)?;
                if let Some((_, to_server)) = seals {
                    let tag = to_server.seal(frame);
                    io.write_all(&tag)?;
                }
                Ok(())
            }
            Self::Link {
                io, seals, sealed, ..
            } => {
                let frame = match seals {
                    Some((_, to_board)) => {
                        let tag = to_board.seal(frame);
                        let body = LinkSealed {
                            frame: Bytes(frame),
                            tag: Bytes(&tag),
                        };
                        let len =
                            encode_body(&body, 0, sealed).map_err(|_| ClientError::TooLarge)?;
                        &sealed[..len]
                    }
                    None => frame,
                };
                let mut encoder = StreamEncoder::for_frame(frame).with_leading_delimiter();
                let mut out = [0_u8; 512];
                loop {
                    let len = encoder.fill(&mut out);
                    if len == 0 {
                        return Ok(());
                    }
                    io.write_all(&out[..len])?;
                }
            }
        }
    }

    /// Takes one whole frame into `buf` if one is there, without waiting.
    pub(crate) fn take(&mut self, buf: &mut [u8], fds: &mut FdSet) -> Result<Step, ClientError> {
        #[cfg(not(target_os = "linux"))]
        let _ = fds;
        match self {
            #[cfg(target_os = "linux")]
            Self::Local(stream) => match shm_fdpass::recv(stream.as_fd(), buf, MAX_FDS, true) {
                Ok(Some(received)) => {
                    *fds = received.fds;
                    Ok(Step::Frame(received.len))
                }
                Ok(None) => Ok(Step::Closed),
                Err(RecvError::Io(error)) if error.kind() == io::ErrorKind::WouldBlock => {
                    Ok(Step::Nothing)
                }
                Err(RecvError::Io(error)) => Err(error.into()),
                Err(RecvError::TooLarge { .. }) => Err(ClientError::TooLarge),
                Err(_) => Err(ClientError::Protocol("a frame arrived damaged")),
            },
            Self::Network { io, seals, partial } => {
                let tag = if seals.is_some() { TAG_LEN } else { 0 };
                loop {
                    if partial.len() >= 4 {
                        let len =
                            u32::from_le_bytes([partial[0], partial[1], partial[2], partial[3]])
                                as usize;
                        if len > buf.len() {
                            return Err(ClientError::TooLarge);
                        }
                        if partial.len() >= 4 + len + tag {
                            buf[..len].copy_from_slice(&partial[4..4 + len]);
                            if let Some((from_server, _)) = seals
                                && !from_server.open(&buf[..len], &partial[4 + len..4 + len + tag])
                            {
                                return Err(ClientError::Protocol("a frame failed its check"));
                            }
                            partial.drain(..4 + len + tag);
                            return Ok(Step::Frame(len));
                        }
                    }
                    // Room for the largest frame and one read, made once: after that the
                    // buffer never grows.
                    let room = 4 + buf.len() + tag + 4096;
                    if partial.capacity() < room {
                        partial.reserve_exact(room - partial.len());
                    }
                    let start = partial.len();
                    partial.resize((start + 4096).min(partial.capacity()), 0);
                    let read = io.read(&mut partial[start..]);
                    partial.truncate(start + *read.as_ref().unwrap_or(&0));
                    match read {
                        Ok(0) => return Ok(Step::Closed),
                        Ok(_) => {}
                        Err(error)
                            if matches!(
                                error.kind(),
                                io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
                            ) =>
                        {
                            return Ok(Step::Nothing);
                        }
                        Err(error) => return Err(error.into()),
                    }
                }
            }
            Self::Link {
                io,
                decoder,
                chunk,
                at,
                len,
                seals,
                ..
            } => loop {
                if *at == *len {
                    match io.read(&mut chunk[..]) {
                        Ok(0) => return Ok(Step::Closed),
                        Ok(read) => (*at, *len) = (0, read),
                        Err(error)
                            if matches!(
                                error.kind(),
                                io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
                            ) =>
                        {
                            return Ok(Step::Nothing);
                        }
                        Err(error) => return Err(error.into()),
                    }
                }
                let (used, pushed) = decoder.push_slice(&chunk[*at..*len]);
                *at += used;
                if let (Ok(Some(view)), Some((from_board, _))) = (&pushed, seals.as_mut()) {
                    if view.kind() != kind::LINK_SEALED {
                        return Err(ClientError::Protocol(
                            "a frame of a signed link came unsealed",
                        ));
                    }
                    let body = LinkSealed::decode(view.payload())
                        .map_err(|_| ClientError::Protocol("a sealed frame that does not read"))?;
                    if !from_board.open(body.frame.0, body.tag.0) {
                        return Err(ClientError::Protocol("a frame failed its check"));
                    }
                    let to = buf
                        .get_mut(..body.frame.0.len())
                        .ok_or(ClientError::TooLarge)?;
                    to.copy_from_slice(body.frame.0);
                    return Ok(Step::Frame(to.len()));
                }
                if let Ok(Some(view)) = pushed {
                    return frame::encode(view.header(), view.payload(), buf)
                        .map(Step::Frame)
                        .map_err(|_| ClientError::TooLarge);
                }
            },
        }
    }
}

/// Makes a descriptor someone else opened non-blocking, as a link's must be.
#[cfg(target_os = "linux")]
pub(crate) fn nonblocking(fd: OwnedFd) -> io::Result<Io> {
    rustix::io::ioctl_fionbio(&fd, true)?;
    Ok(Io::Fd(File::from(fd)))
}
