//! The three ways bytes reach a comm core, behind one pair of calls: send a frame, and take
//! a frame if a whole one is there.

use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::os::fd::{AsFd, BorrowedFd, OwnedFd};
use std::os::unix::fs::OpenOptionsExt;
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::time::{Duration, Instant};

use orion_identity::Identity;
use orion_net_handshake::{
    Fresh, MODE_PLAIN, MODE_SIGNED, SERVER_HELLO_LEN, STATUS_OK, Seal, TAG_LEN, Transcript, hello,
    preamble,
};
use orion_wire::{StreamDecoder, StreamEncoder, frame};
use rustix::event::{PollFd, PollFlags, Timespec, poll};
use rustix::fs::OFlags;
use rustix::termios::{ControlModes, InputModes, OptionalActions, QueueSelector, SpecialCodeIndex};
use shm_fdpass::{FdSet, MAX_FDS, RecvError, SendError};

use crate::error::ClientError;

/// The largest frame a link carries.
const LINK_FRAME: usize = 8192;

/// How long a send waits for the other end to make room before the session counts as lost.
const SEND_WAIT: Duration = Duration::from_secs(5);

/// Writes all of `bytes` to a non-blocking descriptor, waiting for room when there is none.
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

pub(crate) enum Transport {
    Local(UnixStream),
    Network {
        stream: TcpStream,
        seals: Option<(Seal, Seal)>,
        /// Bytes read and not yet a whole frame.
        partial: Vec<u8>,
    },
    Link {
        file: File,
        decoder: Box<StreamDecoder<LINK_FRAME>>,
        chunk: Box<[u8; 1024]>,
        at: usize,
        len: usize,
    },
}

/// Waits until `fd` can be read, or `wait` has passed. `true` if it can.
pub(crate) fn readable(fd: BorrowedFd<'_>, wait: Duration) -> io::Result<bool> {
    wait_for(fd, PollFlags::IN, wait)
}

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
    pub(crate) fn local(path: &Path) -> Result<Self, ClientError> {
        let stream = UnixStream::connect(path)?;
        // Every descriptor of a session is non-blocking, so a runtime can wait on it too.
        // Sending waits for room itself.
        stream.set_nonblocking(true)?;
        Ok(Self::Local(stream))
    }

    pub(crate) fn serial(path: &Path, baud: u32) -> Result<Self, ClientError> {
        Ok(Self::link(open_serial(path, baud)?))
    }

    /// A link over a byte stream someone else opened.
    pub(crate) fn link(file: File) -> Self {
        Self::Link {
            file,
            decoder: Box::new(StreamDecoder::new()),
            chunk: Box::new([0; 1024]),
            at: 0,
            len: 0,
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
        stream.set_write_timeout(None)?;
        stream.set_nonblocking(true)?;
        Ok(Self::Network {
            stream,
            seals,
            partial: Vec::with_capacity(4096),
        })
    }

    pub(crate) fn fd(&self) -> BorrowedFd<'_> {
        match self {
            Self::Local(stream) => stream.as_fd(),
            Self::Network { stream, .. } => stream.as_fd(),
            Self::Link { file, .. } => file.as_fd(),
        }
    }

    pub(crate) const fn passes_descriptors(&self) -> bool {
        matches!(self, Self::Local(_))
    }

    /// The largest frame this way of connecting carries, whatever was asked for.
    pub(crate) const fn frame_limit(&self) -> usize {
        match self {
            Self::Link { .. } => LINK_FRAME,
            _ => usize::MAX,
        }
    }

    pub(crate) fn send(&mut self, frame: &[u8], fds: &[BorrowedFd<'_>]) -> Result<(), ClientError> {
        match self {
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
            Self::Network { stream, seals, .. } => {
                write_all(&*stream, &(frame.len() as u32).to_le_bytes())?;
                write_all(&*stream, frame)?;
                if let Some((_, to_server)) = seals {
                    let tag = to_server.seal(frame);
                    write_all(&*stream, &tag)?;
                }
                Ok(())
            }
            Self::Link { file, .. } => {
                let mut encoder = StreamEncoder::for_frame(frame).with_leading_delimiter();
                let mut out = [0_u8; 512];
                loop {
                    let len = encoder.fill(&mut out);
                    if len == 0 {
                        return Ok(());
                    }
                    write_all(&*file, &out[..len])?;
                }
            }
        }
    }

    /// Takes one whole frame into `buf` if one is there, without waiting.
    pub(crate) fn take(&mut self, buf: &mut [u8], fds: &mut FdSet) -> Result<Step, ClientError> {
        match self {
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
            Self::Network {
                stream,
                seals,
                partial,
            } => {
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
                    let read = stream.read(&mut partial[start..]);
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
                file,
                decoder,
                chunk,
                at,
                len,
            } => loop {
                if *at == *len {
                    match file.read(&mut chunk[..]) {
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
                        // A terminal whose other end is gone reads as an error, not the end.
                        Err(error) if error.raw_os_error() == Some(5) => return Ok(Step::Closed),
                        Err(error) => return Err(error.into()),
                    }
                }
                let (used, pushed) = decoder.push_slice(&chunk[*at..*len]);
                *at += used;
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
pub(crate) fn nonblocking(fd: OwnedFd) -> io::Result<File> {
    rustix::io::ioctl_fionbio(&fd, true)?;
    Ok(File::from(fd))
}
