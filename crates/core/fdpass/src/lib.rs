//! Passing file descriptors over a Unix stream socket.
//!
//! A frame on the socket is `[u32 LE length][payload]`, the same plain length prefix the wire uses
//! on sockets, sent in one `sendmsg`. Descriptors travel as `SCM_RIGHTS` attached to that frame, so
//! each set belongs to exactly one frame. Nothing here allocates.
//!
//! Every descriptor that arrives becomes an [`OwnedFd`] before anything else can go wrong, so an
//! error path closes them all. Received descriptors are close-on-exec.
//!
//! A stream socket can accept part of a frame when it is non-blocking. That leaves the stream out
//! of step, so [`SendError::ShortWrite`] means the connection must be closed.

use core::mem::{MaybeUninit, size_of, zeroed};
use core::ptr;
use std::io;
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, FromRawFd, OwnedFd, RawFd};

/// Bytes of the length prefix.
pub const HEADER_LEN: usize = 4;
/// The most descriptors one frame can carry.
pub const MAX_FDS: usize = 16;
/// The most payload parts one frame can be gathered from.
pub const MAX_PARTS: usize = 4;

const CONTROL_LEN: usize = size_of::<libc::cmsghdr>() + MAX_FDS * size_of::<RawFd>() + 16;

/// Control-message space, aligned the way `cmsghdr` needs.
#[repr(C, align(8))]
struct Control([MaybeUninit<u8>; CONTROL_LEN]);

impl Control {
    const fn new() -> Self {
        Self([MaybeUninit::zeroed(); CONTROL_LEN])
    }
}

/// Why a send failed.
#[derive(Debug)]
pub enum SendError {
    /// More than [`MAX_FDS`] descriptors or [`MAX_PARTS`] parts, or a payload over `u32::MAX`.
    TooLarge,
    /// The socket took part of the frame. The stream is out of step: close the connection.
    ShortWrite {
        /// Bytes the socket took.
        sent: usize,
        /// Bytes the frame has.
        expected: usize,
    },
    /// The system call failed.
    Io(io::Error),
}

/// Why a receive failed. Any descriptors that came with the frame have been closed.
#[derive(Debug)]
pub enum RecvError {
    /// The frame doesn't fit the caller's buffer. It is still queued: close the connection.
    TooLarge {
        /// The frame's payload length.
        len: usize,
    },
    /// More descriptors were sent than the receiver allowed.
    TruncatedControl,
    /// The peer closed part way through a frame.
    ShortRead,
    /// A control message was malformed.
    BadControl,
    /// The system call failed. `WouldBlock` means no whole frame is queued yet.
    Io(io::Error),
}

impl core::fmt::Display for SendError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::TooLarge => f.write_str("frame is too large to send"),
            Self::ShortWrite { sent, expected } => {
                write!(f, "socket took {sent} of {expected} bytes")
            }
            Self::Io(err) => write!(f, "send failed: {err}"),
        }
    }
}

impl core::fmt::Display for RecvError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::TooLarge { len } => write!(f, "frame of {len} bytes doesn't fit the buffer"),
            Self::TruncatedControl => f.write_str("more descriptors arrived than allowed"),
            Self::ShortRead => f.write_str("peer closed inside a frame"),
            Self::BadControl => f.write_str("malformed control message"),
            Self::Io(err) => write!(f, "receive failed: {err}"),
        }
    }
}

impl std::error::Error for SendError {}
impl std::error::Error for RecvError {}

/// The descriptors that came with one frame. Whatever isn't taken is closed on drop.
#[derive(Debug, Default)]
pub struct FdSet {
    fds: [Option<OwnedFd>; MAX_FDS],
    len: usize,
}

impl FdSet {
    /// How many descriptors arrived.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.len
    }

    /// Whether none arrived.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Takes descriptor `index`, if it is still here.
    pub fn take(&mut self, index: usize) -> Option<OwnedFd> {
        self.fds.get_mut(index)?.take()
    }

    fn push(&mut self, fd: OwnedFd) -> bool {
        match self.fds.get_mut(self.len) {
            Some(slot) => {
                *slot = Some(fd);
                self.len += 1;
                true
            }
            None => false,
        }
    }
}

/// One received frame.
#[derive(Debug)]
pub struct Received {
    /// Payload bytes written to the caller's buffer.
    pub len: usize,
    /// The descriptors tied to this frame.
    pub fds: FdSet,
}

fn control_space(fds: usize) -> usize {
    // SAFETY: `CMSG_SPACE` is arithmetic on its argument.
    unsafe { libc::CMSG_SPACE((fds * size_of::<RawFd>()) as u32) as usize }
}

fn retry<T: PartialOrd + From<i8>>(mut call: impl FnMut() -> T) -> io::Result<T> {
    loop {
        let result = call();
        if result >= T::from(0) {
            return Ok(result);
        }
        let err = io::Error::last_os_error();
        if err.kind() != io::ErrorKind::Interrupted {
            return Err(err);
        }
    }
}

/// Sends one frame made of `parts` joined together, with `fds` attached.
///
/// # Errors
///
/// [`SendError`]. `WouldBlock` in [`SendError::Io`] means nothing was sent.
pub fn send(
    socket: BorrowedFd<'_>,
    parts: &[&[u8]],
    fds: &[BorrowedFd<'_>],
    nonblocking: bool,
) -> Result<(), SendError> {
    if fds.len() > MAX_FDS || parts.len() > MAX_PARTS {
        return Err(SendError::TooLarge);
    }
    let payload: usize = parts.iter().map(|part| part.len()).sum();
    let header = u32::try_from(payload)
        .map_err(|_| SendError::TooLarge)?
        .to_le_bytes();

    let empty = libc::iovec {
        iov_base: ptr::null_mut(),
        iov_len: 0,
    };
    let mut iov = [empty; MAX_PARTS + 1];
    iov[0] = libc::iovec {
        iov_base: header.as_ptr().cast_mut().cast(),
        iov_len: HEADER_LEN,
    };
    for (slot, part) in iov[1..].iter_mut().zip(parts) {
        *slot = libc::iovec {
            iov_base: part.as_ptr().cast_mut().cast(),
            iov_len: part.len(),
        };
    }

    let mut control = Control::new();
    // SAFETY: an all-zero `msghdr` is valid: null pointers and zero lengths.
    let mut msg: libc::msghdr = unsafe { zeroed() };
    msg.msg_iov = iov.as_mut_ptr();
    msg.msg_iovlen = (parts.len() + 1) as _;
    if !fds.is_empty() {
        msg.msg_control = control.0.as_mut_ptr().cast();
        msg.msg_controllen = control_space(fds.len()) as _;
        // SAFETY: `msg_control` points at `CONTROL_LEN` aligned bytes, at least `CMSG_SPACE` for
        // `MAX_FDS` descriptors, so the first header and its data fit.
        unsafe {
            let cmsg = libc::CMSG_FIRSTHDR(&msg);
            (*cmsg).cmsg_level = libc::SOL_SOCKET;
            (*cmsg).cmsg_type = libc::SCM_RIGHTS;
            (*cmsg).cmsg_len = libc::CMSG_LEN((fds.len() * size_of::<RawFd>()) as u32) as _;
            let data = libc::CMSG_DATA(cmsg).cast::<RawFd>();
            for (index, fd) in fds.iter().enumerate() {
                data.add(index).write_unaligned(fd.as_raw_fd());
            }
        }
    }

    let mut flags = libc::MSG_NOSIGNAL;
    if nonblocking {
        flags |= libc::MSG_DONTWAIT;
    }
    // SAFETY: `msg` and everything it points at outlive the call.
    let sent = retry(|| unsafe { libc::sendmsg(socket.as_raw_fd(), &msg, flags) })
        .map_err(SendError::Io)? as usize;
    let expected = HEADER_LEN + payload;
    if sent == expected {
        Ok(())
    } else {
        Err(SendError::ShortWrite { sent, expected })
    }
}

/// Moves every descriptor in `msg`'s control data into `out`.
///
/// # Safety
///
/// `msg` must be a `msghdr` the kernel just filled in by `recvmsg`, with its control buffer alive.
unsafe fn collect(msg: &libc::msghdr, out: &mut FdSet) -> Result<(), RecvError> {
    let mut bad = false;
    // SAFETY: the caller guarantees `msg` is valid, and the walk stays within what the kernel wrote.
    unsafe {
        let mut cmsg = libc::CMSG_FIRSTHDR(msg);
        while !cmsg.is_null() {
            if (*cmsg).cmsg_level == libc::SOL_SOCKET && (*cmsg).cmsg_type == libc::SCM_RIGHTS {
                let bytes = ((*cmsg).cmsg_len as usize).saturating_sub(libc::CMSG_LEN(0) as usize);
                let data = libc::CMSG_DATA(cmsg).cast::<RawFd>();
                for index in 0..bytes / size_of::<RawFd>() {
                    // SAFETY: the kernel just installed this descriptor for this process and
                    // nothing else owns it yet.
                    let fd = OwnedFd::from_raw_fd(data.add(index).read_unaligned());
                    // A descriptor that doesn't fit is dropped here, which closes it.
                    bad |= !out.push(fd);
                }
                bad |= !bytes.is_multiple_of(size_of::<RawFd>());
            }
            cmsg = libc::CMSG_NXTHDR(msg, cmsg);
        }
    }
    if bad {
        Err(RecvError::BadControl)
    } else {
        Ok(())
    }
}

/// Reads the frame length without consuming anything. `None` means the peer closed.
fn peek_len(socket: RawFd, flags: i32) -> Result<Option<usize>, RecvError> {
    let mut header = [0_u8; HEADER_LEN];
    // SAFETY: `header` is valid for `HEADER_LEN` bytes for the whole call.
    let got = retry(|| unsafe {
        libc::recv(
            socket,
            header.as_mut_ptr().cast(),
            HEADER_LEN,
            flags | libc::MSG_PEEK,
        )
    })
    .map_err(RecvError::Io)? as usize;
    match got {
        0 => Ok(None),
        HEADER_LEN => Ok(Some(u32::from_le_bytes(header) as usize)),
        _ => Err(RecvError::Io(io::ErrorKind::WouldBlock.into())),
    }
}

/// Receives one frame into `buffer`, accepting up to `max_fds` descriptors.
///
/// Returns `Ok(None)` when the peer has closed the connection. When `nonblocking` is set, a frame
/// that isn't wholly queued yet stays queued and the result is `WouldBlock`.
///
/// # Errors
///
/// [`RecvError`]. Descriptors that came with a failed frame are closed.
pub fn recv(
    socket: BorrowedFd<'_>,
    buffer: &mut [u8],
    max_fds: usize,
    nonblocking: bool,
) -> Result<Option<Received>, RecvError> {
    let fd = socket.as_raw_fd();
    let max_fds = max_fds.min(MAX_FDS);
    let mut flags = libc::MSG_CMSG_CLOEXEC;
    if nonblocking {
        flags |= libc::MSG_DONTWAIT;
    }
    let Some(len) = peek_len(fd, flags)? else {
        return Ok(None);
    };
    if len > buffer.len() {
        return Err(RecvError::TooLarge { len });
    }
    let total = HEADER_LEN + len;
    if nonblocking {
        let mut queued: libc::c_int = 0;
        // SAFETY: FIONREAD writes one int to `queued`.
        if unsafe { libc::ioctl(fd, libc::FIONREAD, &mut queued) } < 0 {
            return Err(RecvError::Io(io::Error::last_os_error()));
        }
        if (queued as usize) < total {
            return Err(RecvError::Io(io::ErrorKind::WouldBlock.into()));
        }
    } else {
        flags |= libc::MSG_WAITALL;
    }

    let mut header = [0_u8; HEADER_LEN];
    let mut iov = [
        libc::iovec {
            iov_base: header.as_mut_ptr().cast(),
            iov_len: HEADER_LEN,
        },
        libc::iovec {
            iov_base: buffer.as_mut_ptr().cast(),
            iov_len: len,
        },
    ];
    let mut control = Control::new();
    // SAFETY: an all-zero `msghdr` is valid: null pointers and zero lengths.
    let mut msg: libc::msghdr = unsafe { zeroed() };
    msg.msg_iov = iov.as_mut_ptr();
    msg.msg_iovlen = iov.len() as _;
    if max_fds > 0 {
        msg.msg_control = control.0.as_mut_ptr().cast();
        msg.msg_controllen = control_space(max_fds) as _;
    }
    // SAFETY: `msg` points at buffers valid for the lengths it declares, for the whole call.
    let got = retry(|| unsafe { libc::recvmsg(fd, &mut msg, flags) }).map_err(RecvError::Io)?;
    let mut fds = FdSet::default();
    // Collected first, so every path below closes them by dropping `fds`.
    // SAFETY: the kernel just filled `msg` in, and `control` is still alive.
    let collected = unsafe { collect(&msg, &mut fds) };
    if msg.msg_flags & libc::MSG_CTRUNC != 0 {
        return Err(RecvError::TruncatedControl);
    }
    collected?;
    if (got as usize) < total {
        return Err(RecvError::ShortRead);
    }
    Ok(Some(Received { len, fds }))
}

/// [`send`] for anything holding a socket.
///
/// # Errors
///
/// As [`send`].
pub fn send_on<S: AsFd>(
    socket: &S,
    parts: &[&[u8]],
    fds: &[BorrowedFd<'_>],
) -> Result<(), SendError> {
    send(socket.as_fd(), parts, fds, false)
}
