//! A blocking client for the fd latest-value channel (`orion_transport_ipc::UnixFdLatestServer`,
//! docs/protocol-compatibility.md): ask for the newest frame, or one newer than a sequence held,
//! and take its payload and descriptors.
//!
//! The wire, each message one fd frame (`[payload_len u32 LE][payload]`, descriptors attached):
//! requests `[version 1][kind u8][0 u16][after_sequence u64 LE][wait_ms u32 LE]`, replies
//! `[version 1][status u8][0 u16][sequence u64 LE][published_at_unix_nanos u64 LE][payload]`.

use std::io;
use std::mem::{size_of, zeroed};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::time::Duration;

const VERSION: u8 = 1;
const KIND_LATEST: u8 = 0;
const KIND_NEXT_AFTER: u8 = 1;
const REPLY_HEADER: usize = 20;
/// The server's defaults: a reply carries at most this much payload and this many descriptors.
pub const MAX_PAYLOAD: usize = 64 * 1024;
pub const MAX_FDS: usize = crate::ORION_LATEST_MAX_FDS;

/// What a reply says about the frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum ReplyStatus {
    /// A frame, with its payload and descriptors.
    Frame = 0,
    /// Nothing published yet.
    Empty = 1,
    /// The newest frame is older than the server's maximum age.
    Stale = 2,
    /// No newer frame within the wait.
    Timeout = 3,
    /// The server has its maximum of clients.
    Busy = 4,
}

/// One reply. `fds` are this process's own copies.
#[derive(Debug)]
pub struct Reply {
    pub status: ReplyStatus,
    /// The frame's sequence (for `Frame` and `Stale`), or the newest known one (for `Timeout`).
    pub sequence: u64,
    pub published_at_unix_nanos: u64,
    pub payload: Vec<u8>,
    pub fds: Vec<OwnedFd>,
}

/// A connection to a latest-value channel.
pub struct LatestClient {
    stream: UnixStream,
}

impl LatestClient {
    pub fn connect(path: &Path) -> io::Result<Self> {
        Ok(Self {
            stream: UnixStream::connect(path)?,
        })
    }

    /// The newest frame, at once.
    pub fn latest(&mut self) -> io::Result<Reply> {
        self.request(KIND_LATEST, 0, 0)
    }

    /// The first frame newer than `sequence`, waiting up to `wait` (the server caps the wait).
    pub fn next_after(&mut self, sequence: u64, wait: Duration) -> io::Result<Reply> {
        let wait_ms = u32::try_from(wait.as_millis()).unwrap_or(u32::MAX);
        // The server may answer a wait up to its own maximum; the socket waits a little longer.
        self.stream
            .set_read_timeout(Some(wait + Duration::from_secs(5)))?;
        self.request(KIND_NEXT_AFTER, sequence, wait_ms)
    }

    fn request(&mut self, kind: u8, sequence: u64, wait_ms: u32) -> io::Result<Reply> {
        let mut request = [0u8; 4 + 16];
        request[..4].copy_from_slice(&16u32.to_le_bytes());
        request[4..8].copy_from_slice(&[VERSION, kind, 0, 0]);
        request[8..16].copy_from_slice(&sequence.to_le_bytes());
        request[16..20].copy_from_slice(&wait_ms.to_le_bytes());
        send_all(self.stream.as_raw_fd(), &request)?;
        let (mut payload, fds) = recv_frame(self.stream.as_raw_fd())?;
        if payload.len() < REPLY_HEADER || payload[0] != VERSION {
            return Err(invalid("reply header"));
        }
        let status = match payload[1] {
            0 => ReplyStatus::Frame,
            1 => ReplyStatus::Empty,
            2 => ReplyStatus::Stale,
            3 => ReplyStatus::Timeout,
            4 => ReplyStatus::Busy,
            _ => return Err(invalid("reply status")),
        };
        let word = |at: usize| {
            let mut bytes = [0u8; 8];
            bytes.copy_from_slice(&payload[at..at + 8]);
            u64::from_le_bytes(bytes)
        };
        let (sequence, published_at_unix_nanos) = (word(4), word(12));
        payload.drain(..REPLY_HEADER);
        Ok(Reply {
            status,
            sequence,
            published_at_unix_nanos,
            payload,
            fds,
        })
    }
}

fn invalid(what: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, format!("fd latest {what}"))
}

fn retry<T: PartialOrd + Default>(mut call: impl FnMut() -> T) -> io::Result<T> {
    loop {
        let result = call();
        if result >= T::default() {
            return Ok(result);
        }
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::Interrupted {
            return Err(error);
        }
    }
}

fn send_all(fd: RawFd, mut bytes: &[u8]) -> io::Result<()> {
    while !bytes.is_empty() {
        // SAFETY: `bytes` is valid for its length; MSG_NOSIGNAL keeps a closed peer an error.
        let sent = retry(|| unsafe {
            libc::send(fd, bytes.as_ptr().cast(), bytes.len(), libc::MSG_NOSIGNAL)
        })?;
        bytes = &bytes[sent as usize..];
    }
    Ok(())
}

/// One fd frame: its payload and the descriptors sent with it.
fn recv_frame(fd: RawFd) -> io::Result<(Vec<u8>, Vec<OwnedFd>)> {
    let mut header = [0u8; 4];
    // SAFETY: a peek into a 4-byte buffer; MSG_WAITALL waits for all four.
    let peeked = retry(|| unsafe {
        libc::recv(
            fd,
            header.as_mut_ptr().cast(),
            header.len(),
            libc::MSG_PEEK | libc::MSG_WAITALL,
        )
    })?;
    if peeked == 0 {
        return Err(io::ErrorKind::UnexpectedEof.into());
    }
    if (peeked as usize) < header.len() {
        return Err(io::ErrorKind::WouldBlock.into());
    }
    let len = u32::from_le_bytes(header) as usize;
    if len > REPLY_HEADER + MAX_PAYLOAD {
        return Err(invalid("reply too large"));
    }
    let mut payload = vec![0u8; len];
    let mut iov = [
        libc::iovec {
            iov_base: header.as_mut_ptr().cast(),
            iov_len: header.len(),
        },
        libc::iovec {
            iov_base: payload.as_mut_ptr().cast(),
            iov_len: payload.len(),
        },
    ];
    // SAFETY: CMSG_SPACE is a pure size computation.
    let space = unsafe { libc::CMSG_SPACE((MAX_FDS * size_of::<RawFd>()) as u32) } as usize;
    let mut control = vec![0u8; space];
    // SAFETY: an all-zero msghdr is valid; the pointers set below outlive the call.
    let mut msg: libc::msghdr = unsafe { zeroed() };
    msg.msg_iov = iov.as_mut_ptr();
    msg.msg_iovlen = iov.len() as _;
    msg.msg_control = control.as_mut_ptr().cast();
    msg.msg_controllen = control.len() as _;
    // SAFETY: `msg` points at live buffers of the sizes given.
    let received = retry(|| unsafe {
        libc::recvmsg(fd, &mut msg, libc::MSG_WAITALL | libc::MSG_CMSG_CLOEXEC)
    })?;
    // SAFETY: walks the control messages the kernel wrote into `control`.
    let fds = unsafe { take_fds(&msg) };
    if (received as usize) < 4 + len {
        return Err(invalid("short reply"));
    }
    if msg.msg_flags & libc::MSG_CTRUNC != 0 {
        return Err(invalid("reply with too many descriptors"));
    }
    Ok((payload, fds))
}

/// The descriptors in `msg`'s SCM_RIGHTS messages, owned (closed when dropped).
unsafe fn take_fds(msg: &libc::msghdr) -> Vec<OwnedFd> {
    let mut fds = Vec::new();
    // SAFETY (whole function): `msg` was filled by recvmsg; the CMSG macros stay in its buffer.
    let mut cmsg = unsafe { libc::CMSG_FIRSTHDR(msg) };
    while !cmsg.is_null() {
        let (level, kind, len) =
            unsafe { ((*cmsg).cmsg_level, (*cmsg).cmsg_type, (*cmsg).cmsg_len) };
        if level == libc::SOL_SOCKET && kind == libc::SCM_RIGHTS {
            // `cmsg_len` is a `usize` on glibc and a `u32` on musl.
            #[allow(clippy::unnecessary_cast)]
            let bytes = (len as usize).saturating_sub(unsafe { libc::CMSG_LEN(0) } as usize);
            let data = unsafe { libc::CMSG_DATA(cmsg) }.cast::<RawFd>();
            for index in 0..bytes / size_of::<RawFd>() {
                let raw = unsafe { data.add(index).read_unaligned() };
                fds.push(unsafe { OwnedFd::from_raw_fd(raw) });
            }
        }
        cmsg = unsafe { libc::CMSG_NXTHDR(msg, cmsg) };
    }
    fds
}
