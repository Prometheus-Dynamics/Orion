use crate::IpcTransportError;
use std::{
    io,
    mem::{size_of, zeroed},
    os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd},
    ptr,
};

const HEADER_BYTES: usize = size_of::<u32>();
pub const DEFAULT_UNIX_FD_FRAME_MAX_PAYLOAD_BYTES: usize = 64 * 1024;
pub const DEFAULT_UNIX_FD_FRAME_MAX_FDS: usize = 16;
#[cfg(target_os = "linux")]
const RECV_CLOEXEC_FLAG: i32 = libc::MSG_CMSG_CLOEXEC;
#[cfg(not(target_os = "linux"))]
const RECV_CLOEXEC_FLAG: i32 = 0;

/// A single Unix-domain socket frame containing a small payload plus optional file descriptors.
///
/// This helper is intentionally transport-generic: callers own the payload schema and the meaning
/// of each descriptor. It is meant for metadata plus fd handoff, not bulk payload transfer.
#[derive(Debug)]
pub struct UnixFdFrame {
    pub payload: Vec<u8>,
    pub fds: Vec<OwnedFd>,
}

impl UnixFdFrame {
    pub fn new(payload: Vec<u8>, fds: Vec<OwnedFd>) -> Self {
        Self { payload, fds }
    }
}

pub fn send_unix_fd_frame<S: AsRawFd>(
    stream: &S,
    frame: &UnixFdFrame,
    max_payload_bytes: usize,
    max_fds: usize,
) -> Result<usize, IpcTransportError> {
    validate_send_frame(frame, max_payload_bytes, max_fds)?;
    send_unix_fd_frame_raw(stream.as_raw_fd(), frame, 0)
        .map_err(|err| IpcTransportError::WriteFailed(err.to_string()))
}

pub async fn send_unix_fd_frame_async(
    stream: &tokio::net::UnixStream,
    frame: &UnixFdFrame,
    max_payload_bytes: usize,
    max_fds: usize,
) -> Result<usize, IpcTransportError> {
    validate_send_frame(frame, max_payload_bytes, max_fds)?;
    loop {
        stream
            .writable()
            .await
            .map_err(|err| IpcTransportError::WriteFailed(err.to_string()))?;
        match send_unix_fd_frame_raw(stream.as_raw_fd(), frame, libc::MSG_DONTWAIT) {
            Ok(sent) => return Ok(sent),
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => continue,
            Err(err) => return Err(IpcTransportError::WriteFailed(err.to_string())),
        }
    }
}

fn validate_send_frame(
    frame: &UnixFdFrame,
    max_payload_bytes: usize,
    max_fds: usize,
) -> Result<(), IpcTransportError> {
    let max_payload_bytes = max_payload_bytes.max(1);
    if frame.payload.len() > max_payload_bytes {
        return Err(IpcTransportError::EncodeFailed(
            "fd frame payload exceeds maximum transport payload size".into(),
        ));
    }
    if frame.fds.len() > max_fds {
        return Err(IpcTransportError::EncodeFailed(
            "fd frame descriptor count exceeds maximum transport descriptor count".into(),
        ));
    }
    Ok(())
}

fn send_unix_fd_frame_raw(raw_fd: RawFd, frame: &UnixFdFrame, flags: i32) -> io::Result<usize> {
    let header = u32::try_from(frame.payload.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "fd frame payload too large"))?
        .to_le_bytes();
    let mut iov = [
        libc::iovec {
            iov_base: header.as_ptr().cast_mut().cast(),
            iov_len: header.len(),
        },
        libc::iovec {
            iov_base: frame.payload.as_ptr().cast_mut().cast(),
            iov_len: frame.payload.len(),
        },
    ];

    let raw_fds = frame.fds.iter().map(AsRawFd::as_raw_fd).collect::<Vec<_>>();
    let mut control = control_buffer_for(raw_fds.len());
    let mut msg: libc::msghdr = unsafe { zeroed() };
    msg.msg_iov = iov.as_mut_ptr();
    msg.msg_iovlen = iov.len();

    if !raw_fds.is_empty() {
        msg.msg_control = control.as_mut_ptr().cast();
        msg.msg_controllen = control.len();
        unsafe {
            let cmsg = libc::CMSG_FIRSTHDR(&msg);
            if cmsg.is_null() {
                return Err(io::Error::other(
                    "failed to allocate fd frame control message",
                ));
            }
            (*cmsg).cmsg_level = libc::SOL_SOCKET;
            (*cmsg).cmsg_type = libc::SCM_RIGHTS;
            (*cmsg).cmsg_len =
                libc::CMSG_LEN(raw_fds.len() as u32 * size_of::<RawFd>() as u32) as libc::size_t;
            ptr::copy_nonoverlapping(
                raw_fds.as_ptr().cast::<u8>(),
                libc::CMSG_DATA(cmsg).cast(),
                raw_fds.len() * size_of::<RawFd>(),
            );
        }
    }

    let expected = HEADER_BYTES + frame.payload.len();
    let sent = retry_interrupted(|| unsafe { libc::sendmsg(raw_fd, &msg, flags) })?;
    let sent =
        usize::try_from(sent).map_err(|_| io::Error::other("sendmsg returned invalid length"))?;
    if sent != expected {
        return Err(io::Error::new(
            io::ErrorKind::WriteZero,
            format!("short fd frame write: sent {sent} of {expected} bytes"),
        ));
    }
    Ok(sent)
}

pub fn recv_unix_fd_frame<S: AsRawFd>(
    stream: &S,
    max_payload_bytes: usize,
    max_fds: usize,
) -> Result<Option<UnixFdFrame>, IpcTransportError> {
    recv_unix_fd_frame_raw(
        stream.as_raw_fd(),
        max_payload_bytes,
        max_fds,
        RECV_CLOEXEC_FLAG,
    )
    .map_err(RawFdFrameError::into_ipc)
}

pub async fn recv_unix_fd_frame_async(
    stream: &tokio::net::UnixStream,
    max_payload_bytes: usize,
    max_fds: usize,
) -> Result<Option<UnixFdFrame>, IpcTransportError> {
    loop {
        stream
            .readable()
            .await
            .map_err(|err| IpcTransportError::ReadFailed(err.to_string()))?;
        match recv_unix_fd_frame_raw(
            stream.as_raw_fd(),
            max_payload_bytes,
            max_fds,
            libc::MSG_DONTWAIT | RECV_CLOEXEC_FLAG,
        ) {
            Ok(frame) => return Ok(frame),
            Err(RawFdFrameError::Io(err)) if err.kind() == io::ErrorKind::WouldBlock => continue,
            Err(err) => return Err(err.into_ipc()),
        }
    }
}

fn recv_unix_fd_frame_raw(
    raw_fd: RawFd,
    max_payload_bytes: usize,
    max_fds: usize,
    flags: i32,
) -> Result<Option<UnixFdFrame>, RawFdFrameError> {
    let max_payload_bytes = max_payload_bytes.max(1);
    let Some(payload_len) = peek_payload_len(raw_fd, flags)? else {
        return Ok(None);
    };
    if payload_len > max_payload_bytes {
        return Err(RawFdFrameError::Decode(
            "fd frame payload exceeds maximum transport payload size".into(),
        ));
    }
    let frame_len = HEADER_BYTES + payload_len;
    if flags & libc::MSG_DONTWAIT != 0 && queued_bytes(raw_fd)? < frame_len {
        return Err(RawFdFrameError::Io(io::Error::from(
            io::ErrorKind::WouldBlock,
        )));
    }

    let mut bytes = vec![0_u8; frame_len];
    let mut iov = [libc::iovec {
        iov_base: bytes.as_mut_ptr().cast(),
        iov_len: bytes.len(),
    }];
    let mut control = control_buffer_for(max_fds);
    let mut msg: libc::msghdr = unsafe { zeroed() };
    msg.msg_iov = iov.as_mut_ptr();
    msg.msg_iovlen = iov.len();
    if max_fds > 0 {
        msg.msg_control = control.as_mut_ptr().cast();
        msg.msg_controllen = control.len();
    }

    let received = retry_interrupted(|| unsafe { libc::recvmsg(raw_fd, &mut msg, flags) })
        .map_err(RawFdFrameError::Io)?;
    if received == 0 {
        return Ok(None);
    }
    if msg.msg_flags & libc::MSG_CTRUNC != 0 {
        return Err(RawFdFrameError::Decode(
            "fd frame control data was truncated".into(),
        ));
    }
    if msg.msg_flags & libc::MSG_TRUNC != 0 {
        return Err(RawFdFrameError::Decode(
            "fd frame payload was truncated".into(),
        ));
    }

    let received = usize::try_from(received)
        .map_err(|_| RawFdFrameError::Io(io::Error::other("recvmsg returned invalid length")))?;
    if received < HEADER_BYTES {
        return Err(RawFdFrameError::Decode(
            "fd frame missing payload length header".into(),
        ));
    }
    let received_payload_len = u32::from_le_bytes(
        bytes[..HEADER_BYTES]
            .try_into()
            .expect("header slice has fixed length"),
    ) as usize;
    if received_payload_len != payload_len {
        return Err(RawFdFrameError::Decode(
            "fd frame payload length header changed while receiving frame".into(),
        ));
    }
    if received < frame_len {
        return Err(RawFdFrameError::Decode(format!(
            "short fd frame read: received {received} of {frame_len} bytes"
        )));
    }
    if received > frame_len {
        return Err(RawFdFrameError::Decode(
            "fd frame contained trailing bytes".into(),
        ));
    }

    let fds = unsafe { collect_fds(&msg, max_fds) }.map_err(|err| match err {
        IpcTransportError::DecodeFailed(message) => RawFdFrameError::Decode(message),
        other => RawFdFrameError::Decode(other.to_string()),
    })?;
    Ok(Some(UnixFdFrame {
        payload: bytes[HEADER_BYTES..frame_len].to_vec(),
        fds,
    }))
}

enum RawFdFrameError {
    Io(io::Error),
    Decode(String),
}

impl RawFdFrameError {
    fn into_ipc(self) -> IpcTransportError {
        match self {
            Self::Io(err) => IpcTransportError::ReadFailed(err.to_string()),
            Self::Decode(message) => IpcTransportError::DecodeFailed(message),
        }
    }
}

fn peek_payload_len(raw_fd: RawFd, flags: i32) -> Result<Option<usize>, RawFdFrameError> {
    let mut header = [0_u8; HEADER_BYTES];
    let mut iov = [libc::iovec {
        iov_base: header.as_mut_ptr().cast(),
        iov_len: header.len(),
    }];
    let mut msg: libc::msghdr = unsafe { zeroed() };
    msg.msg_iov = iov.as_mut_ptr();
    msg.msg_iovlen = iov.len();

    let received =
        retry_interrupted(|| unsafe { libc::recvmsg(raw_fd, &mut msg, flags | libc::MSG_PEEK) })
            .map_err(RawFdFrameError::Io)?;
    if received == 0 {
        return Ok(None);
    }
    let received = usize::try_from(received)
        .map_err(|_| RawFdFrameError::Io(io::Error::other("recvmsg returned invalid length")))?;
    if received < HEADER_BYTES {
        return Err(RawFdFrameError::Io(io::Error::from(
            io::ErrorKind::WouldBlock,
        )));
    }
    Ok(Some(u32::from_le_bytes(header) as usize))
}

fn queued_bytes(raw_fd: RawFd) -> Result<usize, RawFdFrameError> {
    let mut available: libc::c_int = 0;
    let result = unsafe { libc::ioctl(raw_fd, libc::FIONREAD, &mut available) };
    if result < 0 {
        return Err(RawFdFrameError::Io(io::Error::last_os_error()));
    }
    usize::try_from(available)
        .map_err(|_| RawFdFrameError::Io(io::Error::other("FIONREAD returned invalid length")))
}

fn control_buffer_for(fd_count: usize) -> Vec<u8> {
    if fd_count == 0 {
        Vec::new()
    } else {
        vec![0_u8; unsafe { libc::CMSG_SPACE((fd_count * size_of::<RawFd>()) as u32) } as usize]
    }
}

unsafe fn collect_fds(
    msg: &libc::msghdr,
    max_fds: usize,
) -> Result<Vec<OwnedFd>, IpcTransportError> {
    let mut fds = Vec::new();
    let mut cmsg = unsafe { libc::CMSG_FIRSTHDR(msg) };
    while !cmsg.is_null() {
        let is_rights = unsafe {
            (*cmsg).cmsg_level == libc::SOL_SOCKET && (*cmsg).cmsg_type == libc::SCM_RIGHTS
        };
        if is_rights {
            let data_len = unsafe { (*cmsg).cmsg_len as usize }
                .saturating_sub(unsafe { libc::CMSG_LEN(0) as usize });
            if data_len % size_of::<RawFd>() != 0 {
                return Err(IpcTransportError::DecodeFailed(
                    "fd frame control message had invalid descriptor payload length".into(),
                ));
            }
            let count = data_len / size_of::<RawFd>();
            if fds.len() + count > max_fds {
                return Err(IpcTransportError::DecodeFailed(
                    "fd frame descriptor count exceeds maximum transport descriptor count".into(),
                ));
            }
            let data = unsafe { libc::CMSG_DATA(cmsg).cast::<RawFd>() };
            for index in 0..count {
                let fd = unsafe { *data.add(index) };
                fds.push(unsafe { OwnedFd::from_raw_fd(fd) });
            }
        }
        cmsg = unsafe { libc::CMSG_NXTHDR(msg, cmsg) };
    }
    Ok(fds)
}

fn retry_interrupted(mut operation: impl FnMut() -> libc::ssize_t) -> io::Result<libc::ssize_t> {
    loop {
        let result = operation();
        if result >= 0 {
            return Ok(result);
        }
        let err = io::Error::last_os_error();
        if err.kind() != io::ErrorKind::Interrupted {
            return Err(err);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        fs::{self, File},
        io::Read,
        os::fd::{AsRawFd, OwnedFd},
        os::unix::net::UnixStream,
        path::PathBuf,
        time::{SystemTime, UNIX_EPOCH},
    };

    fn temp_file_path(name: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after unix epoch")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "orion-ipc-fd-frame-{name}-{}-{nonce}.txt",
            std::process::id()
        ))
    }

    #[test]
    fn unix_fd_frame_sends_payload_and_descriptors_without_copying_fd_content() {
        let path = temp_file_path("sync");
        fs::write(&path, b"shared-frame-bytes").expect("fd test file should be written");
        let file = File::open(&path).expect("fd test file should open");
        let fd = OwnedFd::from(file);
        let (sender, receiver) = UnixStream::pair().expect("unix socket pair should open");

        let sent = send_unix_fd_frame(
            &sender,
            &UnixFdFrame::new(b"{\"descriptor\":\"metadata-only\"}".to_vec(), vec![fd]),
            DEFAULT_UNIX_FD_FRAME_MAX_PAYLOAD_BYTES,
            DEFAULT_UNIX_FD_FRAME_MAX_FDS,
        )
        .expect("fd frame should send");
        assert_eq!(sent, 4 + "{\"descriptor\":\"metadata-only\"}".len());

        let received = recv_unix_fd_frame(
            &receiver,
            DEFAULT_UNIX_FD_FRAME_MAX_PAYLOAD_BYTES,
            DEFAULT_UNIX_FD_FRAME_MAX_FDS,
        )
        .expect("fd frame should receive")
        .expect("socket should contain a frame");
        assert_eq!(received.payload, b"{\"descriptor\":\"metadata-only\"}");
        assert_eq!(received.fds.len(), 1);

        let received_fd = received
            .fds
            .into_iter()
            .next()
            .expect("received fd should exist");
        let fd_flags = unsafe { libc::fcntl(received_fd.as_raw_fd(), libc::F_GETFD) };
        assert_ne!(
            fd_flags & libc::FD_CLOEXEC,
            0,
            "received fds should be marked close-on-exec"
        );
        let mut imported = File::from(received_fd);
        let mut content = String::new();
        imported
            .read_to_string(&mut content)
            .expect("received fd should read original content");
        assert_eq!(content, "shared-frame-bytes");

        let _ = fs::remove_file(path);
    }

    #[test]
    fn unix_fd_frame_rejects_payloads_and_fd_counts_over_limits() {
        let (sender, _receiver) = UnixStream::pair().expect("unix socket pair should open");
        let oversized =
            send_unix_fd_frame(&sender, &UnixFdFrame::new(vec![0; 8], Vec::new()), 4, 0)
                .expect_err("oversized payload should be rejected");
        assert!(matches!(oversized, IpcTransportError::EncodeFailed(_)));

        let fd = OwnedFd::from(File::open("/dev/null").expect("/dev/null should open"));
        let too_many_fds =
            send_unix_fd_frame(&sender, &UnixFdFrame::new(Vec::new(), vec![fd]), 4, 0)
                .expect_err("too many fds should be rejected");
        assert!(matches!(too_many_fds, IpcTransportError::EncodeFailed(_)));
    }

    #[tokio::test]
    async fn unix_fd_frame_async_helpers_roundtrip() {
        let path = temp_file_path("async");
        fs::write(&path, b"async-shared-frame-bytes")
            .expect("async fd test file should be written");
        let file = File::open(&path).expect("async fd test file should open");
        let fd = OwnedFd::from(file);
        let (sender, receiver) = UnixStream::pair().expect("unix socket pair should open");
        sender
            .set_nonblocking(true)
            .expect("sender should become nonblocking");
        receiver
            .set_nonblocking(true)
            .expect("receiver should become nonblocking");
        let sender =
            tokio::net::UnixStream::from_std(sender).expect("sender should convert to tokio");
        let receiver =
            tokio::net::UnixStream::from_std(receiver).expect("receiver should convert to tokio");

        let send_task = tokio::spawn(async move {
            send_unix_fd_frame_async(
                &sender,
                &UnixFdFrame::new(b"async-metadata".to_vec(), vec![fd]),
                DEFAULT_UNIX_FD_FRAME_MAX_PAYLOAD_BYTES,
                DEFAULT_UNIX_FD_FRAME_MAX_FDS,
            )
            .await
        });
        let received = recv_unix_fd_frame_async(
            &receiver,
            DEFAULT_UNIX_FD_FRAME_MAX_PAYLOAD_BYTES,
            DEFAULT_UNIX_FD_FRAME_MAX_FDS,
        )
        .await
        .expect("async fd frame should receive")
        .expect("async socket should contain a frame");
        let sent = send_task
            .await
            .expect("async fd send task should join")
            .expect("async fd frame should send");

        assert_eq!(sent, 4 + "async-metadata".len());
        assert_eq!(received.payload, b"async-metadata");
        assert_eq!(received.fds.len(), 1);
        let mut imported = File::from(
            received
                .fds
                .into_iter()
                .next()
                .expect("received fd should exist"),
        );
        let mut content = String::new();
        imported
            .read_to_string(&mut content)
            .expect("received fd should read original content");
        assert_eq!(content, "async-shared-frame-bytes");

        let _ = fs::remove_file(path);
    }
}
