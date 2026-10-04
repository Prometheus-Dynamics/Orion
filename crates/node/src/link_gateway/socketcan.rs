//! A raw SocketCAN socket (`PF_CAN`, `CAN_RAW`) behind tokio's `AsyncFd`.

use super::CanLinkConfig;
use super::can_link::{CanFrame, CanIo};
use std::ffi::CString;
use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use tokio::io::unix::AsyncFd;

/// A raw CAN socket bound to one interface, receiving only the configured identifiers.
pub(crate) struct SocketCan {
    fd: AsyncFd<OwnedFd>,
    fd_frames: bool,
}

/// Opens the link's socket: receive filters for every device→host identifier of the link.
pub(super) fn opener(config: CanLinkConfig) -> impl FnMut() -> io::Result<SocketCan> + Send {
    move || {
        let ids: Vec<u32> = config.device_ids().collect();
        SocketCan::open(&config.interface, config.fd, config.extended, &ids)
    }
}

fn cvt(result: libc::c_int) -> io::Result<libc::c_int> {
    if result < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(result)
    }
}

fn set_option<T>(fd: libc::c_int, name: libc::c_int, value: &[T]) -> io::Result<()> {
    let len = libc::socklen_t::try_from(std::mem::size_of_val(value))
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "socket option too large"))?;
    // SAFETY: `value` points to `len` initialized bytes.
    cvt(unsafe { libc::setsockopt(fd, libc::SOL_CAN_RAW, name, value.as_ptr().cast(), len) })
        .map(|_| ())
}

impl SocketCan {
    /// Opens a raw socket on `interface` that receives only `ids` (standard or extended).
    pub(crate) fn open(
        interface: &str,
        fd_frames: bool,
        extended: bool,
        ids: &[u32],
    ) -> io::Result<Self> {
        let name = CString::new(interface)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "interface contains NUL"))?;
        // SAFETY: `name` is a valid NUL-terminated string.
        let index = unsafe { libc::if_nametoindex(name.as_ptr()) };
        if index == 0 {
            return Err(io::Error::last_os_error());
        }
        let ty = libc::SOCK_RAW | libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC;
        // SAFETY: plain socket(2) call; the fd is owned below.
        let raw = cvt(unsafe { libc::socket(libc::PF_CAN, ty, libc::CAN_RAW) })?;
        // SAFETY: `raw` is a freshly created fd that nothing else owns.
        let fd = unsafe { OwnedFd::from_raw_fd(raw) };

        let (flag, mask) = if extended {
            (libc::CAN_EFF_FLAG, libc::CAN_EFF_FLAG | libc::CAN_EFF_MASK)
        } else {
            (0, libc::CAN_EFF_FLAG | libc::CAN_SFF_MASK)
        };
        // Matching CAN_EFF_FLAG and CAN_RTR_FLAG keeps standard and extended traffic apart and
        // drops remote frames in the kernel.
        let filters: Vec<libc::can_filter> = ids
            .iter()
            .map(|&id| libc::can_filter {
                can_id: id | flag,
                can_mask: mask | libc::CAN_RTR_FLAG,
            })
            .collect();
        set_option(fd.as_raw_fd(), libc::CAN_RAW_FILTER, &filters)?;
        if fd_frames {
            let enable: [libc::c_int; 1] = [1];
            set_option(fd.as_raw_fd(), libc::CAN_RAW_FD_FRAMES, &enable)?;
        }

        // SAFETY: an all-zero sockaddr_can is valid; family and ifindex are set below.
        let mut addr: libc::sockaddr_can = unsafe { std::mem::zeroed() };
        addr.can_family = libc::AF_CAN as libc::sa_family_t;
        addr.can_ifindex = libc::c_int::try_from(index)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "interface index"))?;
        let len = std::mem::size_of::<libc::sockaddr_can>() as libc::socklen_t;
        // SAFETY: `addr` is a valid sockaddr_can of `len` bytes.
        cvt(unsafe {
            libc::bind(
                fd.as_raw_fd(),
                (&raw const addr).cast::<libc::sockaddr>(),
                len,
            )
        })?;
        Ok(Self {
            fd: AsyncFd::new(fd)?,
            fd_frames,
        })
    }

    fn read_frame(fd: libc::c_int) -> io::Result<Option<CanFrame>> {
        // SAFETY: an all-zero canfd_frame is valid.
        let mut frame: libc::canfd_frame = unsafe { std::mem::zeroed() };
        // SAFETY: `frame` is valid for CANFD_MTU bytes of writes.
        let n = unsafe { libc::read(fd, (&raw mut frame).cast(), libc::CANFD_MTU) };
        if n < 0 {
            return Err(io::Error::last_os_error());
        }
        let n = n.unsigned_abs();
        let fd_frame = match n {
            libc::CANFD_MTU => true,
            libc::CAN_MTU => false,
            _ => return Ok(None),
        };
        if frame.can_id & (libc::CAN_RTR_FLAG | libc::CAN_ERR_FLAG) != 0 {
            return Ok(None);
        }
        let extended = frame.can_id & libc::CAN_EFF_FLAG != 0;
        let id = if extended {
            frame.can_id & libc::CAN_EFF_MASK
        } else {
            frame.can_id & libc::CAN_SFF_MASK
        };
        let max = if fd_frame {
            libc::CANFD_MAX_DLEN
        } else {
            libc::CAN_MAX_DLEN
        };
        let len = usize::from(frame.len).min(max);
        Ok(Some(CanFrame {
            id,
            extended,
            fd: fd_frame,
            data: frame.data.get(..len).unwrap_or_default().to_vec(),
        }))
    }
}

impl CanIo for SocketCan {
    async fn recv(&self) -> io::Result<CanFrame> {
        loop {
            let mut guard = self.fd.readable().await?;
            match guard.try_io(|fd| Self::read_frame(fd.as_raw_fd())) {
                Ok(Ok(Some(frame))) => return Ok(frame),
                Ok(Ok(None)) => continue,
                Ok(Err(error)) => return Err(error),
                Err(_would_block) => continue,
            }
        }
    }

    fn try_send(&self, frame: &CanFrame) -> io::Result<bool> {
        let id = if frame.extended {
            (frame.id & libc::CAN_EFF_MASK) | libc::CAN_EFF_FLAG
        } else {
            frame.id & libc::CAN_SFF_MASK
        };
        let fd_frame = frame.fd && self.fd_frames;
        let max = if fd_frame {
            libc::CANFD_MAX_DLEN
        } else {
            libc::CAN_MAX_DLEN
        };
        if frame.data.len() > max {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "CAN frame too long",
            ));
        }
        // SAFETY: an all-zero canfd_frame is valid; `can_frame` shares its layout prefix.
        let mut out: libc::canfd_frame = unsafe { std::mem::zeroed() };
        out.can_id = id;
        out.len = frame.data.len() as u8;
        if let Some(data) = out.data.get_mut(..frame.data.len()) {
            data.copy_from_slice(&frame.data);
        }
        let size = if fd_frame {
            libc::CANFD_MTU
        } else {
            libc::CAN_MTU
        };
        // SAFETY: `out` is valid for `size` (<= CANFD_MTU) bytes of reads; a classic `can_frame`
        // has the same layout as the first CAN_MTU bytes of a `canfd_frame`.
        let n = unsafe { libc::write(self.fd.as_raw_fd(), (&raw const out).cast(), size) };
        if n >= 0 {
            return Ok(true);
        }
        let error = io::Error::last_os_error();
        match error.raw_os_error() {
            Some(libc::EAGAIN | libc::ENOBUFS) => Ok(false),
            _ => Err(error),
        }
    }
}
