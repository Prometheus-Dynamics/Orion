//! A serial port in raw mode (termios), non-blocking, behind tokio's `AsyncFd`.

use super::SerialLinkConfig;
use std::ffi::CString;
use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::os::unix::ffi::OsStrExt;
use tokio::io::unix::AsyncFd;

/// An open serial port: raw mode, the configured baud rate, 8N1, no flow control.
pub(super) struct SerialPort {
    fd: AsyncFd<OwnedFd>,
}

fn cvt(result: libc::c_int) -> io::Result<libc::c_int> {
    if result < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(result)
    }
}

fn speed(baud: u32) -> io::Result<libc::speed_t> {
    Ok(match baud {
        1_200 => libc::B1200,
        2_400 => libc::B2400,
        4_800 => libc::B4800,
        9_600 => libc::B9600,
        19_200 => libc::B19200,
        38_400 => libc::B38400,
        57_600 => libc::B57600,
        115_200 => libc::B115200,
        230_400 => libc::B230400,
        460_800 => libc::B460800,
        500_000 => libc::B500000,
        576_000 => libc::B576000,
        921_600 => libc::B921600,
        1_000_000 => libc::B1000000,
        1_152_000 => libc::B1152000,
        1_500_000 => libc::B1500000,
        2_000_000 => libc::B2000000,
        2_500_000 => libc::B2500000,
        3_000_000 => libc::B3000000,
        3_500_000 => libc::B3500000,
        4_000_000 => libc::B4000000,
        other => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("unsupported baud rate {other}"),
            ));
        }
    })
}

impl SerialPort {
    /// Opens and configures the port. Must be called inside a Tokio runtime.
    pub(super) fn open(config: &SerialLinkConfig) -> io::Result<Self> {
        let path = CString::new(config.path.as_os_str().as_bytes())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "path contains NUL"))?;
        let flags = libc::O_RDWR | libc::O_NOCTTY | libc::O_NONBLOCK | libc::O_CLOEXEC;
        // SAFETY: `path` is a valid NUL-terminated string; the returned fd is owned below.
        let raw = cvt(unsafe { libc::open(path.as_ptr(), flags) })?;
        // SAFETY: `raw` is a freshly opened fd that nothing else owns.
        let fd = unsafe { OwnedFd::from_raw_fd(raw) };
        configure(fd.as_raw_fd(), config.baud)?;
        Ok(Self {
            fd: AsyncFd::new(fd)?,
        })
    }

    /// Waits until bytes are available and reads them. `Ok(0)` means hang-up.
    pub(super) async fn read(&self, buf: &mut [u8]) -> io::Result<usize> {
        loop {
            let mut guard = self.fd.readable().await?;
            let result = guard.try_io(|fd| {
                // SAFETY: `buf` is valid for `buf.len()` bytes of writes.
                let n = unsafe { libc::read(fd.as_raw_fd(), buf.as_mut_ptr().cast(), buf.len()) };
                if n < 0 {
                    Err(io::Error::last_os_error())
                } else {
                    Ok(n.unsigned_abs())
                }
            });
            match result {
                Ok(result) => return result,
                Err(_would_block) => continue,
            }
        }
    }

    /// Writes as much of `bytes` as the port accepts without blocking.
    pub(super) fn try_write(&self, bytes: &[u8]) -> io::Result<usize> {
        // SAFETY: `bytes` is valid for `bytes.len()` bytes of reads.
        let n = unsafe { libc::write(self.fd.as_raw_fd(), bytes.as_ptr().cast(), bytes.len()) };
        if n >= 0 {
            return Ok(n.unsigned_abs());
        }
        let error = io::Error::last_os_error();
        if error.kind() == io::ErrorKind::WouldBlock {
            Ok(0)
        } else {
            Err(error)
        }
    }

    /// Waits until the port accepts bytes and writes some of `bytes`.
    pub(super) async fn write(&self, bytes: &[u8]) -> io::Result<usize> {
        loop {
            let mut guard = self.fd.writable().await?;
            let result = guard.try_io(|fd| {
                // SAFETY: `bytes` is valid for `bytes.len()` bytes of reads.
                let n = unsafe { libc::write(fd.as_raw_fd(), bytes.as_ptr().cast(), bytes.len()) };
                if n < 0 {
                    Err(io::Error::last_os_error())
                } else {
                    Ok(n.unsigned_abs())
                }
            });
            match result {
                Ok(result) => return result,
                Err(_would_block) => continue,
            }
        }
    }
}

impl Drop for SerialPort {
    fn drop(&mut self) {
        // Exclusive mode stays on the tty after close (for a pty, while the master is open), so
        // release it, or the port could not be reopened.
        // SAFETY: TIOCNXCL takes no argument; the fd is still open.
        let _ = unsafe { libc::ioctl(self.fd.as_raw_fd(), libc::TIOCNXCL) };
    }
}

fn configure(fd: libc::c_int, baud: u32) -> io::Result<()> {
    let speed = speed(baud)?;
    // SAFETY: an all-zero termios is a valid value to be filled by `tcgetattr`.
    let mut tio: libc::termios = unsafe { std::mem::zeroed() };
    // SAFETY: `fd` is open and `tio` is a valid termios.
    cvt(unsafe { libc::tcgetattr(fd, &mut tio) })?;
    // SAFETY: `tio` is a valid termios.
    unsafe { libc::cfmakeraw(&mut tio) };
    tio.c_cflag &= !(libc::CSIZE | libc::CSTOPB | libc::PARENB | libc::CRTSCTS);
    tio.c_cflag |= libc::CS8 | libc::CLOCAL | libc::CREAD;
    tio.c_iflag &= !(libc::IXON | libc::IXOFF | libc::IXANY);
    // VMIN = 1 makes an empty non-blocking read fail with EAGAIN instead of returning 0, so 0
    // unambiguously means hang-up.
    tio.c_cc[libc::VMIN] = 1;
    tio.c_cc[libc::VTIME] = 0;
    // SAFETY: `tio` is a valid termios.
    cvt(unsafe { libc::cfsetispeed(&mut tio, speed) })?;
    // SAFETY: `tio` is a valid termios.
    cvt(unsafe { libc::cfsetospeed(&mut tio, speed) })?;
    // SAFETY: `fd` is open and `tio` is a valid termios.
    cvt(unsafe { libc::tcsetattr(fd, libc::TCSANOW, &tio) })?;
    // Exclusive mode keeps other processes from opening the port meanwhile; best effort.
    // SAFETY: TIOCEXCL takes no argument.
    let _ = unsafe { libc::ioctl(fd, libc::TIOCEXCL) };
    // SAFETY: `fd` is open.
    let _ = unsafe { libc::tcflush(fd, libc::TCIOFLUSH) };
    Ok(())
}
