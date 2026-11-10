//! A serial port in raw mode, non-blocking, behind tokio's `AsyncFd`.

use std::fs::{File, OpenOptions};
use std::io;
use std::os::unix::fs::OpenOptionsExt;
use std::path::PathBuf;

use rustix::fs::OFlags;
use rustix::termios::{
    ControlModes, InputModes, OptionalActions, QueueSelector, SpecialCodeIndex, ioctl_tiocexcl,
    ioctl_tiocnxcl, tcflush, tcgetattr, tcsetattr,
};
use tokio::net::unix::pipe;

use crate::carrier::{ByteStream, Open};

/// Where a serial link is and how fast it runs: 8 data bits, no parity, one stop bit, no flow
/// control.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Serial {
    /// The device, for example `/dev/ttyAMA0`.
    pub path: PathBuf,
    /// Bits per second.
    pub baud: u32,
}

/// An open serial port.
#[derive(Debug)]
pub struct SerialPort {
    // Tokio's pipe ends are its safe way to wait on a descriptor that is not a socket; a
    // terminal is one, and each end is a duplicate of the one open port.
    rx: pipe::Receiver,
    tx: pipe::Sender,
    port: File,
}

impl SerialPort {
    /// Opens and configures the port. Must be called inside a Tokio runtime.
    ///
    /// # Errors
    ///
    /// If the device can't be opened or does not take the settings.
    pub fn open(serial: &Serial) -> io::Result<Self> {
        let flags = OFlags::NOCTTY | OFlags::NONBLOCK | OFlags::CLOEXEC;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(flags.bits() as i32)
            .open(&serial.path)?;
        let mut tio = tcgetattr(&file)?;
        tio.make_raw();
        tio.control_modes.remove(
            ControlModes::CSIZE
                | ControlModes::CSTOPB
                | ControlModes::PARENB
                | ControlModes::CRTSCTS,
        );
        tio.control_modes
            .insert(ControlModes::CS8 | ControlModes::CLOCAL | ControlModes::CREAD);
        tio.input_modes
            .remove(InputModes::IXON | InputModes::IXOFF | InputModes::IXANY);
        // One byte is enough to wake a read, so an empty non-blocking read says "would block"
        // and 0 can only mean hang-up.
        tio.special_codes[SpecialCodeIndex::VMIN] = 1;
        tio.special_codes[SpecialCodeIndex::VTIME] = 0;
        tio.set_speed(serial.baud).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("unsupported baud rate {}", serial.baud),
            )
        })?;
        tcsetattr(&file, OptionalActions::Now, &tio)?;
        // Exclusive mode keeps other processes from opening the port meanwhile; best effort.
        let _ = ioctl_tiocexcl(&file);
        let _ = tcflush(&file, QueueSelector::IOFlush);
        Ok(Self {
            rx: pipe::Receiver::from_file_unchecked(file.try_clone()?)?,
            tx: pipe::Sender::from_file_unchecked(file.try_clone()?)?,
            port: file,
        })
    }
}

impl Drop for SerialPort {
    fn drop(&mut self) {
        // Exclusive mode stays on the tty after close (for a pty, while the other end is
        // open), so release it, or the port could not be opened again.
        let _ = ioctl_tiocnxcl(&self.port);
    }
}

impl ByteStream for SerialPort {
    async fn read(&self, buf: &mut [u8]) -> io::Result<usize> {
        loop {
            self.rx.readable().await?;
            match self.rx.try_read(buf) {
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
                // A terminal whose other end is gone reads as an error, not as the end.
                Err(error)
                    if error.raw_os_error() == Some(rustix::io::Errno::IO.raw_os_error()) =>
                {
                    return Ok(0);
                }
                result => return result,
            }
        }
    }

    async fn write(&self, bytes: &[u8]) -> io::Result<usize> {
        loop {
            self.tx.writable().await?;
            match self.tx.try_write(bytes) {
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
                result => return result,
            }
        }
    }
}

impl Open for Serial {
    type Stream = SerialPort;

    async fn open(&mut self) -> io::Result<SerialPort> {
        SerialPort::open(self)
    }

    fn describe(&self) -> String {
        format!("{} at {} baud", self.path.display(), self.baud)
    }
}

/// One serial link as it is written in the settings: `name=path@baud`, for example
/// `uart0=/dev/ttyAMA0@115200`. The baud rate may be left out, for 115200.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SerialLink {
    /// The link's name, which is who the device on it is taken to be.
    pub name: orion_types::DeviceName,
    /// The port.
    pub serial: Serial,
}

impl std::str::FromStr for SerialLink {
    type Err = String;

    fn from_str(text: &str) -> Result<Self, String> {
        let (name, port) = text.split_once('=').ok_or("expected name=path[@baud]")?;
        let (path, baud) = match port.rsplit_once('@') {
            Some((path, baud)) => (
                path,
                baud.parse()
                    .map_err(|_| format!("{baud:?} is not a baud rate"))?,
            ),
            None => (port, 115_200),
        };
        if path.is_empty() {
            return Err("expected name=path[@baud]".into());
        }
        Ok(Self {
            name: name
                .trim()
                .parse()
                .map_err(|error| format!("{name:?}: {error}"))?,
            serial: Serial {
                path: PathBuf::from(path.trim()),
                baud,
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_link_is_written_as_name_path_and_speed() {
        let link: SerialLink = "uart0=/dev/ttyAMA0@921600".parse().expect("link");
        assert_eq!(link.name.as_str(), "uart0");
        assert_eq!(
            (link.serial.path.to_str(), link.serial.baud),
            (Some("/dev/ttyAMA0"), 921_600)
        );
        let link: SerialLink = "arm = /dev/ttyUSB0".parse().expect("link");
        assert_eq!((link.name.as_str(), link.serial.baud), ("arm", 115_200));
        for wrong in [
            "/dev/ttyAMA0",
            "uart0=",
            "uart0=/dev/ttyAMA0@fast",
            "=/dev/ttyAMA0",
        ] {
            assert!(wrong.parse::<SerialLink>().is_err(), "{wrong}");
        }
    }

    #[test]
    fn a_port_that_is_not_there_or_a_speed_no_port_has_is_an_error() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        runtime.block_on(async {
            let missing = Serial {
                path: "/nonexistent/port".into(),
                baud: 115_200,
            };
            assert!(SerialPort::open(&missing).is_err());
        });
    }
}
