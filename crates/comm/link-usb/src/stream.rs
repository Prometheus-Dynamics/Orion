//! The byte stream over whatever was handed over.

use std::fs::File;
use std::io;
use std::os::fd::OwnedFd;
use std::rc::Rc;

use orion_link_serial::ByteStream;
use tokio::net::unix::pipe;

use crate::Ended;
use crate::blocking::Blocking;

enum Over {
    /// One descriptor that can be waited on.
    Polled {
        rx: pipe::Receiver,
        tx: pipe::Sender,
    },
    /// Two descriptors that block, a thread in each.
    Blocking(Blocking),
    #[cfg(feature = "bulk")]
    Bulk(Box<crate::bulk::Bulk>),
}

/// A USB link's byte stream.
pub struct UsbStream {
    over: Over,
    ended: Rc<Ended>,
}

impl std::fmt::Debug for UsbStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self.over {
            Over::Polled { .. } => "UsbStream(stream)",
            Over::Blocking(_) => "UsbStream(endpoints)",
            #[cfg(feature = "bulk")]
            Over::Bulk(_) => "UsbStream(bulk)",
        })
    }
}

impl UsbStream {
    fn new(over: Over) -> Self {
        Self {
            over,
            ended: Rc::new(Ended::default()),
        }
    }

    /// Over one descriptor that can be waited on and carries bytes both ways. It is made
    /// non-blocking.
    pub(crate) fn polled(mut fds: Vec<OwnedFd>) -> io::Result<Self> {
        let file = File::from(fds.pop().ok_or(io::ErrorKind::InvalidInput)?);
        // Tokio's pipe ends are its safe way to wait on a descriptor that is not a socket of
        // its own; each end is a duplicate of the one descriptor.
        Ok(Self::new(Over::Polled {
            rx: pipe::Receiver::from_file_unchecked(nonblocking(file.try_clone()?)?)?,
            tx: pipe::Sender::from_file_unchecked(nonblocking(file)?)?,
        }))
    }

    /// Over two descriptors that block: the first to read, the second to write.
    pub(crate) fn endpoints(mut fds: Vec<OwnedFd>) -> io::Result<Self> {
        let write = File::from(fds.pop().ok_or(io::ErrorKind::InvalidInput)?);
        let read = File::from(fds.pop().ok_or(io::ErrorKind::InvalidInput)?);
        Ok(Self::new(Over::Blocking(Blocking::start(read, write)?)))
    }

    /// Over a USB device's bulk endpoints.
    #[cfg(feature = "bulk")]
    pub(crate) fn bulk(
        mut fds: Vec<OwnedFd>,
        interface: u8,
        endpoint_in: u8,
        endpoint_out: u8,
    ) -> io::Result<Self> {
        let device = fds.pop().ok_or(io::ErrorKind::InvalidInput)?;
        let bulk = crate::bulk::Bulk::open(device, interface, endpoint_in, endpoint_out)?;
        Ok(Self::new(Over::Bulk(Box::new(bulk))))
    }

    pub(crate) fn ended(&self) -> Rc<Ended> {
        self.ended.clone()
    }
}

/// The descriptor was opened by someone else, who may have left it blocking.
fn nonblocking(file: File) -> io::Result<File> {
    rustix::io::ioctl_fionbio(&file, true)?;
    Ok(file)
}

impl ByteStream for UsbStream {
    async fn read(&self, buf: &mut [u8]) -> io::Result<usize> {
        if self.ended.ended.get() {
            return Ok(0);
        }
        let read = async {
            match &self.over {
                Over::Polled { rx, .. } => loop {
                    rx.readable().await?;
                    match rx.try_read(buf) {
                        Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
                        // A terminal whose other end is gone reads as an error, not as the end.
                        Err(error) if error.raw_os_error() == Some(5) => return Ok(0),
                        result => return result,
                    }
                },
                Over::Blocking(blocking) => blocking.read(buf).await,
                #[cfg(feature = "bulk")]
                Over::Bulk(bulk) => bulk.read(buf).await,
            }
        };
        tokio::select! {
            read = read => read,
            // The link was offered another handle: this one's device is gone.
            () = self.ended.wake.notified() => Ok(0),
        }
    }

    async fn write(&self, bytes: &[u8]) -> io::Result<usize> {
        if self.ended.ended.get() {
            return Err(io::ErrorKind::BrokenPipe.into());
        }
        match &self.over {
            Over::Polled { tx, .. } => loop {
                tx.writable().await?;
                match tx.try_write(bytes) {
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
                    result => return result,
                }
            },
            Over::Blocking(blocking) => blocking.write(bytes).await,
            #[cfg(feature = "bulk")]
            Over::Bulk(bulk) => bulk.write(bytes).await,
        }
    }
}
