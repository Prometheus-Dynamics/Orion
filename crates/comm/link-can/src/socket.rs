//! The kernel's CAN sockets, through the `socketcan` crate.

use std::io;

use orion_wire::CanLinkIds;
use socketcan::tokio::CanFdSocket;
use socketcan::{
    CanAnyFrame, CanDataFrame, CanFdFrame, CanFilter, EmbeddedFrame, ExtendedId, Id, SocketOptions,
    StandardId,
};

use crate::carrier::{CanFrame, CanIo, OpenCan};

const EFF_FLAG: u32 = 0x8000_0000;
const RTR_FLAG: u32 = 0x4000_0000;
const EFF_MASK: u32 = 0x1FFF_FFFF;
const SFF_MASK: u32 = 0x0000_07FF;

/// A CAN interface, for example `can0`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SocketCanBus {
    /// The interface's name.
    pub interface: String,
    /// Send CAN FD frames. Without it, classic frames of up to 8 bytes.
    pub fd: bool,
}

/// An open raw CAN socket that receives only a bus's device identifiers.
pub struct SocketCan {
    socket: CanFdSocket,
    fd: bool,
}

impl std::fmt::Debug for SocketCan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SocketCan")
    }
}

fn id(raw: u32, extended: bool) -> io::Result<Id> {
    let id = if extended {
        ExtendedId::new(raw).map(Id::Extended)
    } else {
        u16::try_from(raw)
            .ok()
            .and_then(StandardId::new)
            .map(Id::Standard)
    };
    id.ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "a CAN identifier out of range"))
}

fn received(id: Id, data: &[u8]) -> CanFrame {
    let (id, extended) = match id {
        Id::Standard(id) => (u32::from(id.as_raw()), false),
        Id::Extended(id) => (id.as_raw(), true),
    };
    let mut frame = CanFrame {
        id,
        extended,
        len: data.len().min(64) as u8,
        data: [0; 64],
    };
    frame.data[..usize::from(frame.len)].copy_from_slice(&data[..usize::from(frame.len)]);
    frame
}

impl CanIo for SocketCan {
    async fn recv(&self) -> io::Result<CanFrame> {
        loop {
            match self.socket.read_frame().await? {
                CanAnyFrame::Normal(frame) => return Ok(received(frame.id(), frame.data())),
                CanAnyFrame::Fd(frame) => return Ok(received(frame.id(), frame.data())),
                // Remote and error frames carry nothing of the wire.
                CanAnyFrame::Remote(_) | CanAnyFrame::Error(_) => {}
            }
        }
    }

    async fn send(&self, frame: &CanFrame) -> io::Result<()> {
        let too_long = || {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "a CAN frame too long for this bus",
            )
        };
        let id = id(frame.id, frame.extended)?;
        loop {
            let sent = if self.fd {
                let out = CanFdFrame::new(id, frame.bytes()).ok_or_else(too_long)?;
                self.socket.write_frame(&out).await
            } else {
                let out = CanDataFrame::new(id, frame.bytes()).ok_or_else(too_long)?;
                self.socket.write_frame(&out).await
            };
            match sent {
                // The interface's queue is full, which a CAN socket does not report as
                // "would block": wait a moment for the bus to take some.
                Err(error) if error.raw_os_error() == Some(105) => {
                    tokio::time::sleep(std::time::Duration::from_millis(2)).await;
                }
                sent => return sent,
            }
        }
    }
}

impl OpenCan for SocketCanBus {
    type Io = SocketCan;

    async fn open(&mut self, ids: &[CanLinkIds]) -> io::Result<SocketCan> {
        let socket = CanFdSocket::open(&self.interface)?;
        // Matching the extended and remote flags keeps standard and extended traffic apart
        // and drops remote frames in the kernel.
        let filters: Vec<CanFilter> = ids
            .iter()
            .map(|ids| {
                if ids.extended {
                    CanFilter::new(
                        ids.device_to_host | EFF_FLAG,
                        EFF_FLAG | RTR_FLAG | EFF_MASK,
                    )
                } else {
                    CanFilter::new(ids.device_to_host, EFF_FLAG | RTR_FLAG | SFF_MASK)
                }
            })
            .collect();
        socket.set_filters(&filters)?;
        Ok(SocketCan {
            socket,
            fd: self.fd,
        })
    }

    fn describe(&self) -> String {
        format!(
            "{}{}",
            self.interface,
            if self.fd { " (CAN FD)" } else { "" }
        )
    }
}
