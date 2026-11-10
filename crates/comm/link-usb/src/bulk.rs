//! A byte stream over a USB device's bulk endpoints, through the `nusb` crate, from a device
//! descriptor that was handed over already open.

use std::io;
use std::os::fd::OwnedFd;

use nusb::MaybeFuture;
use nusb::io::{EndpointRead, EndpointWrite};
use nusb::transfer::{Bulk as BulkTransfer, In, Out};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Mutex;

/// The size of one transfer. A frame of the wire fits several times over.
const TRANSFER: usize = 16 * 1024;

pub(crate) struct Bulk {
    // One task reads and one writes; the locks are never waited on.
    rx: Mutex<EndpointRead<BulkTransfer>>,
    tx: Mutex<EndpointWrite<BulkTransfer>>,
}

fn other(error: nusb::Error) -> io::Error {
    io::Error::other(error.to_string())
}

impl Bulk {
    pub(crate) fn open(
        device: OwnedFd,
        interface: u8,
        endpoint_in: u8,
        endpoint_out: u8,
    ) -> io::Result<Self> {
        let device = nusb::Device::from_fd(device).wait().map_err(other)?;
        // A kernel driver that has the interface gives it up for as long as this holds it.
        let interface = device
            .detach_and_claim_interface(interface)
            .wait()
            .map_err(other)?;
        let rx = interface
            .endpoint::<BulkTransfer, In>(endpoint_in)
            .map_err(other)?;
        let tx = interface
            .endpoint::<BulkTransfer, Out>(endpoint_out)
            .map_err(other)?;
        Ok(Self {
            rx: Mutex::new(rx.reader(TRANSFER)),
            tx: Mutex::new(tx.writer(TRANSFER)),
        })
    }

    pub(crate) async fn read(&self, buf: &mut [u8]) -> io::Result<usize> {
        self.rx.lock().await.read(buf).await
    }

    pub(crate) async fn write(&self, bytes: &[u8]) -> io::Result<usize> {
        let mut tx = self.tx.lock().await;
        tx.write_all(bytes).await?;
        // Sent now, not when the transfer fills: a frame is not held back for the next one.
        tx.flush().await?;
        Ok(bytes.len())
    }
}
