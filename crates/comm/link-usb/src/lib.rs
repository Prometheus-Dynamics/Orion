//! Links over USB.
//!
//! **The comm core never opens hardware.** The program that owns the board's hardware finds
//! the USB device or sets up the gadget function, opens it, and hands the open descriptors to
//! the comm core over the board's socket, in a `LinkOffer`. From then on the link is a byte
//! stream like a serial port's, carried by `orion_link_serial::StreamCarrier`.
//!
//! A handle is one of:
//!
//! - **a stream**: one descriptor that can be waited on and carries bytes both ways. A gadget's
//!   serial function, a serial port over USB that is already open, a socket.
//! - **endpoints**: two descriptors that block, one to read and one to write. A gadget
//!   function's bulk endpoints. A thread sits in each, so nothing is polled.
//! - **a device** (feature `bulk`): a USB device's descriptor with the interface and the two
//!   bulk endpoints to use.
//!
//! A link waits for its handle and costs nothing until one is offered. When a new one is
//! offered for a link that has one, the device was unplugged and is back, so the old stream
//! ends and the link goes on over the new.

#![cfg(target_os = "linux")]

mod blocking;
#[cfg(feature = "bulk")]
mod bulk;
mod stream;

use std::cell::{Cell, RefCell};
use std::io;
use std::os::fd::OwnedFd;
use std::rc::Rc;

use orion_link_serial::Open;
use orion_msg_session::{LinkOffer, LinkTaken, LinkTransport};
use orion_server::{Admits, Checked, Cx, Error, Incoming, Policy, Service, What};
use orion_wire::{FrameView, kind};
use tokio::sync::Notify;
use tracing::{info, warn};

pub use stream::UsbStream;

/// What ends a stream when its link is offered another handle.
#[derive(Debug, Default)]
pub(crate) struct Ended {
    pub(crate) ended: Cell<bool>,
    pub(crate) wake: Notify,
}

struct Slot {
    name: String,
    /// The handle waiting to be taken by the link.
    offered: Option<UsbStream>,
    /// Told when a handle is offered.
    arrived: Rc<Notify>,
    /// The stream the link is running over now.
    current: Option<Rc<Ended>>,
}

/// The USB links of a comm core, and where their handles arrive.
#[derive(Clone, Default)]
pub struct UsbLinks {
    slots: Rc<RefCell<Vec<Slot>>>,
}

impl std::fmt::Debug for UsbLinks {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "UsbLinks({})", self.slots.borrow().len())
    }
}

impl UsbLinks {
    /// No links yet.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// The link `name`: what to give `StreamCarrier::new` so the link runs over whatever is
    /// offered for that name.
    #[must_use]
    pub fn link(&self, name: &str) -> UsbLink {
        let mut slots = self.slots.borrow_mut();
        let arrived = match slots.iter().find(|slot| slot.name == name) {
            Some(slot) => slot.arrived.clone(),
            None => {
                let arrived = Rc::new(Notify::new());
                slots.push(Slot {
                    name: name.to_owned(),
                    offered: None,
                    arrived: arrived.clone(),
                    current: None,
                });
                arrived
            }
        };
        UsbLink {
            links: self.clone(),
            name: name.to_owned(),
            arrived,
        }
    }

    /// The service that takes `LinkOffer`. Add it to the comm core.
    #[must_use]
    pub fn service(&self) -> UsbLinkService {
        UsbLinkService(self.clone())
    }

    fn offer(&self, name: &str, stream: UsbStream) -> Result<(), Error> {
        let mut slots = self.slots.borrow_mut();
        let slot = slots
            .iter_mut()
            .find(|slot| slot.name == name)
            .ok_or(Error::NotFound)?;
        // The device this link ran over is gone: its stream ends, and the link takes this one.
        if let Some(current) = slot.current.take() {
            current.ended.set(true);
            current.wake.notify_one();
        }
        slot.offered = Some(stream);
        slot.arrived.notify_one();
        Ok(())
    }
}

/// One USB link, waiting for its handle.
pub struct UsbLink {
    links: UsbLinks,
    name: String,
    arrived: Rc<Notify>,
}

impl std::fmt::Debug for UsbLink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "UsbLink({})", self.name)
    }
}

impl Open for UsbLink {
    type Stream = UsbStream;

    async fn open(&mut self) -> io::Result<UsbStream> {
        loop {
            {
                let mut slots = self.links.slots.borrow_mut();
                if let Some(slot) = slots.iter_mut().find(|slot| slot.name == self.name)
                    && let Some(stream) = slot.offered.take()
                {
                    slot.current = Some(stream.ended());
                    return Ok(stream);
                }
            }
            // Nothing to open: the hardware is someone else's. Sleep until it is handed over.
            self.arrived.notified().await;
        }
    }

    fn describe(&self) -> String {
        format!("usb link {}", self.name)
    }
}

/// Takes `LinkOffer` from a program on the board.
#[derive(Debug)]
pub struct UsbLinkService(UsbLinks);

impl Service for UsbLinkService {
    fn kinds(&self) -> &'static [u8] {
        &[kind::LINK_OFFER]
    }

    fn asks<'a>(&self, _frame: &FrameView<'a>) -> Result<(What<'a>, Policy), Error> {
        // Handing over a device is giving a way in. Only a program on the board may, and the
        // socket's permissions decide which.
        Ok((What::Write("links"), Policy::new(Admits::LOCAL)))
    }

    fn handle(&mut self, request: Checked<Incoming<'_>>, cx: &mut Cx<'_>) -> Result<(), Error> {
        let mut incoming = request.into_inner();
        let fds: Vec<OwnedFd> = (0..incoming.fds())
            .filter_map(|index| incoming.take_fd(index))
            .collect();
        let offer: LinkOffer<'_> = incoming.body()?;
        let stream = match (offer.transport, fds.len()) {
            (LinkTransport::Stream, 1) => UsbStream::polled(fds),
            (LinkTransport::Endpoints, 2) => UsbStream::endpoints(fds),
            #[cfg(feature = "bulk")]
            (LinkTransport::Bulk, 1) => {
                UsbStream::bulk(fds, offer.interface, offer.endpoint_in, offer.endpoint_out)
            }
            #[cfg(not(feature = "bulk"))]
            (LinkTransport::Bulk, 1) => return Err(Error::Unsupported),
            (LinkTransport::Unknown(_), _) => return Err(Error::Unsupported),
            // The wrong number of descriptors for what it says they are.
            _ => return Err(Error::Malformed),
        };
        let stream = stream.map_err(|error| {
            warn!(link = offer.link, %error, "an offered handle could not be used");
            Error::Malformed
        })?;
        self.0.offer(offer.link, stream)?;
        info!(link = offer.link, transport = ?offer.transport, "link handle taken");
        cx.reply(&LinkTaken {
            request_seq: cx.request_seq(),
        })
    }
}
