//! A device on any `embedded-can` controller, classic CAN or CAN FD.
//!
//! A bus carries many devices. Each has an address, which picks its two identifiers: the
//! bus's base identifier plus the address, one base for what devices send and one for what
//! they are sent (`crates/comm/link-can`). Set the controller's acceptance filter to the
//! identifier the device is sent on.

use embedded_can::nb::Can;
use embedded_can::{Frame, Id};
use orion_device_core::{CanDevice, DeviceConfig, Event, Link, SegmentMtu};

use crate::{App, RX, TX};

/// Why [`CanPort::service`] stopped.
#[derive(Debug)]
pub enum CanPortError<E> {
    /// The controller's error.
    Can(E),
    /// The controller's frames cannot carry a segment this long: an FD length on a classic
    /// controller.
    TooLong,
}

/// A device session on a CAN controller.
///
/// ```ignore
/// let to_host = Id::Standard(StandardId::new(0x100 + 5).unwrap());
/// let from_host = Id::Standard(StandardId::new(0x180 + 5).unwrap());
/// let mut port = CanPort::new(can, to_host, from_host, SegmentMtu::CLASSIC, "imu-board");
/// loop {
///     port.service(millis(), &mut app)?;
/// }
/// ```
pub struct CanPort<C> {
    can: C,
    to_host: Id,
    from_host: Id,
    device: CanDevice<RX, TX>,
    /// A segment the controller had no room for, and its length.
    pending: ([u8; 64], usize),
}

impl<C: Can> CanPort<C> {
    /// A session for the device `name`, sending on `to_host` and taking frames on
    /// `from_host`. [`SegmentMtu::CLASSIC`] for classic CAN; a longer one only with a CAN FD
    /// controller.
    pub const fn new(
        can: C,
        to_host: Id,
        from_host: Id,
        mtu: SegmentMtu,
        name: &'static str,
    ) -> Self {
        Self {
            can,
            to_host,
            from_host,
            device: CanDevice::new(DeviceConfig::new(name), mtu),
            pending: ([0; 64], 0),
        }
    }

    /// One pass of the main loop: takes the frames the controller has for this device,
    /// moves the session and `app` on to `now_ms`, and queues segments until the controller's
    /// mailboxes are full. What did not fit goes next time.
    ///
    /// # Errors
    ///
    /// The controller's, or [`CanPortError::TooLong`].
    pub fn service(
        &mut self,
        now_ms: u32,
        app: &mut impl App<TX>,
    ) -> Result<(), CanPortError<C::Error>> {
        loop {
            match self.can.receive() {
                Ok(frame) if frame.id() == self.from_host && !frame.is_remote_frame() => {
                    self.device.receive_segment(frame.data(), |link, frame| {
                        app.handle(link, frame);
                    });
                }
                Ok(_) => {}
                Err(nb::Error::WouldBlock) => break,
                Err(nb::Error::Other(error)) => return Err(CanPortError::Can(error)),
            }
        }
        self.device.link.poll(now_ms);
        app.poll(&mut self.device.link, now_ms);
        loop {
            if self.pending.1 == 0 {
                match self.device.next_segment(&mut self.pending.0) {
                    Some(len) => self.pending.1 = len,
                    None => return Ok(()),
                }
            }
            let data = self.pending.0.get(..self.pending.1).unwrap_or_default();
            let frame = C::Frame::new(self.to_host, data).ok_or(CanPortError::TooLong)?;
            match self.can.transmit(&frame) {
                // A frame of lower priority may have been put back: the host's session
                // repairs what is lost.
                Ok(_) => self.pending.1 = 0,
                Err(nb::Error::WouldBlock) => return Ok(()),
                Err(nb::Error::Other(error)) => return Err(CanPortError::Can(error)),
            }
        }
    }

    /// The next thing the firmware should know of.
    pub fn next_event(&mut self) -> Option<Event> {
        self.device.link.next_event()
    }

    /// The session, to send on outside [`CanPort::service`].
    pub fn link(&mut self) -> &mut Link<TX> {
        &mut self.device.link
    }

    /// The controller, to change its filters.
    pub fn can_mut(&mut self) -> &mut C {
        &mut self.can
    }
}
