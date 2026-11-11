//! The device side of the wire, for a microcontroller.
//!
//! A device speaks the same frames as every other program: it says hello with the role
//! `device`, is welcomed, pings, and from then on sends and receives whatever the crates
//! above this one put on the link. This crate is the session and nothing more.
//!
//! - **No allocator, no clock, no I/O.** Buffers are part of the session and their sizes
//!   are the firmware's choice (`RX`, `TX`). The port moves bytes or CAN frames and passes
//!   a millisecond counter, which may wrap.
//! - **Three calls.** Feed what arrived ([`StreamDevice::receive`],
//!   [`CanDevice::receive_segment`]), which reads no clock and may run in an interrupt;
//!   call [`Link::poll`] with the counter; send what [`StreamDevice::transmit`] or
//!   [`CanDevice::next_segment`] hands out.
//! - **Never gives up, never slows down.** Hello is said again at the same pace until the
//!   host answers. A host that goes quiet for a few heartbeats is taken for gone and hello
//!   starts again. Only a host that says no is left alone for a while.
//! - **One frame out at a time.** [`Link::send_with`] takes a frame when the last one has been
//!   handed to the port, and says busy otherwise. The session's own frames (hello, ping,
//!   pong) have a small buffer of their own and go first.
//!
//! ```
//! use orion_device_core::{DeviceConfig, Event, StreamDevice};
//!
//! let mut device = StreamDevice::<128, 128>::new(DeviceConfig::new("imu-board"));
//! let mut out = [0_u8; 64];
//! for now_ms in 0..3_u32 {
//!     // device.receive(bytes_from_the_uart, |link, frame| { /* the layers above */ });
//!     device.link.poll(now_ms);
//!     while let Some(event) = device.link.next_event() {
//!         if let Event::Connected { .. } = event { /* declare topics, register actions */ }
//!     }
//!     let n = device.transmit(&mut out);
//!     // uart.write(&out[..n]);
//!     # let _ = n;
//! }
//! ```

#![cfg_attr(not(test), no_std)]

pub mod codec;
mod link;
mod requests;

pub use link::{DeviceConfig, Event, Link, NAME_MAX, SendError, State};
pub use orion_msg_session::{RejectReason, Roles};
pub use orion_wire::{FrameView, SegmentMtu};
pub use requests::{RETRY_MS, Request, Requests};

use orion_wire::{Reassembler, StreamDecoder};

/// A device on a byte stream: a UART, RS-485, USB serial. `RX` is the longest frame it
/// takes and `TX` the longest it sends.
#[derive(Debug)]
pub struct StreamDevice<const RX: usize, const TX: usize> {
    rx: StreamDecoder<RX>,
    /// The session, and what is waiting to be sent.
    pub link: Link<TX>,
}

impl<const RX: usize, const TX: usize> StreamDevice<RX, TX> {
    /// A device that is not connected yet.
    #[must_use]
    pub const fn new(config: DeviceConfig) -> Self {
        Self {
            rx: StreamDecoder::new(),
            link: Link::new(config, RX),
        }
    }

    /// Feeds bytes from the port. A whole frame that is not the session's own is given to
    /// `up`, with the link to answer on. Bytes that are not a frame are skipped to the next
    /// one. Reads no clock.
    pub fn receive(&mut self, mut bytes: &[u8], mut up: impl FnMut(&mut Link<TX>, &FrameView<'_>)) {
        while !bytes.is_empty() {
            let (used, result) = self.rx.push_slice(bytes);
            if let Ok(Some(frame)) = result
                && !self.link.handle(&frame)
            {
                up(&mut self.link, &frame);
            }
            bytes = bytes.get(used.max(1)..).unwrap_or_default();
        }
    }

    /// Writes bytes for the port into `out` and says how many. Call it again when the port
    /// has room: a frame carries on where it stopped.
    pub fn transmit(&mut self, out: &mut [u8]) -> usize {
        self.link.transmit(out)
    }
}

/// A device on a CAN bus, classic or FD. `RX` is the longest frame it takes and `TX` the
/// longest it sends.
#[derive(Debug)]
pub struct CanDevice<const RX: usize, const TX: usize> {
    rx: Reassembler<RX>,
    mtu: SegmentMtu,
    /// The session, and what is waiting to be sent.
    pub link: Link<TX>,
}

impl<const RX: usize, const TX: usize> CanDevice<RX, TX> {
    /// A device that is not connected yet, on a bus whose frames are `mtu` long.
    #[must_use]
    pub const fn new(config: DeviceConfig, mtu: SegmentMtu) -> Self {
        Self {
            rx: Reassembler::new(),
            mtu,
            link: Link::new(config, RX),
        }
    }

    /// Feeds the data of one CAN frame addressed to this device. A whole frame that is not
    /// the session's own is given to `up`. Reads no clock.
    pub fn receive_segment(
        &mut self,
        segment: &[u8],
        up: impl FnOnce(&mut Link<TX>, &FrameView<'_>),
    ) {
        if let Ok(Some(frame)) = self.rx.push(segment)
            && !self.link.handle(&frame)
        {
            up(&mut self.link, &frame);
        }
    }

    /// Writes the data of the next CAN frame to send into `out`, which must hold a whole
    /// CAN frame's data, and says how long it is. `None` when there is nothing to send.
    pub fn next_segment(&mut self, out: &mut [u8]) -> Option<usize> {
        self.link.next_segment(self.mtu, out)
    }
}
