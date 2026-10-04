//! Wiring a [`CanDevice`] to any `embedded-can` controller (classic CAN or CAN FD).

use embedded_can::Frame;
use embedded_can::nb::Can;
use orion_link::device::{CanDevice, DeviceConfig, DeviceEvent, PublishError};
use orion_link::message::{ProviderRecord, ResourceRecord};
use orion_link::{CanLinkIds, Packet};

use crate::{RX, TX};

/// Why [`CanPort::service`] failed.
#[derive(Debug)]
pub enum CanPortError<E> {
    /// The controller reported an error.
    Can(E),
    /// The link identifiers do not fit the configured identifier type.
    InvalidIds,
}

/// A device session bound to a CAN controller and one pair of link identifiers.
///
/// Configure the controller's acceptance filter for `ids.host_to_device` if it has one.
///
/// ```ignore
/// let ids = CanLinkIds::for_address(CanLinkIds::new(0x100, 0x180, false), 5).unwrap();
/// let mut port = CanPort::new(can, ids, Packet::CLASSIC, "imu-board");
/// loop {
///     port.service(millis())?;
///     while let Some(event) = port.next_event() { /* ... */ }
/// }
/// ```
pub struct CanPort<C> {
    can: C,
    ids: CanLinkIds,
    session: CanDevice<RX, TX>,
}

impl<C: Can> CanPort<C> {
    /// Binds `can` to a new session for `device_name` on the link `ids`. Use [`Packet::FD`] (or a
    /// custom MTU) only with a CAN FD controller whose frame type accepts FD lengths.
    pub fn new(can: C, ids: CanLinkIds, transport: Packet, device_name: &str) -> Self {
        Self {
            can,
            ids,
            session: CanDevice::new(DeviceConfig::provider(device_name), transport),
        }
    }

    /// One main-loop iteration: drains received frames, advances the session clock, and queues
    /// segments until the controller's transmit mailboxes are full (the rest go next time).
    ///
    /// # Errors
    ///
    /// The controller's error, or [`CanPortError::InvalidIds`].
    pub fn service(&mut self, now_ms: u64) -> Result<(), CanPortError<C::Error>> {
        loop {
            match self.can.receive() {
                Ok(frame) => {
                    let _ = self.session.receive_can_frame(&self.ids, &frame);
                }
                Err(nb::Error::WouldBlock) => break,
                Err(nb::Error::Other(err)) => return Err(CanPortError::Can(err)),
            }
        }
        self.session.poll(now_ms);
        let id = self
            .ids
            .device_to_host_id()
            .ok_or(CanPortError::InvalidIds)?;
        while let Some(segment) = self.session.peek_segment() {
            let Some(frame) = C::Frame::new(id, segment.as_bytes()) else {
                // The frame type cannot carry this length (FD MTU on a classic controller).
                return Err(CanPortError::InvalidIds);
            };
            match self.can.transmit(&frame) {
                // `Ok(Some(_))` means a lower-priority pending frame was displaced; the session
                // repairs anything lost through retransmission.
                Ok(_) => self.session.commit_segment(),
                Err(nb::Error::WouldBlock) => break,
                Err(nb::Error::Other(err)) => return Err(CanPortError::Can(err)),
            }
        }
        Ok(())
    }

    /// Replaces the provider snapshot.
    ///
    /// # Errors
    ///
    /// [`PublishError::TooLarge`] if it does not fit [`TX`].
    pub fn publish(
        &mut self,
        provider: &ProviderRecord,
        resources: &[ResourceRecord],
    ) -> Result<(), PublishError> {
        self.session.publish_provider_state(provider, resources)
    }

    /// The next session event.
    pub fn next_event(&mut self) -> Option<DeviceEvent> {
        self.session.next_event()
    }

    /// The session, for state queries and stats.
    pub fn session(&self) -> &CanDevice<RX, TX> {
        &self.session
    }

    /// The controller, for example to adjust its filters.
    pub fn can_mut(&mut self) -> &mut C {
        &mut self.can
    }
}
