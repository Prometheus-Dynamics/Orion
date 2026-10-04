//! Many devices on one CAN bus, demultiplexed by identifier.

use std::collections::BTreeMap;
use std::ops::RangeInclusive;
use std::sync::Arc;
use std::vec::Vec;

use super::{HostConfig, HostEvent, HostSession};
use crate::message::LeaseRecord;
use crate::packet::{CanLinkIds, Segment};
use crate::transport::Packet;

/// A [`HostEvent`] from the device at `address`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BusEvent {
    /// Device address (offset from the base identifiers).
    pub address: u32,
    /// The event.
    pub event: HostEvent,
}

/// A CAN frame to send: data `segment` with identifier `id`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BusFrame {
    /// Destination device address.
    pub address: u32,
    /// CAN identifier (the device's host→device id).
    pub id: u32,
    /// Extended (29-bit) identifier.
    pub extended: bool,
    /// Frame data.
    pub segment: Segment,
}

#[cfg(feature = "embedded-can")]
impl BusFrame {
    /// As an [`embedded_can::Frame`] (for example a SocketCAN frame type). `None` if `F` rejects
    /// the identifier or length.
    #[must_use]
    pub fn to_can_frame<F: embedded_can::Frame>(&self) -> Option<F> {
        let ids = CanLinkIds::new(0, self.id, self.extended);
        self.segment.to_can_frame(ids.host_to_device_id()?)
    }
}

struct BusDevice {
    ids: CanLinkIds,
    session: HostSession<Packet>,
}

/// Host sessions for every device on one CAN bus.
///
/// Device `address` uses `CanLinkIds::for_address(base, address)`. Sessions are created when a
/// device's first frame arrives, for any address in the configured range, so new devices need no
/// host configuration (restrict names with [`HostConfig::allowed_devices`]).
///
/// ```
/// use orion_link::host::{HostBus, HostConfig};
/// use orion_link::message::NodeId;
/// use orion_link::{CanLinkIds, Packet};
///
/// let base = CanLinkIds::new(0x100, 0x180, false);
/// let mut bus = HostBus::new(HostConfig::new(NodeId::new("node-a")), base, Packet::CLASSIC, 1..=63);
/// assert!(!bus.receive(0x7FF, false, &[0xC0])); // not ours
/// bus.poll(0);
/// while let Some(frame) = bus.next_frame() {
///     // socket.write(frame.id, frame.extended, frame.segment.as_bytes());
///     let _ = frame;
/// }
/// ```
pub struct HostBus {
    config: Arc<HostConfig>,
    base: CanLinkIds,
    transport: Packet,
    addresses: RangeInclusive<u32>,
    devices: BTreeMap<u32, BusDevice>,
    /// Address after the one that sent last, for round-robin transmission.
    tx_next: u32,
}

impl HostBus {
    /// A bus whose devices use `base` identifiers plus an address in `addresses`.
    pub fn new(
        config: HostConfig,
        base: CanLinkIds,
        transport: Packet,
        addresses: RangeInclusive<u32>,
    ) -> Self {
        Self {
            config: Arc::new(config),
            base,
            transport,
            addresses,
            devices: BTreeMap::new(),
            tx_next: 0,
        }
    }

    /// The device address a received identifier belongs to, if any.
    #[must_use]
    pub fn address_for_id(&self, id: u32, extended: bool) -> Option<u32> {
        if extended != self.base.extended {
            return None;
        }
        let address = id.checked_sub(self.base.device_to_host)?;
        if !self.addresses.contains(&address) {
            return None;
        }
        let ids = CanLinkIds::for_address(self.base, address)?;
        (ids.device_to_host == id).then_some(address)
    }

    /// Feeds one received CAN data frame. Returns `false` (and ignores it) if the identifier is
    /// not a device→host identifier of this bus.
    pub fn receive(&mut self, id: u32, extended: bool, data: &[u8]) -> bool {
        let Some(address) = self.address_for_id(id, extended) else {
            return false;
        };
        let Some(device) = self.device_mut(address) else {
            return false;
        };
        device.session.receive_segment(data);
        true
    }

    /// Feeds a received [`embedded_can::Frame`]; see [`HostBus::receive`]. Remote frames are
    /// ignored.
    #[cfg(feature = "embedded-can")]
    pub fn receive_can_frame<F: embedded_can::Frame>(&mut self, frame: &F) -> bool {
        if frame.is_remote_frame() {
            return false;
        }
        let (id, extended) = match frame.id() {
            embedded_can::Id::Standard(id) => (u32::from(id.as_raw()), false),
            embedded_can::Id::Extended(id) => (id.as_raw(), true),
        };
        self.receive(id, extended, frame.data())
    }

    fn device_mut(&mut self, address: u32) -> Option<&mut BusDevice> {
        if !self.devices.contains_key(&address) {
            let ids = CanLinkIds::for_address(self.base, address)?;
            let session = HostSession::new(Arc::clone(&self.config), self.transport);
            self.devices.insert(address, BusDevice { ids, session });
        }
        self.devices.get_mut(&address)
    }

    /// Advances every session's timers.
    pub fn poll(&mut self, now_ms: u64) {
        for device in self.devices.values_mut() {
            device.session.poll(now_ms);
        }
    }

    /// The next event from any device.
    pub fn next_event(&mut self) -> Option<BusEvent> {
        self.devices.iter_mut().find_map(|(&address, device)| {
            device
                .session
                .next_event()
                .map(|event| BusEvent { address, event })
        })
    }

    /// The next CAN frame to send, round-robin across devices (consumed). If the socket is busy,
    /// keep the frame and send it later; each device's frames must go out in order.
    pub fn next_frame(&mut self) -> Option<BusFrame> {
        let start = self.tx_next;
        let order: Vec<u32> = self
            .devices
            .range(start..)
            .chain(self.devices.range(..start))
            .map(|(&address, _)| address)
            .collect();
        for address in order {
            let Some(device) = self.devices.get_mut(&address) else {
                continue;
            };
            if let Some(segment) = device.session.next_segment() {
                self.tx_next = address.wrapping_add(1);
                return Some(BusFrame {
                    address,
                    id: device.ids.host_to_device,
                    extended: device.ids.extended,
                    segment,
                });
            }
        }
        None
    }

    /// Replaces the lease set of the device at `address` (creating its session if needed, so
    /// leases can be set before the device appears). Returns whether it changed.
    pub fn set_leases(&mut self, address: u32, leases: Vec<LeaseRecord>) -> bool {
        if !self.addresses.contains(&address) {
            return false;
        }
        self.device_mut(address)
            .is_some_and(|device| device.session.set_leases(leases))
    }

    /// Address of the connected device named `device_name`.
    #[must_use]
    pub fn address_of(&self, device_name: &str) -> Option<u32> {
        self.devices
            .iter()
            .find(|(_, device)| device.session.device_name() == Some(device_name))
            .map(|(&address, _)| address)
    }

    /// The session of the device at `address`.
    #[must_use]
    pub fn session(&self, address: u32) -> Option<&HostSession<Packet>> {
        self.devices.get(&address).map(|device| &device.session)
    }

    /// Addresses that have a session (connected or not).
    pub fn addresses(&self) -> impl Iterator<Item = u32> + '_ {
        self.devices.keys().copied()
    }
}
