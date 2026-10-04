//! Sans-IO host session (feature `std`), driven by the node-side gateway.
//!
//! [`HostSession`] serves one device link: it answers `Hello` with `Welcome` or `Reject`
//! (optionally restricted to an allowlist of device names), acknowledges provider snapshots,
//! answers pings and piggybacks the current lease set on every `Pong`, and reports a device as
//! lost after missed heartbeats. [`HostBus`] serves many devices on one CAN bus, demultiplexed by
//! identifier.
//!
//! Like the device session it performs no I/O and reads no clock, so the gateway owns the serial
//! fd or SocketCAN socket:
//!
//! ```no_run
//! use orion_link::host::{HostConfig, HostEvent, HostSession};
//! use orion_link::message::NodeId;
//! # fn read_serial() -> Vec<u8> { Vec::new() }
//! # fn write_serial(_: &[u8]) {}
//! # fn now_ms() -> u64 { 0 }
//!
//! let mut link = HostSession::stream(HostConfig::new(NodeId::new("node-a")));
//! loop {
//!     link.receive(&read_serial());
//!     link.poll(now_ms());
//!     while let Some(event) = link.next_event() {
//!         match event {
//!             HostEvent::ProviderState { provider, resources, .. } => { /* publish into the node */ }
//!             HostEvent::DeviceLost { .. } => { /* mark its resources unavailable */ }
//!             _ => {}
//!         }
//!     }
//!     while let Some(bytes) = link.transmit() {
//!         write_serial(&bytes);
//!     }
//! }
//! ```

mod bus;
mod core;

use std::boxed::Box;
use std::collections::BTreeSet;
use std::string::String;
use std::sync::Arc;
use std::vec::Vec;

pub use bus::{BusEvent, BusFrame, HostBus};

use self::core::HostCore;
use crate::message::{
    LeaseRecord, NodeId, ProviderRecord, ProviderState, RejectReason, ResourceRecord, Roles,
};
use crate::packet::Segment;
use crate::stream::max_encoded_len;
use crate::transport::{self, Packet, Stream, Transport};

/// Largest frame a host session receives or sends (the per-link receive buffer size).
pub const HOST_MAX_FRAME: usize = 4096;

/// Host-side link settings, shared by every session of a [`HostBus`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostConfig {
    /// The node devices attach to (sent in `Welcome`).
    pub node_id: NodeId,
    /// Interval at which devices ping. Default 1000 ms.
    pub heartbeat_ms: u32,
    /// Heartbeats without any frame before a device is lost. Default 3.
    pub missed_heartbeats: u32,
    /// Host frame limit, capped at [`HOST_MAX_FRAME`]. Default [`HOST_MAX_FRAME`].
    pub max_frame: u32,
    /// Devices announcing a smaller `max_frame` are rejected. Default 32.
    pub min_device_frame: u32,
    /// If set, only these device names are accepted.
    pub allowed_devices: Option<BTreeSet<String>>,
    /// First session id handed out (use something that differs across gateway restarts, such as
    /// the start time). Default 1.
    pub session_seed: u32,
}

impl HostConfig {
    /// Defaults for `node_id`.
    pub fn new(node_id: NodeId) -> Self {
        Self {
            node_id,
            heartbeat_ms: 1_000,
            missed_heartbeats: 3,
            max_frame: HOST_MAX_FRAME as u32,
            min_device_frame: 32,
            allowed_devices: None,
            session_seed: 1,
        }
    }

    /// Adds `device_name` to the allowlist (creating it).
    #[must_use]
    pub fn allow_device(mut self, device_name: impl Into<String>) -> Self {
        self.allowed_devices
            .get_or_insert_with(BTreeSet::new)
            .insert(device_name.into());
        self
    }

    /// Effective frame limit.
    #[must_use]
    pub fn max_frame(&self) -> usize {
        usize::try_from(self.max_frame)
            .unwrap_or(HOST_MAX_FRAME)
            .min(HOST_MAX_FRAME)
    }
}

/// Something the gateway should act on.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum HostEvent {
    /// A device opened a session. For a device that was already connected this means it
    /// restarted: replace everything known about it.
    DeviceConnected {
        /// Announced device name.
        device_name: String,
        /// The new session.
        session_id: u32,
        /// Announced roles.
        roles: Roles,
        /// Negotiated frame limit.
        max_frame: usize,
    },
    /// A new provider snapshot (retransmissions of the same snapshot are not repeated).
    ProviderState {
        /// The device that sent it.
        device_name: String,
        /// Provider record as sent; the gateway owns `node_id`.
        provider: ProviderRecord,
        /// Full resource set.
        resources: Vec<ResourceRecord>,
    },
    /// The device went silent, was replaced by another device on the link, or was rejected
    /// mid-session. Mark its resources unavailable.
    DeviceLost {
        /// The lost device.
        device_name: String,
    },
    /// A `Hello` was refused (for diagnostics).
    DeviceRejected {
        /// Announced name, if the `Hello` could be decoded.
        device_name: Option<String>,
        /// Reason sent to the device.
        reason: RejectReason,
    },
    /// The lease set does not fit the negotiated frame size and was not sent.
    LeasesTooLarge {
        /// Affected device.
        device_name: String,
        /// Encoded frame length.
        frame_len: usize,
        /// Negotiated frame limit.
        max_frame: usize,
    },
}

/// Counters kept by a [`HostSession`] (wrapping). Transport-level drops are in
/// [`HostSession::decoder`]`().stats()`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct HostStats {
    /// Valid frames received.
    pub frames_received: u32,
    /// Frames ignored as duplicate or stale by sequence number.
    pub duplicates: u32,
    /// Frames whose body did not decode.
    pub decode_errors: u32,
    /// Rejects sent.
    pub rejects_sent: u32,
    /// Sessions opened.
    pub sessions: u32,
    /// Devices lost to missed heartbeats.
    pub device_timeouts: u32,
    /// Outgoing frames dropped because the gateway did not drain them.
    pub frames_dropped: u32,
    /// Events dropped because the gateway did not drain them.
    pub events_dropped: u32,
}

/// The host side of one device link. See the [module docs](self).
pub struct HostSession<T: Transport> {
    transport: T,
    rx: Box<T::Decoder<HOST_MAX_FRAME>>,
    cursor: T::Cursor,
    current: Option<Vec<u8>>,
    core: HostCore,
}

impl HostSession<Stream> {
    /// A session over a COBS byte stream (serial port, USB-CDC, TCP).
    pub fn stream(config: HostConfig) -> Self {
        Self::new(Arc::new(config), Stream)
    }
}

impl HostSession<Packet> {
    /// A session over one point-to-point CAN link (the gateway filters identifiers). For several
    /// devices on one bus use [`HostBus`].
    pub fn packet(config: HostConfig, transport: Packet) -> Self {
        Self::new(Arc::new(config), transport)
    }
}

impl<T: Transport> HostSession<T> {
    /// A session sharing `config` with others.
    pub fn new(config: Arc<HostConfig>, transport: T) -> Self {
        Self {
            transport,
            rx: Box::default(),
            cursor: T::Cursor::default(),
            current: None,
            core: HostCore::new(config),
        }
    }

    /// Advances timers to `now_ms` and detects device loss.
    pub fn poll(&mut self, now_ms: u64) {
        self.core.poll(now_ms);
    }

    /// The next event, if any.
    pub fn next_event(&mut self) -> Option<HostEvent> {
        self.core.next_event()
    }

    /// Replaces the device's lease set. If it changed and a session is up, it is sent at once;
    /// it is also re-sent after every `Pong` and on every new session. Returns whether it changed.
    pub fn set_leases(&mut self, leases: Vec<LeaseRecord>) -> bool {
        self.core.set_leases(leases)
    }

    /// The current lease set.
    pub fn leases(&self) -> &[LeaseRecord] {
        self.core.leases()
    }

    /// The connected device's name.
    pub fn device_name(&self) -> Option<&str> {
        self.core.device_name()
    }

    /// Whether a session is up.
    pub fn is_connected(&self) -> bool {
        self.core.device_name().is_some()
    }

    /// The current session id.
    pub fn session_id(&self) -> Option<u32> {
        self.core.session_id()
    }

    /// The latest snapshot received in this session.
    pub fn provider_state(&self) -> Option<&ProviderState> {
        self.core.provider_state()
    }

    /// Session counters.
    pub fn stats(&self) -> HostStats {
        self.core.stats()
    }

    /// The receive decoder, for its stats.
    pub fn decoder(&self) -> &T::Decoder<HOST_MAX_FRAME> {
        &self.rx
    }

    fn current_frame(&mut self) -> Option<&[u8]> {
        if self.current.is_none() {
            self.current = self.core.pop_frame();
        }
        self.current.as_deref()
    }
}

impl HostSession<Stream> {
    /// Feeds bytes read from the link, in chunks of any size.
    pub fn receive(&mut self, bytes: &[u8]) {
        let mut rest = bytes;
        while !rest.is_empty() {
            let (used, result) = self.rx.push_slice(rest);
            if let Ok(Some(frame)) = result {
                self.core.handle_frame(&frame);
            }
            if used == 0 {
                break;
            }
            rest = rest.get(used..).unwrap_or_default();
        }
    }

    /// The stream bytes of the next frame (with a leading `0x00`), or `None` when idle. Write
    /// them completely; partial writes are the gateway's to finish.
    pub fn transmit(&mut self) -> Option<Vec<u8>> {
        let frame = self.current_frame()?;
        let mut out = std::vec![0u8; max_encoded_len(frame.len()).saturating_add(1)];
        let mut cursor = None;
        let (written, _) = transport::stream_fill(frame, &mut cursor, &mut out);
        out.truncate(written);
        self.current = None;
        Some(out)
    }
}

impl HostSession<Packet> {
    /// Feeds the data of one received CAN frame from this link's device.
    pub fn receive_segment(&mut self, data: &[u8]) {
        if let Ok(Some(frame)) = self.rx.push(data) {
            self.core.handle_frame(&frame);
        }
    }

    /// The next segment to send (consumed), or `None` when idle. If the socket is busy, keep the
    /// segment and send it later; segments must go out in order.
    pub fn next_segment(&mut self) -> Option<Segment> {
        let mtu = self.transport.mtu;
        let mut cursor = self.cursor;
        let frame = self.current_frame()?;
        let step = transport::segment_step(frame, mtu, &mut cursor, true);
        self.cursor = cursor;
        match step {
            Some((segment, done)) => {
                if done {
                    self.current = None;
                }
                Some(segment)
            }
            None => {
                self.current = None;
                self.cursor = Default::default();
                None
            }
        }
    }
}
