//! Sans-IO host session (feature `std`), driven by the node-side gateway.
//!
//! [`HostSession`] serves one device link: it answers `Hello` with `Welcome` or `Reject`
//! (optionally restricted to an allowlist of device names), acknowledges provider snapshots,
//! reports status batches (unacknowledged),
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
mod out;
#[cfg(feature = "xfer")]
pub mod xfer;

use std::boxed::Box;
use std::collections::BTreeSet;
use std::string::String;
use std::sync::Arc;
use std::vec::Vec;

pub use bus::{BusEvent, BusFrame, HostBus};

use self::core::HostCore;
use crate::message::{
    LeaseRecord, LinkActionState, NodeId, ProviderRecord, ProviderState, RejectReason,
    ResourceRecord, Roles, StatusEntry,
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
    /// Whether this link serves the client role (revision 1): when false, a `Hello` that does
    /// not include the provider role is rejected, and client requests are answered with
    /// `RequestError { code: UNSUPPORTED }`. Default false.
    pub serve_clients: bool,
    /// Whether this link serves streams (revision 2): declarations and batches from providers,
    /// subscriptions from clients. When false, declarations and subscriptions are answered with
    /// `RequestError { code: UNSUPPORTED }` and batches are dropped. Default false.
    pub serve_streams: bool,
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
            serve_clients: false,
            serve_streams: false,
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
    /// Volatile status values (fire-and-forget; not acknowledged, newest value per key wins).
    Status {
        /// The device that sent them.
        device_name: String,
        /// The entries, in the order sent.
        entries: Vec<StatusEntry>,
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
    /// The device's firmware state changed (feature `xfer`). Sent once per distinct report; the
    /// device resends it until acknowledged, and the host acknowledges every copy.
    #[cfg(feature = "xfer")]
    DeviceInfo {
        /// The device that sent it.
        device_name: String,
        /// The report.
        info: xfer::DeviceInfo,
    },
    /// A reply to a firmware transfer request (feature `xfer`).
    #[cfg(feature = "xfer")]
    Xfer {
        /// The device that replied.
        device_name: String,
        /// The reply.
        reply: xfer::XferReply,
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
    /// The action names the device declared (`DeviceSession::declare_actions`), in this session.
    /// A device that declares nothing sends no event.
    ActionCatalog {
        /// The device that declared them.
        device_name: String,
        /// The names, as the device declared them.
        names: Vec<String>,
    },
    /// The device reported on an action the host sent it (`HostSession::send_action`). The
    /// gateway matches `id` to its own record of the request.
    ActionResult {
        /// The device that sent it.
        device_name: String,
        /// The `id` of the request.
        id: u32,
        /// The state reported.
        state: LinkActionState,
        /// Progress (`Running`), output (`Succeeded`) or reason (`Failed`, `Rejected`), as sent.
        payload: Vec<u8>,
    },
    /// A provider declared a stream (revision 2, links with [`HostConfig::serve_streams`]).
    /// Answer with [`HostSession::send_stream_demand`], or refuse with
    /// [`HostSession::send_request_error`] echoing `req`.
    StreamDeclare {
        /// The session's device name.
        device_name: String,
        /// Request id, for a refusal.
        req: u32,
        /// The stream's id; its owner is the session's provider.
        id: String,
        /// The series.
        series: Vec<orion_control_plane::SeriesDecl>,
    },
    /// A batch of samples from a provider (revision 2); not answered.
    StreamPublish {
        /// The session's device name.
        device_name: String,
        /// The stream's id.
        id: String,
        /// The samples.
        chunks: Vec<orion_control_plane::SampleChunk>,
    },
    /// A client request (revision 1, links with [`HostConfig::serve_clients`]).
    ClientRequest {
        /// The session's device name.
        device_name: String,
        /// The request.
        request: ClientRequest,
    },
}

/// A request from a session with the client role, in [`HostEvent::ClientRequest`]. The gateway
/// answers it with [`HostSession::send_status_changes`], [`HostSession::send_action_results`] or
/// [`HostSession::send_request_error`], echoing `req`.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ClientRequest {
    /// Answer once with the matching status entries (`bootstrap`).
    StatusQuery {
        /// Request id.
        req: u32,
        /// Which entries.
        filter: orion_control_plane::StatusQuery,
    },
    /// Answer with the matching entries, then with their changes until ended.
    StatusWatch {
        /// Request id.
        req: u32,
        /// Which entries.
        filter: orion_control_plane::StatusQuery,
    },
    /// Run an action; answer with its result and each update until it is final.
    ActionRun {
        /// Request id.
        req: u32,
        /// The action (`requested_by` and `wait_ms` empty).
        request: Box<orion_control_plane::ActionRequest>,
    },
    /// Answer once with the matching action results.
    ActionQuery {
        /// Request id.
        req: u32,
        /// Which actions.
        filter: orion_control_plane::ActionQuery,
    },
    /// Answer with the matching results, then with their updates until ended.
    ActionWatch {
        /// Request id.
        req: u32,
        /// Which actions.
        filter: orion_control_plane::ActionQuery,
    },
    /// Subscribe to a stream (revision 2); answer with [`HostSession::send_stream_catalog`],
    /// [`HostSession::send_stream_data`] and [`HostSession::send_stream_reset`] until ended.
    StreamSubscribe {
        /// Request id.
        req: u32,
        /// The stream, series, rate and backfill.
        subscription: Box<orion_control_plane::StreamSubscription>,
    },
    /// End the watch or subscription `req`. Not answered.
    End {
        /// The watch's request id.
        req: u32,
    },
}

/// Why [`HostSession::send_action`] did not queue a request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ActionSendError {
    /// No session with the device is up.
    NotConnected,
    /// The request does not fit the negotiated frame size.
    TooLarge {
        /// Encoded frame length of the request.
        frame_len: usize,
        /// Negotiated frame limit.
        max_frame: usize,
    },
    /// Too many frames are waiting for the gateway to write them.
    Congested,
}

impl std::fmt::Display for ActionSendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotConnected => f.write_str("the device is not connected"),
            Self::TooLarge {
                frame_len,
                max_frame,
            } => write!(
                f,
                "the request needs {frame_len} bytes, the device accepts {max_frame}"
            ),
            Self::Congested => f.write_str("the link's output queue is full"),
        }
    }
}

impl std::error::Error for ActionSendError {}

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
    /// `Status` frames received.
    pub status_received: u32,
}

/// The host side of one device link. See the [module docs](self).
pub struct HostSession<T: Transport> {
    transport: T,
    rx: Box<T::Decoder<HOST_MAX_FRAME>>,
    cursor: T::Cursor,
    /// The frame being sent, taken from the output queue; its buffer is reused.
    current: Vec<u8>,
    has_current: bool,
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
            current: Vec::new(),
            has_current: false,
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

    /// Queues an `ActionRequest` for the connected device. Its results arrive as
    /// [`HostEvent::ActionResult`] with the same `id`. The device runs one action at a time and
    /// refuses the others (`Rejected`, reason `busy`).
    ///
    /// # Errors
    ///
    /// [`ActionSendError`] if there is no session, the request is too large, or the output queue
    /// is full.
    pub fn send_action(
        &mut self,
        id: u32,
        name: &str,
        payload: &[u8],
    ) -> Result<(), ActionSendError> {
        self.core.send_action(id, name, payload)
    }

    /// Queues an `ActionCancel` for the request `id` on the connected device. The device accepts
    /// it while it runs that action and reports the action's final state (normally `Cancelled`).
    /// A cancel for another id is ignored by the device.
    ///
    /// # Errors
    ///
    /// As [`HostSession::send_action`].
    pub fn send_action_cancel(&mut self, id: u32) -> Result<(), ActionSendError> {
        self.core.send_action_cancel(id)
    }

    /// Queues one answer to a status request, split into as many [`kind::STATUS_CHANGES`]
    /// frames as the negotiated frame size needs (all or none of them).
    ///
    /// [`kind::STATUS_CHANGES`]: crate::message::kind::STATUS_CHANGES
    ///
    /// # Errors
    ///
    /// [`ActionSendError::NotConnected`] without a client session, [`ActionSendError::TooLarge`]
    /// if one entry alone does not fit a frame or the answer needs more frames than the output
    /// queue holds, and [`ActionSendError::Congested`] if the queue has no room now (retry
    /// after transmitting).
    pub fn send_status_changes(
        &mut self,
        req: u32,
        bootstrap: bool,
        updated: &[orion_control_plane::StatusEntry],
        expired: &[orion_control_plane::StatusKey],
    ) -> Result<(), ActionSendError> {
        self.core
            .send_status_changes(req, bootstrap, updated, expired)
    }

    /// Queues one answer to an action request, split as [`HostSession::send_status_changes`].
    ///
    /// # Errors
    ///
    /// As [`HostSession::send_status_changes`].
    pub fn send_action_results(
        &mut self,
        req: u32,
        results: &[orion_control_plane::ActionResult],
    ) -> Result<(), ActionSendError> {
        self.core.send_action_results(req, results)
    }

    /// Refuses the client request `req`, which ends. The reason is cut to
    /// [`crate::message::MAX_REASON_BYTES`].
    ///
    /// # Errors
    ///
    /// [`ActionSendError::NotConnected`] or [`ActionSendError::Congested`].
    pub fn send_request_error(
        &mut self,
        req: u32,
        code: crate::message::ErrorCode,
        reason: &str,
    ) -> Result<(), ActionSendError> {
        self.core.send_request_error(req, code, reason)
    }

    /// Queues a `StreamDemand` for a stream the provider session declared.
    ///
    /// # Errors
    ///
    /// [`ActionSendError::NotConnected`] without a provider session of revision 2, and as
    /// [`HostSession::send_status_changes`].
    pub fn send_stream_demand(
        &mut self,
        id: &str,
        subscribers: u32,
        series: &[u16],
    ) -> Result<(), ActionSendError> {
        self.core.send_stream_demand(id, subscribers, series)
    }

    /// Queues a subscription's `StreamCatalog`.
    ///
    /// # Errors
    ///
    /// As [`HostSession::send_status_changes`].
    pub fn send_stream_catalog(
        &mut self,
        req: u32,
        epoch: u64,
        series: &[orion_control_plane::SeriesDecl],
        live: bool,
    ) -> Result<(), ActionSendError> {
        self.core.send_stream_catalog(req, epoch, series, live)
    }

    /// Queues a subscription's samples, split by chunk into as many `StreamData` frames as the
    /// frame size needs (all or none). Chunks larger than [`crate::message::max_chunk_data`]
    /// must be split first (`SampleChunk::split`); `data.more` is set per part here.
    ///
    /// # Errors
    ///
    /// As [`HostSession::send_status_changes`].
    pub fn send_stream_data(
        &mut self,
        data: &crate::message::StreamData,
    ) -> Result<(), ActionSendError> {
        self.core.send_stream_data(data)
    }

    /// Queues a subscription's `StreamReset`.
    ///
    /// # Errors
    ///
    /// As [`HostSession::send_status_changes`].
    pub fn send_stream_reset(
        &mut self,
        req: u32,
        reason: orion_control_plane::StreamResetReason,
    ) -> Result<(), ActionSendError> {
        self.core.send_stream_reset(req, reason)
    }

    /// The session's negotiated frame size.
    pub fn max_frame(&self) -> Option<usize> {
        self.core.max_frame()
    }

    /// The roles the connected session announced.
    pub fn roles(&self) -> Option<Roles> {
        self.core.roles()
    }

    /// Frames the output queue can take now.
    pub fn output_room(&self) -> usize {
        self.core.output_room()
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

    /// Sends a firmware transfer message (feature `xfer`) to the connected device. Returns false
    /// without a session or if the message does not fit the negotiated frame size.
    #[cfg(feature = "xfer")]
    pub fn send_xfer(&mut self, request: &xfer::XferRequest) -> bool {
        self.core.send_xfer(request)
    }

    /// The current session id.
    pub fn session_id(&self) -> Option<u32> {
        self.core.session_id()
    }

    /// The current session's interface revision: the lower of the device's and
    /// [`crate::LINK_INTERFACE_REVISION`].
    pub fn revision(&self) -> Option<u16> {
        self.core.revision()
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
        if !self.has_current {
            self.has_current = self.core.pop_frame_into(&mut self.current);
        }
        self.has_current.then_some(self.current.as_slice())
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
        let mut out = Vec::new();
        self.transmit_into(&mut out).then_some(out)
    }

    /// As [`HostSession::transmit`], into `out` (its contents replaced, its buffer reused): no
    /// allocation once `out` has held a frame of this size. `false` when idle.
    pub fn transmit_into(&mut self, out: &mut Vec<u8>) -> bool {
        let Some(frame) = self.current_frame() else {
            return false;
        };
        out.clear();
        out.resize(max_encoded_len(frame.len()).saturating_add(1), 0);
        let mut cursor = None;
        let (written, _) = transport::stream_fill(frame, &mut cursor, out);
        out.truncate(written);
        self.has_current = false;
        true
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
                    self.has_current = false;
                }
                Some(segment)
            }
            None => {
                self.has_current = false;
                self.cursor = Default::default();
                None
            }
        }
    }
}
