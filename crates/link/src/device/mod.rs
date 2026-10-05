//! Sans-IO device session (feature `device`): `no_std`, no allocator, fixed buffers only.
//!
//! [`DeviceSession`] runs the device side of the link protocol: it sends `Hello` until the host
//! answers `Welcome`, publishes provider snapshots reliably, pings on the negotiated heartbeat,
//! notices when the host goes away, and reports lease changes. It never performs I/O and never
//! reads a clock. A port does three things in its main loop:
//!
//! 1. feed received bytes ([`DeviceSession::receive`]) or CAN frame data
//!    ([`DeviceSession::receive_segment`]); this is clock-free, so it may run in an RX interrupt;
//! 2. call [`DeviceSession::poll`] with a monotonic millisecond timestamp, then drain
//!    [`DeviceSession::next_event`];
//! 3. send what [`DeviceSession::transmit`] (bytes) or [`DeviceSession::next_segment`] (CAN
//!    segments) hands out.
//!
//! ```
//! use orion_link::Stream;
//! use orion_link::device::{DeviceConfig, DeviceEvent, StreamDevice};
//! use orion_link::wire::{Health, ProviderView, ResourceView};
//!
//! // The snapshot can live in flash: views are `const`.
//! const PROVIDER: ProviderView<'static> =
//!     ProviderView::new("provider.imu-board", "unassigned").with_resource_types(&["imu.sample_source"]);
//! const RESOURCES: [ResourceView<'static>; 1] =
//!     [ResourceView::new("imu-board.imu-0", "imu.sample_source", "provider.imu-board")
//!         .with_health(Health::Healthy)];
//!
//! let mut device = StreamDevice::<128, 128>::new(DeviceConfig::provider("imu-board"), Stream);
//! device.publish_provider_state(&PROVIDER, &RESOURCES).unwrap();
//!
//! let mut uart_tx = [0u8; 64];
//! for now_ms in 0..3 {
//!     // device.receive(&bytes_from_uart);
//!     device.poll(now_ms);
//!     while let Some(event) = device.next_event() {
//!         if event == DeviceEvent::LeasesChanged {
//!             for lease in device.leases() { /* start or stop work */ let _ = lease.resource_id; }
//!         }
//!     }
//!     let n = device.transmit(&mut uart_tx);
//!     // uart.write_all(&uart_tx[..n]);
//!     # let _ = n;
//! }
//! ```
//!
//! Memory is fixed (`2 * RX + 2 * TX` plus about 350 bytes on 32-bit targets): the `RX`-byte
//! receive decoder, an `RX`-byte copy of the current lease set (which also holds the outgoing
//! `Hello` outside a session), two `TX`-byte buffers (the encoded latest snapshot and the newest
//! status batch), an 18-byte ping/pong buffer, the node id ([`NODE_ID_CAPACITY`] bytes), a 4-slot
//! event queue, counters, and timers. Nothing allocates, panics, or formats. With feature
//! `alloc`, the same session also accepts the full Orion records (`ProviderRecord`,
//! `ResourceRecord`, `StatusEntry`) and decodes the lease set into `LeaseRecord`s
//! (`DeviceSession::lease_records`).

mod core;
mod events;

pub use events::{DeviceEvent, EVENT_CAPACITY};

use self::core::DeviceCore;
use crate::packet::Segment;
use crate::transport::{self, Packet, Stream, Transport};
use crate::wire::{Leases, ProviderBody, ProviderStateView, ResourceBody, Roles, StatusBody};

/// Longest node id kept from `Welcome` (longer ones are not reported by
/// [`DeviceSession::node_id`]; the session works the same).
pub const NODE_ID_CAPACITY: usize = 32;

/// A device session over a COBS byte stream.
pub type StreamDevice<const RX: usize, const TX: usize, N = &'static str> =
    DeviceSession<Stream, RX, TX, N>;
/// A device session over classic CAN or CAN FD.
pub type CanDevice<const RX: usize, const TX: usize, N = &'static str> =
    DeviceSession<Packet, RX, TX, N>;

/// Device identity and timing. Timing defaults suit links from 9600 baud UART to CAN FD.
///
/// `N` holds the device name: `&'static str` by default, any `AsRef<str>` (for example a
/// `String` with `alloc`, or a fixed inline buffer filled from a serial number).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeviceConfig<N = &'static str> {
    /// Stable name announced in `Hello` (the host may allowlist names).
    pub device_name: N,
    /// Announced roles.
    pub roles: Roles,
    /// First `Hello` retry interval; doubles per attempt up to `hello_retry_max_ms`.
    pub hello_retry_min_ms: u32,
    /// Longest `Hello` retry interval.
    pub hello_retry_max_ms: u32,
    /// Wait after a `Reject`; doubles per consecutive reject up to `reject_retry_max_ms`.
    pub reject_retry_min_ms: u32,
    /// Longest wait after a `Reject`.
    pub reject_retry_max_ms: u32,
    /// First snapshot retransmission delay (measured from the end of the transmission); doubles up
    /// to the heartbeat. Should exceed the link's round-trip time.
    pub state_retry_min_ms: u32,
    /// Heartbeats without any frame from the host before the session is considered lost.
    pub missed_heartbeats: u32,
    /// Shortest spacing between two `Status` frames. Status published faster is coalesced: only
    /// the newest batch is sent.
    pub status_min_interval_ms: u32,
}

impl<N> DeviceConfig<N> {
    /// A provider device with default timing.
    pub const fn provider(device_name: N) -> Self {
        Self {
            device_name,
            roles: Roles::PROVIDER,
            hello_retry_min_ms: 250,
            hello_retry_max_ms: 4_000,
            reject_retry_min_ms: 5_000,
            reject_retry_max_ms: 60_000,
            state_retry_min_ms: 200,
            missed_heartbeats: 3,
            status_min_interval_ms: 100,
        }
    }
}

/// Connection state of a [`DeviceSession`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LinkState {
    /// Sending `Hello` until the host answers.
    Connecting,
    /// Rejected by the host; waiting before the next `Hello`.
    BackingOff,
    /// In a session.
    Connected,
}

/// Why a publish failed. The previous snapshot (or status batch) is kept. No `Display` outside
/// `std`, so the device path links no formatting code.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum PublishError {
    /// The encoded frame exceeds the transmit buffer or the negotiated frame size.
    TooLarge {
        /// Encoded frame length.
        frame_len: usize,
        /// Current limit.
        max_frame: usize,
    },
}

#[cfg(feature = "std")]
impl ::core::fmt::Display for PublishError {
    fn fmt(&self, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
        match self {
            Self::TooLarge {
                frame_len,
                max_frame,
            } => write!(f, "frame needs {frame_len} bytes, limit is {max_frame}"),
        }
    }
}

#[cfg(feature = "std")]
impl std::error::Error for PublishError {}

/// Counters kept by a [`DeviceSession`] (wrapping). Transport-level drops are in
/// [`DeviceSession::decoder`]`().stats()`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DeviceStats {
    /// Valid frames received.
    pub frames_received: u32,
    /// Frames completely handed to the transport.
    pub frames_sent: u32,
    /// Frames ignored as duplicate or stale by sequence number.
    pub duplicates: u32,
    /// Frames whose body did not decode.
    pub decode_errors: u32,
    /// Control frames that did not fit the transmit buffer (for example a too-long device name).
    pub encode_errors: u32,
    /// Snapshot retransmissions.
    pub state_retransmits: u32,
    /// Snapshot or status transmissions cut short by a newer publish.
    pub aborted_frames: u32,
    /// Sessions established.
    pub connects: u32,
    /// Sessions lost to missed heartbeats.
    pub host_timeouts: u32,
    /// Rejects received.
    pub rejects: u32,
    /// Events dropped because the queue was full.
    pub events_dropped: u32,
    /// `Status` frames sent.
    pub status_sent: u32,
    /// Status batches replaced by a newer one before they were sent.
    pub status_replaced: u32,
    /// Status batches dropped because they no longer fit the negotiated frame size.
    pub status_dropped: u32,
}

/// The device side of a link session. See the [module docs](self).
///
/// `T` is [`Stream`] or [`Packet`]; `RX` bounds received frames and `TX` bounds sent frames (both
/// header + payload + CRC); `N` holds the device name. The device announces `min(RX, TX)` as its
/// `max_frame`.
pub struct DeviceSession<T: Transport, const RX: usize, const TX: usize, N = &'static str> {
    transport: T,
    rx: T::Decoder<RX>,
    cursor: T::Cursor,
    core: DeviceCore<N, RX, TX>,
}

impl<T: Transport, const RX: usize, const TX: usize, N: AsRef<str>> DeviceSession<T, RX, TX, N> {
    /// A new session, initially [`LinkState::Connecting`]. Nothing is sent before the first
    /// [`DeviceSession::poll`].
    ///
    /// Always inlined, so `slot.write(DeviceSession::new(..))` into a static `MaybeUninit`
    /// builds the session in place rather than on the stack.
    #[inline(always)]
    pub fn new(config: DeviceConfig<N>, transport: T) -> Self {
        Self {
            transport,
            rx: T::Decoder::<RX>::default(),
            cursor: T::Cursor::default(),
            core: DeviceCore::new(config),
        }
    }

    /// Advances timers to `now_ms` (any monotonic millisecond clock; only differences matter):
    /// schedules `Hello`, pings, and snapshot retransmissions, and detects host loss.
    pub fn poll(&mut self, now_ms: u64) {
        self.core.poll(now_ms);
    }

    /// The next queued event, if any. Drain after every [`DeviceSession::poll`] and receive call.
    pub fn next_event(&mut self) -> Option<DeviceEvent> {
        self.core.next_event()
    }

    /// Replaces the provider snapshot: [`crate::wire::ProviderView`] and
    /// [`crate::wire::ResourceView`]s (or, with `alloc`, `ProviderRecord` and `ResourceRecord`s).
    /// It is encoded immediately into the snapshot buffer (the inputs can be dropped afterwards),
    /// sent as soon as connected, retransmitted until the host acknowledges it, and re-sent
    /// automatically after every reconnect. Only the newest snapshot is kept; if an older one is
    /// mid-transmission, that transmission is aborted.
    ///
    /// # Errors
    ///
    /// [`PublishError::TooLarge`] if the frame exceeds `TX` or the negotiated frame size.
    pub fn publish_provider_state<P, R>(
        &mut self,
        provider: &P,
        resources: &[R],
    ) -> Result<(), PublishError>
    where
        P: ProviderBody + ?Sized,
        R: ResourceBody,
    {
        let body = ProviderStateView {
            provider,
            resources,
        };
        if self.core.publish(&body)? {
            self.cursor = T::Cursor::default();
        }
        Ok(())
    }

    /// Publishes volatile status values ([`crate::wire::StatusView`]s, or `StatusEntry`s with
    /// `alloc`); the node files them under this device's provider.
    ///
    /// Fire-and-forget: nothing is acknowledged or retransmitted. Only the newest batch is kept
    /// (encoded at once into the status buffer); it is sent once connected, at most every
    /// `status_min_interval_ms`, so publishing faster than that (or while disconnected) simply
    /// replaces the pending batch. Publish every key that should stay current in each batch; the
    /// node keeps each value for its TTL.
    ///
    /// # Errors
    ///
    /// [`PublishError::TooLarge`] if the frame exceeds `TX` or the negotiated frame size (the
    /// pending batch is kept).
    pub fn publish_status<S: StatusBody>(&mut self, entries: &[S]) -> Result<(), PublishError> {
        if self.core.publish_status(&entries)? {
            self.cursor = T::Cursor::default();
        }
        Ok(())
    }

    /// Whether a status batch is waiting to be sent.
    pub fn status_pending(&self) -> bool {
        self.core.status_pending()
    }

    /// The current lease set (empty when not connected or before the host sent one). Strings
    /// borrow from the session.
    pub fn leases(&self) -> Leases<'_> {
        self.core.leases()
    }

    /// Connection state.
    pub fn link_state(&self) -> LinkState {
        self.core.link_state()
    }

    /// Whether a session is established.
    pub fn is_connected(&self) -> bool {
        self.link_state() == LinkState::Connected
    }

    /// The host node, while connected (`None` if its id is longer than [`NODE_ID_CAPACITY`]).
    pub fn node_id(&self) -> Option<&str> {
        self.core.node_id()
    }

    /// The current session id, while connected.
    pub fn session_id(&self) -> Option<u32> {
        self.core.session_id()
    }

    /// Largest frame that may currently be sent: the negotiated size while connected, otherwise
    /// `min(RX, TX)`.
    pub fn max_frame(&self) -> usize {
        self.core.max_frame()
    }

    /// Whether the latest snapshot still awaits an acknowledgement.
    pub fn state_pending(&self) -> bool {
        self.core.state_pending()
    }

    /// The configuration.
    pub fn config(&self) -> &DeviceConfig<N> {
        self.core.config()
    }

    /// Session counters.
    pub fn stats(&self) -> DeviceStats {
        self.core.stats()
    }

    /// The receive decoder ([`crate::StreamDecoder`] or [`crate::Reassembler`]), for its stats.
    pub fn decoder(&self) -> &T::Decoder<RX> {
        &self.rx
    }

    /// The transport.
    pub fn transport(&self) -> &T {
        &self.transport
    }
}

#[cfg(feature = "alloc")]
impl<T: Transport, const RX: usize, const TX: usize, N: AsRef<str>> DeviceSession<T, RX, TX, N> {
    /// The current lease set as `LeaseRecord`s (empty when there is none). Allocates.
    pub fn lease_records(&self) -> alloc::vec::Vec<crate::message::LeaseRecord> {
        crate::message::decode_leases(self.core.leases_payload()).unwrap_or_default()
    }
}

impl<const RX: usize, const TX: usize, N: AsRef<str>> DeviceSession<Stream, RX, TX, N> {
    /// Feeds received stream bytes, in chunks of any size (one byte from an RX interrupt is
    /// fine). Corrupt frames are dropped and counted by the decoder. Clock-free.
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

    /// Writes the next bytes to send into `out` and returns how many were written (0 when idle).
    /// Resumable: a frame may span many calls, so any FIFO or DMA chunk size works. Call until it
    /// returns 0, or as often as the UART can take bytes.
    pub fn transmit(&mut self, out: &mut [u8]) -> usize {
        let mut written = 0;
        loop {
            let rest = out.get_mut(written..).unwrap_or_default();
            if rest.is_empty() {
                break;
            }
            let Some(frame) = self.core.next_frame() else {
                break;
            };
            let (n, done) = transport::stream_fill(frame, &mut self.cursor, rest);
            written = written.saturating_add(n);
            if done {
                self.core.frame_sent();
            } else if n == 0 {
                break;
            }
        }
        written
    }
}

impl<const RX: usize, const TX: usize, N: AsRef<str>> DeviceSession<Packet, RX, TX, N> {
    /// Feeds the data of one received CAN frame. The caller filters by identifier (the link's
    /// host→device id) and skips remote frames. Clock-free.
    pub fn receive_segment(&mut self, data: &[u8]) {
        if let Ok(Some(frame)) = self.rx.push(data) {
            self.core.handle_frame(&frame);
        }
    }

    /// The next segment to send without consuming it, for controllers whose transmit can report
    /// "busy": send it, then call [`DeviceSession::commit_segment`] once accepted.
    pub fn peek_segment(&mut self) -> Option<Segment> {
        self.segment(false)
    }

    /// Marks the segment last returned by [`DeviceSession::peek_segment`] as sent.
    pub fn commit_segment(&mut self) {
        let _ = self.segment(true);
    }

    /// The next segment to send (already consumed). Send it as one CAN frame with the link's
    /// device→host identifier.
    pub fn next_segment(&mut self) -> Option<Segment> {
        self.segment(true)
    }

    /// Writes the data of the next segment into `out` and returns its length (0 when idle). Send
    /// it as one CAN frame with the link's device→host identifier. `out` must hold a full segment
    /// (8 bytes for classic CAN, the MTU for CAN FD; 64 always suffices). Equivalent to
    /// [`DeviceSession::next_segment`] without building a [`Segment`] value, which saves a copy
    /// (and the `memcpy` routine) on small cores.
    pub fn transmit_segment(&mut self, out: &mut [u8]) -> usize {
        let mtu = self.transport.mtu;
        let Some(frame) = self.core.next_frame() else {
            return 0;
        };
        match transport::segment_into(frame, mtu, &mut self.cursor, out) {
            Some((len, done)) => {
                if done {
                    self.core.frame_sent();
                }
                len
            }
            None => 0,
        }
    }

    fn segment(&mut self, commit: bool) -> Option<Segment> {
        let mtu = self.transport.mtu;
        let frame = self.core.next_frame()?;
        match transport::segment_step(frame, mtu, &mut self.cursor, commit) {
            Some((segment, done)) => {
                if commit && done {
                    self.core.frame_sent();
                }
                Some(segment)
            }
            None => {
                self.cursor = Default::default();
                self.core.frame_sent();
                None
            }
        }
    }
}

#[cfg(feature = "embedded-can")]
impl<const RX: usize, const TX: usize, N: AsRef<str>> DeviceSession<Packet, RX, TX, N> {
    /// Feeds a received [`embedded_can::Frame`] if it carries `ids.host_to_device`; other
    /// identifiers and remote frames are ignored. Returns whether the frame was for this link.
    pub fn receive_can_frame<F: embedded_can::Frame>(
        &mut self,
        ids: &crate::CanLinkIds,
        frame: &F,
    ) -> bool {
        if frame.is_remote_frame() || !ids.is_host_to_device(frame.id()) {
            return false;
        }
        self.receive_segment(frame.data());
        true
    }

    /// The next segment as an [`embedded_can::Frame`] with `ids.device_to_host`, without consuming
    /// it (see [`DeviceSession::peek_segment`]). `None` when idle, if the identifier is out of
    /// range, or if `F` cannot carry the segment length (a classic frame type with a CAN FD MTU).
    pub fn peek_can_frame<F: embedded_can::Frame>(&mut self, ids: &crate::CanLinkIds) -> Option<F> {
        let id = ids.device_to_host_id()?;
        self.peek_segment()?.to_can_frame(id)
    }
}
