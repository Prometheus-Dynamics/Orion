//! Transport-independent device state machine: works on whole frames, with fixed buffers only.

use crate::frame::{self, FRAME_OVERHEAD, FrameHeader, FrameView, copy_prefix};
use crate::transport::seq_newer;
use crate::wire::{
    self, Encode, FirmwareSink, HelloView, Leases, RawVarint, Reader, RejectReason, WelcomeView,
    kind,
};
#[cfg(feature = "actions")]
use crate::wire::{
    ActionCatalogBody, ActionRequestView, ActionResultBody, ActionState, decode_action_cancel,
};

#[cfg(feature = "stats")]
use super::DeviceStats;
use super::events::{DeviceEvent, EventQueue};
#[cfg(feature = "xfer")]
use super::xfer::{REPLY_FRAME_MAX, XferState};
use super::{DeviceConfig, LinkState, NODE_ID_CAPACITY, PublishError};

/// Counts one event in [`DeviceStats`]. Compiles to nothing without the `stats` feature.
macro_rules! bump {
    ($core:ident, $field:ident) => {
        #[cfg(feature = "stats")]
        {
            $core.stats.$field = $core.stats.$field.wrapping_add(1);
        }
    };
}

/// Smallest heartbeat the device accepts from a host, so a bad `Welcome` cannot cause a ping flood.
const MIN_HEARTBEAT_MS: u32 = 10;

/// Longest `Ping` / `Pong` frame: header, a `u64` varint, CRC.
const PING_FRAME_MAX: usize = FRAME_OVERHEAD + 10;

/// Reply length the transfer code may need in the control buffer (feature `xfer`).
#[cfg(feature = "xfer")]
const XFER_REPLY_MAX: usize = REPLY_FRAME_MAX;
#[cfg(not(feature = "xfer"))]
const XFER_REPLY_MAX: usize = 0;

/// Size of the control buffer: pings, pongs, a busy refusal with `actions` (an `ActionResult` with
/// a 4-byte reason and a `u32` id), and transfer replies with `xfer`.
#[cfg(feature = "actions")]
const ACTION_CONTROL_MAX: usize = PING_FRAME_MAX + 1;
#[cfg(not(feature = "actions"))]
const ACTION_CONTROL_MAX: usize = PING_FRAME_MAX;
const CONTROL_MAX: usize = if ACTION_CONTROL_MAX > XFER_REPLY_MAX {
    ACTION_CONTROL_MAX
} else {
    XFER_REPLY_MAX
};

/// Reason sent with the refusal of a request that arrives while another one is running.
#[cfg(feature = "actions")]
const BUSY_REASON: &[u8] = b"busy";

/// Whether the millisecond deadline `at` has been reached at `now`.
///
/// Time is kept as a wrapping `u32` inside the session (the `u64` clock passed to `poll` is
/// truncated): every interval the session schedules is far below the 24-day half range, and 32-bit
/// arithmetic is a fraction of the code size of 64-bit arithmetic on small cores.
fn reached(now: u32, at: u32) -> bool {
    now.wrapping_sub(at) < 1 << 31
}

/// `a * b`, saturating, by shift-and-add: `u32::saturating_mul` needs a 64-bit multiply routine
/// on cores without a widening multiply (Cortex-M0/M0+), which costs more flash than this.
fn saturating_mul(mut a: u32, mut b: u32) -> u32 {
    let mut product = 0u32;
    while b != 0 {
        if b & 1 != 0 {
            product = product.saturating_add(a);
        }
        a = a.saturating_add(a);
        b >>= 1;
    }
    product
}

/// Which buffer a frame lives in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Source {
    /// `Ping`, `Pong`: encoded into the small control buffer when selected.
    Control,
    /// `Hello`: encoded into the lease buffer when selected. `Hello` is only sent outside a
    /// session, when there is no lease set to keep, so the two never need the buffer at once (a
    /// `Leases` frame arriving while a `Hello` is still being sent is dropped and repaired by the
    /// host's next lease resend, as if it had been lost).
    Hello,
    /// The latest provider snapshot (payload kept for retransmission).
    State,
    /// The newest unsent status batch.
    Status,
    /// An `ActionResult` for the device's action (encoded in the action buffer).
    #[cfg(feature = "actions")]
    Action,
    /// The `DeviceInfo` report (its payload kept for retransmission).
    #[cfg(feature = "xfer")]
    Info,
    /// The action catalog (its payload kept for retransmission).
    #[cfg(feature = "actions")]
    Catalog,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Outgoing {
    source: Source,
    len: usize,
}

#[derive(Debug, Clone, Copy)]
struct Session {
    session_id: u32,
    heartbeat_ms: u32,
    /// `missed_heartbeats * heartbeat_ms`: host silence after which the session is lost.
    timeout_ms: u32,
    max_frame: usize,
    /// Newest host sequence number accepted.
    last_seq: u16,
    next_ping_at: u32,
    /// Length of the node id in `DeviceCore::node_id`, if it fit.
    node_id_len: Option<usize>,
}

#[derive(Debug, Clone, Copy)]
enum Link {
    /// Sending `Hello` with backoff until a `Welcome` arrives.
    Connecting,
    /// Rejected; silent until `hello_at`.
    Backoff,
    Connected(Session),
}

/// The latest published snapshot, kept encoded (payload only) in the snapshot buffer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Snapshot {
    payload_len: usize,
    /// Not yet acknowledged in the current session.
    pending: bool,
    /// Sequence number of the latest transmission in this session (`None` before the first). Only an
    /// `Ack` for exactly this number acknowledges the slot: sequence numbers are unique per frame,
    /// so an ack for an earlier transmission, or for another stream, never matches (docs/link-protocol.md,
    /// "Reliability").
    latest: Option<u16>,
    backoff_ms: u32,
    /// Retransmission deadline; `None` sends at once.
    next_at: Option<u32>,
}

/// The action the device is running (or has finished, until its final result is sent).
#[cfg(feature = "actions")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ActionSlot {
    id: u32,
    /// Lengths of the name and payload stored at the start of the action buffer.
    name_len: usize,
    payload_len: usize,
    /// A result was reported, so the request bytes are gone (see `DeviceCore::action`).
    reported: bool,
    /// A final result was reported; the slot is freed once that result is sent.
    finished: bool,
}

/// `ST` is the status buffer size: 0 for a device that never publishes status.
pub(crate) struct DeviceCore<N, const RX: usize, const TX: usize, const ST: usize> {
    config: DeviceConfig<N>,
    control: [u8; CONTROL_MAX],
    snapshot_buf: [u8; TX],
    status_buf: [u8; ST],
    /// Payload of the last reported lease set of this session; outside a session, the `Hello`.
    leases_buf: [u8; RX],
    leases_len: Option<usize>,
    node_id: [u8; NODE_ID_CAPACITY],
    snapshot: Option<Snapshot>,
    /// Payload length of the newest unsent status batch (fire-and-forget, newest wins).
    status_len: Option<usize>,
    out: Option<Outgoing>,
    link: Link,
    seq: u16,
    now: u32,
    heard: bool,
    last_heard: u32,
    /// When the next `Hello` is due (`None`: at the next `poll`).
    hello_at: Option<u32>,
    hello_backoff_ms: u32,
    reject_backoff_ms: u32,
    hello_due: bool,
    ping_due: bool,
    /// The `now_ms` of a host `Ping` to echo, verbatim (empty: no `Pong` due).
    pong: RawVarint,
    /// Earliest time for the next `Status` frame (`None`: any time).
    next_status_at: Option<u32>,
    events: EventQueue,
    /// The one action in flight.
    #[cfg(feature = "actions")]
    action_slot: Option<ActionSlot>,
    /// Holds the request bytes (name, then payload), then the encoded result being sent.
    #[cfg(feature = "actions")]
    action_buf: [u8; RX],
    /// Body length and finality of the result waiting in `action_buf`.
    #[cfg(feature = "actions")]
    action_out: Option<(usize, bool)>,
    /// The result being transmitted is final.
    #[cfg(feature = "actions")]
    out_final: bool,
    /// A request id to refuse (the device was busy).
    #[cfg(feature = "actions")]
    refusal: Option<u32>,
    /// The declared action names, encoded (payload only) in `catalog_buf`, and their reliable
    /// transmission state. `None` for a device that declares nothing.
    #[cfg(feature = "actions")]
    catalog: Option<Snapshot>,
    /// Holds the encoded `ActionCatalog` payload, and then its frame while it is sent.
    #[cfg(feature = "actions")]
    catalog_buf: [u8; RX],
    #[cfg(feature = "xfer")]
    xfer: XferState,
    #[cfg(feature = "stats")]
    stats: DeviceStats,
}

impl<N: AsRef<str>, const RX: usize, const TX: usize, const ST: usize> DeviceCore<N, RX, TX, ST> {
    /// Largest frame this device can receive and send.
    const LOCAL_MAX: usize = if RX < TX { RX } else { TX };

    // Inlined so a session can be built directly in its final place (a static) instead of on the
    // stack and then copied, which matters on MCUs with a few KiB of RAM.
    #[inline(always)]
    pub(crate) fn new(config: DeviceConfig<N>) -> Self {
        let hello_backoff_ms = config.hello_retry_min_ms.max(1);
        let reject_backoff_ms = config.reject_retry_min_ms.max(1);
        Self {
            config,
            control: [0; CONTROL_MAX],
            snapshot_buf: [0; TX],
            status_buf: [0; ST],
            leases_buf: [0; RX],
            leases_len: None,
            node_id: [0; NODE_ID_CAPACITY],
            snapshot: None,
            status_len: None,
            out: None,
            link: Link::Connecting,
            seq: 0,
            now: 0,
            heard: false,
            last_heard: 0,
            hello_at: None,
            hello_backoff_ms,
            reject_backoff_ms,
            hello_due: false,
            ping_due: false,
            pong: RawVarint::EMPTY,
            next_status_at: None,
            events: EventQueue::new(),
            #[cfg(feature = "actions")]
            action_slot: None,
            #[cfg(feature = "actions")]
            action_buf: [0; RX],
            #[cfg(feature = "actions")]
            action_out: None,
            #[cfg(feature = "actions")]
            out_final: false,
            #[cfg(feature = "actions")]
            refusal: None,
            #[cfg(feature = "actions")]
            catalog: None,
            #[cfg(feature = "actions")]
            catalog_buf: [0; RX],
            #[cfg(feature = "xfer")]
            xfer: XferState::new(),
            #[cfg(feature = "stats")]
            stats: DeviceStats::default(),
        }
    }

    pub(crate) fn config(&self) -> &DeviceConfig<N> {
        &self.config
    }

    #[cfg(feature = "stats")]
    pub(crate) fn stats(&self) -> DeviceStats {
        self.stats
    }

    pub(crate) fn link_state(&self) -> LinkState {
        match self.link {
            Link::Connecting => LinkState::Connecting,
            Link::Backoff => LinkState::BackingOff,
            Link::Connected(_) => LinkState::Connected,
        }
    }

    fn session(&self) -> Option<&Session> {
        match &self.link {
            Link::Connected(session) => Some(session),
            _ => None,
        }
    }

    pub(crate) fn node_id(&self) -> Option<&str> {
        let len = self.session()?.node_id_len?;
        wire::str_from_utf8(self.node_id.get(..len)?).ok()
    }

    pub(crate) fn session_id(&self) -> Option<u32> {
        self.session().map(|session| session.session_id)
    }

    pub(crate) fn max_frame(&self) -> usize {
        self.session()
            .map_or(Self::LOCAL_MAX, |session| session.max_frame)
    }

    pub(crate) fn state_pending(&self) -> bool {
        self.snapshot.is_some_and(|snapshot| snapshot.pending)
    }

    pub(crate) fn status_pending(&self) -> bool {
        self.status_len.is_some()
    }

    pub(crate) fn next_event(&mut self) -> Option<DeviceEvent> {
        self.events.pop()
    }

    /// The payload of the current lease set (empty when there is none).
    pub(crate) fn leases_payload(&self) -> &[u8] {
        self.leases_len
            .and_then(|len| self.leases_buf.get(..len))
            .unwrap_or_default()
    }

    pub(crate) fn leases(&self) -> Leases<'_> {
        match self.leases_len {
            Some(_) => Leases::decode(self.leases_payload()).unwrap_or(Leases::empty()),
            None => Leases::empty(),
        }
    }

    fn emit(&mut self, event: DeviceEvent) {
        if !self.events.push(event) {
            bump!(self, events_dropped);
        }
    }

    fn next_seq(&mut self) -> u16 {
        self.seq = self.seq.wrapping_add(1);
        self.seq
    }

    // ---- timers ----------------------------------------------------------------------------

    pub(crate) fn poll(&mut self, now: u64) {
        let now = now as u32;
        self.now = now;
        if self.heard {
            self.heard = false;
            self.last_heard = now;
        }
        let mut lost = false;
        if let Link::Connected(session) = &mut self.link {
            if now.wrapping_sub(self.last_heard) >= session.timeout_ms {
                lost = true;
            } else if reached(now, session.next_ping_at) {
                self.ping_due = true;
                session.next_ping_at = now.wrapping_add(session.heartbeat_ms);
            }
        }
        if lost {
            bump!(self, host_timeouts);
            self.reconnect();
        }
        let hello_due = self.hello_at.is_none_or(|at| reached(now, at));
        if matches!(self.link, Link::Backoff) && hello_due {
            self.link = Link::Connecting;
        }
        if matches!(self.link, Link::Connecting) && hello_due {
            self.hello_due = true;
            self.hello_at = Some(now.wrapping_add(self.hello_backoff_ms));
            self.hello_backoff_ms = self
                .hello_backoff_ms
                .saturating_mul(2)
                .min(self.config.hello_retry_max_ms.max(1));
        }
    }

    /// Ends the current session, if any: emits `Disconnected` and forgets its lease set.
    fn leave_session(&mut self) {
        if matches!(self.link, Link::Connected(_)) {
            self.emit(DeviceEvent::Disconnected);
        }
        self.leases_len = None;
        self.ping_due = false;
        self.pong.clear();
        #[cfg(feature = "xfer")]
        self.xfer.on_connected();
    }

    /// Leaves the session and sends `Hello` at once.
    fn reconnect(&mut self) {
        self.leave_session();
        self.link = Link::Connecting;
        self.hello_at = None;
        self.hello_backoff_ms = self.config.hello_retry_min_ms.max(1);
    }

    // ---- receive ---------------------------------------------------------------------------

    pub(crate) fn handle_frame<F: FirmwareSink>(&mut self, frame: &FrameView<'_>, fw: &mut F) {
        bump!(self, frames_received);
        if !frame.header().is_current_version() {
            // REJECT is frozen across versions, so a host speaking another version can still
            // tell us why it refuses. Everything else from another version is ignored.
            if frame.kind() == kind::REJECT {
                let reason =
                    wire::decode_reject(frame.payload()).unwrap_or(RejectReason::VersionMismatch);
                self.on_reject(reason);
            }
            return;
        }
        match frame.kind() {
            kind::WELCOME => match WelcomeView::decode(frame.payload()) {
                Ok(welcome) => self.on_welcome(frame.seq(), &welcome),
                Err(_) => self.decode_error(),
            },
            kind::REJECT => match wire::decode_reject(frame.payload()) {
                Ok(reason) => self.on_reject(reason),
                Err(_) => self.decode_error(),
            },
            _ => self.on_session_frame(frame, fw),
        }
    }

    fn decode_error(&mut self) {
        bump!(self, decode_errors);
    }

    fn on_welcome(&mut self, seq: u16, welcome: &WelcomeView<'_>) {
        self.heard = true;
        if self
            .session()
            .is_some_and(|session| session.session_id == welcome.session_id)
        {
            // Duplicate Welcome (answer to a repeated Hello).
            return;
        }
        self.leave_session();
        let max_frame = usize::try_from(welcome.max_frame)
            .unwrap_or(usize::MAX)
            .clamp(FRAME_OVERHEAD, Self::LOCAL_MAX.max(FRAME_OVERHEAD));
        let heartbeat_ms = welcome.heartbeat_ms.max(MIN_HEARTBEAT_MS);
        let timeout_ms = saturating_mul(heartbeat_ms, self.config.missed_heartbeats.max(1));
        let node_id = welcome.node_id.as_bytes();
        let node_id_len = self.node_id.get_mut(..node_id.len()).map(|dst| {
            copy_prefix(dst, node_id);
            node_id.len()
        });
        self.link = Link::Connected(Session {
            session_id: welcome.session_id,
            heartbeat_ms,
            timeout_ms,
            max_frame,
            last_seq: seq,
            next_ping_at: self.now.wrapping_add(heartbeat_ms),
            node_id_len,
        });
        bump!(self, connects);
        #[cfg(feature = "xfer")]
        self.xfer.request_info();
        self.hello_due = false;
        self.hello_backoff_ms = self.config.hello_retry_min_ms.max(1);
        self.reject_backoff_ms = self.config.reject_retry_min_ms.max(1);
        self.emit(DeviceEvent::Connected {
            session_id: welcome.session_id,
        });
        // A new session must learn the latest snapshot even if an earlier session acked it.
        if let Some(snapshot) = self.snapshot {
            let frame_len = frame::frame_len(snapshot.payload_len);
            if frame_len > max_frame {
                self.snapshot = None;
                self.emit(DeviceEvent::StateTooLarge {
                    frame_len,
                    max_frame,
                });
            } else {
                self.snapshot = Some(Snapshot {
                    pending: true,
                    latest: None,
                    backoff_ms: self.config.state_retry_min_ms.max(1),
                    next_at: None,
                    ..snapshot
                });
            }
        }
        // The host has not seen the declared names in this session either.
        #[cfg(feature = "actions")]
        if let Some(catalog) = self.catalog {
            self.catalog = Some(Snapshot {
                pending: true,
                latest: None,
                backoff_ms: self.config.state_retry_min_ms.max(1),
                next_at: None,
                ..catalog
            });
        }
    }

    fn on_reject(&mut self, reason: RejectReason) {
        if reason == RejectReason::NoSession {
            // The host lost our session (for example it restarted): reconnect right away.
            if matches!(self.link, Link::Connected(_)) {
                self.reconnect();
            }
            return;
        }
        if matches!(self.link, Link::Backoff) {
            return; // duplicate
        }
        self.leave_session();
        bump!(self, rejects);
        self.emit(DeviceEvent::Rejected(reason));
        self.link = Link::Backoff;
        self.hello_due = false;
        self.hello_at = Some(self.now.wrapping_add(self.reject_backoff_ms));
        self.reject_backoff_ms = self
            .reject_backoff_ms
            .saturating_mul(2)
            .min(self.config.reject_retry_max_ms.max(1));
        self.hello_backoff_ms = self.config.hello_retry_min_ms.max(1);
    }

    #[cfg_attr(not(feature = "xfer"), allow(unused_variables))]
    fn on_session_frame<F: FirmwareSink>(&mut self, frame: &FrameView<'_>, fw: &mut F) {
        let Link::Connected(session) = &mut self.link else {
            return;
        };
        if !seq_newer(frame.seq(), session.last_seq) {
            bump!(self, duplicates);
            return;
        }
        session.last_seq = frame.seq();
        self.heard = true;
        let payload = frame.payload();
        match frame.kind() {
            kind::ACK => match wire::decode_ack(payload) {
                Ok(seq) => self.on_ack(seq),
                Err(_) => self.decode_error(),
            },
            kind::LEASES => self.on_leases(payload),
            #[cfg(feature = "actions")]
            kind::ACTION_REQUEST => self.on_action_request(payload),
            #[cfg(feature = "actions")]
            kind::ACTION_CANCEL => self.on_action_cancel(payload),
            kind::PING => match self.pong.read(&mut Reader::new(payload)) {
                Ok(()) => {}
                Err(_) => self.decode_error(),
            },
            #[cfg(feature = "xfer")]
            kind::XFER_BEGIN
            | kind::XFER_CHUNK
            | kind::XFER_END
            | kind::ACTIVATE
            | kind::CONFIRM
            | kind::XFER_ABORT => self.on_xfer(frame, fw),
            // Pong only proves liveness; unknown and reserved kinds are ignored.
            _ => {}
        }
    }

    /// Host → device firmware transfer frames (feature `xfer`; otherwise they are unknown kinds).
    #[cfg(feature = "xfer")]
    fn on_xfer<F: FirmwareSink>(&mut self, frame: &FrameView<'_>, fw: &mut F) {
        if !self.xfer.on_frame(frame.kind(), frame.payload(), fw) {
            self.decode_error();
        }
    }

    fn on_ack(&mut self, acked: u16) {
        #[cfg(feature = "xfer")]
        self.xfer.on_ack(acked);
        // Every reliable stream (catalog, snapshot) acknowledges only its latest transmission.
        #[cfg(feature = "actions")]
        if let Some(catalog) = &mut self.catalog
            && catalog.pending
            && catalog.latest == Some(acked)
        {
            catalog.pending = false;
        }
        if let Some(snapshot) = &mut self.snapshot
            && snapshot.pending
            && snapshot.latest == Some(acked)
        {
            snapshot.pending = false;
            self.emit(DeviceEvent::StateAcked);
        }
    }

    fn on_leases(&mut self, payload: &[u8]) {
        let hello_in_flight = self.out.is_some_and(|out| out.source == Source::Hello);
        if hello_in_flight || (self.leases_len.is_some() && self.leases_payload() == payload) {
            return;
        }
        // A frame never exceeds `RX`, so its payload always fits the lease buffer.
        let Some(dst) = self.leases_buf.get_mut(..payload.len()) else {
            self.decode_error();
            return;
        };
        if Leases::decode(payload).is_err() {
            self.decode_error();
            return;
        }
        copy_prefix(dst, payload);
        self.leases_len = Some(payload.len());
        self.emit(DeviceEvent::LeasesChanged);
    }

    // ---- actions ---------------------------------------------------------------------------

    /// Accepts one request (one action in flight). A repeat of the running request is ignored;
    /// any other request while the device is busy is refused with `busy`.
    #[cfg(feature = "actions")]
    fn on_action_request(&mut self, payload: &[u8]) {
        let request = match ActionRequestView::decode(payload) {
            Ok(request) => request,
            Err(_) => {
                self.decode_error();
                return;
            }
        };
        if let Some(slot) = self.action_slot {
            if slot.id != request.id {
                self.refusal = Some(request.id);
            }
            return;
        }
        let name_len = request.name.len();
        let total = name_len.saturating_add(request.payload.len());
        let Some(buf) = self.action_buf.get_mut(..total) else {
            self.refusal = Some(request.id);
            return;
        };
        copy_prefix(buf, request.name.as_bytes());
        if let Some(rest) = buf.get_mut(name_len..) {
            copy_prefix(rest, request.payload);
        }
        self.action_slot = Some(ActionSlot {
            id: request.id,
            name_len,
            payload_len: request.payload.len(),
            reported: false,
            finished: false,
        });
        self.emit(DeviceEvent::ActionRequested);
    }

    /// Host's `ActionCancel`. Accepted while the device is busy: a cancel for the running action
    /// (not yet final) queues `ActionCancelRequested` for the application; any other id is ignored.
    #[cfg(feature = "actions")]
    fn on_action_cancel(&mut self, payload: &[u8]) {
        let Ok(id) = decode_action_cancel(payload) else {
            self.decode_error();
            return;
        };
        if self
            .action_slot
            .is_some_and(|slot| slot.id == id && !slot.finished)
        {
            self.emit(DeviceEvent::ActionCancelRequested { id });
        }
    }

    /// Replaces the declared action names (see `DeviceSession::declare_actions`). Returns whether a
    /// transmission of the previous catalog was aborted.
    #[cfg(feature = "actions")]
    pub(crate) fn declare_actions(&mut self, names: &[&str]) -> Result<bool, PublishError> {
        let body = ActionCatalogBody { names };
        let frame_len = frame::frame_len(wire::encoded_len(&body));
        if !names.is_empty() && frame_len > Self::LOCAL_MAX {
            return Err(PublishError::TooLarge {
                frame_len,
                max_frame: Self::LOCAL_MAX,
            });
        }
        let aborted = self.out.is_some_and(|out| out.source == Source::Catalog);
        if aborted {
            self.out = None;
            bump!(self, aborted_frames);
        }
        if names.is_empty() {
            self.catalog = None;
            return Ok(aborted);
        }
        let payload_len = wire::write_payload(&body, &mut self.catalog_buf);
        self.catalog = Some(Snapshot {
            payload_len,
            pending: true,
            latest: None,
            backoff_ms: self.config.state_retry_min_ms.max(1),
            next_at: None,
        });
        Ok(aborted)
    }

    /// The request the device should run. Available until the first report: a report overwrites
    /// the request bytes.
    #[cfg(feature = "actions")]
    pub(crate) fn action(&self) -> Option<ActionRequestView<'_>> {
        let slot = self.action_slot?;
        if slot.reported {
            return None;
        }
        let end = slot.name_len.checked_add(slot.payload_len)?;
        let name = wire::str_from_utf8(self.action_buf.get(..slot.name_len)?).ok()?;
        let payload = self.action_buf.get(slot.name_len..end)?;
        Some(ActionRequestView {
            id: slot.id,
            name,
            payload,
        })
    }

    /// Queues a result for the running action. Returns whether a result being transmitted was
    /// aborted (the caller then resets its transmit cursor).
    #[cfg(feature = "actions")]
    pub(crate) fn report_action(
        &mut self,
        state: ActionState,
        payload: &[u8],
    ) -> Result<bool, super::ActionReportError> {
        use super::ActionReportError;
        let slot = self.action_slot.ok_or(ActionReportError::NoAction)?;
        if slot.finished {
            return Err(ActionReportError::Finished);
        }
        let body = ActionResultBody {
            id: slot.id,
            state,
            payload,
        };
        let frame_len = frame::frame_len(wire::encoded_len(&body));
        let max_frame = self.max_frame().min(Self::LOCAL_MAX);
        if frame_len > max_frame {
            return Err(ActionReportError::TooLarge {
                frame_len,
                max_frame,
            });
        }
        let aborted = self.out.is_some_and(|out| out.source == Source::Action);
        if aborted {
            self.out = None;
            bump!(self, aborted_frames);
        }
        let body_len = wire::write_payload(&body, &mut self.action_buf);
        self.action_out = Some((body_len, state.is_final()));
        self.action_slot = Some(ActionSlot {
            reported: true,
            finished: state.is_final(),
            ..slot
        });
        Ok(aborted)
    }

    /// Encodes the queued action result for transmission.
    #[cfg(feature = "actions")]
    fn select_action(&mut self, max_frame: usize) -> Option<Outgoing> {
        let (body_len, is_final) = self.action_out.take()?;
        if frame::frame_len(body_len) > max_frame {
            // Cannot be sent under the negotiated frame size. A final result is dropped, which
            // frees the device for its next request.
            if is_final {
                self.action_slot = None;
            }
            return None;
        }
        let seq = self.next_seq();
        let len = frame::encode_in_place(
            FrameHeader::new(kind::ACTION_RESULT, seq),
            body_len,
            &mut self.action_buf,
        )
        .ok()?;
        self.out_final = is_final;
        Some(Outgoing {
            source: Source::Action,
            len,
        })
    }

    // ---- publish ---------------------------------------------------------------------------

    /// Encodes `body` into the buffer of `source` unless it is too large (then nothing changes).
    /// Returns the payload length and whether a transmission from that buffer was aborted (the
    /// caller must then reset its transmit cursor).
    fn stage(&mut self, source: Source, body: &dyn Encode) -> Result<(usize, bool), PublishError> {
        let frame_len = frame::frame_len(wire::encoded_len(body));
        let capacity = match source {
            Source::Status => ST,
            _ => TX,
        };
        let max_frame = self.max_frame().min(capacity);
        if frame_len > max_frame {
            return Err(PublishError::TooLarge {
                frame_len,
                max_frame,
            });
        }
        let aborted = self.out.is_some_and(|out| out.source == source);
        if aborted {
            self.out = None;
            bump!(self, aborted_frames);
        }
        let buf: &mut [u8] = match source {
            Source::Status => &mut self.status_buf,
            _ => &mut self.snapshot_buf,
        };
        Ok((wire::write_payload(body, buf), aborted))
    }

    /// Stores a new snapshot. Returns whether a transmission of the previous one was aborted.
    pub(crate) fn publish(&mut self, body: &dyn Encode) -> Result<bool, PublishError> {
        let (payload_len, aborted) = self.stage(Source::State, body)?;
        self.snapshot = Some(Snapshot {
            payload_len,
            pending: true,
            latest: None,
            backoff_ms: self.config.state_retry_min_ms.max(1),
            next_at: None,
        });
        Ok(aborted)
    }

    /// Stores a status batch, replacing an unsent one. Returns whether a transmission of the
    /// previous batch was aborted.
    pub(crate) fn publish_status(&mut self, body: &dyn Encode) -> Result<bool, PublishError> {
        let (payload_len, aborted) = self.stage(Source::Status, body)?;
        if self.status_len.replace(payload_len).is_some() {
            bump!(self, status_replaced);
        }
        Ok(aborted)
    }

    // ---- transmit --------------------------------------------------------------------------

    /// The frame being transmitted, selecting the next one if idle.
    pub(crate) fn next_frame(&mut self) -> Option<&[u8]> {
        if self.out.is_none() {
            self.out = self.select();
        }
        let out = self.out?;
        let buf: &[u8] = match out.source {
            Source::Control => &self.control,
            Source::Hello => &self.leases_buf,
            Source::State => &self.snapshot_buf,
            Source::Status => &self.status_buf,
            #[cfg(feature = "actions")]
            Source::Action => &self.action_buf,
            #[cfg(feature = "actions")]
            Source::Catalog => &self.catalog_buf,
            #[cfg(feature = "xfer")]
            Source::Info => self.xfer.info_buf(),
        };
        buf.get(..out.len)
    }

    /// The current frame has been handed to the transport completely.
    pub(crate) fn frame_sent(&mut self) {
        let Some(out) = self.out.take() else {
            return;
        };
        bump!(self, frames_sent);
        #[cfg(feature = "actions")]
        if out.source == Source::Action && self.out_final {
            self.action_slot = None;
        }
        if out.source == Source::State {
            let cap = self
                .session()
                .map_or(u32::MAX, |session| session.heartbeat_ms);
            let now = self.now;
            if let Some(snapshot) = &mut self.snapshot {
                snapshot.next_at = Some(now.wrapping_add(snapshot.backoff_ms));
                snapshot.backoff_ms = snapshot.backoff_ms.saturating_mul(2).min(cap.max(1));
            }
        }
    }

    fn select(&mut self) -> Option<Outgoing> {
        let seq = self.seq.wrapping_add(1);
        let max_frame = match &self.link {
            Link::Connected(session) => session.max_frame,
            Link::Connecting if self.hello_due => {
                self.hello_due = false;
                let hello = HelloView {
                    device_name: self.config.device_name.as_ref(),
                    roles: self.config.roles,
                    max_frame: u32::try_from(Self::LOCAL_MAX).unwrap_or(u32::MAX),
                };
                let buf = self
                    .leases_buf
                    .get_mut(..Self::LOCAL_MAX)
                    .unwrap_or_default();
                let result = wire::encode_frame_dyn(kind::HELLO, seq, &hello, buf);
                return self.control_frame(Source::Hello, result);
            }
            _ => return None,
        };
        let control = self
            .control
            .get_mut(..max_frame.min(CONTROL_MAX))
            .unwrap_or_default();
        if !self.pong.is_empty() {
            let result = wire::encode_frame_dyn(kind::PONG, seq, &self.pong, control);
            self.pong.clear();
            return self.control_frame(Source::Control, result);
        }
        if self.ping_due {
            self.ping_due = false;
            // The host only echoes `now_ms`, so the session's 32-bit clock is enough.
            let result = wire::encode_frame_dyn(kind::PING, seq, &self.now, control);
            return self.control_frame(Source::Control, result);
        }
        #[cfg(feature = "actions")]
        if let Some(id) = self.refusal.take() {
            let refusal = ActionResultBody {
                id,
                state: ActionState::Rejected,
                payload: BUSY_REASON,
            };
            let result = wire::encode_frame_dyn(kind::ACTION_RESULT, seq, &refusal, control);
            return self.control_frame(Source::Control, result);
        }
        #[cfg(feature = "actions")]
        if let Some(outgoing) = self.select_action(max_frame) {
            return Some(outgoing);
        }
        #[cfg(feature = "actions")]
        if let Some(outgoing) = self.select_catalog(max_frame) {
            return Some(outgoing);
        }
        #[cfg(feature = "xfer")]
        if let Some(reply) = self.xfer.take_reply() {
            let buf = self
                .control
                .get_mut(..max_frame.min(CONTROL_MAX))
                .unwrap_or_default();
            let result = reply.encode(seq, buf);
            return self.control_frame(Source::Control, result);
        }
        #[cfg(feature = "xfer")]
        if let Some(info) = self.select_info() {
            return Some(info);
        }
        self.select_snapshot()
            .or_else(|| self.select_status(max_frame))
    }

    /// The `ActionCatalog` frame, while the host has not acknowledged its latest transmission.
    /// Retransmitted with doubling intervals up to the heartbeat, like the snapshot.
    #[cfg(feature = "actions")]
    fn select_catalog(&mut self, max_frame: usize) -> Option<Outgoing> {
        let catalog = self.catalog?;
        if !catalog.pending || catalog.next_at.is_some_and(|at| !reached(self.now, at)) {
            return None;
        }
        if frame::frame_len(catalog.payload_len) > max_frame {
            // Declared before a smaller frame size was negotiated: it waits for a session that fits.
            return None;
        }
        let seq = self.next_seq();
        let len = frame::encode_in_place(
            FrameHeader::new(kind::ACTION_CATALOG, seq),
            catalog.payload_len,
            &mut self.catalog_buf,
        )
        .ok()?;
        let cap = self
            .session()
            .map_or(u32::MAX, |session| session.heartbeat_ms);
        self.catalog = Some(Snapshot {
            latest: Some(seq),
            next_at: Some(self.now.wrapping_add(catalog.backoff_ms)),
            backoff_ms: catalog.backoff_ms.saturating_mul(2).min(cap.max(1)),
            ..catalog
        });
        Some(Outgoing {
            source: Source::Catalog,
            len,
        })
    }

    /// The `DeviceInfo` report, when one is due (feature `xfer`).
    #[cfg(feature = "xfer")]
    fn select_info(&mut self) -> Option<Outgoing> {
        let next_at = self.xfer.info_next_at()?;
        if next_at.is_some_and(|at| !reached(self.now, at)) {
            return None;
        }
        let cap = self
            .session()
            .map_or(u32::MAX, |session| session.heartbeat_ms);
        let seq = self.next_seq();
        let len = self.xfer.info_frame(seq, self.now, cap)?;
        Some(Outgoing {
            source: Source::Info,
            len,
        })
    }

    /// Whether the firmware state must be encoded again before the next transmission.
    #[cfg(feature = "xfer")]
    pub(crate) fn info_due(&self) -> bool {
        self.xfer.info_due() && self.session().is_some()
    }

    /// Encodes the firmware state as the pending `DeviceInfo` (see [`Self::info_due`]).
    #[cfg(feature = "xfer")]
    pub(crate) fn encode_info(
        &mut self,
        info: &crate::wire::FirmwareInfo<'_>,
        rollback: Option<crate::wire::TrialRollback<'_>>,
    ) {
        let max_frame = self.max_frame();
        if !self.xfer.encode_info(
            info,
            rollback,
            RX,
            max_frame,
            self.config.state_retry_min_ms,
        ) {
            bump!(self, encode_errors);
        }
    }

    /// Requests a fresh `DeviceInfo` (the firmware state changed).
    #[cfg(feature = "xfer")]
    pub(crate) fn firmware_changed(&mut self) {
        self.xfer.request_info();
    }

    fn control_frame(
        &mut self,
        source: Source,
        result: Result<usize, wire::WireError>,
    ) -> Option<Outgoing> {
        match result {
            Ok(len) => {
                let _ = self.next_seq();
                Some(Outgoing { source, len })
            }
            Err(_) => {
                bump!(self, encode_errors);
                None
            }
        }
    }

    fn select_status(&mut self, max_frame: usize) -> Option<Outgoing> {
        if self.next_status_at.is_some_and(|at| !reached(self.now, at)) {
            return None;
        }
        let payload_len = self.status_len.take()?;
        if frame::frame_len(payload_len) > max_frame {
            // Published before a smaller frame size was negotiated.
            bump!(self, status_dropped);
            return None;
        }
        let seq = self.next_seq();
        let header = FrameHeader::new(kind::STATUS, seq);
        let len = frame::encode_in_place(header, payload_len, &mut self.status_buf).ok()?;
        bump!(self, status_sent);
        self.next_status_at = Some(self.now.wrapping_add(self.config.status_min_interval_ms));
        Some(Outgoing {
            source: Source::Status,
            len,
        })
    }

    fn select_snapshot(&mut self) -> Option<Outgoing> {
        let snapshot = self.snapshot?;
        if !snapshot.pending || snapshot.next_at.is_some_and(|at| !reached(self.now, at)) {
            return None;
        }
        let seq = self.next_seq();
        let len = frame::encode_in_place(
            FrameHeader::new(kind::PROVIDER_STATE, seq),
            snapshot.payload_len,
            &mut self.snapshot_buf,
        )
        .ok()?;
        if snapshot.latest.is_some() {
            bump!(self, state_retransmits);
        }
        self.snapshot = Some(Snapshot {
            latest: Some(seq),
            ..snapshot
        });
        Some(Outgoing {
            source: Source::State,
            len,
        })
    }
}
