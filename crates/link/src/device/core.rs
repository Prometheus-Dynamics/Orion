//! Transport-independent device state machine: works on whole frames, with fixed buffers only.

use crate::frame::{self, FRAME_OVERHEAD, FrameHeader, FrameView, copy_prefix};
use crate::transport::seq_newer;
use crate::wire::{
    self, Encode, HelloView, Leases, RawVarint, Reader, RejectReason, WelcomeView, kind,
};

use super::events::{DeviceEvent, EventQueue};
use super::{DeviceConfig, DeviceStats, LinkState, NODE_ID_CAPACITY, PublishError};

/// Smallest heartbeat the device accepts from a host, so a bad `Welcome` cannot cause a ping flood.
const MIN_HEARTBEAT_MS: u32 = 10;

/// Longest `Ping` / `Pong` frame: header, a `u64` varint, CRC.
const PING_FRAME_MAX: usize = FRAME_OVERHEAD + 10;

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
    /// Sequence numbers of the first and latest transmission of this snapshot in this session.
    seqs: Option<(u16, u16)>,
    backoff_ms: u32,
    /// Retransmission deadline; `None` sends at once.
    next_at: Option<u32>,
}

pub(crate) struct DeviceCore<N, const RX: usize, const TX: usize> {
    config: DeviceConfig<N>,
    control: [u8; PING_FRAME_MAX],
    snapshot_buf: [u8; TX],
    status_buf: [u8; TX],
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
    stats: DeviceStats,
}

impl<N: AsRef<str>, const RX: usize, const TX: usize> DeviceCore<N, RX, TX> {
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
            control: [0; PING_FRAME_MAX],
            snapshot_buf: [0; TX],
            status_buf: [0; TX],
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
            stats: DeviceStats::default(),
        }
    }

    pub(crate) fn config(&self) -> &DeviceConfig<N> {
        &self.config
    }

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
            self.stats.events_dropped = self.stats.events_dropped.wrapping_add(1);
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
            self.stats.host_timeouts = self.stats.host_timeouts.wrapping_add(1);
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
    }

    /// Leaves the session and sends `Hello` at once.
    fn reconnect(&mut self) {
        self.leave_session();
        self.link = Link::Connecting;
        self.hello_at = None;
        self.hello_backoff_ms = self.config.hello_retry_min_ms.max(1);
    }

    // ---- receive ---------------------------------------------------------------------------

    pub(crate) fn handle_frame(&mut self, frame: &FrameView<'_>) {
        self.stats.frames_received = self.stats.frames_received.wrapping_add(1);
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
            _ => self.on_session_frame(frame),
        }
    }

    fn decode_error(&mut self) {
        self.stats.decode_errors = self.stats.decode_errors.wrapping_add(1);
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
        self.stats.connects = self.stats.connects.wrapping_add(1);
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
                    seqs: None,
                    backoff_ms: self.config.state_retry_min_ms.max(1),
                    next_at: None,
                    ..snapshot
                });
            }
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
        self.stats.rejects = self.stats.rejects.wrapping_add(1);
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

    fn on_session_frame(&mut self, frame: &FrameView<'_>) {
        let Link::Connected(session) = &mut self.link else {
            return;
        };
        if !seq_newer(frame.seq(), session.last_seq) {
            self.stats.duplicates = self.stats.duplicates.wrapping_add(1);
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
            kind::PING => match self.pong.read(&mut Reader::new(payload)) {
                Ok(()) => {}
                Err(_) => self.decode_error(),
            },
            // Pong only proves liveness; unknown and reserved kinds are ignored.
            _ => {}
        }
    }

    fn on_ack(&mut self, acked: u16) {
        let Some(snapshot) = &mut self.snapshot else {
            return;
        };
        let Some((first, latest)) = snapshot.seqs else {
            return;
        };
        let in_range = acked.wrapping_sub(first) <= latest.wrapping_sub(first);
        if snapshot.pending && in_range {
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

    // ---- publish ---------------------------------------------------------------------------

    /// Encodes `body` into the buffer of `source` unless it is too large (then nothing changes).
    /// Returns the payload length and whether a transmission from that buffer was aborted (the
    /// caller must then reset its transmit cursor).
    fn stage(&mut self, source: Source, body: &dyn Encode) -> Result<(usize, bool), PublishError> {
        let frame_len = frame::frame_len(wire::encoded_len(body));
        let max_frame = self.max_frame().min(TX);
        if frame_len > max_frame {
            return Err(PublishError::TooLarge {
                frame_len,
                max_frame,
            });
        }
        let aborted = self.out.is_some_and(|out| out.source == source);
        if aborted {
            self.out = None;
            self.stats.aborted_frames = self.stats.aborted_frames.wrapping_add(1);
        }
        let buf = match source {
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
            seqs: None,
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
            self.stats.status_replaced = self.stats.status_replaced.wrapping_add(1);
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
        };
        buf.get(..out.len)
    }

    /// The current frame has been handed to the transport completely.
    pub(crate) fn frame_sent(&mut self) {
        let Some(out) = self.out.take() else {
            return;
        };
        self.stats.frames_sent = self.stats.frames_sent.wrapping_add(1);
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
            .get_mut(..max_frame.min(PING_FRAME_MAX))
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
        self.select_snapshot()
            .or_else(|| self.select_status(max_frame))
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
                self.stats.encode_errors = self.stats.encode_errors.wrapping_add(1);
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
            self.stats.status_dropped = self.stats.status_dropped.wrapping_add(1);
            return None;
        }
        let seq = self.next_seq();
        let header = FrameHeader::new(kind::STATUS, seq);
        let len = frame::encode_in_place(header, payload_len, &mut self.status_buf).ok()?;
        self.stats.status_sent = self.stats.status_sent.wrapping_add(1);
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
        if snapshot.seqs.is_some() {
            self.stats.state_retransmits = self.stats.state_retransmits.wrapping_add(1);
        }
        let first = snapshot.seqs.map_or(seq, |(first, _)| first);
        self.snapshot = Some(Snapshot {
            seqs: Some((first, seq)),
            ..snapshot
        });
        Some(Outgoing {
            source: Source::State,
            len,
        })
    }
}
