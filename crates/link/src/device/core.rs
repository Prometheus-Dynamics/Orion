//! Transport-independent device state machine: works on whole frames.

use crate::crc::crc32c;
use crate::frame::{self, FRAME_OVERHEAD, FrameHeader, FrameView};
use crate::message::{
    self, HelloRef, NodeId, ProviderRecord, RejectReason, ResourceRecord, Welcome, kind,
};
use crate::transport::seq_newer;

use super::events::{DeviceEvent, EventQueue};
use super::{DeviceConfig, DeviceStats, LinkState, PublishError};

/// Smallest heartbeat the device accepts from a host, so a bad `Welcome` cannot cause a ping flood.
const MIN_HEARTBEAT_MS: u32 = 10;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Source {
    /// The frame lives in the control buffer.
    Control,
    /// The frame lives in the snapshot buffer.
    State,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Outgoing {
    source: Source,
    len: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Session {
    node_id: NodeId,
    session_id: u32,
    heartbeat_ms: u32,
    max_frame: usize,
    /// Newest host sequence number accepted.
    last_seq: u16,
    next_ping_at: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Link {
    /// Sending `Hello` with backoff until a `Welcome` arrives.
    Connecting,
    /// Rejected; silent until `next_hello_at`.
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
    next_at: u64,
}

pub(crate) struct DeviceCore<const TX: usize> {
    config: DeviceConfig,
    /// Largest frame this device can receive and send: `min(RX, TX)`.
    local_max: usize,
    control: [u8; TX],
    snapshot_buf: [u8; TX],
    snapshot: Option<Snapshot>,
    out: Option<Outgoing>,
    link: Link,
    seq: u16,
    now: u64,
    heard: bool,
    last_heard: u64,
    next_hello_at: u64,
    hello_backoff_ms: u32,
    reject_backoff_ms: u32,
    hello_due: bool,
    ping_due: bool,
    pong_due: Option<u64>,
    /// `(len, crc32c)` of the last delivered lease payload, to report each set once.
    leases_digest: Option<(usize, u32)>,
    events: EventQueue,
    stats: DeviceStats,
}

impl<const TX: usize> DeviceCore<TX> {
    pub(crate) fn new(config: DeviceConfig, rx_capacity: usize) -> Self {
        let hello_backoff_ms = config.hello_retry_min_ms.max(1);
        let reject_backoff_ms = config.reject_retry_min_ms.max(1);
        Self {
            config,
            local_max: rx_capacity.min(TX),
            control: [0; TX],
            snapshot_buf: [0; TX],
            snapshot: None,
            out: None,
            link: Link::Connecting,
            seq: 0,
            now: 0,
            heard: false,
            last_heard: 0,
            next_hello_at: 0,
            hello_backoff_ms,
            reject_backoff_ms,
            hello_due: false,
            ping_due: false,
            pong_due: None,
            leases_digest: None,
            events: EventQueue::new(),
            stats: DeviceStats::default(),
        }
    }

    pub(crate) fn config(&self) -> &DeviceConfig {
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

    pub(crate) fn node_id(&self) -> Option<&NodeId> {
        match &self.link {
            Link::Connected(session) => Some(&session.node_id),
            _ => None,
        }
    }

    pub(crate) fn session_id(&self) -> Option<u32> {
        match &self.link {
            Link::Connected(session) => Some(session.session_id),
            _ => None,
        }
    }

    pub(crate) fn max_frame(&self) -> usize {
        match &self.link {
            Link::Connected(session) => session.max_frame,
            _ => self.local_max,
        }
    }

    pub(crate) fn state_pending(&self) -> bool {
        self.snapshot.is_some_and(|snapshot| snapshot.pending)
    }

    pub(crate) fn next_event(&mut self) -> Option<DeviceEvent> {
        self.events.pop()
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
        self.now = now;
        if self.heard {
            self.heard = false;
            self.last_heard = now;
        }
        let missed = u64::from(self.config.missed_heartbeats.max(1));
        let mut lost = false;
        if let Link::Connected(session) = &mut self.link {
            let timeout = u64::from(session.heartbeat_ms).saturating_mul(missed);
            if now.saturating_sub(self.last_heard) >= timeout {
                lost = true;
            } else if now >= session.next_ping_at {
                self.ping_due = true;
                session.next_ping_at = now.saturating_add(u64::from(session.heartbeat_ms));
            }
        }
        if lost {
            self.stats.host_timeouts = self.stats.host_timeouts.wrapping_add(1);
            self.reconnect();
        }
        if self.link == Link::Backoff && now >= self.next_hello_at {
            self.link = Link::Connecting;
        }
        if self.link == Link::Connecting && now >= self.next_hello_at {
            self.hello_due = true;
            self.next_hello_at = now.saturating_add(u64::from(self.hello_backoff_ms));
            self.hello_backoff_ms = self
                .hello_backoff_ms
                .saturating_mul(2)
                .min(self.config.hello_retry_max_ms.max(1));
        }
    }

    /// Leaves the session (emitting `Disconnected` if there was one) and sends `Hello` at once.
    fn reconnect(&mut self) {
        if matches!(self.link, Link::Connected(_)) {
            self.emit(DeviceEvent::Disconnected);
        }
        self.link = Link::Connecting;
        self.next_hello_at = self.now;
        self.hello_backoff_ms = self.config.hello_retry_min_ms.max(1);
        self.ping_due = false;
        self.pong_due = None;
    }

    // ---- receive ---------------------------------------------------------------------------

    pub(crate) fn handle_frame(&mut self, frame: &FrameView<'_>) {
        self.stats.frames_received = self.stats.frames_received.wrapping_add(1);
        if !frame.header().is_current_version() {
            // REJECT is frozen across versions, so a host speaking another version can still
            // tell us why it refuses. Everything else from another version is ignored.
            if frame.kind() == kind::REJECT {
                let reason = message::decode_body(kind::REJECT, frame.payload())
                    .unwrap_or(RejectReason::VersionMismatch);
                self.on_reject(reason);
            }
            return;
        }
        match frame.kind() {
            kind::WELCOME => {
                match message::decode_body::<Welcome>(kind::WELCOME, frame.payload()) {
                    Ok(welcome) => self.on_welcome(frame.seq(), welcome),
                    Err(_) => self.decode_error(),
                }
            }
            kind::REJECT => match message::decode_body(kind::REJECT, frame.payload()) {
                Ok(reason) => self.on_reject(reason),
                Err(_) => self.decode_error(),
            },
            _ => self.on_session_frame(frame),
        }
    }

    fn decode_error(&mut self) {
        self.stats.decode_errors = self.stats.decode_errors.wrapping_add(1);
    }

    fn on_welcome(&mut self, seq: u16, welcome: Welcome) {
        self.heard = true;
        if let Link::Connected(session) = &self.link
            && session.session_id == welcome.session_id
        {
            // Duplicate Welcome (answer to a repeated Hello).
            return;
        }
        if matches!(self.link, Link::Connected(_)) {
            self.emit(DeviceEvent::Disconnected);
        }
        let max_frame = usize::try_from(welcome.max_frame)
            .unwrap_or(usize::MAX)
            .clamp(FRAME_OVERHEAD, self.local_max.max(FRAME_OVERHEAD));
        let heartbeat_ms = welcome.heartbeat_ms.max(MIN_HEARTBEAT_MS);
        self.link = Link::Connected(Session {
            node_id: welcome.node_id.clone(),
            session_id: welcome.session_id,
            heartbeat_ms,
            max_frame,
            last_seq: seq,
            next_ping_at: self.now.saturating_add(u64::from(heartbeat_ms)),
        });
        self.stats.connects = self.stats.connects.wrapping_add(1);
        self.hello_due = false;
        self.ping_due = false;
        self.pong_due = None;
        self.hello_backoff_ms = self.config.hello_retry_min_ms.max(1);
        self.reject_backoff_ms = self.config.reject_retry_min_ms.max(1);
        self.leases_digest = None;
        self.emit(DeviceEvent::Connected {
            node_id: welcome.node_id,
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
                    next_at: 0,
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
        if self.link == Link::Backoff {
            return; // duplicate
        }
        if matches!(self.link, Link::Connected(_)) {
            self.emit(DeviceEvent::Disconnected);
        }
        self.stats.rejects = self.stats.rejects.wrapping_add(1);
        self.emit(DeviceEvent::Rejected(reason));
        self.link = Link::Backoff;
        self.hello_due = false;
        self.ping_due = false;
        self.pong_due = None;
        self.next_hello_at = self.now.saturating_add(u64::from(self.reject_backoff_ms));
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
        match frame.kind() {
            kind::ACK => match message::decode_body(kind::ACK, frame.payload()) {
                Ok(seq) => self.on_ack(seq),
                Err(_) => self.decode_error(),
            },
            kind::LEASES => self.on_leases(frame.payload()),
            kind::PING => match message::decode_body::<u64>(kind::PING, frame.payload()) {
                Ok(now_ms) => self.pong_due = Some(now_ms),
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
        let digest = (payload.len(), crc32c(payload));
        if self.leases_digest == Some(digest) {
            return;
        }
        match message::decode_leases(payload) {
            Ok(leases) => {
                self.leases_digest = Some(digest);
                self.emit(DeviceEvent::Leases(leases));
            }
            Err(_) => self.decode_error(),
        }
    }

    // ---- publish ---------------------------------------------------------------------------

    /// Stores a new snapshot. Returns whether a transmission of the previous snapshot was
    /// aborted (the caller must then reset its transmit cursor).
    pub(crate) fn publish(
        &mut self,
        provider: &ProviderRecord,
        resources: &[ResourceRecord],
    ) -> Result<bool, PublishError> {
        let payload_len = message::provider_state_payload_len(provider, resources)
            .map_err(|_| PublishError::Encode)?;
        let frame_len = frame::frame_len(payload_len);
        let max_frame = self.max_frame().min(TX);
        if frame_len > max_frame {
            return Err(PublishError::TooLarge {
                frame_len,
                max_frame,
            });
        }
        let aborted = self.out.is_some_and(|out| out.source == Source::State);
        if aborted {
            self.out = None;
            self.stats.aborted_frames = self.stats.aborted_frames.wrapping_add(1);
        }
        let buf = self.snapshot_buf.get_mut(..frame_len).unwrap_or_default();
        let written = message::write_provider_state_payload(provider, resources, buf)
            .map_err(|_| PublishError::Encode);
        match written {
            Ok(written) => {
                self.snapshot = Some(Snapshot {
                    payload_len: written,
                    pending: true,
                    seqs: None,
                    backoff_ms: self.config.state_retry_min_ms.max(1),
                    next_at: 0,
                });
                Ok(aborted)
            }
            Err(err) => {
                // The buffer was partially overwritten; the old snapshot is gone.
                self.snapshot = None;
                Err(err)
            }
        }
    }

    // ---- transmit --------------------------------------------------------------------------

    /// The frame being transmitted, selecting the next one if idle.
    pub(crate) fn next_frame(&mut self) -> Option<&[u8]> {
        if self.out.is_none() {
            self.out = self.select();
        }
        let out = self.out?;
        let buf = match out.source {
            Source::Control => &self.control[..],
            Source::State => &self.snapshot_buf[..],
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
            let cap = match &self.link {
                Link::Connected(session) => session.heartbeat_ms,
                _ => u32::MAX,
            };
            let now = self.now;
            if let Some(snapshot) = &mut self.snapshot {
                snapshot.next_at = now.saturating_add(u64::from(snapshot.backoff_ms));
                snapshot.backoff_ms = snapshot.backoff_ms.saturating_mul(2).min(cap.max(1));
            }
        }
    }

    fn select(&mut self) -> Option<Outgoing> {
        match &self.link {
            Link::Connecting if self.hello_due => {
                self.hello_due = false;
                let hello = HelloRef {
                    device_name: &self.config.device_name,
                    roles: self.config.roles,
                    max_frame: u32::try_from(self.local_max).unwrap_or(u32::MAX),
                };
                let seq = self.seq.wrapping_add(1);
                let result = message::encode_with(kind::HELLO, seq, &hello, &mut self.control);
                self.control_frame(result)
            }
            Link::Connected(session) => {
                let max_frame = session.max_frame;
                if let Some(now_ms) = self.pong_due.take() {
                    return self.encode_control(kind::PONG, &now_ms, max_frame);
                }
                if self.ping_due {
                    self.ping_due = false;
                    let now_ms = self.now;
                    return self.encode_control(kind::PING, &now_ms, max_frame);
                }
                self.select_snapshot()
            }
            _ => None,
        }
    }

    fn encode_control<T: serde::Serialize>(
        &mut self,
        kind: u8,
        body: &T,
        max_frame: usize,
    ) -> Option<Outgoing> {
        let seq = self.seq.wrapping_add(1);
        let limit = max_frame.min(TX);
        let buf = self.control.get_mut(..limit).unwrap_or_default();
        let result = message::encode_with(kind, seq, body, buf);
        self.control_frame(result)
    }

    fn control_frame(&mut self, result: Result<usize, message::MessageError>) -> Option<Outgoing> {
        match result {
            Ok(len) => {
                let _ = self.next_seq();
                Some(Outgoing {
                    source: Source::Control,
                    len,
                })
            }
            Err(_) => {
                self.stats.encode_errors = self.stats.encode_errors.wrapping_add(1);
                None
            }
        }
    }

    fn select_snapshot(&mut self) -> Option<Outgoing> {
        let snapshot = self.snapshot?;
        if !snapshot.pending || self.now < snapshot.next_at {
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
