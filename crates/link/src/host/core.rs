//! Transport-independent host state machine for one device link: works on whole frames.

use std::collections::VecDeque;
use std::string::String;
use std::sync::Arc;
use std::vec;
use std::vec::Vec;

use serde::Serialize;

use crate::frame::{FRAME_OVERHEAD, FrameView};
use crate::message::{
    self, Hello, LeaseRecord, Message, ProviderState, RejectReason, Roles, Welcome, kind,
};

use super::ActionSendError;

mod client;
mod executor;
mod streams;
use crate::transport::seq_newer;

#[cfg(feature = "xfer")]
use super::xfer::{DeviceInfo, XferReply, XferRequest};
use super::{HOST_MAX_FRAME, HostConfig, HostEvent, HostStats};

/// Frames answers and actions may fill in a link's output queue; past it they wait
/// (`Congested`), so a queued answer is never cut.
const OUT_CAPACITY: usize = 32;
/// Room above [`OUT_CAPACITY`] for the session's own frames (`Pong`, `Ack`, `Leases`, `Reject`):
/// a peer that keeps pinging while it reads nothing must not push queued answers out. Past it,
/// the new frame is dropped and counted (`frames_dropped`); each of them is resent or superseded
/// (leases go out again after the next `Pong`).
const CONTROL_RESERVE: usize = 8;
/// Events queued per link; the oldest is dropped beyond this.
const EVENT_CAPACITY: usize = 64;

#[derive(Debug, Clone)]
struct Conn {
    device_name: String,
    session_id: u32,
    max_frame: usize,
    /// Newest device sequence number accepted.
    last_seq: u16,
    /// The device has sent session traffic, so it has seen our `Welcome`.
    confirmed: bool,
    /// The session's interface revision: the lower of the device's and the host's.
    revision: u16,
    /// The roles the session announced.
    roles: Roles,
}

#[derive(Debug)]
pub(crate) struct HostCore {
    config: Arc<HostConfig>,
    conn: Option<Conn>,
    seq: u16,
    next_session_id: u32,
    out: super::out::OutQueue,
    events: VecDeque<HostEvent>,
    leases: Vec<LeaseRecord>,
    last_state: Option<ProviderState>,
    /// The executor state last reported in this session (revision 3).
    last_executor_state: Option<std::boxed::Box<message::ExecutorState>>,
    /// The device's last reported firmware state in this session (feature `xfer`).
    #[cfg(feature = "xfer")]
    last_info: Option<DeviceInfo>,
    /// The action names the device declared in this session, if it declared any.
    last_catalog: Option<Vec<String>>,
    heard: bool,
    last_heard: u64,
    no_session_sent: bool,
    stats: HostStats,
}

/// Encodes a frame of at most `limit` bytes on the stack and hands it to `then`, so a queued
/// frame is copied once into the output ring at its own length.
pub(super) fn with_frame<E, R>(
    limit: usize,
    encode: impl FnOnce(&mut [u8]) -> Result<usize, E>,
    then: impl FnOnce(&[u8]) -> R,
) -> Result<R, E> {
    let mut scratch = [0u8; HOST_MAX_FRAME];
    let limit = limit.clamp(FRAME_OVERHEAD, HOST_MAX_FRAME);
    let buf = scratch.get_mut(..limit).unwrap_or_default();
    let len = encode(buf)?;
    Ok(then(buf.get(..len).unwrap_or_default()))
}

impl HostCore {
    pub(crate) fn new(config: Arc<HostConfig>) -> Self {
        let next_session_id = config.session_seed.max(1);
        Self {
            config,
            conn: None,
            seq: 0,
            next_session_id,
            out: super::out::OutQueue::default(),
            events: VecDeque::new(),
            leases: Vec::new(),
            last_state: None,
            last_executor_state: None,
            #[cfg(feature = "xfer")]
            last_info: None,
            last_catalog: None,
            heard: false,
            last_heard: 0,
            no_session_sent: false,
            stats: HostStats::default(),
        }
    }

    pub(crate) fn stats(&self) -> HostStats {
        self.stats
    }

    pub(crate) fn device_name(&self) -> Option<&str> {
        self.conn.as_ref().map(|conn| conn.device_name.as_str())
    }

    pub(crate) fn session_id(&self) -> Option<u32> {
        self.conn.as_ref().map(|conn| conn.session_id)
    }

    pub(crate) fn revision(&self) -> Option<u16> {
        self.conn.as_ref().map(|conn| conn.revision)
    }

    pub(crate) fn max_frame(&self) -> Option<usize> {
        self.conn.as_ref().map(|conn| conn.max_frame)
    }

    pub(crate) fn roles(&self) -> Option<Roles> {
        self.conn.as_ref().map(|conn| conn.roles)
    }

    pub(crate) fn output_room(&self) -> usize {
        OUT_CAPACITY.saturating_sub(self.out.len())
    }

    pub(crate) fn leases(&self) -> &[LeaseRecord] {
        &self.leases
    }

    pub(crate) fn provider_state(&self) -> Option<&ProviderState> {
        self.last_state.as_ref()
    }

    pub(crate) fn next_event(&mut self) -> Option<HostEvent> {
        self.events.pop_front()
    }

    /// Moves the next frame into `out`; `false` when none is queued.
    pub(crate) fn pop_frame_into(&mut self, out: &mut Vec<u8>) -> bool {
        self.out.pop_into(out)
    }

    fn emit(&mut self, event: HostEvent) {
        if self.events.len() >= EVENT_CAPACITY {
            self.events.pop_front();
            self.stats.events_dropped = self.stats.events_dropped.wrapping_add(1);
        }
        self.events.push_back(event);
    }

    fn queue<T: Serialize + ?Sized>(&mut self, kind: u8, body: &T, limit: usize) -> bool {
        let seq = self.seq.wrapping_add(1);
        let queued = with_frame(
            limit,
            |buf| message::encode_with(kind, seq, body, buf),
            |frame| self.push_control(frame),
        );
        if queued.is_ok() {
            self.seq = seq;
        }
        queued.is_ok()
    }

    /// Queues one of the session's own frames, within [`CONTROL_RESERVE`] above the answers'
    /// room; past it the frame is dropped, never one queued before it.
    fn push_control(&mut self, frame: &[u8]) {
        if self.out.len() >= OUT_CAPACITY + CONTROL_RESERVE {
            self.stats.frames_dropped = self.stats.frames_dropped.wrapping_add(1);
            return;
        }
        self.out.push_back(frame);
    }

    fn reject(&mut self, device_name: Option<String>, reason: RejectReason) {
        self.stats.rejects_sent = self.stats.rejects_sent.wrapping_add(1);
        let _ = self.queue(kind::REJECT, &reason, HOST_MAX_FRAME);
        if reason != RejectReason::NoSession {
            self.emit(HostEvent::DeviceRejected {
                device_name,
                reason,
            });
        }
    }

    /// Ends the current session, reporting the device as lost.
    fn lose(&mut self) {
        if let Some(conn) = self.conn.take() {
            self.out.clear();
            self.last_state = None;
            self.last_catalog = None;
            #[cfg(feature = "xfer")]
            {
                self.last_info = None;
            }
            self.emit(HostEvent::DeviceLost {
                device_name: conn.device_name,
            });
        }
    }

    fn queue_leases(&mut self) {
        let Some(conn) = &self.conn else {
            return;
        };
        // A client-only session holds no resources.
        if !conn.roles.contains(Roles::PROVIDER) {
            return;
        }
        let limit = conn.max_frame;
        let leases = core::mem::take(&mut self.leases);
        if !self.queue(kind::LEASES, leases.as_slice(), limit) {
            let device_name = self.device_name().map(String::from).unwrap_or_default();
            let mut probe = vec![0u8; HOST_MAX_FRAME];
            let frame_len = message::encode_leases(&leases, 0, &mut probe).unwrap_or(usize::MAX);
            self.emit(HostEvent::LeasesTooLarge {
                device_name,
                frame_len,
                max_frame: limit,
            });
        }
        self.leases = leases;
    }

    pub(crate) fn set_leases(&mut self, leases: Vec<LeaseRecord>) -> bool {
        if leases == self.leases {
            return false;
        }
        self.leases = leases;
        self.queue_leases();
        true
    }

    pub(crate) fn mark_heard(&mut self) {
        if self.conn.is_some() {
            self.heard = true;
        }
    }

    pub(crate) fn poll(&mut self, now: u64) {
        self.no_session_sent = false;
        if self.heard {
            self.heard = false;
            self.last_heard = now;
        }
        if self.conn.is_some() {
            let timeout = u64::from(self.config.heartbeat_ms)
                .saturating_mul(u64::from(self.config.missed_heartbeats.max(1)));
            if now.saturating_sub(self.last_heard) >= timeout {
                self.stats.device_timeouts = self.stats.device_timeouts.wrapping_add(1);
                self.lose();
            }
        }
    }

    pub(crate) fn handle_frame(&mut self, frame: &FrameView<'_>) {
        self.stats.frames_received = self.stats.frames_received.wrapping_add(1);
        if !frame.header().is_current_version() {
            if frame.kind() == kind::HELLO {
                self.reject(None, RejectReason::VersionMismatch);
            }
            return;
        }
        if frame.kind() == kind::HELLO {
            match Message::decode(frame) {
                Ok(Message::Hello(hello)) => self.on_hello(frame.seq(), hello),
                _ => self.decode_error(),
            }
            return;
        }
        let Some(conn) = &mut self.conn else {
            // Session traffic without a session (for example after a host restart): tell the
            // device to say Hello again, at most once per poll.
            if !self.no_session_sent {
                self.no_session_sent = true;
                self.reject(None, RejectReason::NoSession);
            }
            return;
        };
        if !seq_newer(frame.seq(), conn.last_seq) {
            self.stats.duplicates = self.stats.duplicates.wrapping_add(1);
            return;
        }
        conn.last_seq = frame.seq();
        conn.confirmed = true;
        let limit = conn.max_frame;
        let (revision, roles) = (conn.revision, conn.roles);
        self.heard = true;
        let introduced = kind::revision(frame.kind());
        if introduced > revision {
            // Kinds of a later revision than the session's: ignored as any unknown kind.
            return;
        }
        if kind::is_client_request(frame.kind()) {
            self.on_client_request(frame, roles);
            return;
        }
        if kind::is_provider_stream(frame.kind()) {
            self.on_provider_stream(frame, roles);
            return;
        }
        if frame.kind() == kind::EXECUTOR_STATE {
            self.on_executor_state(frame, roles, limit);
            return;
        }
        if !roles.contains(Roles::PROVIDER)
            && matches!(
                frame.kind(),
                kind::PROVIDER_STATE | kind::STATUS | kind::ACTION_CATALOG | kind::ACTION_RESULT
            )
        {
            // A client-only session publishes nothing.
            self.decode_error();
            return;
        }
        match frame.kind() {
            kind::PROVIDER_STATE => match Message::decode(frame) {
                Ok(Message::ProviderState(state)) => self.on_state(frame.seq(), state, limit),
                _ => self.decode_error(),
            },
            kind::ACTION_CATALOG => match Message::decode(frame) {
                Ok(Message::ActionCatalog(catalog)) => {
                    self.on_action_catalog(frame.seq(), catalog.names, limit);
                }
                _ => self.decode_error(),
            },
            kind::ACTION_RESULT => match Message::decode(frame) {
                Ok(Message::ActionResult(result)) => {
                    if let Some(device_name) = self.device_name().map(String::from) {
                        self.emit(HostEvent::ActionResult {
                            device_name,
                            id: result.id,
                            state: result.state,
                            payload: result.payload,
                        });
                    }
                }
                _ => self.decode_error(),
            },
            #[cfg(feature = "xfer")]
            kind::DEVICE_INFO => self.on_device_info(frame.seq(), frame.payload(), limit),
            #[cfg(feature = "xfer")]
            kind::XFER_ACK | kind::XFER_RESULT | kind::XFER_ABORT => {
                self.on_xfer_reply(frame.kind(), frame.payload());
            }
            kind::STATUS => match message::decode_status(frame.payload()) {
                Ok(entries) => {
                    self.stats.status_received = self.stats.status_received.wrapping_add(1);
                    if let Some(device_name) = self.device_name().map(String::from) {
                        self.emit(HostEvent::Status {
                            device_name,
                            entries,
                        });
                    }
                }
                Err(_) => self.decode_error(),
            },
            kind::PING => match Message::decode(frame) {
                Ok(Message::Ping { now_ms }) => {
                    let _ = self.queue(kind::PONG, &now_ms, limit);
                    // Piggyback the lease set so a lost Leases is repaired within a heartbeat.
                    self.queue_leases();
                }
                _ => self.decode_error(),
            },
            // Pong and unknown or reserved kinds only prove liveness.
            _ => {}
        }
    }

    fn decode_error(&mut self) {
        self.stats.decode_errors = self.stats.decode_errors.wrapping_add(1);
    }

    /// Queues an `ActionRequest` for the connected device.
    pub(crate) fn send_action(
        &mut self,
        id: u32,
        name: &str,
        payload: &[u8],
    ) -> Result<(), ActionSendError> {
        self.queue_action(|seq, buf| message::encode_action_request(id, name, payload, seq, buf))
    }

    /// Queues an `ActionCancel` for the request `id` on the connected device.
    pub(crate) fn send_action_cancel(&mut self, id: u32) -> Result<(), ActionSendError> {
        self.queue_action(|seq, buf| message::encode_action_cancel(id, seq, buf))
    }

    /// Encodes an action frame for the connected device and queues it.
    fn queue_action(
        &mut self,
        encode: impl FnOnce(u16, &mut [u8]) -> Result<usize, message::MessageError>,
    ) -> Result<(), ActionSendError> {
        let Some(conn) = &self.conn else {
            return Err(ActionSendError::NotConnected);
        };
        let max_frame = conn.max_frame;
        if self.out.len() >= OUT_CAPACITY {
            return Err(ActionSendError::Congested);
        }
        let seq = self.seq.wrapping_add(1);
        let out = &mut self.out;
        with_frame(
            HOST_MAX_FRAME,
            |buf| encode(seq, buf),
            |frame| {
                if frame.len() > max_frame {
                    return Err(ActionSendError::TooLarge {
                        frame_len: frame.len(),
                        max_frame,
                    });
                }
                out.push_back(frame);
                Ok(())
            },
        )
        .map_err(|_| ActionSendError::TooLarge {
            frame_len: HOST_MAX_FRAME,
            max_frame,
        })??;
        self.seq = seq;
        Ok(())
    }

    /// The device declared its action names. Acknowledged like every state report; a repeat of
    /// the same list is not reported again.
    fn on_action_catalog(&mut self, seq: u16, names: Vec<String>, limit: usize) {
        let _ = self.queue(kind::ACK, &seq, limit);
        if self.last_catalog.as_ref() == Some(&names) {
            return;
        }
        let Some(device_name) = self.device_name().map(String::from) else {
            return;
        };
        self.last_catalog = Some(names.clone());
        self.emit(HostEvent::ActionCatalog { device_name, names });
    }

    fn on_hello(&mut self, seq: u16, hello: Hello) {
        let config = Arc::clone(&self.config);
        let unnamed = hello.device_name.trim().is_empty();
        let not_allowed = config
            .allowed_devices
            .as_ref()
            .is_some_and(|allowed| !allowed.contains(&hello.device_name));
        if unnamed || not_allowed {
            if self.device_name() != Some(hello.device_name.as_str()) {
                self.lose();
            }
            self.reject(Some(hello.device_name), RejectReason::UnknownDevice);
            return;
        }
        let revision = hello.revision.min(crate::LINK_INTERFACE_REVISION);
        let client = config.serve_clients && revision >= 1 && hello.roles.contains(Roles::CLIENT);
        let executor = config.serve_devices
            && config.serve_executors
            && revision >= 3
            && hello.roles.contains(Roles::EXECUTOR);
        let provider = config.serve_devices && hello.roles.contains(Roles::PROVIDER);
        if !provider && !client && !executor {
            self.reject(Some(hello.device_name), RejectReason::UnsupportedRoles);
            return;
        }
        // Only the roles this host serves count for the session.
        let roles = Roles(
            if provider { Roles::PROVIDER.0 } else { 0 }
                | if client { Roles::CLIENT.0 } else { 0 }
                | if executor { Roles::EXECUTOR.0 } else { 0 },
        );
        if hello.max_frame < config.min_device_frame {
            self.reject(Some(hello.device_name), RejectReason::FrameTooSmall);
            return;
        }
        let max_frame = usize::try_from(hello.max_frame)
            .unwrap_or(usize::MAX)
            .min(config.max_frame())
            .max(FRAME_OVERHEAD);
        self.heard = true;
        if let Some(conn) = &mut self.conn
            && conn.device_name == hello.device_name
            && !conn.confirmed
        {
            // The device did not get our Welcome yet: repeat it within the same session.
            conn.last_seq = seq;
            conn.max_frame = max_frame;
            conn.revision = revision;
            conn.roles = roles;
            let session_id = conn.session_id;
            self.send_welcome(session_id, max_frame);
            return;
        }
        if self
            .conn
            .as_ref()
            .is_some_and(|conn| conn.device_name != hello.device_name)
        {
            self.lose();
        }
        let session_id = self.next_session_id;
        self.next_session_id = self.next_session_id.wrapping_add(1).max(1);
        self.out.clear();
        self.last_state = None;
        self.last_executor_state = None;
        self.last_catalog = None;
        #[cfg(feature = "xfer")]
        {
            self.last_info = None;
        }
        self.conn = Some(Conn {
            device_name: hello.device_name.clone(),
            session_id,
            max_frame,
            last_seq: seq,
            confirmed: false,
            revision,
            roles,
        });
        self.stats.sessions = self.stats.sessions.wrapping_add(1);
        self.emit(HostEvent::DeviceConnected {
            device_name: hello.device_name,
            session_id,
            roles,
            max_frame,
        });
        self.send_welcome(session_id, max_frame);
    }

    fn send_welcome(&mut self, session_id: u32, max_frame: usize) {
        let welcome = Welcome {
            node_id: self.config.node_id.clone(),
            session_id,
            heartbeat_ms: self.config.heartbeat_ms,
            max_frame: u32::try_from(max_frame).unwrap_or(u32::MAX),
            revision: crate::LINK_INTERFACE_REVISION,
        };
        let _ = self.queue(kind::WELCOME, &welcome, max_frame);
        self.queue_leases();
    }

    /// Queues a frame whose body `encode` writes (the device codec of `crate::wire`).
    #[cfg(feature = "xfer")]
    fn queue_wire(
        &mut self,
        limit: usize,
        encode: impl FnOnce(u16, &mut [u8]) -> Result<usize, crate::wire::WireError>,
    ) -> bool {
        let seq = self.seq.wrapping_add(1);
        let queued = with_frame(
            limit,
            |buf| encode(seq, buf),
            |frame| self.push_control(frame),
        );
        if queued.is_ok() {
            self.seq = seq;
        }
        queued.is_ok()
    }

    /// Sends a firmware transfer message to the connected device. False if there is no session,
    /// or the message does not fit the negotiated frame size.
    #[cfg(feature = "xfer")]
    pub(crate) fn send_xfer(&mut self, request: &XferRequest) -> bool {
        let Some(limit) = self.conn.as_ref().map(|conn| conn.max_frame) else {
            return false;
        };
        use crate::wire::{
            ActivateView, ConfirmView, XferBeginView, XferChunkView, XferEndView, kind,
        };
        match request {
            XferRequest::Begin {
                xfer_id,
                component,
                version,
                total_len,
                chunk_len,
                image_crc32c,
                target_slot,
            } => {
                let body = XferBeginView {
                    xfer_id: *xfer_id,
                    component,
                    version,
                    total_len: *total_len,
                    chunk_len: *chunk_len,
                    image_crc32c: *image_crc32c,
                    target_slot: *target_slot,
                };
                self.queue_wire(limit, |seq, buf| {
                    crate::wire::encode_frame(kind::XFER_BEGIN, seq, &body, buf)
                })
            }
            XferRequest::Chunk {
                xfer_id,
                offset,
                crc32c,
                data,
            } => {
                let body = XferChunkView {
                    xfer_id: *xfer_id,
                    offset: *offset,
                    crc32c: *crc32c,
                    data,
                };
                self.queue_wire(limit, |seq, buf| {
                    crate::wire::encode_frame(kind::XFER_CHUNK, seq, &body, buf)
                })
            }
            XferRequest::End { xfer_id } => {
                let body = XferEndView { xfer_id: *xfer_id };
                self.queue_wire(limit, |seq, buf| {
                    crate::wire::encode_frame(kind::XFER_END, seq, &body, buf)
                })
            }
            XferRequest::Activate { xfer_id, mode } => {
                let body = ActivateView {
                    xfer_id: *xfer_id,
                    mode: *mode,
                };
                self.queue_wire(limit, |seq, buf| {
                    crate::wire::encode_frame(kind::ACTIVATE, seq, &body, buf)
                })
            }
            XferRequest::Confirm { slot } => {
                let body = ConfirmView { slot: *slot };
                self.queue_wire(limit, |seq, buf| {
                    crate::wire::encode_frame(kind::CONFIRM, seq, &body, buf)
                })
            }
        }
    }

    /// `DeviceInfo`: acknowledged every time (the device resends until it sees the ack), reported
    /// when it differs from the last one in this session.
    #[cfg(feature = "xfer")]
    fn on_device_info(&mut self, seq: u16, payload: &[u8], limit: usize) {
        let _ = self.queue_wire(limit, |ack_seq, buf| {
            crate::wire::encode_frame(kind::ACK, ack_seq, &seq, buf)
        });
        let Ok(view) = crate::wire::DeviceInfoView::decode(payload) else {
            self.decode_error();
            return;
        };
        let info = DeviceInfo::from_view(&view);
        if self.last_info.as_ref() == Some(&info) {
            return;
        }
        self.last_info = Some(info.clone());
        if let Some(device_name) = self.device_name().map(String::from) {
            self.emit(HostEvent::DeviceInfo { device_name, info });
        }
    }

    /// A device's reply to a transfer request.
    #[cfg(feature = "xfer")]
    fn on_xfer_reply(&mut self, kind_: u8, payload: &[u8]) {
        let reply = match kind_ {
            kind::XFER_ACK => XferReply::ack(payload),
            kind::XFER_RESULT => XferReply::result(payload),
            _ => XferReply::abort(payload),
        };
        match reply {
            Some(reply) => {
                if let Some(device_name) = self.device_name().map(String::from) {
                    self.emit(HostEvent::Xfer { device_name, reply });
                }
            }
            None => self.decode_error(),
        }
    }

    fn on_state(&mut self, seq: u16, state: ProviderState, limit: usize) {
        let _ = self.queue(kind::ACK, &seq, limit);
        if self.last_state.as_ref() == Some(&state) {
            return; // retransmission of a snapshot we already have
        }
        let Some(device_name) = self.device_name().map(String::from) else {
            return;
        };
        self.last_state = Some(state.clone());
        self.emit(HostEvent::ProviderState {
            device_name,
            provider: state.provider,
            resources: state.resources,
        });
    }
}

#[cfg(test)]
mod out_queue_tests {
    use super::*;

    #[test]
    fn a_queued_frame_holds_only_its_own_bytes() {
        let mut core = HostCore::new(Arc::new(HostConfig::new(orion_core::NodeId::new("node-a"))));
        assert!(core.queue(kind::PING, &7u64, HOST_MAX_FRAME));
        assert!(core.queue(kind::PING, &8u64, HOST_MAX_FRAME));
        assert!(core.out.len() == 2 && core.out.mark_bytes() < 64);
        let mut frame = Vec::new();
        assert!(core.pop_frame_into(&mut frame) && frame.len() < 32);
        assert!(core.pop_frame_into(&mut frame) && !core.pop_frame_into(&mut frame));
    }
}
