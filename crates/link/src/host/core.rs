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
use crate::transport::seq_newer;

use super::{HOST_MAX_FRAME, HostConfig, HostEvent, HostStats};

/// Frames queued for transmission per link; the oldest is dropped beyond this.
const OUT_CAPACITY: usize = 32;
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
}

#[derive(Debug)]
pub(crate) struct HostCore {
    config: Arc<HostConfig>,
    conn: Option<Conn>,
    seq: u16,
    next_session_id: u32,
    out: VecDeque<Vec<u8>>,
    events: VecDeque<HostEvent>,
    leases: Vec<LeaseRecord>,
    last_state: Option<ProviderState>,
    heard: bool,
    last_heard: u64,
    no_session_sent: bool,
    stats: HostStats,
}

impl HostCore {
    pub(crate) fn new(config: Arc<HostConfig>) -> Self {
        let next_session_id = config.session_seed.max(1);
        Self {
            config,
            conn: None,
            seq: 0,
            next_session_id,
            out: VecDeque::new(),
            events: VecDeque::new(),
            leases: Vec::new(),
            last_state: None,
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

    pub(crate) fn leases(&self) -> &[LeaseRecord] {
        &self.leases
    }

    pub(crate) fn provider_state(&self) -> Option<&ProviderState> {
        self.last_state.as_ref()
    }

    pub(crate) fn next_event(&mut self) -> Option<HostEvent> {
        self.events.pop_front()
    }

    pub(crate) fn pop_frame(&mut self) -> Option<Vec<u8>> {
        self.out.pop_front()
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
        let mut buf = vec![0u8; limit.clamp(FRAME_OVERHEAD, HOST_MAX_FRAME)];
        match message::encode_with(kind, seq, body, &mut buf) {
            Ok(len) => {
                self.seq = seq;
                buf.truncate(len);
                if self.out.len() >= OUT_CAPACITY {
                    self.out.pop_front();
                    self.stats.frames_dropped = self.stats.frames_dropped.wrapping_add(1);
                }
                self.out.push_back(buf);
                true
            }
            Err(_) => false,
        }
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
            self.emit(HostEvent::DeviceLost {
                device_name: conn.device_name,
            });
        }
    }

    fn queue_leases(&mut self) {
        let Some(conn) = &self.conn else {
            return;
        };
        let (limit, device_name) = (conn.max_frame, conn.device_name.clone());
        let leases = core::mem::take(&mut self.leases);
        if !self.queue(kind::LEASES, leases.as_slice(), limit) {
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
        self.heard = true;
        match frame.kind() {
            kind::PROVIDER_STATE => match Message::decode(frame) {
                Ok(Message::ProviderState(state)) => self.on_state(frame.seq(), state, limit),
                _ => self.decode_error(),
            },
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
        if !hello.roles.contains(Roles::PROVIDER) {
            self.reject(Some(hello.device_name), RejectReason::UnsupportedRoles);
            return;
        }
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
        self.conn = Some(Conn {
            device_name: hello.device_name.clone(),
            session_id,
            max_frame,
            last_seq: seq,
            confirmed: false,
        });
        self.stats.sessions = self.stats.sessions.wrapping_add(1);
        self.emit(HostEvent::DeviceConnected {
            device_name: hello.device_name,
            session_id,
            roles: hello.roles,
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
        };
        let _ = self.queue(kind::WELCOME, &welcome, max_frame);
        self.queue_leases();
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
