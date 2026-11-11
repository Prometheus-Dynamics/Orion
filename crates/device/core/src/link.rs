//! The session: hello until welcomed, pings, noticing a host that is gone, and the one
//! frame at a time that goes out.

use orion_msg_session::{RejectReason, Roles};
use orion_wire::frame::{self, FRAME_OVERHEAD, FrameHeader};
use orion_wire::packet::SegmenterState;
use orion_wire::stream::EncoderState;
use orion_wire::{FrameView, SegmentMtu, Segmenter, StreamEncoder, kind};

use crate::codec::{Reader, Writer};

/// The longest name a device may have.
pub const NAME_MAX: usize = 48;
/// Room for the session's own frames: a hello with the longest name is the longest.
const CONTROL: usize = FRAME_OVERHEAD + NAME_MAX + 16;
/// How many events wait to be read.
const EVENTS: usize = 4;
/// The longest name of a host that is kept.
const NODE_MAX: usize = 32;

/// Who a device is, and its timing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeviceConfig {
    /// Its name, up to [`NAME_MAX`] bytes: for the host's log. The host knows a device by
    /// the link it is on.
    pub name: &'static str,
    /// What it acts as, beside being a device.
    pub roles: Roles,
    /// How long between hellos while nobody answers. It does not grow.
    pub hello_ms: u32,
    /// How long a host that said no is left alone.
    pub rejected_ms: u32,
    /// How many heartbeats of silence mean the host is gone.
    pub missed_heartbeats: u32,
}

impl DeviceConfig {
    /// A device called `name`, with timing that suits links from a slow UART to CAN FD.
    #[must_use]
    pub const fn new(name: &'static str) -> Self {
        Self {
            name,
            roles: Roles::DEVICE,
            hello_ms: 250,
            rejected_ms: 5_000,
            missed_heartbeats: 3,
        }
    }
}

/// Where the session is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum State {
    /// Saying hello until the host answers.
    Connecting,
    /// Told no: waiting before saying hello again.
    Rejected,
    /// In a session.
    Connected,
}

/// Something the firmware should know of. Small, and read with [`Link::next_event`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Event {
    /// The host took the device. Whatever the host is to know (topics, actions) is said
    /// again now: a session starts with nothing.
    Connected {
        /// The session's number.
        session: u32,
    },
    /// The session ended: the host went quiet. Hello is being said again.
    Disconnected,
    /// The host said no, and why. Hello is said again after a while.
    Rejected(RejectReason),
}

/// Why a frame was not taken to be sent.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SendError {
    /// There is no session.
    NotConnected,
    /// The frame before it has not gone out yet. Try again after the port has been fed.
    Busy,
    /// It is longer than the send buffer, or than the host takes.
    TooLarge,
}

/// Which buffer is going out, and how far it has got.
#[derive(Clone, Copy, Debug)]
enum Sending {
    Nothing,
    Fresh { own: bool },
    Stream { own: bool, at: EncoderState },
    Packet { own: bool, at: SegmenterState },
}

/// Whether the wrapping counter `now` has reached `at`.
const fn reached(now: u32, at: u32) -> bool {
    now.wrapping_sub(at) < u32::MAX / 2
}

/// The session of one device, and its way out. `TX` is the longest frame it sends.
#[derive(Debug)]
pub struct Link<const TX: usize> {
    config: DeviceConfig,
    /// The longest frame this device takes, said in hello.
    rx_max: u32,
    state: State,
    session: u32,
    heartbeat_ms: u32,
    /// The longest frame the host takes, once it has said.
    max_frame: usize,
    node: [u8; NODE_MAX],
    node_len: u8,
    seq: u16,
    /// Whether `poll` has been called: the counter starts wherever the port's does.
    started: bool,
    /// Set when a frame arrives, by code that has no clock, and read by `poll`.
    heard: bool,
    /// Set by a reject, for `poll` to start the wait from.
    turned_away: bool,
    /// Set by a welcome, for `poll` to send the first ping from.
    welcomed: bool,
    last_heard: u32,
    /// When the next hello, or the next ping, is due.
    next_at: u32,
    events: [Option<Event>; EVENTS],
    /// The session's own frame waiting to go, if `own_len` is not zero.
    own: [u8; CONTROL],
    own_len: u8,
    /// A pong owed to the host, sent when the session's buffer is free.
    owed_pong: Option<u64>,
    /// The frame from above waiting to go, if `out_len` is not zero.
    out: [u8; TX],
    out_len: usize,
    sending: Sending,
}

impl<const TX: usize> Link<TX> {
    pub(crate) const fn new(config: DeviceConfig, rx_max: usize) -> Self {
        Self {
            config,
            rx_max: rx_max as u32,
            state: State::Connecting,
            session: 0,
            heartbeat_ms: 0,
            max_frame: 0,
            node: [0; NODE_MAX],
            node_len: 0,
            seq: 0,
            started: false,
            heard: false,
            turned_away: false,
            welcomed: false,
            last_heard: 0,
            next_at: 0,
            events: [None; EVENTS],
            own: [0; CONTROL],
            own_len: 0,
            owed_pong: None,
            out: [0; TX],
            out_len: 0,
            sending: Sending::Nothing,
        }
    }

    /// Where the session is.
    #[must_use]
    pub const fn state(&self) -> State {
        self.state
    }

    /// The session's number, in a session.
    #[must_use]
    pub const fn session(&self) -> Option<u32> {
        match self.state {
            State::Connected => Some(self.session),
            _ => None,
        }
    }

    /// The name of the host, in a session, as its bytes. Cut short if it is longer than
    /// 32 bytes.
    #[must_use]
    pub fn node(&self) -> &[u8] {
        self.node
            .get(..usize::from(self.node_len))
            .unwrap_or_default()
    }

    /// The longest frame that can be sent now: the smaller of the send buffer and what the
    /// host takes.
    #[must_use]
    pub const fn max_frame(&self) -> usize {
        if self.max_frame < TX {
            self.max_frame
        } else {
            TX
        }
    }

    /// Whether [`Link::send_with`] would take a frame now.
    #[must_use]
    pub const fn is_free(&self) -> bool {
        matches!(self.state, State::Connected) && self.out_len == 0
    }

    /// The next thing the firmware should know of.
    pub fn next_event(&mut self) -> Option<Event> {
        let event = self.events.first().copied().flatten()?;
        self.events.rotate_left(1);
        if let Some(last) = self.events.last_mut() {
            *last = None;
        }
        Some(event)
    }

    fn emit(&mut self, event: Event) {
        match self.events.iter_mut().find(|slot| slot.is_none()) {
            Some(slot) => *slot = Some(event),
            // Full: the newest is what matters. Where the session is can always be asked.
            None => {
                if let Some(last) = self.events.last_mut() {
                    *last = Some(event);
                }
            }
        }
    }

    fn next_seq(&mut self) -> u16 {
        self.seq = self.seq.wrapping_add(1);
        self.seq
    }

    /// Puts one of the session's own frames in its buffer: `kind`, with the fields
    /// `write` writes. Whether there was room: a frame of the session's that is still
    /// going out is not written over.
    fn stage_own(&mut self, kind: u8, write: impl FnOnce(&mut Writer<'_>)) -> bool {
        if self.own_len != 0 {
            return false;
        }
        let mut writer = Writer::new(frame::payload_area(&mut self.own));
        write(&mut writer);
        let Some(used) = writer.finish() else {
            return false;
        };
        let seq = self.next_seq();
        match frame::encode_in_place(FrameHeader::new(kind, seq), used, &mut self.own) {
            Ok(len) => {
                self.own_len = len as u8;
                true
            }
            Err(_) => false,
        }
    }

    fn leave(&mut self, now: u32) {
        if matches!(self.state, State::Connected) {
            self.emit(Event::Disconnected);
        }
        self.state = State::Connecting;
        // What was waiting was for a session that is over.
        self.out_len = 0;
        self.owed_pong = None;
        if matches!(
            self.sending,
            Sending::Fresh { own: false }
                | Sending::Stream { own: false, .. }
                | Sending::Packet { own: false, .. }
        ) {
            // Half a frame went out. The host drops it: its end never comes whole.
            self.sending = Sending::Nothing;
        }
        self.next_at = now;
    }

    /// Timers. `now_ms` is a millisecond counter that only goes forward, and may wrap.
    pub fn poll(&mut self, now_ms: u32) {
        if !self.started {
            self.started = true;
            self.next_at = now_ms;
            self.last_heard = now_ms;
        }
        if core::mem::take(&mut self.heard) {
            self.last_heard = now_ms;
        }
        if core::mem::take(&mut self.turned_away) {
            self.next_at = now_ms.wrapping_add(self.config.rejected_ms);
        }
        if core::mem::take(&mut self.welcomed) {
            // The first ping is due at once.
            self.next_at = now_ms;
        }
        if matches!(self.state, State::Rejected) && reached(now_ms, self.next_at) {
            self.state = State::Connecting;
        }
        if matches!(self.state, State::Connected) {
            let quiet = self
                .heartbeat_ms
                .saturating_mul(self.config.missed_heartbeats);
            if now_ms.wrapping_sub(self.last_heard) > quiet {
                // Gone. Hello starts again in this same call.
                self.leave(now_ms);
            } else {
                if let Some(now) = self.owed_pong
                    && self.stage_own(kind::PONG, |fields| {
                        fields.varint(now);
                    })
                {
                    self.owed_pong = None;
                }
                if reached(now_ms, self.next_at)
                    && self.stage_own(kind::PING, |fields| {
                        fields.varint(u64::from(now_ms));
                    })
                {
                    self.next_at = now_ms.wrapping_add(self.heartbeat_ms);
                }
            }
        }
        if matches!(self.state, State::Connecting) && reached(now_ms, self.next_at) {
            let config = self.config;
            let name = config.name.as_bytes();
            let name = name.get(..NAME_MAX).unwrap_or(name);
            let rx_max = self.rx_max;
            // Hello: name, roles, the longest frame taken, the interface revision.
            let staged = self.stage_own(kind::HELLO, |fields| {
                fields
                    .bytes(name)
                    .varint(u64::from(config.roles.and(Roles::DEVICE).0))
                    .varint(u64::from(rx_max))
                    .varint(u64::from(frame::INTERFACE_REVISION));
            });
            if staged {
                // The same wait every time.
                self.next_at = now_ms.wrapping_add(self.config.hello_ms);
            }
        }
    }

    /// Takes a frame that arrived. Whether it was the session's own: one that is not is
    /// for the layers above. Reads no clock.
    pub(crate) fn handle(&mut self, frame: &FrameView<'_>) -> bool {
        match (frame.kind(), self.state) {
            (kind::WELCOME, State::Connecting) => {
                // Welcome: node, session, heartbeat, the longest frame to send, revision.
                let mut fields = Reader::new(frame.payload());
                let (Some(node), Some(session), Some(heartbeat_ms), Some(max_frame)) =
                    (fields.bytes(), fields.u32(), fields.u32(), fields.u32())
                else {
                    return true;
                };
                let kept = node.len().min(NODE_MAX);
                for (to, from) in self.node.iter_mut().zip(node) {
                    *to = *from;
                }
                self.node_len = kept as u8;
                self.session = session;
                // A host that names no heartbeat is not one to divide by.
                self.heartbeat_ms = if heartbeat_ms < 10 { 10 } else { heartbeat_ms };
                self.max_frame = max_frame as usize;
                self.state = State::Connected;
                self.heard = true;
                self.welcomed = true;
                self.emit(Event::Connected { session });
                true
            }
            (kind::REJECT, State::Connecting) => {
                if let Some(reason) = Reader::new(frame.payload()).u8() {
                    self.state = State::Rejected;
                    self.turned_away = true;
                    self.emit(Event::Rejected(RejectReason::from_code(reason)));
                }
                true
            }
            (kind::PING, State::Connected) => {
                self.heard = true;
                if let Some(now) = Reader::new(frame.payload()).varint() {
                    self.owed_pong = Some(now);
                }
                true
            }
            (kind::PONG, State::Connected) => {
                self.heard = true;
                true
            }
            (kind::WELCOME | kind::REJECT | kind::HELLO, _) => true,
            (_, State::Connected) => {
                self.heard = true;
                false
            }
            // Not in a session: nothing above has anything to hear.
            _ => true,
        }
    }

    /// Takes a frame of kind `kind` whose payload `write` writes, saying how long it is
    /// or `None` if it does not fit.
    ///
    /// # Errors
    ///
    /// [`SendError`]. Nothing was taken.
    pub fn send_with(
        &mut self,
        kind: u8,
        write: impl FnOnce(&mut [u8]) -> Option<usize>,
    ) -> Result<u16, SendError> {
        if !matches!(self.state, State::Connected) {
            return Err(SendError::NotConnected);
        }
        if self.out_len != 0 {
            return Err(SendError::Busy);
        }
        let limit = self.max_frame();
        let room = self.out.get_mut(..limit).ok_or(SendError::TooLarge)?;
        let used = write(frame::payload_area(room)).ok_or(SendError::TooLarge)?;
        let seq = self.seq.wrapping_add(1);
        let len = frame::encode_in_place(FrameHeader::new(kind, seq), used, room)
            .map_err(|_| SendError::TooLarge)?;
        self.seq = seq;
        self.out_len = len;
        Ok(seq)
    }

    /// The frame to send now and whether it is the session's own, starting one if none is
    /// going. The session's own go first.
    fn current(&mut self) -> Option<bool> {
        match self.sending {
            Sending::Nothing => {
                let own = if self.own_len != 0 {
                    true
                } else if self.out_len != 0 {
                    false
                } else {
                    return None;
                };
                self.sending = Sending::Fresh { own };
                Some(own)
            }
            Sending::Fresh { own } | Sending::Stream { own, .. } | Sending::Packet { own, .. } => {
                Some(own)
            }
        }
    }

    fn finished(&mut self, own: bool) {
        if own {
            self.own_len = 0;
        } else {
            self.out_len = 0;
        }
        self.sending = Sending::Nothing;
    }

    pub(crate) fn transmit(&mut self, out: &mut [u8]) -> usize {
        let mut written = 0;
        while written < out.len() {
            let Some(own) = self.current() else {
                break;
            };
            let bytes = if own {
                self.own.get(..usize::from(self.own_len))
            } else {
                self.out.get(..self.out_len)
            };
            let Some(bytes) = bytes else {
                break;
            };
            let mut encoder = match self.sending {
                Sending::Stream { at, .. } => StreamEncoder::resume(bytes, at),
                _ => StreamEncoder::for_frame(bytes),
            };
            written += encoder.fill(out.get_mut(written..).unwrap_or_default());
            if encoder.is_done() {
                self.finished(own);
            } else {
                self.sending = Sending::Stream {
                    own,
                    at: encoder.save(),
                };
            }
        }
        written
    }

    pub(crate) fn next_segment(&mut self, mtu: SegmentMtu, out: &mut [u8]) -> Option<usize> {
        let own = self.current()?;
        let bytes = if own {
            self.own.get(..usize::from(self.own_len))?
        } else {
            self.out.get(..self.out_len)?
        };
        let mut segmenter = match self.sending {
            Sending::Packet { at, .. } => Segmenter::resume(bytes, mtu, at),
            _ => Segmenter::for_frame(bytes, mtu),
        };
        let len = segmenter.next_into(out).ok().flatten();
        if segmenter.is_done() {
            self.finished(own);
        } else {
            self.sending = Sending::Packet {
                own,
                at: segmenter.save(),
            };
        }
        len
    }
}
