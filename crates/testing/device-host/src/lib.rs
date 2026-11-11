//! A host played by a test, for the crates in `crates/device`.
//!
//! The host reads and writes every body with the general codec, so a test that passes shows
//! that what a device writes by hand is what any host reads. It welcomes the device,
//! answers its pings unless told to go quiet, and hands the test every other frame the
//! device sent.
//!
//! A [`Rig`] is a device on a byte stream, the host at the other end, a millisecond counter,
//! and one layer above the session (topics, actions, firmware) driven the way a firmware's
//! main loop drives it.

use orion_device_core::{DeviceConfig, Event, Link, StreamDevice};
use orion_msg_session::{Ping, Pong, Welcome};
use orion_wire::{
    Body, Decode, FrameView, StreamDecoder, decode_body, encode_body, encode_frame, frame, kind,
};

/// A frame the device sent, kept whole so the host can decode it as any host would.
#[derive(Clone, Debug)]
pub struct Sent(Vec<u8>);

impl Sent {
    /// The frame.
    ///
    /// # Panics
    ///
    /// Never: it was a frame when it arrived.
    #[must_use]
    pub fn view(&self) -> FrameView<'_> {
        frame::decode(&self.0).expect("a frame")
    }

    /// Its kind.
    #[must_use]
    pub fn kind(&self) -> u8 {
        self.view().kind()
    }

    /// Its `seq`, which an answer names.
    #[must_use]
    pub fn seq(&self) -> u16 {
        self.view().seq()
    }

    /// Its body, decoded by the general codec.
    ///
    /// # Panics
    ///
    /// If it is not a `B`.
    #[must_use]
    pub fn body<'a, B: Body + Decode<'a>>(&'a self) -> B {
        assert_eq!(self.kind(), B::KIND, "the kind of frame");
        decode_body(&self.view()).expect("a body")
    }
}

/// The host's end of the stream.
#[derive(Debug, Default)]
pub struct Host {
    rx: StreamDecoder<1024>,
    seq: u16,
    session: u32,
}

impl Host {
    /// One frame from the host, as the bytes on the stream.
    ///
    /// # Panics
    ///
    /// If `body` does not fit in 900 bytes.
    pub fn frame<B: Body + serde::Serialize>(&mut self, body: &B) -> Vec<u8> {
        self.seq = self.seq.wrapping_add(1);
        let mut frame = [0_u8; 900];
        let len = encode_body(body, self.seq, &mut frame).expect("encode");
        let mut wire = [0_u8; 1024];
        let n = encode_frame(&frame[..len], &mut wire).expect("cobs");
        wire[..n].to_vec()
    }

    /// The `seq` of the last frame from the host.
    #[must_use]
    pub const fn last_seq(&self) -> u16 {
        self.seq
    }

    fn welcome(&mut self) -> Vec<u8> {
        self.session += 1;
        self.frame(&Welcome {
            node: "board".parse().expect("name"),
            session: self.session,
            heartbeat_ms: 100,
            max_frame: 4096,
            revision: 1,
        })
    }

    fn take<const RX: usize, const TX: usize>(
        &mut self,
        device: &mut StreamDevice<RX, TX>,
    ) -> Vec<Sent> {
        let mut frames = Vec::new();
        let mut out = [0_u8; 32];
        loop {
            let n = device.transmit(&mut out);
            if n == 0 {
                return frames;
            }
            for byte in &out[..n] {
                if let Ok(Some(view)) = self.rx.push(*byte) {
                    let mut whole = vec![0_u8; frame::frame_len(view.payload().len())];
                    let len =
                        frame::encode(view.header(), view.payload(), &mut whole).expect("a frame");
                    whole.truncate(len);
                    frames.push(Sent(whole));
                }
            }
        }
    }
}

/// How a layer is driven: polled after the session with the same counter, and given every
/// frame the session does not take. What it made of a frame is written down as text, for
/// the test to compare.
#[derive(Clone, Copy)]
pub struct Drive<L, const TX: usize> {
    /// Called after `Link::poll`.
    pub poll: fn(&mut L, &mut Link<TX>, u32),
    /// Called with a frame that is not the session's own.
    pub handle: fn(&mut L, &mut Link<TX>, &FrameView<'_>) -> String,
}

/// A device, its layer, and the host.
pub struct Rig<L, const RX: usize, const TX: usize> {
    /// The device.
    pub device: StreamDevice<RX, TX>,
    /// The layer above its session.
    pub layer: L,
    /// The host.
    pub host: Host,
    /// The counter, in milliseconds.
    pub now: u32,
    /// The host has stopped answering pings.
    pub quiet: bool,
    /// What the layer made of each frame it was given, oldest first.
    pub heard: Vec<String>,
    drive: Drive<L, TX>,
}

impl<L, const RX: usize, const TX: usize> Rig<L, RX, TX> {
    /// A device with `layer`, welcomed into a session.
    pub fn new(layer: L, drive: Drive<L, TX>) -> Self {
        let mut rig = Self {
            device: StreamDevice::new(DeviceConfig::new("test-device")),
            layer,
            host: Host::default(),
            now: 1,
            quiet: false,
            heard: Vec::new(),
            drive,
        };
        rig.connect();
        rig
    }

    /// Welcomes the device into a new session.
    ///
    /// # Panics
    ///
    /// If the device does not take it.
    pub fn connect(&mut self) {
        self.tick();
        let welcome = self.host.welcome();
        self.feed(&welcome);
        assert!(matches!(
            self.device.link.next_event(),
            Some(Event::Connected { .. })
        ));
    }

    /// One pass of a main loop at the current time. Returns what the device sent that is not
    /// the session's own.
    pub fn tick(&mut self) -> Vec<Sent> {
        self.device.link.poll(self.now);
        while self.device.link.next_event().is_some() {}
        (self.drive.poll)(&mut self.layer, &mut self.device.link, self.now);
        let mut rest = Vec::new();
        for sent in self.host.take(&mut self.device) {
            match sent.kind() {
                kind::PING if !self.quiet => {
                    let ping: Ping = sent.body();
                    let pong = self.host.frame(&Pong {
                        now_ms: ping.now_ms,
                    });
                    self.feed(&pong);
                }
                kind::HELLO | kind::PING | kind::PONG => {}
                _ => rest.push(sent),
            }
        }
        rest
    }

    /// Passes until the device has nothing more to say at this time.
    pub fn drain(&mut self) -> Vec<Sent> {
        let mut all = Vec::new();
        loop {
            let sent = self.tick();
            if sent.is_empty() {
                return all;
            }
            all.extend(sent);
        }
    }

    /// `ms` passes of a millisecond each. Returns what the device sent meanwhile.
    pub fn run(&mut self, ms: u32) -> Vec<Sent> {
        let mut all = Vec::new();
        for _ in 0..ms {
            self.now = self.now.wrapping_add(1);
            all.extend(self.tick());
        }
        all
    }

    /// Bytes from the host.
    pub fn feed(&mut self, bytes: &[u8]) {
        let layer = &mut self.layer;
        let heard = &mut self.heard;
        let handle = self.drive.handle;
        self.device.receive(bytes, |link, frame| {
            heard.push(handle(layer, link, frame));
        });
    }

    /// A frame from the host.
    pub fn send<B: Body + serde::Serialize>(&mut self, body: &B) {
        let bytes = self.host.frame(body);
        self.feed(&bytes);
    }

    /// What the layer made of the last frame.
    #[must_use]
    pub fn last_heard(&self) -> Option<&str> {
        self.heard.last().map(String::as_str)
    }
}
