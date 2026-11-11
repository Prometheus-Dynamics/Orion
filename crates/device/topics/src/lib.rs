//! Topics from a microcontroller.
//!
//! A firmware describes its topics once, as constants, and [`Topics`] does the rest: it
//! declares each one when a session starts, says it again if the answer does not come,
//! and from the answer on lets the firmware publish samples. Whether anyone listens comes
//! back as a demand, so a device can stop measuring what nobody reads.
//!
//! The declarations stay in flash: nothing about a topic is copied into RAM but where it
//! stands in this session. Samples go out one frame at a time, as the link takes them;
//! a sample the link has no room for is refused, never queued, so a device always sends
//! its newest value rather than a backlog.
//!
//! Payloads are the same bytes every other publisher writes: a number is 8 bytes little
//! endian, a bool 1 byte, text and bytes the whole payload. [`Topics::publish_f64`] and its
//! siblings write the fixed ones.
//!
//! ```
//! use orion_device_core::{DeviceConfig, StreamDevice};
//! use orion_device_topics::{Field, FieldType, Heard, TopicSpec, Topics};
//!
//! static TOPICS: [TopicSpec; 1] = [TopicSpec::latest(
//!     "imu",
//!     &[Field::new("yaw", FieldType::Float, "deg"), Field::new("ok", FieldType::Bool, "")],
//! )];
//!
//! let mut device = StreamDevice::<128, 128>::new(DeviceConfig::new("imu-board"));
//! let mut topics = Topics::new(&TOPICS);
//! # let bytes: &[u8] = &[];
//! device.receive(bytes, |link, frame| match topics.handle(frame) {
//!     Heard::Demand { subscribers, .. } => { /* start or stop measuring */ }
//!     _ => {}
//! });
//! device.link.poll(0);
//! topics.poll(&mut device.link, 0);
//! let _ = topics.publish_f64(&mut device.link, 0, 0, 0, 12.5);
//! ```

#![cfg_attr(not(test), no_std)]

mod spec;

pub use orion_device_core::{RETRY_MS, Request, SendError};
pub use orion_msg_topics::{FieldType, SampleFlags};
pub use spec::{Field, TopicSpec};

use orion_device_core::codec::{Reader, Writer};
use orion_device_core::{Link, Requests};
use orion_wire::{FrameView, kind};

/// Room for a payload's length: three varint bytes cover any frame a device sends.
const LEN_ROOM: usize = 3;

/// Where one topic stands in this session.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TopicState {
    /// Not declared in this session, or declared with no answer yet.
    Declaring(Request),
    /// The host took it: samples may be published.
    Open {
        /// The host's number for it, which samples carry.
        handle: u32,
        /// How many listen.
        subscribers: u32,
    },
    /// The host said no, with an error code. It is not asked again in this session.
    Refused {
        /// The error's code.
        code: u16,
    },
}

/// What a frame from the host meant to the topics.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Heard<'a> {
    /// Not about a topic of this device: for another layer.
    Other,
    /// About one, and nothing for the firmware to do.
    Quiet,
    /// The host took topic `topic`: it can be published from now.
    Open {
        /// Its index in the declarations.
        topic: usize,
    },
    /// The host refused topic `topic`.
    Refused {
        /// Its index in the declarations.
        topic: usize,
        /// The error's code.
        code: u16,
    },
    /// How many listen to topic `topic` changed, or one asked for something.
    Demand {
        /// Its index in the declarations.
        topic: usize,
        /// How many listen now.
        subscribers: u32,
        /// A subscriber's request, unread by Orion. Empty when unchanged.
        request: &'a [u8],
    },
}

/// Why a sample was not published.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PublishError {
    /// There is no topic with that index.
    NoSuchTopic,
    /// The host has not taken the topic in this session.
    NotOpen,
    /// The link did not take the frame.
    Link(SendError),
}

/// The topics of a device, `N` of them.
#[derive(Debug)]
pub struct Topics<const N: usize> {
    specs: &'static [TopicSpec; N],
    declared: Requests<N>,
    /// For each open topic: its handle and how many listen.
    open: [Option<(u32, u32)>; N],
}

impl<const N: usize> Topics<N> {
    /// Topics that are not declared yet.
    #[must_use]
    pub const fn new(specs: &'static [TopicSpec; N]) -> Self {
        Self {
            specs,
            declared: Requests::new(),
            open: [None; N],
        }
    }

    /// Where topic `topic` stands.
    #[must_use]
    pub fn state(&self, topic: usize) -> Option<TopicState> {
        let request = self.declared.get(topic)?;
        Some(match (request, self.open.get(topic).copied().flatten()) {
            (Request::Answered, Some((handle, subscribers))) => TopicState::Open {
                handle,
                subscribers,
            },
            (Request::Refused { code }, _) => TopicState::Refused { code },
            (request, _) => TopicState::Declaring(request),
        })
    }

    /// Declares what is not declared in this session, and again what was not answered in
    /// [`RETRY_MS`], one frame a call while the link has room. A new session starts every
    /// topic over. Call it after [`Link::poll`], with the same counter.
    pub fn poll<const TX: usize>(&mut self, link: &mut Link<TX>, now_ms: u32) {
        let specs = self.specs;
        let new_session = self
            .declared
            .poll(link, now_ms, kind::TOPIC_DECLARE, |topic, out| {
                specs.get(topic)?.write_declare(out)
            });
        if new_session {
            self.open = [None; N];
        }
    }

    /// Reads a frame the session did not take: the answer to a declaration, a refusal of
    /// one, or a demand.
    pub fn handle<'a>(&mut self, frame: &FrameView<'a>) -> Heard<'a> {
        let mut r = Reader::new(frame.payload());
        match frame.kind() {
            kind::TOPIC_OPENED => {
                let (Some(seq), Some(handle)) = (r.u16(), r.u32()) else {
                    return Heard::Other;
                };
                let Some(topic) = self.declared.answer(seq) else {
                    return Heard::Other;
                };
                if let Some(open) = self.open.get_mut(topic) {
                    *open = Some((handle, 0));
                }
                Heard::Open { topic }
            }
            kind::REFUSAL => {
                let (Some(seq), Some(code)) = (r.u16(), r.u16()) else {
                    return Heard::Other;
                };
                match self.declared.refuse(seq, code) {
                    Some(topic) => Heard::Refused { topic, code },
                    None => Heard::Other,
                }
            }
            kind::TOPIC_DEMAND => {
                let (Some(handle), Some(count)) = (r.u32(), r.u32()) else {
                    return Heard::Other;
                };
                let request = r.bytes().unwrap_or_default();
                for (topic, open) in self.open.iter_mut().enumerate() {
                    if let Some((open, subscribers)) = open
                        && *open == handle
                    {
                        *subscribers = count;
                        return Heard::Demand {
                            topic,
                            subscribers: count,
                            request,
                        };
                    }
                }
                Heard::Quiet
            }
            _ => Heard::Other,
        }
    }

    /// How many listen to topic `topic`. Zero before the host takes it.
    #[must_use]
    pub fn subscribers(&self, topic: usize) -> u32 {
        self.open
            .get(topic)
            .copied()
            .flatten()
            .map_or(0, |(_, subscribers)| subscribers)
    }

    /// Publishes one sample of field `field` of topic `topic`, at `time_ns` on the host's
    /// clock, whose payload `write` writes.
    ///
    /// # Errors
    ///
    /// [`PublishError`]. Nothing was sent.
    pub fn publish_with<const TX: usize>(
        &mut self,
        link: &mut Link<TX>,
        topic: usize,
        field: u16,
        time_ns: u64,
        flags: SampleFlags,
        write: impl FnOnce(&mut Writer<'_>),
    ) -> Result<u16, PublishError> {
        let handle = match self.open.get(topic) {
            None => return Err(PublishError::NoSuchTopic),
            Some(Some((handle, _))) => *handle,
            Some(None) => return Err(PublishError::NotOpen),
        };
        link.send_with(kind::TOPIC_SAMPLE, |out| {
            let head = Writer::new(out)
                .varint(u64::from(handle))
                .varint(u64::from(field))
                .varint(time_ns)
                .u8(flags.0)
                .finish()?;
            // The payload's length goes before it and is known only once it is written, so
            // the payload is written after room for the longest length a frame can have
            // and moved down to meet it.
            let at = head + LEN_ROOM;
            let len = {
                let mut w = Writer::new(out.get_mut(at..)?);
                write(&mut w);
                w.finish()?
            };
            let len_len = Writer::new(out.get_mut(head..at)?)
                .varint(len as u64)
                .finish()?;
            let start = head + len_len;
            for i in 0..len {
                let byte = *out.get(at + i)?;
                *out.get_mut(start + i)? = byte;
            }
            Some(start + len)
        })
        .map_err(PublishError::Link)
    }

    /// Publishes `payload` as it is.
    ///
    /// # Errors
    ///
    /// [`PublishError`]. Nothing was sent.
    pub fn publish<const TX: usize>(
        &mut self,
        link: &mut Link<TX>,
        topic: usize,
        field: u16,
        time_ns: u64,
        payload: &[u8],
    ) -> Result<u16, PublishError> {
        self.publish_with(link, topic, field, time_ns, SampleFlags::NONE, |w| {
            w.raw(payload);
        })
    }

    /// Publishes a `Float` field.
    ///
    /// # Errors
    ///
    /// [`PublishError`]. Nothing was sent.
    pub fn publish_f64<const TX: usize>(
        &mut self,
        link: &mut Link<TX>,
        topic: usize,
        field: u16,
        time_ns: u64,
        value: f64,
    ) -> Result<u16, PublishError> {
        self.publish(link, topic, field, time_ns, &value.to_le_bytes())
    }

    /// Publishes an `Int` field.
    ///
    /// # Errors
    ///
    /// [`PublishError`]. Nothing was sent.
    pub fn publish_i64<const TX: usize>(
        &mut self,
        link: &mut Link<TX>,
        topic: usize,
        field: u16,
        time_ns: u64,
        value: i64,
    ) -> Result<u16, PublishError> {
        self.publish(link, topic, field, time_ns, &value.to_le_bytes())
    }

    /// Publishes a `Bool` field.
    ///
    /// # Errors
    ///
    /// [`PublishError`]. Nothing was sent.
    pub fn publish_bool<const TX: usize>(
        &mut self,
        link: &mut Link<TX>,
        topic: usize,
        field: u16,
        time_ns: u64,
        value: bool,
    ) -> Result<u16, PublishError> {
        self.publish(link, topic, field, time_ns, &[u8::from(value)])
    }
}
