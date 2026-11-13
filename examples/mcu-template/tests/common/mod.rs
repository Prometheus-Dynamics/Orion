//! A host played by the tests: it welcomes the device, answers its pings, opens its topics
//! with one listener, lists its actions and takes its firmware prefix, and keeps every
//! other frame the device sent. It reads and writes every body with the general codec, so
//! what the template sends is what any host reads.

#![allow(dead_code)]

use orion_device_host::Sent;
use orion_msg_actions::Registered;
use orion_msg_files::{FileSink, FileSinkReady};
use orion_msg_session::{Hello, Ping, Pong, Welcome};
use orion_msg_topics::{Declare, Demand, Opened};
use orion_wire::{
    Body, Bytes, FrameView, Reassembler, SegmentMtu, Segmenter, StreamDecoder, encode_body,
    encode_frame, kind,
};

/// The host's side of one device.
#[derive(Default)]
pub struct Peer {
    seq: u16,
    session: u32,
    next_handle: u32,
    /// The device's name, from its hello.
    pub name: String,
    /// Topics opened: name and handle.
    pub topics: Vec<(String, u32)>,
    /// How many actions were listed.
    pub actions: usize,
    /// The largest chunk the firmware prefix takes, once asked for.
    pub max_chunk: Option<u32>,
    /// Every other frame, in order.
    pub seen: Vec<Sent>,
}

impl Peer {
    /// A whole frame from the host.
    pub fn raw<B: Body>(&mut self, body: &B) -> Vec<u8> {
        self.seq = self.seq.wrapping_add(1);
        let mut frame = vec![0_u8; 1024];
        let len = encode_body(body, self.seq, &mut frame).expect("encode");
        frame.truncate(len);
        frame
    }

    /// Answers what needs an answer, and keeps the rest. Returns the frames to send.
    pub fn answer(&mut self, sent: Sent) -> Vec<Vec<u8>> {
        match sent.kind() {
            kind::HELLO => {
                let hello: Hello = sent.body();
                self.name = hello.name.to_owned();
                self.session += 1;
                let welcome = Welcome {
                    node: "board".parse().expect("name"),
                    session: self.session,
                    heartbeat_ms: 100,
                    max_frame: 4096,
                    revision: 1,
                    idle_ms: 0,
                };
                vec![self.raw(&welcome)]
            }
            kind::PING => {
                let ping: Ping = sent.body();
                vec![self.raw(&Pong {
                    now_ms: ping.now_ms,
                })]
            }
            kind::PONG => Vec::new(),
            kind::TOPIC_DECLARE => {
                let declare: Declare = sent.body();
                self.next_handle += 1;
                let handle = 100 + self.next_handle;
                self.topics
                    .push((declare.topic.as_str().to_owned(), handle));
                let opened = self.raw(&Opened {
                    request_seq: sent.seq(),
                    handle,
                    shared_memory: false,
                    ring_bytes: 4096,
                });
                let demand = self.raw(&Demand {
                    handle,
                    subscribers: 1,
                    request: Bytes(&[]),
                });
                vec![opened, demand]
            }
            kind::ACTION_REGISTER => {
                self.actions += 1;
                vec![self.raw(&Registered {
                    request_seq: sent.seq(),
                })]
            }
            kind::FILE_SINK => {
                let sink: FileSink = sent.body();
                self.max_chunk = Some(sink.max_chunk);
                vec![self.raw(&FileSinkReady {
                    request_seq: sent.seq(),
                })]
            }
            _ => {
                self.seen.push(sent);
                Vec::new()
            }
        }
    }

    /// The frames the device sent of `kind`, taken out of `seen`.
    pub fn take(&mut self, kind: u8) -> Vec<Sent> {
        let (taken, kept) = std::mem::take(&mut self.seen)
            .into_iter()
            .partition(|sent| sent.kind() == kind);
        self.seen = kept;
        taken
    }
}

/// A whole frame as the bytes on a stream.
pub fn stream(frame: &[u8]) -> Vec<u8> {
    let mut out = vec![0_u8; frame.len() * 2 + 8];
    let n = encode_frame(frame, &mut out).expect("encode");
    out.truncate(n);
    out
}

/// A whole frame as the data of classic CAN frames.
pub fn segments(frame: &[u8]) -> Vec<Vec<u8>> {
    Segmenter::for_frame(frame, SegmentMtu::CLASSIC)
        .map(|segment| segment.as_bytes().to_vec())
        .collect()
}

/// The device's end of a stream, read by the host.
#[derive(Default)]
pub struct StreamReader(StreamDecoder<1024>);

impl StreamReader {
    /// The frames whole in `bytes`.
    pub fn read(&mut self, bytes: &[u8]) -> Vec<Sent> {
        let mut frames = Vec::new();
        for byte in bytes {
            if let Ok(Some(view)) = self.0.push(*byte) {
                frames.push(Sent::of(&view));
            }
        }
        frames
    }
}

/// The device's CAN frames, read by the host.
#[derive(Default)]
pub struct CanReader(Reassembler<1024>);

impl CanReader {
    /// The frame `data` completes, if it does.
    pub fn read(&mut self, data: &[u8]) -> Option<Sent> {
        let view: FrameView<'_> = self.0.push(data).ok()??;
        Some(Sent::of(&view))
    }
}
