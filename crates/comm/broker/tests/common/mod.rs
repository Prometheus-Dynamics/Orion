//! A small topic client for the broker's tests: what `crates/client` will do properly.

#![allow(dead_code)]

use std::os::fd::OwnedFd;

use orion_msg_session::{Hello, Refusal, Welcome};
use orion_msg_topics::{
    Bell, Declare, Demand, Field, FieldType, Open, Opened, Subscribe, TopicState, Unsubscribe,
};
use orion_server::mem::MemConnection;
use orion_server::{Error, FdSet, Roles};
use orion_types::TopicRef;
use orion_unix::UnixConnection;
use orion_wire::frame;
use orion_wire::{Body, Bytes, Decode, List, encode_body, kind};
use shm_ring::{Reader, Writer};
use shm_ring_doorbell::Doorbell;

/// Either kind of connection, as a client uses it.
pub enum Wire {
    Mem(MemConnection),
    Unix(UnixConnection),
}

impl Wire {
    async fn send(&mut self, bytes: &[u8], fds: &[OwnedFd]) {
        match self {
            Self::Mem(connection) => connection.send(bytes, fds).await,
            Self::Unix(connection) => connection.send(bytes, fds).await,
        }
        .expect("send");
    }

    async fn recv(&mut self, buf: &mut [u8], fds: &mut FdSet) -> Option<usize> {
        match self {
            Self::Mem(connection) => connection.recv(buf, fds).await,
            Self::Unix(connection) => connection.recv(buf, fds).await,
        }
        .expect("recv")
    }
}

/// One message from the broker, with what a test checks about it.
#[derive(Debug)]
pub enum Event {
    Opened {
        request_seq: u16,
        handle: u32,
        shared: bool,
        ring_bytes: u64,
        fds: Vec<OwnedFd>,
    },
    Catalog(Vec<u8>),
    State {
        request_seq: u16,
        handle: u32,
        status: orion_msg_topics::TopicStatus,
    },
    Bell {
        handle: u32,
        subscriber: u32,
        present: bool,
        fds: Vec<OwnedFd>,
    },
    Demand {
        handle: u32,
        subscribers: u32,
        request: Vec<u8>,
    },
    Refused {
        request_seq: u16,
        error: Option<Error>,
        detail: String,
    },
    Other(u8),
    Closed,
}

pub struct Client {
    wire: Wire,
    buf: Vec<u8>,
    seq: u16,
}

pub const FIELDS: [Field<'static>; 2] = [
    Field {
        name: "value",
        ty: FieldType::Float,
        unit: "C",
        type_name: "",
    },
    Field {
        name: "state",
        ty: FieldType::Int,
        unit: "",
        type_name: "",
    },
];

impl Client {
    pub async fn open(wire: Wire, name: &str) -> Self {
        Self::open_at(wire, name, 0).await
    }

    /// As [`Client::open`], saying it is at the additive level `level`.
    pub async fn open_at(wire: Wire, name: &str, level: u16) -> Self {
        let mut client = Self {
            wire,
            buf: vec![0; 64 * 1024],
            seq: 0,
        };
        let hello = Hello {
            name,
            roles: Roles::CLIENT.and(Roles::PROVIDER),
            max_frame: 64 * 1024,
            revision: 1,
            level,
        };
        client.send(&hello, &[]).await;
        let mut fds = FdSet::default();
        let len = client
            .wire
            .recv(&mut client.buf, &mut fds)
            .await
            .expect("welcome");
        let frame = frame::decode(&client.buf[..len]).expect("frame");
        let _: Welcome = orion_wire::decode_body(&frame).expect("welcome");
        client
    }

    pub async fn send<B: Body>(&mut self, body: &B, fds: &[OwnedFd]) -> u16 {
        self.seq += 1;
        let mut out = vec![0_u8; 4096];
        let len = encode_body(body, self.seq, &mut out).expect("encode");
        self.wire.send(&out[..len], fds).await;
        self.seq
    }

    /// The next message from the broker.
    pub async fn next(&mut self) -> Event {
        let mut set = FdSet::default();
        let Some(len) = self.wire.recv(&mut self.buf, &mut set).await else {
            return Event::Closed;
        };
        let fds: Vec<OwnedFd> = (0..set.len()).filter_map(|index| set.take(index)).collect();
        let frame = frame::decode(&self.buf[..len]).expect("frame");
        let payload = frame.payload();
        match frame.kind() {
            kind::TOPIC_OPENED => {
                let opened = Opened::decode(payload).expect("opened");
                Event::Opened {
                    request_seq: opened.request_seq,
                    handle: opened.handle,
                    shared: opened.shared_memory,
                    ring_bytes: opened.ring_bytes,
                    fds,
                }
            }
            kind::TOPIC_DECLARE => Event::Catalog(payload.to_vec()),
            kind::TOPIC_STATE => {
                let state = TopicState::decode(payload).expect("state");
                Event::State {
                    request_seq: state.request_seq,
                    handle: state.handle,
                    status: state.status,
                }
            }
            kind::TOPIC_BELL => {
                let bell = Bell::decode(payload).expect("bell");
                Event::Bell {
                    handle: bell.handle,
                    subscriber: bell.subscriber,
                    present: bell.present,
                    fds,
                }
            }
            kind::TOPIC_DEMAND => {
                let demand = Demand::decode(payload).expect("demand");
                Event::Demand {
                    handle: demand.handle,
                    subscribers: demand.subscribers,
                    request: demand.request.to_vec(),
                }
            }
            kind::REFUSAL => {
                let refusal = Refusal::decode(payload).expect("refusal");
                Event::Refused {
                    request_seq: refusal.request_seq,
                    error: refusal.error(),
                    detail: refusal.detail.to_owned(),
                }
            }
            other => Event::Other(other),
        }
    }

    pub fn declaration<'a>(topic: &str, fields: &'a [Field<'a>], history: u32) -> Declare<'a> {
        Declare {
            topic: topic.parse().expect("topic"),
            fields: List::new(fields),
            history,
            expiry_ms: 0,
            max_payload: 64,
            on_subscribe: Default::default(),
        }
    }

    /// Declares `topic` and returns its handle, its writer and a second write end to keep.
    pub async fn declare(&mut self, topic: &str, history: u32) -> (u32, Writer, OwnedFd) {
        let declare = Self::declaration(topic, &FIELDS, history);
        let seq = self.send(&declare, &[]).await;
        match self.next().await {
            Event::Opened {
                request_seq,
                handle,
                shared: true,
                mut fds,
                ..
            } if request_seq == seq => {
                let fd = fds.pop().expect("the write end");
                let keep = fd.try_clone().expect("clone");
                (handle, Writer::attach(fd).expect("writer"), keep)
            }
            other => panic!("declare: {other:?}"),
        }
    }

    /// Sends a subscribe for `topic` with `doorbell` attached, and returns its `seq`.
    pub async fn subscribe(
        &mut self,
        topic: &str,
        doorbell: Option<&Doorbell>,
        request: &[u8],
    ) -> u16 {
        let subscribe = Subscribe {
            topic: TopicRef::parse(topic).expect("topic"),
            request: Bytes(request),
            max_held: 0,
            max_hold_ms: 0,
        };
        let fds = match doorbell {
            Some(doorbell) => {
                let (eventfd, page) = doorbell.bell_fds().expect("bell");
                vec![eventfd, page]
            }
            None => Vec::new(),
        };
        self.send(&subscribe, &fds).await
    }

    /// Reads the declaration and the `Opened` that answer a subscribe, and attaches a reader.
    pub async fn opened(&mut self, seq: u16) -> (u32, Reader, Vec<u8>) {
        let Event::Catalog(catalog) = self.next().await else {
            panic!("no declaration before the ring");
        };
        match self.next().await {
            Event::Opened {
                request_seq,
                handle,
                shared: true,
                mut fds,
                ..
            } if request_seq == seq => (
                handle,
                Reader::attach(fds.pop().expect("the read end")).expect("reader"),
                catalog,
            ),
            other => panic!("subscribe: {other:?}"),
        }
    }

    pub async fn unsubscribe(&mut self, handle: u32) {
        self.send(&Unsubscribe { handle }, &[]).await;
    }

    pub async fn open_topic(&mut self, topic: &str, write: bool) -> u16 {
        let open = Open {
            topic: TopicRef::parse(topic).expect("topic"),
            write,
        };
        self.send(&open, &[]).await
    }
}
