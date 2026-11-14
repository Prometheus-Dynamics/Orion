//! A node from before the additive level, at the far end of a link: what a client meets when
//! the board was not updated.
//!
//! It welcomes every hello with the revision it was given and no level, as a node built before
//! the level did, and drops every other frame without a word, as such a node does with a kind it
//! does not serve. A signed link's hello is one of those. What it dropped is kept, so a test can
//! say nothing was sent.

use std::io::{Read, Write};
use std::os::unix::net::UnixStream;
use std::sync::{Arc, Mutex};
use std::thread;

use orion_msg_session::{Hello, Pong, Reject, RejectReason, Welcome};
use orion_wire::body::encode_fields;
use orion_wire::frame::{self, FrameHeader};
use orion_wire::{Decode, StreamDecoder, encode_frame, kind};

/// The far end of a link with an older node on it.
#[derive(Debug)]
pub struct OlderNode {
    heard: Arc<Mutex<Vec<u8>>>,
}

impl OlderNode {
    /// Starts it, speaking wire revision `revision`. The other end of the link is returned, for
    /// the client.
    ///
    /// # Panics
    ///
    /// If the link cannot be made.
    #[must_use]
    pub fn start(revision: u16) -> (Self, UnixStream) {
        Self::with(revision, false)
    }

    /// Starts one that turns every hello away as of another wire revision, naming its own,
    /// `revision`.
    ///
    /// # Panics
    ///
    /// If the link cannot be made.
    #[must_use]
    pub fn rejecting(revision: u16) -> (Self, UnixStream) {
        Self::with(revision, true)
    }

    fn with(revision: u16, reject: bool) -> (Self, UnixStream) {
        let (theirs, ours) = UnixStream::pair().expect("a link");
        let heard = Arc::new(Mutex::new(Vec::new()));
        let kept = Arc::clone(&heard);
        thread::spawn(move || serve(&ours, revision, reject, &kept));
        (Self { heard }, theirs)
    }

    /// The kinds it was sent and dropped, in order: everything but hello and ping.
    ///
    /// # Panics
    ///
    /// If its thread panicked.
    #[must_use]
    pub fn heard(&self) -> Vec<u8> {
        self.heard.lock().expect("heard").clone()
    }
}

fn serve(mut link: &UnixStream, revision: u16, reject: bool, heard: &Mutex<Vec<u8>>) {
    let mut decoder = StreamDecoder::<4096>::new();
    let mut chunk = [0_u8; 1024];
    let mut seq = 0_u16;
    while let Ok(n) = link.read(&mut chunk) {
        if n == 0 {
            return;
        }
        for byte in &chunk[..n] {
            let Ok(Some(frame)) = decoder.push(*byte) else {
                continue;
            };
            let answer = match frame.kind() {
                kind::HELLO if reject => {
                    let hello = Hello::decode(frame.payload()).expect("hello");
                    let mut payload = [0_u8; 16];
                    let len = encode_fields(
                        &Reject {
                            reason: RejectReason::RevisionMismatch,
                            node_revision: revision,
                            peer_revision: hello.revision,
                        },
                        &mut payload,
                    )
                    .expect("reject");
                    Some((kind::REJECT, payload[..len].to_vec()))
                }
                kind::HELLO => Some(welcome(revision)),
                kind::PING => Pong::decode(frame.payload()).ok().map(|_| {
                    let mut payload = [0_u8; 16];
                    let len = encode_fields(&Pong { now_ms: 0 }, &mut payload).expect("pong");
                    (kind::PONG, payload[..len].to_vec())
                }),
                other => {
                    heard.lock().expect("heard").push(other);
                    None
                }
            };
            if let Some((kind, payload)) = answer {
                seq = seq.wrapping_add(1);
                let mut framed = [0_u8; 512];
                let len = frame::encode(FrameHeader::new(kind, seq), &payload, &mut framed)
                    .expect("frame");
                let mut wire = [0_u8; 600];
                let len = encode_frame(&framed[..len], &mut wire).expect("stream");
                if link.write_all(&wire[..len]).is_err() {
                    return;
                }
            }
        }
    }
}

/// A welcome as a node from before the level wrote it: no level on the end.
fn welcome(revision: u16) -> (u8, Vec<u8>) {
    let mut payload = [0_u8; 128];
    let len = encode_fields(
        &Welcome {
            node: "older".parse().expect("name"),
            session: 1,
            heartbeat_ms: 60_000,
            max_frame: 4096,
            revision,
            idle_ms: 0,
            level: 0,
        },
        &mut payload,
    )
    .expect("welcome");
    // The level, zero, is the last byte: an older node never wrote it.
    (kind::WELCOME, payload[..len - 1].to_vec())
}
