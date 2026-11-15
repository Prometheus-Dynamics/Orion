//! A link outlives its sessions: what the board said to the last one may still be in it.

#![cfg(target_os = "linux")]

use std::io::{Read, Write};
use std::os::unix::net::UnixStream;

use orion_connect::{ClientError, Identity, Options, Session};
use orion_msg_session::{LINK_SIGN_UNSUPPORTED, LinkSignAnswer, Pong};
use orion_wire::{StreamEncoder, encode_body};

struct Half(UnixStream);

impl orion_connect::LinkRead for Half {
    fn read(&mut self, into: &mut [u8]) -> std::io::Result<usize> {
        self.0.read(into)
    }
}

impl orion_connect::LinkWrite for Half {
    fn write_all(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        Write::write_all(&mut self.0, bytes)
    }
}

/// `frame` as the board puts it on a link.
fn on_link(frame: &[u8]) -> Vec<u8> {
    let mut encoder = StreamEncoder::for_frame(frame).with_leading_delimiter();
    let (mut all, mut out) = (Vec::new(), [0_u8; 256]);
    loop {
        let len = encoder.fill(&mut out);
        if len == 0 {
            return all;
        }
        all.extend_from_slice(&out[..len]);
    }
}

#[test]
fn the_last_sessions_heartbeat_left_in_a_link_does_not_answer_the_next_signed_hello() {
    let (client, mut board) = UnixStream::pair().expect("a pair");
    let far = std::thread::spawn(move || {
        // The tool's signed hello arrives.
        let mut buf = [0_u8; 512];
        assert!(board.read(&mut buf).expect("hello") > 0);
        // Before the answer, a pong to the session that was on the link before, never read.
        let mut frame = [0_u8; 64];
        let len = encode_body(&Pong { now_ms: 4580 }, 0, &mut frame).expect("pong");
        board.write_all(&on_link(&frame[..len])).expect("pong");
        let len = encode_body(
            &LinkSignAnswer {
                status: LINK_SIGN_UNSUPPORTED,
                why: "",
            },
            0,
            &mut frame,
        )
        .expect("answer");
        board.write_all(&on_link(&frame[..len])).expect("answer");
        board
    });
    let options = Options {
        identity: Some(Identity::generate()),
        ..Options::default()
    };
    let read = Half(client.try_clone().expect("the other half"));
    let error =
        Session::over(read, Half(client), None, "tool", options).expect_err("unsigned link");
    // The board's answer, not the stale pong, is what the tool was told.
    assert!(
        matches!(error, ClientError::Denied(why) if why == "this link takes no signed sessions"),
        "{error}"
    );
    drop(far.join().expect("the board"));
}
