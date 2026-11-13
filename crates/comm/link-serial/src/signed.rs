//! A signed session on a link: the board's side of the handshake.
//!
//! A frame that opens with `LinkSignHello` is a tool proving a key, not a device saying hello.
//! The carrier answers it before the server sees anything: the same values and signatures as
//! `orion+tcp`, carried in link frames. A key that proves itself and is not revoked makes the
//! session that key's (an operator, or a pinned peer), and every frame after travels sealed
//! with a MAC, so nothing on the cable can be changed, added, dropped or replayed. A link with
//! no trust store, a proof that does not verify, or a revoked key gets a refusal and nothing
//! else: the next frame on the link starts over.

use std::io;
use std::time::Duration;

use orion_keys::SharedKeys;
use orion_msg_session::{
    LINK_SIGN_OK, LINK_SIGN_REFUSED, LINK_SIGN_UNSUPPORTED, LinkSealed, LinkSignAnswer,
    LinkSignChallenge, LinkSignHello, LinkSignProve,
};
use orion_net_handshake::{
    CLIENT_HELLO_LEN, Fresh, SIGNATURE_LEN, Seal, TAG_LEN, Transcript, hello,
};
use orion_server::Principal;
use orion_wire::{Body, Bytes, Decode, encode_body, frame, kind};

/// How long a tool has to prove its key.
pub(crate) const HANDSHAKE: Duration = Duration::from_secs(5);

/// A signed session's keys and who it is.
pub(crate) struct Signed {
    pub principal: Principal,
    pub reading: Seal,
    pub writing: Seal,
}

fn bad(why: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, why)
}

fn refused(why: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::PermissionDenied, why)
}

/// Whether `frame` asks for a signed session.
pub(crate) fn asks(frame: &[u8]) -> bool {
    frame::decode(frame).is_ok_and(|view| view.kind() == kind::LINK_SIGN_HELLO)
}

/// `body` as a frame in `out`.
pub(crate) fn encode<'o, B: Body>(body: &B, out: &'o mut [u8]) -> io::Result<&'o [u8]> {
    let len = encode_body(body, 0, out).map_err(|_| bad("a handshake message too long"))?;
    Ok(&out[..len])
}

/// The two halves of the handshake's I/O: send a frame, and wait for the next one.
pub(crate) trait Frames {
    async fn send(&mut self, frame: &[u8]) -> io::Result<()>;
    /// The next whole frame, in the buffer it was read into. `None` on hang-up.
    async fn next(&mut self) -> io::Result<Option<&[u8]>>;
}

async fn answer(frames: &mut impl Frames, status: u8, why: &str) -> io::Result<()> {
    let mut out = [0_u8; 256];
    frames
        .send(encode(&LinkSignAnswer { status, why }, &mut out)?)
        .await
}

/// Answers the `LinkSignHello` in `first`, with the board's `keys`; `None` keys refuse it.
pub(crate) async fn welcome(
    frames: &mut impl Frames,
    first: &[u8],
    keys: Option<&SharedKeys>,
) -> io::Result<Signed> {
    let view = frame::decode(first).map_err(|_| bad("not a frame"))?;
    let asked = LinkSignHello::decode(view.payload()).map_err(|_| bad("not a signed hello"))?;
    let Ok(client) = <[u8; CLIENT_HELLO_LEN]>::try_from(asked.hello.0) else {
        return Err(bad("a signed hello of the wrong length"));
    };
    let Some(keys) = keys else {
        answer(
            frames,
            LINK_SIGN_UNSUPPORTED,
            "this link takes no signed sessions",
        )
        .await?;
        return Err(refused("a signed session on a link with no trust store"));
    };
    let fresh = Fresh::new();
    let (transcript, signature) = {
        let keys = keys.lock();
        let transcript = Transcript {
            client,
            server: hello(&fresh, &keys.identity().public()),
        };
        (transcript, transcript.sign_server(keys.identity()))
    };
    let mut out = [0_u8; 512];
    frames
        .send(encode(
            &LinkSignChallenge {
                hello: Bytes(&transcript.server),
                signature: Bytes(&signature),
            },
            &mut out,
        )?)
        .await?;

    let proof = frames
        .next()
        .await?
        .ok_or_else(|| bad("gone during the handshake"))?;
    let view = frame::decode(proof).map_err(|_| bad("not a frame"))?;
    let genuine = view.kind() == kind::LINK_SIGN_PROVE
        && LinkSignProve::decode(view.payload()).is_ok_and(|prove| {
            prove.signature.0.len() == SIGNATURE_LEN && transcript.client_signed(prove.signature.0)
        });
    let key = transcript.client_key();
    let principal = keys.lock().principal(&key);
    let (true, Some((to_board, to_tool)), false) = (
        genuine,
        transcript.keys(&fresh, false),
        principal == Principal::Anonymous,
    ) else {
        // A proof that does not verify, or a key that was revoked.
        answer(frames, LINK_SIGN_REFUSED, "the key was refused").await?;
        return Err(refused("a key on the link was refused"));
    };
    answer(frames, LINK_SIGN_OK, "").await?;
    Ok(Signed {
        principal,
        reading: Seal::new(to_board),
        writing: Seal::new(to_tool),
    })
}

/// `frame` sealed into `out`, as a `LinkSealed` frame.
pub(crate) fn seal<'o>(seal: &mut Seal, frame: &[u8], out: &'o mut [u8]) -> io::Result<&'o [u8]> {
    let tag = seal.seal(frame);
    encode(
        &LinkSealed {
            frame: Bytes(frame),
            tag: Bytes(&tag),
        },
        out,
    )
}

/// The frame inside the sealed frame `sealed`, checked, copied into `buf`.
pub(crate) fn open(seal: &mut Seal, sealed: &[u8], buf: &mut [u8]) -> io::Result<usize> {
    let view = frame::decode(sealed).map_err(|_| bad("not a frame"))?;
    if view.kind() != kind::LINK_SEALED {
        return Err(bad("a frame of a signed session came unsealed"));
    }
    let body =
        LinkSealed::decode(view.payload()).map_err(|_| bad("a sealed frame that does not read"))?;
    if body.tag.0.len() != TAG_LEN || !seal.open(body.frame.0, body.tag.0) {
        return Err(bad("a frame failed its check"));
    }
    let to = buf
        .get_mut(..body.frame.0.len())
        .ok_or_else(|| bad("a frame longer than the buffer"))?;
    to.copy_from_slice(body.frame.0);
    Ok(to.len())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pair() -> (Seal, Seal) {
        (Seal::new([5; 32]), Seal::new([5; 32]))
    }

    #[test]
    fn a_sealed_frame_opens_once_in_order_and_not_changed() {
        let (mut sending, mut receiving) = pair();
        let mut out = [0_u8; 256];
        let mut buf = [0_u8; 64];
        let first = seal(&mut sending, b"first frame", &mut out)
            .expect("seal")
            .to_vec();
        let second = seal(&mut sending, b"second", &mut out)
            .expect("seal")
            .to_vec();
        // In order: both open, as they were.
        let len = open(&mut receiving, &first, &mut buf).expect("first");
        assert_eq!(&buf[..len], b"first frame");
        let len = open(&mut receiving, &second, &mut buf).expect("second");
        assert_eq!(&buf[..len], b"second");
        // Played back: it was for a count already past.
        assert!(open(&mut receiving, &second, &mut buf).is_err());

        // Changed on the way: one byte of the frame inside.
        let (mut sending, mut receiving) = pair();
        let mut changed = seal(&mut sending, b"run reboot", &mut out)
            .expect("seal")
            .to_vec();
        let at = changed.len() - 30;
        changed[at] ^= 1;
        // The outer frame's own check would catch a stray bit; a forger recomputes it.
        let view = frame::decode(&changed);
        if view.is_err() {
            let body = &changed[frame::HEADER_LEN..changed.len() - frame::CRC_LEN];
            let mut again = [0_u8; 256];
            let len = frame::encode(
                frame::FrameHeader::new(kind::LINK_SEALED, 0),
                body,
                &mut again,
            )
            .expect("encode");
            changed = again[..len].to_vec();
        }
        assert!(open(&mut receiving, &changed, &mut buf).is_err());

        // Dropped: the next one does not open.
        let (mut sending, mut receiving) = pair();
        let _lost = seal(&mut sending, b"lost", &mut out)
            .expect("seal")
            .to_vec();
        let after = seal(&mut sending, b"after", &mut out)
            .expect("seal")
            .to_vec();
        assert!(open(&mut receiving, &after, &mut buf).is_err());

        // A plain frame where a sealed one belongs.
        let (_, mut receiving) = pair();
        let plain = encode(&LinkSignAnswer { status: 0, why: "" }, &mut out)
            .expect("plain")
            .to_vec();
        assert!(open(&mut receiving, &plain, &mut buf).is_err());
    }
}
