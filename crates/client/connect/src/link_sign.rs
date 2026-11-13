//! The tool's side of a signed session on a link: prove this client's key to the board at the
//! other end of a serial port or a USB cable, check the board's, and from then on seal every
//! frame (`orion_msg_session::LinkSign*`).

use std::time::{Duration, Instant};

use orion_identity::Identity;
use orion_msg_session::{
    LINK_SIGN_OK, LINK_SIGN_UNSUPPORTED, LinkSignAnswer, LinkSignChallenge, LinkSignHello,
    LinkSignProve,
};
use orion_net_handshake::{Fresh, SIGNATURE_LEN, Seal, Transcript, hello};
use orion_wire::{Body, Bytes, Decode, encode_body, frame, kind};

use crate::error::ClientError;
use crate::fd::FdSet;
use crate::transport::{Step, Transport};

const DAMAGED: ClientError = ClientError::Protocol("a handshake message that does not read");

fn send<B: Body>(transport: &mut Transport, body: &B) -> Result<(), ClientError> {
    let mut out = [0_u8; 512];
    let len = encode_body(body, 0, &mut out).map_err(|_| ClientError::TooLarge)?;
    transport.send(&out[..len], &[])
}

/// The next whole frame, waited for until `deadline`.
fn next(
    transport: &mut Transport,
    buf: &mut [u8],
    deadline: Instant,
) -> Result<usize, ClientError> {
    loop {
        match transport.take(buf, &mut FdSet::default())? {
            Step::Frame(len) => return Ok(len),
            Step::Closed => return Err(ClientError::Lost),
            Step::Nothing => {
                let left = deadline.saturating_duration_since(Instant::now());
                if left.is_zero() || !transport.wait(left)? {
                    return Err(ClientError::Io(std::io::ErrorKind::TimedOut.into()));
                }
            }
        }
    }
}

/// Why the board said no.
fn refused(payload: &[u8]) -> ClientError {
    match LinkSignAnswer::decode(payload) {
        Ok(answer) if answer.status == LINK_SIGN_UNSUPPORTED => {
            ClientError::Denied("this link takes no signed sessions")
        }
        Ok(_) => ClientError::Denied("the board refused this client's key"),
        Err(_) => DAMAGED,
    }
}

impl Transport {
    /// Proves `identity` to the board on this link, and checks it is the board `expect` names.
    /// The frames after it are sealed.
    pub(crate) fn sign_link(
        &mut self,
        identity: &Identity,
        expect: Option<&[u8; 32]>,
        timeout: Duration,
    ) -> Result<(), ClientError> {
        let deadline = Instant::now() + timeout;
        let fresh = Fresh::new();
        let client = hello(&fresh, &identity.public());
        send(
            self,
            &LinkSignHello {
                hello: Bytes(&client),
            },
        )?;

        let mut buf = [0_u8; 1024];
        let len = next(self, &mut buf, deadline)?;
        let view = frame::decode(&buf[..len]).map_err(|_| DAMAGED)?;
        match view.kind() {
            kind::LINK_SIGN_CHALLENGE => {}
            kind::LINK_SIGN_ANSWER => return Err(refused(view.payload())),
            // A board from before signed links takes this for a device that did not say hello.
            _ => {
                return Err(ClientError::Denied(
                    "the board does not take a signed session on a link",
                ));
            }
        }
        let challenge = LinkSignChallenge::decode(view.payload()).map_err(|_| DAMAGED)?;
        let server = <[u8; 96]>::try_from(challenge.hello.0).map_err(|_| DAMAGED)?;
        let transcript = Transcript { client, server };
        if challenge.signature.0.len() != SIGNATURE_LEN
            || !transcript.server_signed(challenge.signature.0)
        {
            return Err(ClientError::Denied(
                "the board did not prove it holds its key",
            ));
        }
        if let Some(asked) = expect.filter(|asked| **asked != transcript.server_key()) {
            return Err(ClientError::OtherKey {
                asked: *asked,
                answered: transcript.server_key(),
            });
        }
        let (to_board, to_client) = transcript.keys(&fresh, true).ok_or(ClientError::Denied(
            "the board sent a key exchange that gives no secret",
        ))?;
        let signature = transcript.sign_client(identity);
        send(
            self,
            &LinkSignProve {
                signature: Bytes(&signature),
            },
        )?;

        let len = next(self, &mut buf, deadline)?;
        let view = frame::decode(&buf[..len]).map_err(|_| DAMAGED)?;
        if view.kind() != kind::LINK_SIGN_ANSWER {
            return Err(DAMAGED);
        }
        match LinkSignAnswer::decode(view.payload()) {
            Ok(answer) if answer.status == LINK_SIGN_OK => {
                self.seal_link(Seal::new(to_client), Seal::new(to_board));
                Ok(())
            }
            _ => Err(refused(view.payload())),
        }
    }
}
