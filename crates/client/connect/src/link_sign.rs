//! The tool's side of a signed session on a link: prove this client's key to the board at the
//! other end of a serial port or a USB cable, check the board's, and from then on seal every
//! frame (`orion_msg_session::LinkSign*`).

use std::time::{Duration, Instant};

use orion_identity::Identity;
use orion_msg_session::{
    Hello, LINK_SIGN_OK, LINK_SIGN_UNSUPPORTED, LinkSignAnswer, LinkSignChallenge, LinkSignHello,
    LinkSignProve, Roles, Welcome,
};
use orion_net_handshake::{Fresh, SIGNATURE_LEN, Seal, Transcript, hello};
use orion_wire::{
    Body, Bytes, Decode, INTERFACE_LEVEL, INTERFACE_REVISION, encode_body, frame, kind,
};

use crate::Feature;
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
        let len = match next(self, &mut buf, deadline) {
            Ok(len) => len,
            Err(error) if is_timeout(&error) => return Err(self.older(error, timeout)),
            Err(error) => return Err(error),
        };
        let view = frame::decode(&buf[..len]).map_err(|_| DAMAGED)?;
        match view.kind() {
            kind::LINK_SIGN_CHALLENGE => {}
            kind::LINK_SIGN_ANSWER => return Err(refused(view.payload())),
            // A board from before signed links takes this for a device that did not say hello.
            _ => {
                let error =
                    ClientError::Denied("the board does not take a signed session on a link");
                return Err(self.older(error, timeout));
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

    /// Why a board did not take a signed session: `error`, unless a plain hello shows it is at
    /// a level below signed links, and then that, with both levels. Nothing is signed, and the
    /// plain session is let go.
    fn older(&mut self, error: ClientError, timeout: Duration) -> ClientError {
        match self.level(timeout) {
            Some(level) if level < Feature::SignedLink.level() => ClientError::Level {
                feature: Feature::SignedLink,
                needs: Feature::SignedLink.level(),
                node: level,
            },
            _ => error,
        }
    }

    /// The level a plain `Welcome` names, if one comes within `timeout`.
    fn level(&mut self, timeout: Duration) -> Option<u16> {
        let deadline = Instant::now() + timeout;
        send(
            self,
            &Hello {
                name: "probe",
                roles: Roles::CLIENT,
                max_frame: 1024,
                revision: INTERFACE_REVISION,
                level: INTERFACE_LEVEL,
            },
        )
        .ok()?;
        let mut buf = [0_u8; 1024];
        loop {
            let len = next(self, &mut buf, deadline).ok()?;
            let view = frame::decode(&buf[..len]).ok()?;
            match view.kind() {
                kind::WELCOME => return Welcome::decode(view.payload()).ok().map(|w| w.level),
                kind::REJECT => return None,
                _ => {}
            }
        }
    }
}

fn is_timeout(error: &ClientError) -> bool {
    matches!(error, ClientError::Io(io) if io.kind() == std::io::ErrorKind::TimedOut)
}
