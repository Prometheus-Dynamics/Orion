//! What the comm core said at hello, and the key the node proved.

use std::time::Duration;

use orion_identity::{hello, random};
use orion_msg_session::Identify;
use orion_types::NodeName;
use orion_wire::{Bytes, kind};

use crate::{Client, ClientError, Session};

/// What the comm core said at hello. [`Welcomed::needs`] says whether its node may be asked for
/// a [`Feature`].
///
/// More fields may come in a later release, so it is not built outside this crate.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct Welcomed {
    /// The node.
    pub node: NodeName,
    /// This session's number there.
    pub session: u32,
    /// How often each side pings.
    pub heartbeat: Duration,
    /// How long either side may go unheard before the session is over.
    pub idle: Duration,
    /// The largest frame either side sends.
    pub max_frame: u32,
    /// The wire revision: always this client's own, since another is turned away.
    pub revision: u16,
    /// The node's additive level: 0 from a node that does not say.
    pub level: u16,
    /// Whether this session's frames are signed: a network address or a link where this client
    /// proved a key and the node proved its own.
    pub signed: bool,
    /// The key the node proved it holds during the signed handshake, so a tool can pin it.
    /// `None` on a plain session: [`Session::node_key`] asks for it, and keeps it here.
    pub node_key: Option<[u8; 32]>,
}

const NOT_SIGNED: ClientError =
    ClientError::Denied("the node's answer is not signed by the key it names");

/// The key in an answer to Identify asked with `nonce`, if the answer is signed by it.
fn proved(payload: &[u8], nonce: &[u8]) -> Result<[u8; 32], ClientError> {
    let document = hello::verify(payload, nonce).ok_or(NOT_SIGNED)?;
    <[u8; 32]>::try_from(document.public_key.0).map_err(|_| NOT_SIGNED)
}

impl Session {
    /// The key the node proves it holds. On a signed session it is the one proved at
    /// connect. Otherwise the node is asked once, with a fresh nonce, and its signed answer is
    /// checked and kept. Whether that key is the node the caller meant is the caller's to
    /// decide.
    ///
    /// Frames other than the answer that arrive meanwhile are dropped, as with
    /// [`Session::answer`]: on a session shared by handlers, use [`Client::node_key`].
    ///
    /// # Errors
    ///
    /// [`ClientError::Denied`] if the answer is not signed by the key it names, and as
    /// [`Session::answer`].
    pub fn node_key(&mut self) -> Result<[u8; 32], ClientError> {
        if let Some(key) = self.welcomed.node_key {
            return Ok(key);
        }
        let nonce: [u8; hello::MAX_NONCE] = random();
        let seq = self.send(&Identify {
            nonce: Bytes(&nonce),
        })?;
        let key = proved(self.answer(seq, kind::IDENTITY)?.payload(), &nonce)?;
        self.welcomed.node_key = Some(key);
        Ok(key)
    }

    /// Keeps `key` as the key the node proved, if none is kept yet.
    pub fn node_proved(&mut self, key: [u8; 32]) {
        self.welcomed.node_key.get_or_insert(key);
    }
}

impl Client {
    /// As [`Session::node_key`]; frames that arrive meanwhile go to the handlers as usual.
    ///
    /// # Errors
    ///
    /// As [`Session::node_key`], and as [`Client::answered`].
    pub fn node_key(&self) -> Result<[u8; 32], ClientError> {
        if let Some(key) = self.session(|session| session.welcomed().node_key) {
            return Ok(key);
        }
        let nonce: [u8; hello::MAX_NONCE] = random();
        self.0.identity.replace(None);
        self.0.asking.set(true);
        let asked = self
            .send(&Identify {
                nonce: Bytes(&nonce),
            })
            .and_then(|seq| self.answered(seq, || self.0.identity.borrow().is_some()));
        self.0.asking.set(false);
        asked?;
        let payload = self.0.identity.take().unwrap_or_default();
        let key = proved(&payload, &nonce)?;
        self.session(|session| session.node_proved(key));
        Ok(key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_answer_not_signed_by_the_key_it_names_proves_nothing() {
        let nonce = [7_u8; hello::MAX_NONCE];
        for payload in [&b""[..], b"not a hello document", &[0_u8; 200]] {
            assert!(matches!(
                proved(payload, &nonce),
                Err(ClientError::Denied(_))
            ));
        }
    }
}
