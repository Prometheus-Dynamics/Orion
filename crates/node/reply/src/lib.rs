//! What node logic is asked, and how it answers.
//!
//! Node logic has no connections of its own. A request reaches it inside a `NodeRequest` from
//! the comm core, already checked, and its answer goes back through the comm core to the
//! session that asked. Every part of node logic sees a request as an [`Asked`] and answers
//! through an [`Out`], and knows nothing else of how either travels.

use orion_msg_session::{NodeRequest, Refusal};
use orion_types::Error;
use orion_wire::body::encode_fields;
use orion_wire::{Body, Decode};

/// One request, as the comm core passed it on.
#[derive(Clone, Copy, Debug)]
pub struct Asked<'a> {
    /// The session that asked.
    pub session: u32,
    /// The `seq` of its request.
    pub seq: u16,
    /// The kind of its request.
    pub kind: u8,
    /// The caller's class: the bit of `orion_check::PrincipalClass`.
    pub class: u8,
    /// The caller's user id, for a program on this board.
    pub uid: u32,
    /// The caller's key, node or device name. Empty for a program on this board.
    pub caller: &'a str,
    /// The request's payload.
    pub payload: &'a [u8],
}

impl<'a> Asked<'a> {
    /// The request inside what the comm core sent.
    #[must_use]
    pub const fn new(passed: &NodeRequest<'a>) -> Self {
        Self {
            session: passed.session,
            seq: passed.seq,
            kind: passed.kind,
            class: passed.class,
            uid: passed.uid,
            caller: passed.caller,
            payload: passed.payload.0,
        }
    }

    /// Who asked, as the audit log names them: `local:<uid>`, `operator:<key>`,
    /// `peer:<node>`, `device:<name>` or `anonymous`.
    #[must_use]
    pub fn principal(&self) -> String {
        // The bits of `orion_check::PrincipalClass`.
        match self.class {
            2 | 4 => format!("local:{}", self.uid),
            8 => format!("operator:{}", self.caller),
            16 => format!("peer:{}", self.caller),
            32 => format!("device:{}", self.caller),
            _ => "anonymous".to_owned(),
        }
    }

    /// The request's body, as a `B`.
    ///
    /// # Errors
    ///
    /// [`Error::Malformed`] if it does not decode.
    pub fn body<B: Body + Decode<'a>>(&self) -> Result<B, Error> {
        B::decode(self.payload)
    }
}

/// Where answers go: to a session, through the comm core. A send that fails is the
/// connection's to notice, not the caller's: node logic connects again and nothing it keeps
/// depended on the frame arriving.
pub trait Out {
    /// Sends a frame of `kind` with `payload` to `session`. With `may_drop`, the frame is one
    /// of a stream and is dropped if the session is not reading; otherwise that session is
    /// closed.
    fn frame(&mut self, session: u32, kind: u8, payload: &[u8], may_drop: bool);

    /// Room to encode a body in.
    fn scratch(&mut self) -> &mut Vec<u8>;
}

impl dyn Out + '_ {
    fn encoded<B: Body>(&mut self, session: u32, body: &B, may_drop: bool) -> Result<(), Error> {
        let mut scratch = std::mem::take(self.scratch());
        let result = encode_fields(body, &mut scratch)
            .map(|len| self.frame(session, B::KIND, &scratch[..len], may_drop));
        *self.scratch() = scratch;
        result
    }

    /// Sends `body` to `session`.
    ///
    /// # Errors
    ///
    /// [`Error::BufferTooSmall`] if the body does not fit a frame. Nothing was sent.
    pub fn send<B: Body>(&mut self, session: u32, body: &B) -> Result<(), Error> {
        self.encoded(session, body, false)
    }

    /// Sends `body` to `session` as one of a stream: dropped if the session is not reading.
    ///
    /// # Errors
    ///
    /// As [`Out::send`].
    pub fn push<B: Body>(&mut self, session: u32, body: &B) -> Result<(), Error> {
        self.encoded(session, body, true)
    }

    /// Refuses a request.
    pub fn refuse(&mut self, asked: &Asked<'_>, error: Error) {
        let detail = error.to_string();
        let _ = self.send(
            asked.session,
            &Refusal {
                request_seq: asked.seq,
                code: error.code(),
                detail: &detail,
            },
        );
    }
}
