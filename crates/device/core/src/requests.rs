//! Things a device asks the host for once a session: a topic declared, an action
//! registered, a file prefix taken. Each is asked when a session starts, asked again if the
//! answer does not come, and not asked again once answered or refused, until the next
//! session.

use crate::Link;

/// How long to wait for an answer before asking again.
pub const RETRY_MS: u32 = 1_000;

/// Where one request stands in this session.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Request {
    /// Not asked in this session yet.
    Unsent,
    /// Asked in the frame `seq` at `at_ms`, with no answer yet.
    Asked {
        /// The request's `seq`, which the answer names.
        seq: u16,
        /// When it was sent.
        at_ms: u32,
    },
    /// The host said yes.
    Answered,
    /// The host said no, with an error code.
    Refused {
        /// The error's code.
        code: u16,
    },
}

/// `N` requests and the session they belong to.
#[derive(Debug)]
pub struct Requests<const N: usize> {
    states: [Request; N],
    session: Option<u32>,
}

impl<const N: usize> Default for Requests<N> {
    fn default() -> Self {
        Self::new()
    }
}

impl<const N: usize> Requests<N> {
    /// None asked yet.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            states: [Request::Unsent; N],
            session: None,
        }
    }

    /// Where request `index` stands.
    #[must_use]
    pub fn get(&self, index: usize) -> Option<Request> {
        self.states.get(index).copied()
    }

    /// Whether the session changed since the last call, which starts every request over.
    /// Then asks the first that is due, as a frame of kind `kind` whose payload `write`
    /// writes for that index. One frame a call, while the link has room.
    pub fn poll<const TX: usize>(
        &mut self,
        link: &mut Link<TX>,
        now_ms: u32,
        kind: u8,
        mut write: impl FnMut(usize, &mut [u8]) -> Option<usize>,
    ) -> bool {
        let new_session = link.session() != self.session;
        if new_session {
            self.session = link.session();
            self.states = [Request::Unsent; N];
        }
        if self.session.is_none() || !link.is_free() {
            return new_session;
        }
        for (index, state) in self.states.iter_mut().enumerate() {
            let due = match *state {
                Request::Unsent => true,
                Request::Asked { at_ms, .. } => now_ms.wrapping_sub(at_ms) >= RETRY_MS,
                Request::Answered | Request::Refused { .. } => false,
            };
            if due {
                if let Ok(seq) = link.send_with(kind, |out| write(index, out)) {
                    *state = Request::Asked { seq, at_ms: now_ms };
                }
                break;
            }
        }
        new_session
    }

    /// The request that went out as `seq`, now answered.
    pub fn answer(&mut self, seq: u16) -> Option<usize> {
        self.settle(seq, Request::Answered)
    }

    /// The request that went out as `seq`, now refused with `code`.
    pub fn refuse(&mut self, seq: u16, code: u16) -> Option<usize> {
        self.settle(seq, Request::Refused { code })
    }

    fn settle(&mut self, seq: u16, to: Request) -> Option<usize> {
        let index = self.states.iter().position(
            |state| matches!(*state, Request::Asked { seq: asked, .. } if asked == seq),
        )?;
        if let Some(state) = self.states.get_mut(index) {
            *state = to;
        }
        Some(index)
    }
}
