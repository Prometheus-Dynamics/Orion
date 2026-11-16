//! How long a first connect keeps trying. A program may start before the comm core is up, or
//! while it restarts, so the obvious way in, [`Session::connect`], keeps trying until it is
//! welcomed or told no: nothing a program has to know to ask for. A one-shot is asked for by
//! name, [`Session::connect_once`], or bounded with [`Patience::Within`].
//!
//! Tries are paced as [`Session::reconnect`] paces them: the way in is an access path, tried
//! again every [`orion_supervise::ACCESS_RETRY_MS`], never backed off into a long wait.
//!
//! What ends the trying is an answer: a refusal ([`ClientError::may_retry`] is `false`). Trying
//! again does not change one.

use std::time::{Duration, Instant};

use orion_supervise::{Decision, Path, Status, Supervisor};

use crate::{Address, ClientError, Identity, Options, Session};

/// How long a first connect, or a hello over a link, keeps trying while nobody answers.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Patience {
    /// Until welcomed or refused. The default.
    #[default]
    Forever,
    /// For up to this long in all, then the last failure.
    Within(Duration),
    /// One try: nobody answering within [`Options::timeout`] is the failure.
    Once,
}

impl Patience {
    /// How long one hello is waited for over a link that loses frames, said again on a capped
    /// backoff all the while: `None` for as long as it takes.
    pub(crate) const fn over_a_link(self, timeout: Duration) -> Option<Duration> {
        match self {
            Self::Forever => None,
            Self::Within(within) => Some(within),
            Self::Once => Some(timeout),
        }
    }
}

impl Options {
    /// These options again: the key is copied, not shared.
    pub(crate) fn again(&self) -> Self {
        Self {
            roles: self.roles,
            identity: self
                .identity
                .as_ref()
                .map(|identity| Identity::from_secret(&identity.secret())),
            max_frame: self.max_frame,
            timeout: self.timeout,
            reply: self.reply,
            patience: self.patience,
        }
    }
}

impl Session {
    /// Connects to the comm core at `address` as the program `name`, and says hello. Nobody
    /// answering is not an answer: by default ([`Patience::Forever`]) this keeps trying until
    /// it is welcomed or refused, so a program may start before the comm core. To be told how
    /// the trying goes, use [`Session::connect_watching`]; for one try,
    /// [`Session::connect_once`].
    ///
    /// # Errors
    ///
    /// A refusal, at once: the comm core turned the session away, or, on the network, the node
    /// is not who was expected or refuses this client (see [`ClientError::may_retry`]). With
    /// [`Patience::Within`] or [`Patience::Once`], also the last failure once that is spent.
    pub fn connect(address: &Address, name: &str, options: Options) -> Result<Self, ClientError> {
        Self::connect_watching(address, name, options, |_| true)
    }

    /// One try at connecting: nothing there, or nobody answering within the options' timeout,
    /// is the failure. For a tool that says so to a person, not for a program that needs the
    /// board.
    ///
    /// # Errors
    ///
    /// As [`Session::connect`], and whatever the one try met.
    pub fn connect_once(
        address: &Address,
        name: &str,
        options: Options,
    ) -> Result<Self, ClientError> {
        let options = Options {
            patience: Patience::Once,
            ..options
        };
        Self::connect_watching(address, name, options, |_| true)
    }

    /// [`Session::connect`], telling `keep_trying` how it goes before each wait: tries failed
    /// since the start, and when the next is. Return `false` to stop, with the last failure.
    ///
    /// # Errors
    ///
    /// As [`Session::connect`], and the last failure if `keep_trying` said to stop.
    pub fn connect_watching(
        address: &Address,
        name: &str,
        options: Options,
        mut keep_trying: impl FnMut(&Status) -> bool,
    ) -> Result<Self, ClientError> {
        let start = Instant::now();
        let stop_by = match options.patience {
            Patience::Forever => None,
            Patience::Within(within) => Some(start + within),
            Patience::Once => Some(start),
        };
        let mut supervisor = Supervisor::new(Path::Access);
        loop {
            let error = match crate::open(address, &options).and_then(|transport| {
                Self::hello(transport, Some(address.clone()), name, options.again())
            }) {
                Ok(session) => return Ok(session),
                Err(error) if error.may_retry() => error,
                Err(error) => return Err(error),
            };
            let now_ms = start.elapsed().as_millis() as u64;
            let wait = Duration::from_millis(match supervisor.on_failure(now_ms) {
                Decision::RetryAt(at_ms) => at_ms.saturating_sub(now_ms),
                Decision::StayOff => u64::from(orion_supervise::ACCESS_RETRY_MS),
            });
            if stop_by.is_some_and(|by| Instant::now() + wait > by)
                || !keep_trying(&supervisor.status())
            {
                return Err(error);
            }
            std::thread::sleep(wait);
        }
    }
}
