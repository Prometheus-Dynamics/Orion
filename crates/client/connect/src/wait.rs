//! Waiting for the session inside a tokio runtime.

use std::time::{Duration, Instant};

use tokio::io::Interest;
use tokio::net::UnixDatagram;

use orion_msg_session::Refusal;
use orion_wire::kind;

use crate::{ClientError, Incoming, Session};

impl Session {
    /// As [`Session::next`], inside a tokio runtime: waits without blocking the thread.
    /// Sending still writes at once, which a comm core that is reading never holds up.
    ///
    /// # Errors
    ///
    /// As [`Session::next`].
    pub async fn next_async(
        &mut self,
        wait: Duration,
    ) -> Result<Option<Incoming<'_>>, ClientError> {
        // Tokio waits on a duplicate of the session's descriptor. Which kind of socket it
        // is told it has does not matter: it is only ever asked whether there is
        // something to read.
        let duplicate = self.transport.fd().try_clone_to_owned()?;
        let waiter = UnixDatagram::from_std(std::os::unix::net::UnixDatagram::from(duplicate))?;
        let deadline = Instant::now() + wait;
        loop {
            if self.take()? {
                return Ok(Some(self.incoming()));
            }
            let now = Instant::now();
            self.keep_alive(now)?;
            if now >= deadline {
                return Ok(None);
            }
            let until = self.wake_at(deadline);
            let ready = tokio::time::timeout(
                until.saturating_duration_since(now),
                waiter.ready(Interest::READABLE),
            )
            .await;
            if let Ok(ready) = ready {
                ready?;
                // Readiness is forgotten here and asked for again after the next look, so
                // a frame that is only partly here does not spin.
                let _ = waiter.try_io(Interest::READABLE, || {
                    Err::<(), _>(std::io::Error::from(std::io::ErrorKind::WouldBlock))
                });
            }
        }
    }

    /// As [`Session::answer`], inside a tokio runtime, bounded the same way
    /// ([`Session::reply_left`]).
    ///
    /// # Errors
    ///
    /// As [`Session::answer`].
    pub async fn answer_async(&mut self, seq: u16, want: u8) -> Result<Incoming<'_>, ClientError> {
        let since = Instant::now();
        loop {
            let left = self.reply_left(since)?;
            let Some(incoming) = self.next_async(left).await? else {
                continue;
            };
            if incoming.kind() == kind::REFUSAL {
                let refusal: Refusal<'_> = incoming.body()?;
                if refusal.request_seq == seq {
                    return Err(ClientError::refused(
                        refusal.error().unwrap_or(orion_types::Error::Internal),
                        refusal.detail,
                    ));
                }
            } else if incoming.kind() == want {
                break;
            }
        }
        Ok(self.incoming())
    }
}
