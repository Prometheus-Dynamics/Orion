//! Reconnects of the long-lived client streams (action, status, workload, and provider watches).
//!
//! A watch outlives the node it is connected to: when `orion-node` restarts, the stream drops and
//! the watch re-registers. The service's retry policy (no retries by default) does not fit that,
//! so reconnects retry without a limit and back off. A refusal that a retry cannot fix ends the
//! watch with its error instead.

use std::{future::Future, time::Duration};

use tokio::time::sleep;

use crate::ClientError;

/// First pause before reconnecting a dropped stream; doubles up to [`RECONNECT_BACKOFF_MAX`].
pub(crate) const RECONNECT_BACKOFF_MIN: Duration = Duration::from_millis(50);
pub(crate) const RECONNECT_BACKOFF_MAX: Duration = Duration::from_secs(5);

impl ClientError {
    /// Whether a reconnect that failed with this error may succeed later: the node is down,
    /// restarting, or the socket failed. A refusal the node gave (`Rejected`, for example a claim
    /// that another client holds), a role or protocol mismatch, or an invalid identity will fail
    /// the same way on every retry.
    pub(crate) fn is_transient(&self) -> bool {
        match self {
            Self::SendFailed | Self::NoMessageAvailable | Self::AddressAlreadyRegistered => true,
            #[cfg(feature = "ipc")]
            Self::Ipc(_) => true,
            Self::Rejected(_)
            | Self::InvalidIdentityName(_)
            | Self::RoleMismatch { .. }
            | Self::ProtocolMismatch { .. } => false,
        }
    }
}

/// Calls `connect` until it succeeds, pausing between attempts from [`RECONNECT_BACKOFF_MIN`] to
/// [`RECONNECT_BACKOFF_MAX`]. Returns the error at once when it is not transient.
pub(crate) async fn reconnect_with_backoff<T, F, Fut>(mut connect: F) -> Result<T, ClientError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, ClientError>>,
{
    let mut delay = RECONNECT_BACKOFF_MIN;
    loop {
        match connect().await {
            Ok(value) => return Ok(value),
            Err(error) if !error.is_transient() => return Err(error),
            Err(_) => {
                sleep(delay).await;
                delay = (delay * 2).min(RECONNECT_BACKOFF_MAX);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    #[test]
    fn refusals_and_mismatches_are_not_transient() {
        assert!(!ClientError::Rejected("claimed by another client".into()).is_transient());
        assert!(!ClientError::InvalidIdentityName("bad name".into()).is_transient());
        assert!(
            !ClientError::ProtocolMismatch {
                local: 5,
                remote: 6
            }
            .is_transient()
        );
        assert!(ClientError::NoMessageAvailable.is_transient());
        assert!(ClientError::SendFailed.is_transient());
        assert!(ClientError::AddressAlreadyRegistered.is_transient());
    }

    #[tokio::test]
    async fn reconnect_retries_transient_failures_until_it_connects() {
        let attempts = Cell::new(0usize);
        let value = reconnect_with_backoff(|| {
            attempts.set(attempts.get() + 1);
            let attempt = attempts.get();
            async move {
                if attempt < 4 {
                    Err(ClientError::NoMessageAvailable)
                } else {
                    Ok(attempt)
                }
            }
        })
        .await
        .expect("a node that comes back should be reconnected to");
        assert_eq!(value, 4);
        assert_eq!(attempts.get(), 4);
    }

    #[tokio::test]
    async fn reconnect_stops_at_the_first_refusal() {
        let attempts = Cell::new(0usize);
        let error = reconnect_with_backoff::<(), _, _>(|| {
            attempts.set(attempts.get() + 1);
            async { Err(ClientError::Rejected("claimed by another client".into())) }
        })
        .await
        .expect_err("a refusal should end the reconnects");
        assert_eq!(
            error,
            ClientError::Rejected("claimed by another client".into())
        );
        assert_eq!(attempts.get(), 1, "a refused reconnect is not retried");
    }
}
