//! A way in that cannot open yet: tried again, with the rest of the comm core running.

use std::fmt::Display;
use std::io;
use std::time::Duration;

/// How often a way in that cannot open is tried again: `first`, doubling to `most`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Retry {
    /// The first wait.
    pub first: Duration,
    /// The longest wait.
    pub most: Duration,
}

impl Default for Retry {
    /// 5 seconds, then 10.
    fn default() -> Self {
        Self {
            first: Duration::from_secs(5),
            most: Duration::from_secs(10),
        }
    }
}

/// Whether `error`, from opening a listening socket, is one that may clear by itself: the
/// address is taken or not on this machine yet, the interface is not there yet, or a port
/// that needs a capability. Anything else, such as settings that cannot work, is not.
#[must_use]
pub fn may_clear(error: &io::Error) -> bool {
    // ENODEV: an interface named for the socket that is not there.
    const NO_DEVICE: i32 = 19;
    matches!(
        error.kind(),
        io::ErrorKind::AddrInUse
            | io::ErrorKind::AddrNotAvailable
            | io::ErrorKind::PermissionDenied
    ) || error.raw_os_error() == Some(NO_DEVICE)
}

/// Opens `way` at `at` with `open`, waiting as `retry` says between tries until it opens. Says
/// so once when it cannot, and once when it opens after that.
pub async fn open_retrying<T>(
    way: &str,
    at: &dyn Display,
    retry: Retry,
    mut open: impl AsyncFnMut() -> io::Result<T>,
) -> T {
    let mut tries = Tries {
        way,
        at,
        retry,
        wait: retry.first,
        warned: false,
    };
    loop {
        match open().await {
            Ok(opened) => {
                tries.opened();
                return opened;
            }
            Err(error) => tries.failed(&error).await,
        }
    }
}

/// The waiting and the words of [`open_retrying`], kept out of it so that each kind of way in
/// does not carry its own copy.
struct Tries<'a> {
    way: &'a str,
    at: &'a dyn Display,
    retry: Retry,
    wait: Duration,
    warned: bool,
}

impl Tries<'_> {
    fn opened(&self) {
        if self.warned {
            tracing::info!("{} is open on {}", self.way, self.at);
        }
    }

    async fn failed(&mut self, error: &io::Error) {
        if !self.warned {
            tracing::warn!(
                "{} cannot open on {}: {error}; trying again every {}-{} s, and the rest of the \
                 comm core runs meanwhile",
                self.way,
                self.at,
                self.retry.first.as_secs(),
                self.retry.most.as_secs()
            );
            self.warned = true;
        }
        tokio::time::sleep(self.wait).await;
        self.wait = (self.wait * 2).min(self.retry.most);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_what_may_clear_by_itself_is_tried_again() {
        for kind in [
            io::ErrorKind::AddrInUse,
            io::ErrorKind::AddrNotAvailable,
            io::ErrorKind::PermissionDenied,
        ] {
            assert!(may_clear(&io::Error::from(kind)), "{kind:?}");
        }
        assert!(may_clear(&io::Error::from_raw_os_error(19)), "ENODEV");
        for kind in [
            io::ErrorKind::Other,
            io::ErrorKind::InvalidInput,
            io::ErrorKind::NotFound,
        ] {
            assert!(!may_clear(&io::Error::from(kind)), "{kind:?}");
        }
        assert!(!may_clear(&io::Error::from_raw_os_error(22)), "EINVAL");
    }
}
