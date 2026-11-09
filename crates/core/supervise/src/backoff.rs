/// The first delay after a failure.
pub const FIRST_DELAY_MS: u32 = 100;
/// The longest any retry waits.
pub const MAX_DELAY_MS: u32 = 10_000;
/// How often an access path retries, however long it has been failing.
pub const ACCESS_RETRY_MS: u32 = 250;

/// What kind of path is being retried.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Path {
    /// A way into the machine. Never backed off.
    Access,
    /// Anything else.
    Normal,
}

/// Retry delays. There is no attempt limit, by design.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Backoff {
    path: Path,
    next_ms: u32,
    failures: u32,
}

impl Backoff {
    /// Delays for `path`.
    #[must_use]
    pub const fn new(path: Path) -> Self {
        Self {
            path,
            next_ms: match path {
                Path::Access => ACCESS_RETRY_MS,
                Path::Normal => FIRST_DELAY_MS,
            },
            failures: 0,
        }
    }

    /// The delay before the next retry.
    pub const fn on_failure(&mut self) -> u32 {
        self.failures = self.failures.saturating_add(1);
        let delay = self.next_ms;
        if matches!(self.path, Path::Normal) {
            let doubled = self.next_ms.saturating_mul(2);
            self.next_ms = if doubled > MAX_DELAY_MS {
                MAX_DELAY_MS
            } else {
                doubled
            };
        }
        delay
    }

    /// Clears the backoff after a success.
    pub const fn on_success(&mut self) {
        *self = Self::new(self.path);
    }

    /// Failures since the last success.
    #[must_use]
    pub const fn failures(&self) -> u32 {
        self.failures
    }

    /// Which path this is.
    #[must_use]
    pub const fn path(&self) -> Path {
        self.path
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normal_paths_double_up_to_the_cap_and_stay_there() {
        let mut backoff = Backoff::new(Path::Normal);
        let delays: [u32; 9] = core::array::from_fn(|_| backoff.on_failure());
        assert_eq!(
            delays,
            [100, 200, 400, 800, 1600, 3200, 6400, 10_000, 10_000]
        );
        for _ in 0..1_000_000 {
            assert_eq!(backoff.on_failure(), MAX_DELAY_MS);
        }
    }

    #[test]
    fn access_paths_are_never_backed_off() {
        let mut backoff = Backoff::new(Path::Access);
        for _ in 0..10_000 {
            assert_eq!(backoff.on_failure(), ACCESS_RETRY_MS);
        }
    }

    #[test]
    fn it_clears_itself_on_recovery() {
        let mut backoff = Backoff::new(Path::Normal);
        backoff.on_failure();
        backoff.on_failure();
        assert_eq!(backoff.failures(), 2);
        backoff.on_success();
        assert_eq!(backoff.failures(), 0);
        assert_eq!(backoff.on_failure(), FIRST_DELAY_MS);
    }
}
