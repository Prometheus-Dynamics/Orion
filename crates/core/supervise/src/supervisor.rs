use crate::{Backoff, LogLimit, Path};

/// How a component is doing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum State {
    /// Not started yet.
    Starting,
    /// Working.
    Running,
    /// Failed, and waiting to retry.
    Retrying,
    /// Switched off. See [`Off`].
    Off,
}

/// Why a component is off.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Off {
    /// Someone switched it off.
    Manual,
    /// A configured [`AutoOff`] policy switched it off.
    Auto,
}

/// A configured policy: switch off after `failures` failures within `window_ms`. Nothing has one
/// unless someone wrote it down.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AutoOff {
    /// How many failures.
    pub failures: u32,
    /// Within how long.
    pub window_ms: u64,
}

/// An access path was given an [`AutoOff`] policy, which it must refuse.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AutoOffRefused;

impl core::fmt::Display for AutoOffRefused {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("an access path cannot switch itself off")
    }
}

impl core::error::Error for AutoOffRefused {}

/// What the caller should do after a failure.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Decision {
    /// Try again at this time.
    RetryAt(u64),
    /// Stop until switched back on.
    StayOff,
}

/// A component's state as anyone may see it. Small and plain, so it can become a topic.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Status {
    /// Its state.
    pub state: State,
    /// Why it's off, if it is.
    pub off: Option<Off>,
    /// Restarts since this supervisor began.
    pub restarts: u32,
    /// Failures since the last success.
    pub failures: u32,
    /// When the next retry is due.
    pub retry_at_ms: Option<u64>,
}

/// Where a component's status goes.
pub trait Report {
    /// The status changed.
    fn report(&mut self, status: &Status);
}

/// Applies the failure rule to one component.
#[derive(Clone, Copy, Debug)]
pub struct Supervisor {
    backoff: Backoff,
    state: State,
    off: Option<Off>,
    restarts: u32,
    retry_at_ms: Option<u64>,
    auto_off: Option<AutoOff>,
    window_start_ms: u64,
    window_failures: u32,
    log: LogLimit,
}

impl Supervisor {
    /// Applies the rule for `path`.
    #[must_use]
    pub const fn new(path: Path) -> Self {
        Self {
            backoff: Backoff::new(path),
            state: State::Starting,
            off: None,
            restarts: 0,
            retry_at_ms: None,
            auto_off: None,
            window_start_ms: 0,
            window_failures: 0,
            log: LogLimit::new(10_000),
        }
    }

    /// Adds a configured auto-off policy.
    ///
    /// # Errors
    ///
    /// [`AutoOffRefused`] on an access path.
    pub const fn with_auto_off(mut self, policy: AutoOff) -> Result<Self, AutoOffRefused> {
        if matches!(self.backoff.path(), Path::Access) {
            return Err(AutoOffRefused);
        }
        self.auto_off = Some(policy);
        Ok(self)
    }

    /// The component is working. Backoff clears itself.
    pub fn on_success(&mut self) {
        self.backoff.on_success();
        self.retry_at_ms = None;
        self.window_failures = 0;
        if self.off.is_none() {
            self.state = State::Running;
        }
    }

    /// The component failed at `now_ms`.
    pub fn on_failure(&mut self, now_ms: u64) -> Decision {
        if self.off.is_some() {
            return Decision::StayOff;
        }
        self.restarts = self.restarts.saturating_add(1);
        if let Some(policy) = self.auto_off {
            if self.window_failures == 0
                || now_ms.saturating_sub(self.window_start_ms) > policy.window_ms
            {
                self.window_start_ms = now_ms;
                self.window_failures = 0;
            }
            self.window_failures = self.window_failures.saturating_add(1);
            if self.window_failures >= policy.failures {
                self.off = Some(Off::Auto);
                self.state = State::Off;
                self.retry_at_ms = None;
                return Decision::StayOff;
            }
        }
        let at = now_ms.saturating_add(u64::from(self.backoff.on_failure()));
        self.state = State::Retrying;
        self.retry_at_ms = Some(at);
        Decision::RetryAt(at)
    }

    /// Whether the component should be running at `now_ms`.
    #[must_use]
    pub fn may_run(&self, now_ms: u64) -> bool {
        self.off.is_none() && self.retry_at_ms.is_none_or(|at| now_ms >= at)
    }

    /// Switches it off by hand. Works on every path.
    pub const fn switch_off(&mut self) {
        self.off = Some(Off::Manual);
        self.state = State::Off;
        self.retry_at_ms = None;
    }

    /// Switches it back on, with a clean slate.
    pub fn switch_on(&mut self) {
        self.off = None;
        self.state = State::Starting;
        self.retry_at_ms = None;
        self.window_failures = 0;
        self.backoff.on_success();
    }

    /// Whether to log a failure at `now_ms`. `Some(n)` means log, and that `n` were held back.
    pub fn log_failure(&mut self, now_ms: u64) -> Option<u32> {
        self.log.allow(now_ms)
    }

    /// The current status.
    #[must_use]
    pub const fn status(&self) -> Status {
        Status {
            state: self.state,
            off: self.off,
            restarts: self.restarts,
            failures: self.backoff.failures(),
            retry_at_ms: self.retry_at_ms,
        }
    }

    /// Sends the current status to `report`.
    pub fn report(&self, report: &mut impl Report) {
        report.report(&self.status());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ACCESS_RETRY_MS, MAX_DELAY_MS};

    #[test]
    fn it_never_gives_up_by_default() {
        let mut supervisor = Supervisor::new(Path::Normal);
        let mut now = 0;
        for _ in 0..100_000 {
            let Decision::RetryAt(at) = supervisor.on_failure(now) else {
                panic!("gave up");
            };
            assert!(at - now <= u64::from(MAX_DELAY_MS));
            assert!(!supervisor.may_run(at - 1));
            assert!(supervisor.may_run(at));
            now = at;
        }
        assert_eq!(supervisor.status().restarts, 100_000);
    }

    #[test]
    fn backoff_state_is_visible_and_clears_on_recovery() {
        let mut supervisor = Supervisor::new(Path::Normal);
        assert_eq!(supervisor.on_failure(1_000), Decision::RetryAt(1_100));
        let status = supervisor.status();
        assert_eq!(status.state, State::Retrying);
        assert_eq!((status.failures, status.retry_at_ms), (1, Some(1_100)));
        supervisor.on_success();
        let status = supervisor.status();
        assert_eq!(status.state, State::Running);
        assert_eq!((status.failures, status.retry_at_ms), (0, None));
        assert_eq!(status.restarts, 1);
    }

    #[test]
    fn an_access_path_retries_at_the_same_short_interval_forever() {
        let mut supervisor = Supervisor::new(Path::Access);
        let mut now = 0;
        for _ in 0..1_000 {
            assert_eq!(
                supervisor.on_failure(now),
                Decision::RetryAt(now + u64::from(ACCESS_RETRY_MS))
            );
            now += u64::from(ACCESS_RETRY_MS);
        }
    }

    #[test]
    fn manual_off_works_everywhere_and_comes_back() {
        for path in [Path::Normal, Path::Access] {
            let mut supervisor = Supervisor::new(path);
            supervisor.switch_off();
            assert!(!supervisor.may_run(u64::MAX));
            assert_eq!(supervisor.status().off, Some(Off::Manual));
            assert_eq!(supervisor.on_failure(5), Decision::StayOff);
            supervisor.switch_on();
            assert!(supervisor.may_run(0));
            assert_eq!(supervisor.status().state, State::Starting);
        }
    }

    #[test]
    fn auto_off_happens_only_by_policy_and_within_its_window() {
        let policy = AutoOff {
            failures: 3,
            window_ms: 1_000,
        };
        let mut supervisor = Supervisor::new(Path::Normal).with_auto_off(policy).unwrap();
        // Spread out, so the window keeps restarting.
        for now in [0, 2_000, 4_000, 6_000] {
            assert!(matches!(supervisor.on_failure(now), Decision::RetryAt(_)));
        }
        // Close together.
        assert!(matches!(supervisor.on_failure(8_000), Decision::RetryAt(_)));
        assert!(matches!(supervisor.on_failure(8_100), Decision::RetryAt(_)));
        assert_eq!(supervisor.on_failure(8_200), Decision::StayOff);
        assert_eq!(supervisor.status().off, Some(Off::Auto));
        assert!(!supervisor.may_run(u64::MAX));
    }

    #[test]
    fn access_paths_refuse_auto_off() {
        let policy = AutoOff {
            failures: 1,
            window_ms: 1,
        };
        assert_eq!(
            Supervisor::new(Path::Access)
                .with_auto_off(policy)
                .unwrap_err(),
            AutoOffRefused
        );
    }

    #[test]
    fn status_reaches_the_reporter() {
        struct Last(Option<Status>);
        impl Report for Last {
            fn report(&mut self, status: &Status) {
                self.0 = Some(*status);
            }
        }
        let mut last = Last(None);
        let mut supervisor = Supervisor::new(Path::Normal);
        supervisor.on_failure(0);
        supervisor.report(&mut last);
        assert_eq!(last.0.unwrap().failures, 1);
    }
}
