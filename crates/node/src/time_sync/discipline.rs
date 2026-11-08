//! What to do with a clock offset: the rules, without the kernel, so they are tested on their
//! own; plus the Linux calls that carry them out.
//!
//! The rules follow `systemd-timesyncd`:
//! - small offsets are slewed;
//! - a large offset (over [`STEP_THRESHOLD_NS`]) is stepped, but only after two samples agree, so
//!   one bad answer cannot move the clock;
//! - after the first synchronization, the clock is never stepped backwards: a large negative
//!   offset is reported and left alone.

/// Offsets above this are stepped instead of slewed (timesyncd's 0.4 s).
pub(crate) const STEP_THRESHOLD_NS: i64 = 400_000_000;
/// Two large offsets agree when they differ by at most this.
const AGREE_NS: i64 = 100_000_000;
/// Answers whose round trip took longer are not used.
pub(crate) const MAX_DELAY_NS: i64 = 1_000_000_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Action {
    /// Nothing to do yet (a large offset waiting for a second, agreeing sample).
    Wait,
    Slew(i64),
    Step(i64),
    /// A large backward offset after the first synchronization: reported, not applied.
    Refuse(i64),
    /// The answer took too long to say anything.
    Discard,
}

#[derive(Debug, Default)]
pub(crate) struct Discipline {
    synced: bool,
    pending_step_ns: Option<i64>,
}

impl Discipline {
    pub(crate) fn synced(&self) -> bool {
        self.synced
    }

    pub(crate) fn on_sample(&mut self, offset_ns: i64, delay_ns: i64) -> Action {
        if delay_ns > MAX_DELAY_NS {
            return Action::Discard;
        }
        if offset_ns.unsigned_abs() <= STEP_THRESHOLD_NS.unsigned_abs() {
            self.pending_step_ns = None;
            self.synced = true;
            return Action::Slew(offset_ns);
        }
        if self.synced && offset_ns < 0 {
            self.pending_step_ns = None;
            return Action::Refuse(offset_ns);
        }
        match self.pending_step_ns.take() {
            Some(previous) if (previous - offset_ns).abs() <= AGREE_NS => {
                self.synced = true;
                Action::Step(offset_ns)
            }
            _ => {
                self.pending_step_ns = Some(offset_ns);
                Action::Wait
            }
        }
    }
}

/// Clock changes, behind a trait so the loop is tested without root.
pub(crate) trait Clock: Send + Sync {
    /// Moves the wall clock by `offset_ns` at once.
    fn step(&self, offset_ns: i64) -> std::io::Result<()>;
    /// Hands `offset_ns` to the kernel's clock discipline to slew away, and marks the clock
    /// synchronized with `max_error_ns` as its error bound.
    fn slew(&self, offset_ns: i64, max_error_ns: i64) -> std::io::Result<()>;
}

/// The kernel clock: `clock_settime` and `adjtimex`, which need `CAP_SYS_TIME`.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct KernelClock;

#[cfg(target_os = "linux")]
impl Clock for KernelClock {
    fn step(&self, offset_ns: i64) -> std::io::Result<()> {
        // SAFETY: plain old data; zero is a valid timespec.
        let mut now: libc::timespec = unsafe { std::mem::zeroed() };
        // SAFETY: `now` is a valid, exclusively borrowed timespec.
        if unsafe { libc::clock_gettime(libc::CLOCK_REALTIME, &mut now) } != 0 {
            return Err(std::io::Error::last_os_error());
        }
        let total = i128::from(now.tv_sec) * 1_000_000_000
            + i128::from(now.tv_nsec)
            + i128::from(offset_ns);
        let target = libc::timespec {
            tv_sec: total.div_euclid(1_000_000_000) as _,
            tv_nsec: total.rem_euclid(1_000_000_000) as _,
        };
        // SAFETY: `target` is a valid timespec.
        if unsafe { libc::clock_settime(libc::CLOCK_REALTIME, &target) } != 0 {
            return Err(std::io::Error::last_os_error());
        }
        // The step leaves nothing to slew; this also clears `STA_UNSYNC`.
        self.slew(0, 0)
    }

    // `timex` fields are `c_long`, which is `i64` only on 64-bit targets.
    #[allow(clippy::useless_conversion)]
    fn slew(&self, offset_ns: i64, max_error_ns: i64) -> std::io::Result<()> {
        // SAFETY: plain old data; zero is a valid timex.
        let mut timex: libc::timex = unsafe { std::mem::zeroed() };
        timex.modes = libc::ADJ_STATUS
            | libc::ADJ_NANO
            | libc::ADJ_OFFSET
            | libc::ADJ_TIMECONST
            | libc::ADJ_MAXERROR
            | libc::ADJ_ESTERROR;
        // The kernel PLL, synchronized (`STA_UNSYNC` cleared), offsets in nanoseconds.
        timex.status = libc::STA_PLL;
        // The kernel caps a single offset at 0.5 s; larger ones are stepped.
        timex.offset = offset_ns.clamp(-500_000_000, 500_000_000) as libc::c_long;
        timex.constant = 4;
        let error_us = (max_error_ns / 1_000).clamp(0, i64::from(i32::MAX));
        timex.maxerror = error_us as libc::c_long;
        timex.esterror = error_us as libc::c_long;
        // SAFETY: `timex` is a valid, exclusively borrowed struct.
        if unsafe { libc::adjtimex(&mut timex) } < 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(())
    }
}

#[cfg(not(target_os = "linux"))]
impl Clock for KernelClock {
    fn step(&self, _offset_ns: i64) -> std::io::Result<()> {
        Err(std::io::Error::other("setting the clock is Linux only"))
    }

    fn slew(&self, _offset_ns: i64, _max_error_ns: i64) -> std::io::Result<()> {
        Err(std::io::Error::other("setting the clock is Linux only"))
    }
}

/// Whether `systemd-timesyncd` runs: then the node only reports, so two disciplines never fight.
pub(crate) fn timesyncd_running() -> bool {
    let Ok(processes) = std::fs::read_dir("/proc") else {
        return false;
    };
    processes.filter_map(Result::ok).any(|process| {
        // The kernel keeps 15 bytes of a process name.
        std::fs::read_to_string(process.path().join("comm"))
            .is_ok_and(|name| name.trim_end() == "systemd-timesyn")
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const S: i64 = 1_000_000_000;

    #[test]
    fn a_large_offset_steps_only_when_two_samples_agree() {
        let mut discipline = Discipline::default();
        // The test board's case: about 1.9 h behind.
        let behind = 6_840 * S;
        assert_eq!(discipline.on_sample(behind, S / 100), Action::Wait);
        assert!(!discipline.synced());
        assert_eq!(
            discipline.on_sample(behind + S / 50, S / 100),
            Action::Step(behind + S / 50)
        );
        assert!(discipline.synced());
        assert_eq!(
            discipline.on_sample(S / 1000, S / 100),
            Action::Slew(S / 1000)
        );
    }

    #[test]
    fn disagreeing_samples_keep_waiting_and_slow_answers_are_discarded() {
        let mut discipline = Discipline::default();
        assert_eq!(discipline.on_sample(10 * S, S / 100), Action::Wait);
        assert_eq!(discipline.on_sample(20 * S, S / 100), Action::Wait);
        assert_eq!(discipline.on_sample(20 * S, S / 100), Action::Step(20 * S));
        assert_eq!(discipline.on_sample(S, 2 * S), Action::Discard);
    }

    #[test]
    fn after_the_first_sync_the_clock_never_steps_backwards() {
        let mut discipline = Discipline::default();
        assert_eq!(discipline.on_sample(S / 10, S / 100), Action::Slew(S / 10));
        assert_eq!(
            discipline.on_sample(-5 * S, S / 100),
            Action::Refuse(-5 * S)
        );
        assert_eq!(
            discipline.on_sample(-5 * S, S / 100),
            Action::Refuse(-5 * S)
        );
        // Forward still steps, after agreement.
        assert_eq!(discipline.on_sample(5 * S, S / 100), Action::Wait);
        assert_eq!(discipline.on_sample(5 * S, S / 100), Action::Step(5 * S));
    }
}
