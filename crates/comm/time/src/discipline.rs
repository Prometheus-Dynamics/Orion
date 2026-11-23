//! What to do with a clock offset: the rules, without the kernel, so they are tested on their
//! own.
//!
//! The rules follow `systemd-timesyncd`:
//! - small offsets are slewed;
//! - a large offset (over [`STEP_THRESHOLD_NS`]) is stepped, but only after two samples agree, so
//!   one bad answer cannot move the clock;
//! - after the first synchronization, the clock is never stepped backwards: a large negative
//!   offset is reported and left alone;
//! - after the first synchronization, a step forward of over an hour needs more than one
//!   source's word: two different sources that agree, or an operator's clock. The first step
//!   after start is free of this: the board was off, for nobody knows how long.

use orion_measure::clock;

/// Offsets above this are stepped instead of slewed (timesyncd's 0.4 s).
pub(crate) const STEP_THRESHOLD_NS: i64 = 400_000_000;
/// Two large offsets agree when they differ by at most this.
const AGREE_NS: i64 = 100_000_000;
/// A later step forward beyond this needs a second source, or an operator.
pub(crate) const LONE_STEP_NS: i64 = 3_600 * 1_000_000_000;
/// Answers whose round trip took longer are not used.
pub(crate) const MAX_DELAY_NS: i64 = 1_000_000_000;

/// 2026-01-01, in Unix milliseconds.
const NEVER_BEFORE_MS: i64 = 1_767_225_600_000;

/// A decimal number of seconds as milliseconds, or 0 for anything else.
const fn seconds_as_ms(text: Option<&str>) -> i64 {
    let Some(text) = text else { return 0 };
    let bytes = text.as_bytes();
    let (mut at, mut seconds) = (0, 0_i64);
    while at < bytes.len() {
        if !bytes[at].is_ascii_digit() || seconds > i64::MAX / 100_000 {
            return 0;
        }
        seconds = seconds * 10 + (bytes[at] - b'0') as i64;
        at += 1;
    }
    seconds * 1000
}

/// No clock is right before this build existed (never before 2026-01-01): a source saying so is
/// broken. `SOURCE_DATE_EPOCH`, when the build sets it, moves the floor up.
pub(crate) const EARLIEST_PLAUSIBLE_MS: i64 = {
    let built = seconds_as_ms(option_env!("SOURCE_DATE_EPOCH"));
    if built > NEVER_BEFORE_MS {
        built
    } else {
        NEVER_BEFORE_MS
    }
};
const DAY_MS: i64 = 86_400_000;
/// How far before the last synchronized time a source may be: a clock is not a day younger than
/// it was. Beyond this the source is wrong, not the board.
const MAX_BEHIND_SAVED_MS: i64 = DAY_MS;
/// How far past it: a board may sit unpowered for months, not for this long.
const MAX_AHEAD_SAVED_MS: i64 = 400 * DAY_MS;

/// Why a source's time cannot be right, or `None`. `saved_ms` is the last synchronized time.
/// Every source is held to the floor. Only a fleet peer is held to the saved time: a board off
/// for a week must still take the right time from the internet or the computer managing it.
pub(crate) fn implausible(
    source_time_ms: i64,
    saved_ms: Option<i64>,
    fleet_peer: bool,
) -> Option<String> {
    if source_time_ms < EARLIEST_PLAUSIBLE_MS {
        return Some(format!(
            "its time ({source_time_ms} ms) is before this build existed"
        ));
    }
    let saved_ms = saved_ms.filter(|_| fleet_peer)?;
    if source_time_ms < saved_ms.saturating_sub(MAX_BEHIND_SAVED_MS) {
        return Some(format!(
            "its time ({source_time_ms} ms) is over a day behind the last synchronized time"
        ));
    }
    if source_time_ms > saved_ms.saturating_add(MAX_AHEAD_SAVED_MS) {
        return Some(format!(
            "its time ({source_time_ms} ms) is over 400 days past the last synchronized time"
        ));
    }
    None
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Action {
    /// Nothing to do yet (a large offset waiting for a second, agreeing sample).
    Wait,
    /// Nothing to do yet: a step of over an hour, after the first synchronization, waiting
    /// for a source other than the one that said it, or an operator, to agree.
    Second,
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
    /// A large offset waiting for agreement, and who said it.
    pending: Option<(i64, String)>,
}

impl Discipline {
    pub(crate) const fn synced(&self) -> bool {
        self.synced
    }

    /// Who said the large offset that waits for agreement.
    pub(crate) fn pending_from(&self) -> Option<&str> {
        self.pending.as_ref().map(|(_, from)| from.as_str())
    }

    /// What to do with `source`'s sample. `operator` says it is an operator's clock, which
    /// alone may step the clock far.
    pub(crate) fn on_sample(
        &mut self,
        offset_ns: i64,
        delay_ns: i64,
        source: &str,
        operator: bool,
    ) -> Action {
        if delay_ns > MAX_DELAY_NS {
            return Action::Discard;
        }
        if offset_ns.unsigned_abs() <= STEP_THRESHOLD_NS.unsigned_abs() {
            self.pending = None;
            self.synced = true;
            return Action::Slew(offset_ns);
        }
        if self.synced && offset_ns < 0 {
            self.pending = None;
            return Action::Refuse(offset_ns);
        }
        // Once synchronized, one source alone does not move the clock far.
        let lone = self.synced && offset_ns > LONE_STEP_NS && !operator;
        match self.pending.take() {
            Some((previous, from))
                if (previous - offset_ns).abs() <= AGREE_NS && (!lone || from != source) =>
            {
                self.synced = true;
                Action::Step(offset_ns)
            }
            _ => {
                self.pending = Some((offset_ns, source.to_owned()));
                if lone { Action::Second } else { Action::Wait }
            }
        }
    }
}

/// Clock changes, behind a trait so the loop is tested without root.
pub trait Clock {
    /// Moves the wall clock by `offset_ns` at once.
    ///
    /// # Errors
    ///
    /// The kernel's: most often no permission.
    fn step(&self, offset_ns: i64) -> std::io::Result<()>;
    /// Hands `offset_ns` to the kernel's clock discipline to slew away, and marks the clock
    /// synchronized with `max_error_ns` as its error bound.
    ///
    /// # Errors
    ///
    /// The kernel's: most often no permission.
    fn slew(&self, offset_ns: i64, max_error_ns: i64) -> std::io::Result<()>;
    /// Marks the clock unsynchronized, leaving its time alone: nothing vouches for it now.
    ///
    /// # Errors
    ///
    /// The kernel's: most often no permission.
    fn unsync(&self) -> std::io::Result<()>;
}

/// The kernel clock: `clock_settime` and `adjtimex`, which need `CAP_SYS_TIME`.
#[derive(Clone, Copy, Debug, Default)]
pub struct KernelClock;

impl Clock for KernelClock {
    fn step(&self, offset_ns: i64) -> std::io::Result<()> {
        clock::step(offset_ns)
    }

    fn slew(&self, offset_ns: i64, max_error_ns: i64) -> std::io::Result<()> {
        clock::slew(offset_ns, max_error_ns)
    }

    fn unsync(&self) -> std::io::Result<()> {
        clock::unsync()
    }
}

/// Whether `systemd-timesyncd` runs: then the comm core only reports, so two disciplines never
/// fight.
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
        // A board with no clock battery: about 1.9 h behind.
        let behind = 6_840 * S;
        assert_eq!(
            discipline.on_sample(behind, S / 100, "a", false),
            Action::Wait
        );
        assert!(!discipline.synced());
        assert_eq!(
            discipline.on_sample(behind + S / 50, S / 100, "a", false),
            Action::Step(behind + S / 50)
        );
        assert!(discipline.synced());
        assert_eq!(
            discipline.on_sample(S / 1000, S / 100, "a", false),
            Action::Slew(S / 1000)
        );
    }

    #[test]
    fn disagreeing_samples_keep_waiting_and_slow_answers_are_discarded() {
        let mut discipline = Discipline::default();
        assert_eq!(
            discipline.on_sample(10 * S, S / 100, "a", false),
            Action::Wait
        );
        assert_eq!(
            discipline.on_sample(20 * S, S / 100, "a", false),
            Action::Wait
        );
        assert_eq!(
            discipline.on_sample(20 * S, S / 100, "a", false),
            Action::Step(20 * S)
        );
        assert_eq!(discipline.on_sample(S, 2 * S, "a", false), Action::Discard);
    }

    #[test]
    fn after_the_first_sync_the_clock_never_steps_backwards() {
        let mut discipline = Discipline::default();
        assert_eq!(
            discipline.on_sample(S / 10, S / 100, "a", false),
            Action::Slew(S / 10)
        );
        assert_eq!(
            discipline.on_sample(-5 * S, S / 100, "a", false),
            Action::Refuse(-5 * S)
        );
        assert_eq!(
            discipline.on_sample(-5 * S, S / 100, "a", false),
            Action::Refuse(-5 * S)
        );
        // Forward still steps, after agreement.
        assert_eq!(
            discipline.on_sample(5 * S, S / 100, "a", false),
            Action::Wait
        );
        assert_eq!(
            discipline.on_sample(5 * S, S / 100, "a", false),
            Action::Step(5 * S)
        );
    }

    #[test]
    fn the_floor_is_a_number_of_seconds_or_the_fixed_date() {
        assert_eq!(seconds_as_ms(Some("1800000000")), 1_800_000_000_000);
        assert_eq!(seconds_as_ms(Some("soon")), 0);
        assert_eq!(seconds_as_ms(Some("99999999999999999999999")), 0);
        assert_eq!(seconds_as_ms(None), 0);
        const { assert!(EARLIEST_PLAUSIBLE_MS >= NEVER_BEFORE_MS) };
    }

    #[test]
    fn once_synchronized_a_step_of_over_an_hour_needs_a_second_source_or_an_operator() {
        let hours = 5 * 3_600 * S;
        // Before the first synchronization one source is enough, however far: the board was
        // off.
        let mut discipline = Discipline::default();
        assert_eq!(
            discipline.on_sample(hours, S / 100, "a", false),
            Action::Wait
        );
        assert_eq!(
            discipline.on_sample(hours, S / 100, "a", false),
            Action::Step(hours)
        );
        // After it, the same source saying so twice moves nothing.
        assert_eq!(
            discipline.on_sample(hours, S / 100, "a", false),
            Action::Second
        );
        assert_eq!(discipline.pending_from(), Some("a"));
        assert_eq!(
            discipline.on_sample(hours, S / 100, "a", false),
            Action::Second
        );
        // Another source that disagrees does not either; one that agrees does.
        assert_eq!(
            discipline.on_sample(hours + 10 * S, S / 100, "b", false),
            Action::Second
        );
        assert_eq!(
            discipline.on_sample(hours + 10 * S, S / 100, "c", false),
            Action::Step(hours + 10 * S)
        );
        // An operator's clock is believed as before: two samples that agree.
        assert_eq!(
            discipline.on_sample(hours, S / 100, "op", true),
            Action::Wait
        );
        assert_eq!(
            discipline.on_sample(hours, S / 100, "op", true),
            Action::Step(hours)
        );
        // Up to an hour, one source is enough, as before.
        let minutes = 50 * 60 * S;
        assert_eq!(
            discipline.on_sample(minutes, S / 100, "a", false),
            Action::Wait
        );
        assert_eq!(
            discipline.on_sample(minutes, S / 100, "a", false),
            Action::Step(minutes)
        );
    }
}
