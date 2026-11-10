//! Lookup by frame time.
//!
//! Everything computed from a frame carries the frame's time. So "the pose that goes with this
//! frame" is a lookup: the value of a field at or before time *t*.
//!
//! - **Hit**: there is a sample for exactly *t*.
//! - **Lagged**: the newest sample at or before *t* is for an earlier frame. The reader gets
//!   it, and how far behind it is.
//! - **Missing**: the field has nothing at or before *t*: it was never published, or what it
//!   had that far back has been overwritten.
//!
//! [`wait_at_or_before`] waits, a bounded time, while the publisher is still on an earlier
//! frame, on the reader's doorbell. The waiting is all on the reader's side: the publisher
//! never waits for anyone.
//!
//! A field's samples are expected in time order, as frames are. The search relies on it. Out
//! of order, the answer is still a real sample at or before *t*, but maybe not the newest one.
//!
//! This is the pairing the frame pipelines on `dev` do with a one-deep slot per producer, and
//! it gives the same answers wherever a one-deep slot can. With history it also finds the exact
//! frame for a reader that is *behind* the publisher, where a one-deep slot could only offer
//! the publisher's newest value.

#![cfg(target_os = "linux")]

use std::io;
use std::time::{Duration, Instant};

use orion_ring::{FieldId, FrameTime, Mem, Reader, Sample};
use orion_ring_doorbell::Doorbell;

/// How many of the newest samples are checked one by one before searching. A reader is nearly
/// always asking about the newest frame or the one before.
const NEAR: u64 = 4;

/// What a field held at a frame time.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Lookup<'a> {
    /// The sample for exactly that time.
    Hit(Sample<'a>),
    /// The newest sample before that time.
    Lagged {
        /// The sample.
        sample: Sample<'a>,
        /// How much earlier than the time asked for it is, in nanoseconds.
        lag_ns: u64,
    },
    /// Nothing at or before that time.
    Missing,
}

impl<'a> Lookup<'a> {
    /// The sample, hit or lagged.
    #[must_use]
    pub const fn sample(self) -> Option<Sample<'a>> {
        match self {
            Self::Hit(sample) | Self::Lagged { sample, .. } => Some(sample),
            Self::Missing => None,
        }
    }

    /// Whether this is the sample for exactly the time asked for.
    #[must_use]
    pub const fn is_hit(&self) -> bool {
        matches!(self, Self::Hit(_))
    }
}

/// Which of `field`'s samples is the newest at or before `time`: its number within the field.
fn find<M: Mem>(reader: &Reader<M>, field: FieldId, time: FrameTime) -> Option<u64> {
    let head = reader.lane_head(field);
    let kept = head.min(u64::from(reader.geometry().history()));
    let oldest = head - kept;
    // The newest few, newest first.
    let near = kept.min(NEAR);
    for back in 1..=near {
        if reader.time_at(field, head - back)? <= time {
            return Some(head - back);
        }
    }
    // Then a search of the rest, which are all older than the ones just checked. `low` is
    // always a sample at or before `time`, or the oldest; `high` is always one after `time`.
    let (mut low, mut high) = (oldest, head - near);
    if low == high || reader.time_at(field, low)? > time {
        return None;
    }
    while high - low > 1 {
        let middle = low + (high - low) / 2;
        if reader.time_at(field, middle)? <= time {
            low = middle;
        } else {
            high = middle;
        }
    }
    Some(low)
}

/// The value of `field` at or before `time`.
///
/// A sample that the writer overwrites during the search counts as gone: with the writer far
/// enough ahead to do that, the answer is `Missing`, and the next call searches afresh.
pub fn at_or_before<M: Mem>(reader: &mut Reader<M>, field: FieldId, time: FrameTime) -> Lookup<'_> {
    let Some(lane_seq) = find(reader, field, time) else {
        return Lookup::Missing;
    };
    match reader.at(field, lane_seq) {
        Some(sample) if sample.time == time => Lookup::Hit(sample),
        Some(sample) if sample.time < time => Lookup::Lagged {
            lag_ns: time.saturating_since(sample.time),
            sample,
        },
        _ => Lookup::Missing,
    }
}

/// Whether `field`'s publisher has reached `time`: its newest sample is for that frame or a
/// later one. While it hasn't, a sample for `time` may still be coming.
fn reached<M: Mem>(reader: &Reader<M>, field: FieldId, time: FrameTime) -> bool {
    let head = reader.lane_head(field);
    head > 0
        && reader
            .time_at(field, head - 1)
            .is_some_and(|newest| newest >= time)
}

/// The value of `field` at or before `time`, waiting up to `max_wait` on `doorbell` while the
/// publisher is still on an earlier frame (or has published nothing yet). Returns as soon as
/// the publisher reaches `time`; after `max_wait` it returns what there is, which is then
/// `Lagged` or `Missing`.
///
/// The publisher must ring this reader's doorbell when it publishes, as every publisher with
/// subscribers does.
///
/// # Errors
///
/// If waiting on the doorbell fails.
pub fn wait_at_or_before<'a, M: Mem>(
    reader: &'a mut Reader<M>,
    doorbell: &mut Doorbell,
    field: FieldId,
    time: FrameTime,
    max_wait: Duration,
) -> io::Result<Lookup<'a>> {
    if !reached(reader, field, time) && !max_wait.is_zero() {
        let deadline = Instant::now() + max_wait;
        loop {
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                break;
            }
            // `Rung` can be for another topic, or left over: look again either way.
            doorbell.wait(Some(left), || reached(reader, field, time))?;
            if reached(reader, field, time) {
                break;
            }
        }
    }
    Ok(at_or_before(reader, field, time))
}
