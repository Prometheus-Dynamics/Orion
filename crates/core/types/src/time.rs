//! The time a sample belongs to is the ring family's: one type for the whole stack.

/// The time a sample belongs to: nanoseconds of `CLOCK_MONOTONIC` on the board that took it.
///
/// A sample and everything computed from it carry the same value, which is what makes "the
/// value at or before this sample" a lookup. The monotonic clock is never stepped, so a lookup
/// does not jump when time sync sets the wall clock. A sample carried to another board is moved
/// onto that board's monotonic clock on the way in; `orion.time` says how far each board's wall
/// clock is from its monotonic one, for whoever wants a sample's wall time. `now()` reads it on
/// a Unix target.
pub use shm_sample_time::SampleTime;
