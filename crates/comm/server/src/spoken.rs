//! What an error is told as, to a session at a given additive level.
//!
//! An error added at a level is told only to a session at that level or above. A session
//! below it is told what it was told before the error existed, so a client of an older build
//! never meets a code it does not know (which it would read as `Internal`).

use orion_types::Error;

/// The level from which a session is told [`Error::Restoring`].
pub const RESTORING_LEVEL: u16 = 2;
/// The level from which a session is told [`Error::LeaseLost`].
pub const LEASE_LOST_LEVEL: u16 = 2;
/// The level from which a session is told [`Error::Layout`].
pub const LAYOUT_LEVEL: u16 = 2;
/// The level from which a session is told [`Error::RunGone`].
pub const RUN_GONE_LEVEL: u16 = 3;
/// The level from which a session is told [`Error::Stale`].
pub const STALE_LEVEL: u16 = 7;

/// `error` as a session at `level` is told it.
#[must_use]
pub const fn spoken(error: Error, level: u16) -> Error {
    match error {
        // Before the level, a lookup in the gap after a restart found nothing.
        Error::Restoring if level < RESTORING_LEVEL => Error::NotFound,
        // Before the level, a lease another holds was only ever busy.
        Error::LeaseLost if level < LEASE_LOST_LEVEL => Error::Busy,
        // Before the level, a ring that does not read right was only ever malformed.
        Error::Layout if level < LAYOUT_LEVEL => Error::Malformed,
        // Before the level, a run that is not there was only ever not found.
        Error::RunGone if level < RUN_GONE_LEVEL => Error::NotFound,
        // Before the level no write said what it was based on. One that carried a stamp all
        // the same is told the nearest thing it knows.
        Error::Stale if level < STALE_LEVEL => Error::AlreadyExists,
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn restoring_is_told_from_its_level_and_not_found_below_it() {
        assert_eq!(spoken(Error::Restoring, RESTORING_LEVEL), Error::Restoring);
        assert_eq!(
            spoken(Error::Restoring, RESTORING_LEVEL + 1),
            Error::Restoring
        );
        assert_eq!(
            spoken(Error::Restoring, RESTORING_LEVEL - 1),
            Error::NotFound
        );
        assert_eq!(spoken(Error::Restoring, 0), Error::NotFound);
        // Nothing else changes.
        for error in Error::ALL {
            if !matches!(
                error,
                Error::Restoring | Error::LeaseLost | Error::Layout | Error::RunGone | Error::Stale
            ) {
                assert_eq!(spoken(*error, 0), *error);
            }
        }
    }

    #[test]
    fn a_lost_lease_is_told_from_its_level_and_busy_below_it() {
        assert_eq!(spoken(Error::LeaseLost, LEASE_LOST_LEVEL), Error::LeaseLost);
        assert_eq!(spoken(Error::LeaseLost, LEASE_LOST_LEVEL - 1), Error::Busy);
        assert_eq!(spoken(Error::LeaseLost, 0), Error::Busy);
    }

    #[test]
    fn another_layout_is_told_from_its_level_and_malformed_below_it() {
        assert_eq!(spoken(Error::Layout, LAYOUT_LEVEL), Error::Layout);
        assert_eq!(spoken(Error::Layout, LAYOUT_LEVEL - 1), Error::Malformed);
        assert_eq!(spoken(Error::Layout, 0), Error::Malformed);
    }

    #[test]
    fn a_run_gone_is_told_from_its_level_and_not_found_below_it() {
        assert_eq!(spoken(Error::RunGone, RUN_GONE_LEVEL), Error::RunGone);
        assert_eq!(spoken(Error::RunGone, RUN_GONE_LEVEL - 1), Error::NotFound);
        assert_eq!(spoken(Error::RunGone, 0), Error::NotFound);
        assert_eq!(spoken(Error::Stale, STALE_LEVEL), Error::Stale);
        assert_eq!(spoken(Error::Stale, STALE_LEVEL - 1), Error::AlreadyExists);
    }

    #[test]
    fn the_level_is_one_this_build_speaks() {
        const { assert!(RESTORING_LEVEL <= orion_wire::INTERFACE_LEVEL) }
        const { assert!(LEASE_LOST_LEVEL <= orion_wire::INTERFACE_LEVEL) }
        const { assert!(LAYOUT_LEVEL <= orion_wire::INTERFACE_LEVEL) }
        const { assert!(RUN_GONE_LEVEL <= orion_wire::INTERFACE_LEVEL) }
        const { assert!(STALE_LEVEL <= orion_wire::INTERFACE_LEVEL) }
    }
}
