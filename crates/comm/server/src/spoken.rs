//! What an error is told as, to a session at a given additive level.
//!
//! An error added at a level is told only to a session at that level or above. A session
//! below it is told what it was told before the error existed, so a client of an older build
//! never meets a code it does not know (which it would read as `Internal`).

use orion_types::Error;

/// The level from which a session is told [`Error::Restoring`].
pub const RESTORING_LEVEL: u16 = 2;

/// `error` as a session at `level` is told it.
#[must_use]
pub const fn spoken(error: Error, level: u16) -> Error {
    match error {
        // Before the level, a lookup in the gap after a restart found nothing.
        Error::Restoring if level < RESTORING_LEVEL => Error::NotFound,
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
            if *error != Error::Restoring {
                assert_eq!(spoken(*error, 0), *error);
            }
        }
    }

    #[test]
    fn the_level_is_one_this_build_speaks() {
        const { assert!(RESTORING_LEVEL <= orion_wire::INTERFACE_LEVEL) }
    }
}
