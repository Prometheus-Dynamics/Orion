//! Why the comm core could not start.

use std::fmt;

/// Why the comm core could not start.
#[derive(Debug)]
pub struct StartError(pub(crate) String);

impl fmt::Display for StartError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for StartError {}

pub(crate) fn failed(what: impl fmt::Display, why: impl fmt::Display) -> StartError {
    StartError(format!("{what}: {why}"))
}
