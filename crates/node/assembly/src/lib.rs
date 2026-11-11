//! Node logic as one entry point.
//!
//! [`run_node`] is what the `node` mode of the binary calls. It reads the settings, from the
//! same file the comm core reads and from the environment, puts every part of node logic
//! together, and runs until the service manager says to stop:
//!
//! | Part | What it is |
//! | --- | --- |
//! | records | the store, saved to the state directory, with watches |
//! | resources | who provides what, lost when its provider goes, held by leases |
//! | actions | the open list, detached runs, runs carried to other nodes |
//! | files | received in chunks, handed to receivers, fetched from a URL |
//! | workloads | what should run, on which executor, started when its files are here |
//! | audit | who did what, in a capped file |
//!
//! All of it is one process with one thread, connected to the comm core as its first client.
//! The parts that would hold that thread up each have a helper that exists while it works:
//! saving, hashing, fetching, and the session to another node.
//!
//! The settings file is shared with the comm core, so each program is told keys that are the
//! other's. Only a key that is under one of node logic's own names and was not read is
//! reported: that one is a typo.

#![cfg(target_os = "linux")]

use std::path::Path;

use orion_config::{ConfigError, Source, Warning};
use tracing::warn;

pub use orion_node_app::{Node, NodeSettings, StartError, run_until};

/// The names node logic's own settings are under. A key under one of these that nobody read
/// is reported; any other is the comm core's.
pub const OWN: &[&str] = &["records.", "files.", "audit.", "workloads."];

/// Why node logic did not run.
#[derive(Debug)]
pub enum RunError {
    /// The settings could not be read.
    Config(ConfigError),
    /// Node logic could not start.
    Start(StartError),
}

impl std::fmt::Display for RunError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Config(error) => write!(f, "{}: {}", error.key, error.message),
            Self::Start(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for RunError {}

/// Takes node logic's settings out of `source`, and the keys under its own names that
/// nobody read.
///
/// # Errors
///
/// A value that doesn't parse.
pub fn settings(source: &mut Source) -> Result<(NodeSettings, Vec<Warning>), ConfigError> {
    let settings = NodeSettings::from_source(source)?;
    let unknown = source
        .warnings()
        .into_iter()
        .filter(|warning| {
            let key = warning.key.to_ascii_lowercase().replace('_', ".");
            let key = key.strip_prefix("orion.").unwrap_or(&key);
            OWN.iter().any(|own| key.starts_with(own))
        })
        .collect();
    Ok((settings, unknown))
}

/// Node logic as a program: reads the settings file at `path`, if there is one, and the
/// environment, and runs until `SIGTERM` or `SIGINT`.
///
/// # Errors
///
/// If the settings cannot be read, or node logic cannot start.
pub fn run_node(path: Option<&Path>) -> Result<(), RunError> {
    let mut source = Source::load(path, std::env::vars()).map_err(RunError::Config)?;
    let (settings, unknown) = settings(&mut source).map_err(RunError::Config)?;
    for warning in &unknown {
        warn!("{warning}");
    }
    orion_node_app::run(settings, &[]).map_err(RunError::Start)
}
