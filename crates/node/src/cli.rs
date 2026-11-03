//! Command-line arguments of `orion-node`. The node is configured from `ORION_NODE_*` environment
//! variables, so the arguments only print the version or the usage. Any other argument is an
//! error that exits before the node starts, so a mistyped flag never starts a second node.

use std::ffi::OsString;

pub(crate) const USAGE: &str = "\
usage: orion-node [-V | --version | -h | --help]

Starts an Orion node. The configuration comes from ORION_NODE_* environment variables
(see docs/node-env.md); the node takes no other arguments.

  -V, --version   print the version and exit
  -h, --help      print this usage and exit";

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Action {
    /// Start the node with the configuration from the environment.
    Run,
    Version,
    Help,
    /// An argument the node does not take (the node does not start).
    Unexpected(String),
}

/// Parses the arguments after the program name.
pub(crate) fn parse_args<I>(args: I) -> Action
where
    I: IntoIterator<Item = OsString>,
{
    let mut args = args.into_iter();
    let Some(first) = args.next() else {
        return Action::Run;
    };
    if let Some(extra) = args.next() {
        return Action::Unexpected(extra.to_string_lossy().into_owned());
    }
    match first.to_str() {
        Some("-V" | "--version") => Action::Version,
        Some("-h" | "--help") => Action::Help,
        _ => Action::Unexpected(first.to_string_lossy().into_owned()),
    }
}

#[cfg(test)]
mod tests {
    use super::{Action, parse_args};
    use std::ffi::OsString;

    fn parse(args: &[&str]) -> Action {
        parse_args(args.iter().map(OsString::from))
    }

    #[test]
    fn no_arguments_runs_the_node() {
        assert_eq!(parse(&[]), Action::Run);
    }

    #[test]
    fn version_and_help_flags_are_recognized() {
        assert_eq!(parse(&["--version"]), Action::Version);
        assert_eq!(parse(&["-V"]), Action::Version);
        assert_eq!(parse(&["--help"]), Action::Help);
        assert_eq!(parse(&["-h"]), Action::Help);
    }

    #[test]
    fn unknown_arguments_are_unexpected_even_next_to_a_known_flag() {
        assert_eq!(parse(&["--bogus"]), Action::Unexpected("--bogus".into()));
        assert_eq!(parse(&["run"]), Action::Unexpected("run".into()));
        assert_eq!(
            parse(&["--version", "extra"]),
            Action::Unexpected("extra".into())
        );
        assert_eq!(
            parse(&["--help", "--version"]),
            Action::Unexpected("--version".into())
        );
        assert_eq!(parse(&["--"]), Action::Unexpected("--".into()));
    }
}
