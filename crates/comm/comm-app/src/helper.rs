//! The helper programs the comm core starts are the comm core's own program, started again
//! with a word that says which helper to be. This is where those words are known.

use std::ffi::OsString;

/// If `args`, a program's arguments after its name, ask for one of the helpers, runs it
/// and gives its exit code. `None` if they ask for something else: the program goes on to
/// be what it is.
///
/// A binary that holds the comm core calls this first thing in `main`.
#[must_use]
pub fn helper(args: impl IntoIterator<Item = OsString>) -> Option<i32> {
    let mut args = args.into_iter();
    let mode = args.next()?;
    match mode.to_str()? {
        // A session: becomes the user who logged in, then their shell.
        orion_ssh::helper::MODE => Some(orion_ssh::helper::main(args)),
        // The sftp subsystem, as the user.
        #[cfg(feature = "ssh-sftp")]
        orion_ssh_sftp::MODE => Some(orion_ssh_sftp::main()),
        // One SSH connection, read with no rights.
        #[cfg(feature = "ssh-sandbox")]
        orion_ssh_sandbox::child::MODE => Some(orion_ssh_sandbox::child::main()),
        _ => None,
    }
}
