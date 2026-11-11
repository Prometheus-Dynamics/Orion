//! Commands that die with the executor.
//!
//! Without this, a command outlives an executor that is killed, and the executor that comes
//! after it, knowing nothing of the command, is asked to start it again: two copies. The
//! kernel is asked to send the command `SIGKILL` when the thread that started it ends
//! (`PR_SET_PDEATHSIG`), which for an executor is when the executor does.

#![allow(unsafe_code)]

use std::os::unix::process::CommandExt;
use std::process::Command;

use rustix::process::{Signal, getpid, getppid, set_parent_process_death_signal};

/// Makes the command `command` will start die with the thread that starts it.
pub(crate) fn dies_with_this_process(command: &mut Command) {
    let parent = getpid();
    let in_child = move || {
        set_parent_process_death_signal(Some(Signal::KILL))?;
        // The executor may have died between the fork and the line above, and then nothing
        // would ever send the signal. The command is not started.
        if getppid() == Some(parent) {
            Ok(())
        } else {
            Err(std::io::Error::from(rustix::io::Errno::SRCH))
        }
    };
    // SAFETY: `pre_exec` runs the closure in the child after `fork` and before `exec`, where
    // only async-signal-safe work is allowed. It makes two system calls (`prctl` and `getppid`)
    // and allocates nothing: `std::io::Error::from(Errno)` is a
    // number, not a box.
    unsafe {
        command.pre_exec(in_child);
    }
}
