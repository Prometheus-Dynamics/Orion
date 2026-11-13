//! A shell on the board, over the session: what SSH gives a person, for a tool on any address
//! the session reaches, a USB link included.
//!
//! A shell is opened as an account, runs as that account (the same helper as an SSH login),
//! and is carried in both directions under credit: each side may have at most its window of
//! bytes unacknowledged in flight, and says how much it has taken with [`ShellCredit`]. A shell
//! that prints faster than the caller reads waits; it never fills the session, so the
//! session's other calls and topics go on beside it.

use orion_wire::{Bytes, body, kind};

body! {
    /// Opens a shell. Answered with [`ShellOpened`], or refused.
    pub struct ShellOpen<'a> = kind::SHELL_OPEN {
        /// The account to run as.
        pub account: &'a str,
        /// The command for the account's shell to run. Empty for a login shell.
        pub command: Bytes<'a>,
        /// The terminal's kind, as `TERM` names it. Empty for no terminal: then the output and
        /// the errors are apart, and the end of input is the end of the program's input.
        pub term: &'a str,
        /// The terminal's width in characters.
        pub cols: u32,
        /// Its height.
        pub rows: u32,
        /// How many bytes of output the board may send before the caller gives credit.
        pub window: u32,
    }
}

body! {
    /// A shell is open.
    pub struct ShellOpened = kind::SHELL_OPENED {
        /// The `seq` of the request.
        pub request_seq: u16,
        /// Its number on this session.
        pub shell: u32,
        /// How many bytes of input the caller may send before the board gives credit.
        pub window: u32,
    }
}

body! {
    /// Bytes of a shell: input from the caller, output or errors from the board.
    pub struct ShellData<'a> = kind::SHELL_DATA {
        /// The shell.
        pub shell: u32,
        /// 0: input to the board, or output from it. 1: errors, from the board, apart only
        /// without a terminal.
        pub stream: u8,
        /// The bytes.
        pub data: Bytes<'a>,
    }
}

body! {
    /// The sender has taken `bytes` more of what the other side sent: it may send that many
    /// more.
    pub struct ShellCredit = kind::SHELL_CREDIT {
        /// The shell.
        pub shell: u32,
        /// How many bytes.
        pub bytes: u32,
    }
}

body! {
    /// The caller's terminal changed size.
    pub struct ShellResize = kind::SHELL_RESIZE {
        /// The shell.
        pub shell: u32,
        /// Width in characters.
        pub cols: u32,
        /// Height.
        pub rows: u32,
    }
}

body! {
    /// The caller will send no more input.
    pub struct ShellEof = kind::SHELL_EOF {
        /// The shell.
        pub shell: u32,
    }
}

body! {
    /// The shell ended, after the last of its output.
    pub struct ShellExit = kind::SHELL_EXIT {
        /// The shell.
        pub shell: u32,
        /// Its exit code; 255 when it was ended without one.
        pub code: u32,
    }
}

body! {
    /// The caller is done with the shell: it is ended, whatever it was doing.
    pub struct ShellClose = kind::SHELL_CLOSE {
        /// The shell.
        pub shell: u32,
    }
}
