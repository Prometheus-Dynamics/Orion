//! A safe Rust API over `liborion`'s C surface (docs/capi.md), linked one of two ways:
//!
//! - `static` (the default): the library's Rust core (`orion-capi`) is compiled into the program.
//!   Nothing to install; the program carries the version it was built with.
//! - `shared`: the program links `liborion.so.1` (`-lorion`; the directory in `LIBORION_LIB_DIR`
//!   at build time if set) and runs with whichever compatible library is installed.
//!
//! Pick one: `shared` needs `default-features = false`. With both on (`--all-features`, as the
//! workspace's CI builds), `static` wins, as with the opt-in allocators; with neither the build
//! fails. Both modes call the same `extern "C"` functions, so a program behaves the same either
//! way; [`Link::connect`] and [`Latest::connect`] check the library's version first
//! (`orion_check_version`).

#![cfg(unix)]

#[cfg(not(any(feature = "static", feature = "shared")))]
compile_error!("liborion-rs: enable `static` (the default) or `shared`");

// The static build links the core, which defines the C functions `sys` declares.
#[cfg(feature = "static")]
use orion_capi as _;

pub mod sys;

mod event;
mod latest;
mod link;

pub use event::{Event, EventKind, Value};
pub use latest::{Frame, FrameStatus, Latest};
pub use link::{Link, Subject};
pub use sys::{AbiVersion, Status};

use std::ffi::{CString, c_void};
use std::fmt;

/// A failed call: the library's status and the text of its last failure on this thread.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Error {
    /// The call's status: a negative errno or one of the shared codes.
    pub status: Status,
    /// What `orion_last_error` said.
    pub message: String,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.message.is_empty() {
            write!(f, "liborion: status {:?}", self.status)
        } else {
            write!(f, "liborion: {} ({:?})", self.message, self.status)
        }
    }
}

impl std::error::Error for Error {}

/// `Ok` for `OK`, else the status with this thread's last error text.
pub(crate) fn check(status: Status) -> Result<(), Error> {
    if status == Status::OK {
        Ok(())
    } else {
        Err(error(status))
    }
}

pub(crate) fn error(status: Status) -> Error {
    Error {
        status,
        message: last_error(),
    }
}

/// An argument error raised before any call (a string with a NUL in it).
pub(crate) fn invalid(what: &str) -> Error {
    Error {
        status: Status::INVALID_ARGUMENT,
        message: format!("{what} contains a NUL byte"),
    }
}

pub(crate) fn c_string(text: &str, what: &str) -> Result<CString, Error> {
    CString::new(text).map_err(|_| invalid(what))
}

pub(crate) fn c_option(text: Option<&str>, what: &str) -> Result<Option<CString>, Error> {
    text.map(|text| c_string(text, what)).transpose()
}

pub(crate) fn ptr(text: &Option<CString>) -> *const core::ffi::c_char {
    text.as_ref()
        .map_or(core::ptr::null(), |text| text.as_ptr())
}

extern "C" fn append(context: *mut c_void, text: *const u8, len: usize) {
    // SAFETY: `context` is the `String` `last_error` passed, alive for the call; the library
    // passes `len` valid bytes at `text`.
    let (out, bytes) = unsafe {
        (
            &mut *context.cast::<String>(),
            core::slice::from_raw_parts(text, len),
        )
    };
    out.push_str(&String::from_utf8_lossy(bytes));
}

/// The text of this thread's last failure in the library.
pub fn last_error() -> String {
    let mut text = String::new();
    let sink = sys::MessageSink {
        context: (&raw mut text).cast(),
        write: Some(append),
    };
    // SAFETY: the sink writes into `text`, which outlives the call.
    let _ = unsafe { sys::orion_last_error(sink) };
    text
}

/// The library's version.
pub fn version() -> AbiVersion {
    // SAFETY: no arguments.
    unsafe { sys::orion_version() }
}

/// Whether the library serves this wrapper: `OK`, or `VERSION_MISMATCH` (a different major, an
/// older minor, or other layouts).
pub fn check_version() -> Result<(), Error> {
    let mine = AbiVersion::new(
        sys::ORION_ABI_MAJOR,
        sys::ORION_ABI_MINOR,
        sys::ORION_LAYOUT_HASH,
    );
    // SAFETY: a valid version.
    let status = unsafe { sys::orion_check_version(&mine) };
    if status == Status::OK {
        Ok(())
    } else {
        let library = version();
        Err(Error {
            status,
            message: format!(
                "liborion {}.{} (layout {:#x}) does not serve this wrapper ({}.{}, {:#x})",
                library.major, library.minor, library.layout, mine.major, mine.minor, mine.layout
            ),
        })
    }
}

/// Milliseconds for a timeout argument: `None` waits as long as it takes (-1).
pub(crate) fn timeout_ms(timeout: Option<std::time::Duration>) -> i32 {
    timeout.map_or(-1, |timeout| {
        i32::try_from(timeout.as_millis()).unwrap_or(i32::MAX)
    })
}
