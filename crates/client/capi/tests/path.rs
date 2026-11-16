//! A socket path too long for a Unix socket is the caller's mistake: `ORION_E_ARGUMENT`, with
//! words that name it, never `ORION_E_LOST`.

#![cfg(target_os = "linux")]

use std::ffi::{CStr, CString};

#[test]
fn a_socket_path_too_long_is_an_argument_error() {
    let path = format!("unix:/nonexistent-orion-capi/{}", "s".repeat(120));
    let address = CString::new(path).expect("address");
    let mut out = std::ptr::null_mut();
    // SAFETY: both strings are NUL-terminated and `out` has room for one pointer.
    let status =
        unsafe { orion_c::orion_connect(address.as_ptr(), c"tool".as_ptr(), 100, &raw mut out) };
    assert_eq!(status, orion_c::ORION_E_ARGUMENT);
    assert!(out.is_null());
    // SAFETY: the library's own NUL-terminated string.
    let said = unsafe { CStr::from_ptr(orion_c::orion_last_error()) }.to_string_lossy();
    assert!(said.contains("144 bytes") && said.contains("107"), "{said}");
}
