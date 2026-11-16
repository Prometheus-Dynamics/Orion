//! A socket path no Unix socket can be at is said as such, before anything is tried, and is
//! never taken for a connection that may come back.

#![cfg(target_os = "linux")]

use std::path::PathBuf;

use orion_connect::{Address, ClientError, Options, SOCKET_PATH_MAX, Session, socket_path};

/// A path of exactly `len` bytes under a directory that is not there.
fn path_of(len: usize) -> PathBuf {
    let head = "/nonexistent-orion-path-test/";
    PathBuf::from(format!("{head}{}", "s".repeat(len - head.len())))
}

#[test]
fn a_path_too_long_for_a_socket_is_refused_with_its_length_and_never_lost() {
    // 107 bytes fit; 108 do not (sun_path is 108 with its NUL).
    assert_eq!(SOCKET_PATH_MAX, 107);
    assert!(socket_path(&path_of(107)).is_ok());
    let long = path_of(108);
    let error = socket_path(&long).expect_err("too long");
    assert!(
        matches!(&error, ClientError::PathTooLong { path, len: 108 } if *path == long),
        "{error:?}"
    );

    let error = Session::connect(&Address::Local(long.clone()), "tool", Options::default())
        .expect_err("too long");
    assert!(
        matches!(error, ClientError::PathTooLong { len: 108, .. }),
        "{error:?}"
    );
    // Connecting again cannot help: a loop that tries again while `is_lost` stops here.
    assert!(!error.is_lost());
    let words = error.to_string();
    assert!(
        words.contains(&long.display().to_string())
            && words.contains("108")
            && words.contains("107"),
        "{words}"
    );

    // One that fits is tried, and what is not there is a connection that may come.
    let error = Session::connect(&Address::Local(path_of(107)), "tool", Options::default())
        .expect_err("nothing there");
    assert!(error.is_lost(), "{error:?}");
}
