//! The `orion-node` binary's arguments: `--version` and `--help` exit 0 without starting a node,
//! and any other argument exits 2 before anything is started.

use std::process::{Command, Output, Stdio};
use std::time::{Duration, Instant};

/// Runs the binary and waits for it. A binary that starts a node instead of exiting is killed
/// (its own child) after a few seconds and the test fails.
fn run(args: &[&str]) -> Output {
    let mut child = Command::new(env!("CARGO_BIN_EXE_orion-node"))
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn orion-node");
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if child.try_wait().expect("poll orion-node").is_some() {
            return child.wait_with_output().expect("collect orion-node output");
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("orion-node {args:?} did not exit; it probably started a node");
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn version_flags_print_the_version_and_exit_zero() {
    for flag in ["--version", "-V"] {
        let output = run(&[flag]);
        assert_eq!(output.status.code(), Some(0), "{flag}");
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert_eq!(
            stdout.trim(),
            format!("orion-node {}", env!("CARGO_PKG_VERSION")),
            "{flag}"
        );
        assert!(output.stderr.is_empty(), "{flag} wrote to stderr");
    }
}

#[test]
fn help_flags_print_usage_and_exit_zero() {
    for flag in ["--help", "-h"] {
        let output = run(&[flag]);
        assert_eq!(output.status.code(), Some(0), "{flag}");
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(stdout.starts_with("usage: orion-node"), "{flag}: {stdout}");
        assert!(output.stderr.is_empty(), "{flag} wrote to stderr");
    }
}

#[test]
fn unknown_arguments_exit_two_without_starting_a_node() {
    for args in [&["--bogus"][..], &["--version", "extra"], &["run"]] {
        let output = run(args);
        assert_eq!(output.status.code(), Some(2), "{args:?}");
        assert!(output.stdout.is_empty(), "{args:?} wrote to stdout");
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(stderr.contains("unexpected argument"), "{args:?}: {stderr}");
        assert!(stderr.contains("usage: orion-node"), "{args:?}: {stderr}");
    }
}
