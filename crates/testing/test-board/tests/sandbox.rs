//! A service manager's sandbox that refuses `openat2`, as systemd's does for a unit with
//! `RestrictSUIDSGID=yes`: nothing under a root can be opened, and the board says why, by
//! name, in every refusal and in its hello. On a board this showed as "Function not
//! implemented" on every push, fetch and listing in a root.
//!
//! The test runs itself again under a seccomp filter of its own that answers `openat2` with
//! "not implemented" and lets every other call through. `bwrap` loads the filter. Where
//! `bwrap` cannot be run it says so and passes: that it cannot is not this test's to decide.

use std::io::Cursor;
use std::process::{Command, Stdio};

use orion::files::FileState;
use orion_scratch::Scratch;
use orion_test_board::{Options, TestBoard};

/// Set when the test runs again inside the sandbox.
const INSIDE: &str = "ORION_SANDBOX_INSIDE";
/// What the run inside says when it got to the end.
const DONE: &str = "sandbox: openat2 refused, and said so by name";
/// The number of `openat2`, the same on every architecture Linux has added since.
const OPENAT2: u32 = 437;
/// "Function not implemented".
const ENOSYS: u32 = 38;

fn own_test(name: &str) -> String {
    match module_path!().split_once("::") {
        Some((_, module)) => format!("{module}::{name}"),
        None => name.to_owned(),
    }
}

/// The filter, as the kernel takes it: the call's number is loaded, `openat2` is answered
/// with `ENOSYS`, and every other call is allowed.
fn filter() -> Vec<u8> {
    let instruction = |code: u16, jump_true: u8, jump_false: u8, value: u32| {
        let mut bytes = Vec::with_capacity(8);
        bytes.extend_from_slice(&code.to_ne_bytes());
        bytes.extend_from_slice(&[jump_true, jump_false]);
        bytes.extend_from_slice(&value.to_ne_bytes());
        bytes
    };
    [
        // Load the number of the call.
        instruction(0x20, 0, 0, 0),
        // If it is openat2 go on, else skip one.
        instruction(0x15, 0, 1, OPENAT2),
        // Answer with the error.
        instruction(0x06, 0, 0, 0x0005_0000 | ENOSYS),
        // Allow.
        instruction(0x06, 0, 0, 0x7fff_0000),
    ]
    .concat()
}

fn inside() {
    let app = Scratch::new("sandbox-app");
    std::fs::write(app.join("there.txt"), b"there").expect("a file in the root");
    let board = TestBoard::start_with(
        "sandbox",
        Options {
            name: "sandbox".to_owned(),
            conf: format!("files.root.app = {} rw\n", app.display()),
            ..Options::default()
        },
    );
    let orion = board.connect("tool").expect("connect");
    let named = orion_file_store::NO_OPENAT2;

    // A push into the root: rejected, by name.
    let pushed = orion
        .files()
        .push(
            "roots/app/new/a.txt",
            &mut Cursor::new(b"data".to_vec()),
            |_, _| {},
        )
        .expect("answered");
    assert_eq!(pushed.state, FileState::Rejected);
    assert!(pushed.detail.contains(named), "{}", pushed.detail);

    // A fetch of a file that is there, and of the root's index: not given, by name.
    for name in ["roots/app/there.txt", "roots/app/index"] {
        let mut into = Cursor::new(Vec::new());
        let said = match orion.files().fetch(name, &mut into, 0, |_, _| {}) {
            Ok(transfer) => {
                assert_ne!(transfer.state, FileState::Committed, "{name} was given");
                transfer.detail
            }
            Err(error) => error.to_string(),
        };
        assert!(said.contains(named), "{name}: {said}");
    }

    // And where a person looks first: the board's hello, once node logic has said it.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let node = orion.node().expect("node");
        let fact = node.facts.iter().find(|fact| fact.name == "files");
        if let Some(fact) = fact {
            assert_eq!(fact.value, named.as_bytes());
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "no files fact: {:?}",
            node.facts
        );
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    println!("{DONE}");
}

/// Why a process cannot be run under a seccomp filter here, if it cannot.
fn cannot(filter: &std::path::Path) -> Option<String> {
    let made = Command::new("sh")
        .args([
            "-c",
            r#"exec bwrap --dev-bind / / --seccomp 3 -- true 3<"$0""#,
        ])
        .arg(filter)
        .stdout(Stdio::null())
        .output();
    match made {
        Err(error) => Some(format!("sh could not be run: {error}")),
        Ok(out) if !out.status.success() => Some(format!(
            "bwrap cannot load a seccomp filter here: {}",
            String::from_utf8_lossy(&out.stderr).trim()
        )),
        Ok(_) => None,
    }
}

#[test]
fn where_openat2_is_refused_the_board_says_so_by_name_and_not_function_not_implemented() {
    if std::env::var_os(INSIDE).is_some() {
        inside();
        return;
    }
    // Without the sandbox, nothing is refused and nothing is said: a root works.
    assert_eq!(orion_file_store::probe(std::path::Path::new("/")), Ok(()));

    let dir = Scratch::new("sandbox-filter");
    let filter_file = dir.join("filter.bin");
    std::fs::write(&filter_file, filter()).expect("the filter");
    if let Some(why) = cannot(&filter_file) {
        println!("{why}: the sandbox was not tried");
        return;
    }
    let ran = Command::new("sh")
        .args(["-c", r#"filter="$0"; exec bwrap --dev-bind / / --seccomp 3 -- "$@" 3<"$filter""#])
        .arg(&filter_file)
        .arg(std::env::current_exe().expect("this test"))
        .args([
            "--exact",
            own_test(
                "where_openat2_is_refused_the_board_says_so_by_name_and_not_function_not_implemented",
            )
            .as_str(),
            "--nocapture",
        ])
        .env(INSIDE, "1")
        .output()
        .expect("the test again, inside");
    let said = format!(
        "{}{}",
        String::from_utf8_lossy(&ran.stdout),
        String::from_utf8_lossy(&ran.stderr)
    );
    assert!(ran.status.success(), "{said}");
    assert!(said.contains(DONE), "{said}");
}
