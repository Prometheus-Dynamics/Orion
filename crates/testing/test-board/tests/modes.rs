//! What Orion makes in a root shared by a group: directories as files, by what the process's
//! umask leaves, so the group that shares the root can write in a directory Orion made.
//!
//! A umask is the whole process's, so the test runs itself again as a child, and sets the
//! umask a unit sets (`UMask=0007`) there, before anything else runs.

use std::io::Cursor;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::Path;
use std::process::Command;
use std::time::Duration;

use orion::actions::{ActionCall, Outcome, Target};
use orion::files::FileState;
use orion_scratch::Scratch;
use orion_test_board::{Options, TestBoard};
use rustix::fs::Mode;

/// Set when the test runs again as the child.
const INSIDE: &str = "ORION_MODES_INSIDE";
/// What the child says when it got to the end.
const DONE: &str = "modes: directories made as files are, group and all";

fn own_test(name: &str) -> String {
    match module_path!().split_once("::") {
        Some((_, module)) => format!("{module}::{name}"),
        None => name.to_owned(),
    }
}

/// The permission bits of `path`, with setuid, setgid and sticky.
fn mode(path: &Path) -> u32 {
    std::fs::metadata(path).expect("there").mode() & 0o7777
}

fn inside() {
    rustix::process::umask(Mode::from_raw_mode(0o007));
    // The root as an image makes one for an application and Orion to share: the group's,
    // setgid, so what is made in it is the group's too.
    let app = Scratch::new("modes-app");
    std::fs::set_permissions(&*app, std::fs::Permissions::from_mode(0o2770)).expect("chmod");
    let board = TestBoard::start_with(
        "modes",
        Options {
            name: "modes".to_owned(),
            conf: format!("files.root.app = {} rw\n", app.display()),
            ..Options::default()
        },
    );
    let orion = board.connect("tool").expect("connect");

    // A directory made by the action.
    let made = orion
        .run(&ActionCall {
            name: "files.mkdir",
            target: Target::Node("modes".parse().expect("a node")),
            args: b"roots/app/made",
            deadline_ms: 0,
            detached: false,
        })
        .expect("run")
        .finish(Duration::from_secs(20))
        .expect("finished");
    assert_eq!(made.outcome, Outcome::Succeeded, "{}", made.detail);
    assert_eq!(mode(&app.join("made")), 0o2770, "the action's directory");

    // Directories made on the way to a pushed file, and the file.
    let pushed = orion
        .files()
        .push(
            "roots/app/a/b/file",
            &mut Cursor::new(b"data".to_vec()),
            |_, _| {},
        )
        .expect("push");
    assert_eq!(pushed.state, FileState::Committed, "{}", pushed.detail);
    assert_eq!(mode(&app.join("a")), 0o2770, "a directory on the way");
    assert_eq!(mode(&app.join("a/b")), 0o2770, "a directory on the way");
    assert_eq!(mode(&app.join("a/b/file")), 0o660, "the file");

    // The reverse: a directory the application made, as the group's; Orion writes in it.
    std::fs::create_dir(app.join("theirs")).expect("mkdir");
    assert_eq!(mode(&app.join("theirs")), 0o2770);
    let pushed = orion
        .files()
        .push(
            "roots/app/theirs/file",
            &mut Cursor::new(b"data".to_vec()),
            |_, _| {},
        )
        .expect("push");
    assert_eq!(pushed.state, FileState::Committed, "{}", pushed.detail);
    assert_eq!(mode(&app.join("theirs/file")), 0o660);
    println!("{DONE}");
}

#[test]
fn what_is_made_in_a_root_the_group_shares_is_the_groups_to_write() {
    if std::env::var_os(INSIDE).is_some() {
        inside();
        return;
    }
    let ran = Command::new(std::env::current_exe().expect("this test"))
        .args([
            "--exact",
            own_test("what_is_made_in_a_root_the_group_shares_is_the_groups_to_write").as_str(),
            "--nocapture",
        ])
        .env(INSIDE, "1")
        .output()
        .expect("the test again, as a child");
    let said = format!(
        "{}{}",
        String::from_utf8_lossy(&ran.stdout),
        String::from_utf8_lossy(&ran.stderr)
    );
    assert!(ran.status.success(), "{said}");
    assert!(said.contains(DONE), "{said}");
}
