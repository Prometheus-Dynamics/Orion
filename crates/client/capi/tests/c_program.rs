//! A C program publishes and subscribes, against the static library and the shared one.

use std::path::{Path, PathBuf};
use std::process::Command;

use orion_stubs::Board;

/// The functions the library exports, which must be exactly the header's.
const SURFACE: [&str; 15] = [
    "orion_check_version",
    "orion_last_error",
    "orion_connect",
    "orion_reconnect",
    "orion_pump",
    "orion_close",
    "orion_declare",
    "orion_publish",
    "orion_publisher_subscribers",
    "orion_publisher_free",
    "orion_subscribe",
    "orion_wait",
    "orion_next",
    "orion_latest",
    "orion_subscriber_free",
];

fn manifest() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

/// Where cargo put a library: beside this test when it built it for the test, or one
/// directory up when it was built on its own.
fn library(name: &str) -> PathBuf {
    let test = std::env::current_exe().expect("this test");
    let beside = test.parent().expect("a directory");
    [beside, beside.parent().expect("the target directory")]
        .iter()
        .map(|dir| dir.join(name))
        .find(|path| path.exists())
        .unwrap_or_else(|| panic!("{name} is not in {}", beside.display()))
}

fn compile(out: &Path, link: &[&str]) {
    let status = Command::new("cc")
        .args(["-std=c11", "-Wall", "-Wextra", "-Werror", "-O1"])
        .arg("-I")
        .arg(manifest().join("include"))
        .arg(manifest().join("tests/c/pubsub.c"))
        .args(link)
        .arg("-o")
        .arg(out)
        .status()
        .expect("a C compiler");
    assert!(status.success(), "the C program does not build");
}

#[test]
fn the_header_and_the_library_have_the_same_surface() {
    let header = std::fs::read_to_string(manifest().join("include/orion.h")).expect("the header");
    for function in SURFACE {
        assert!(
            header.contains(&format!("{function}(")),
            "{function} is not in the header"
        );
    }
    // Every function the header declares is one the library has.
    let declared = header.matches("orion_").count();
    assert!(declared >= SURFACE.len());
    for line in header.lines().filter(|line| {
        line.contains("orion_") && line.contains('(') && !line.trim_start().starts_with(['*', '/'])
    }) {
        let name: String = line[line.find("orion_").expect("checked")..]
            .chars()
            .take_while(|c| c.is_alphanumeric() || *c == '_')
            .collect();
        assert!(
            SURFACE.contains(&name.as_str()),
            "{name} is in the header and not in the library's list"
        );
    }
    assert!(header.contains(&format!("#define ORION_ABI_MAJOR {}", orion_c::ABI_MAJOR)));
    assert!(header.contains(&format!("#define ORION_ABI_MINOR {}", orion_c::ABI_MINOR)));
    for (name, value) in [
        ("ORION_E_ARGUMENT", orion_c::ORION_E_ARGUMENT),
        ("ORION_E_LOST", orion_c::ORION_E_LOST),
        ("ORION_E_REFUSED", orion_c::ORION_E_REFUSED),
        ("ORION_E_TIMEOUT", orion_c::ORION_E_TIMEOUT),
        ("ORION_E_BUFFER", orion_c::ORION_E_BUFFER),
        ("ORION_E_VERSION", orion_c::ORION_E_VERSION),
        ("ORION_E_INTERNAL", orion_c::ORION_E_INTERNAL),
    ] {
        assert!(
            header.contains(&format!("{name} = {value}")),
            "{name} is not {value} in the header"
        );
    }
}

#[test]
fn a_c_program_publishes_and_subscribes() {
    let (archive, shared) = (library("liborion_c.a"), library("liborion_c.so"));
    // What the shared library exports of Orion's is the header's list and nothing else.
    let symbols = Command::new("nm")
        .args(["-D", "--defined-only"])
        .arg(&shared)
        .output()
        .expect("nm");
    let mut exported: Vec<String> = String::from_utf8_lossy(&symbols.stdout)
        .lines()
        .filter_map(|line| line.split_whitespace().last().map(str::to_owned))
        .filter(|name| name.starts_with("orion_"))
        .collect();
    exported.sort();
    let mut listed: Vec<String> = SURFACE.iter().map(|name| (*name).to_owned()).collect();
    listed.sort();
    assert_eq!(exported, listed);

    let board = Board::start("capi");
    let dir = board.dir().to_owned();

    // The static form: the program carries the library.
    let with_archive = dir.join("pubsub-static");
    compile(
        &with_archive,
        &[archive.to_str().expect("path"), "-lpthread", "-ldl", "-lm"],
    );
    let ran = Command::new(&with_archive)
        .arg(board.socket())
        .output()
        .expect("the program runs");
    assert!(
        ran.status.success(),
        "static: {}",
        String::from_utf8_lossy(&ran.stderr)
    );
    assert_eq!(String::from_utf8_lossy(&ran.stdout), "ok\n");

    // The shared form: the program names the library by its major version, and the loader
    // finds it under that name.
    let versioned = dir.join("liborion_c.so.1");
    std::fs::copy(&shared, &versioned).expect("copy");
    let with_shared = dir.join("pubsub-shared");
    compile(&with_shared, &[versioned.to_str().expect("path")]);
    let needs = Command::new("readelf")
        .arg("-d")
        .arg(&with_shared)
        .output()
        .expect("readelf");
    let needs = String::from_utf8_lossy(&needs.stdout);
    assert!(
        needs.contains("liborion_c.so.1"),
        "the program does not ask for the library by its major:\n{needs}"
    );
    let ran = Command::new(&with_shared)
        .arg(board.socket())
        .env("LD_LIBRARY_PATH", &dir)
        .output()
        .expect("the program runs");
    assert!(
        ran.status.success(),
        "shared: {}",
        String::from_utf8_lossy(&ran.stderr)
    );
    assert_eq!(String::from_utf8_lossy(&ran.stdout), "ok\n");
    // Without the library it does not start at all.
    let lost = Command::new(&with_shared)
        .arg(board.socket())
        .env_remove("LD_LIBRARY_PATH")
        .output()
        .expect("spawn");
    assert!(!lost.status.success());
}
