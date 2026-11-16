//! The stub and the node share the action list. A stand-in that answers runs by rules of its
//! own drifts from the node and hides what the node does (it hid a run handle collision once),
//! so nothing in the workspace answers action runs but the relay, which passes them to node
//! logic, and the stub's router, which is node logic's own list behind the door.

use std::path::{Path, PathBuf};

fn sources(dir: &Path, found: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            if path.file_name().is_some_and(|name| name != "target") {
                sources(&path, found);
            }
        } else if path.extension().is_some_and(|ext| ext == "rs") {
            found.push(path);
        }
    }
}

// Put together, so that this file does not match itself.
const SERVICE: &str = concat!("impl Service", " for");
const SERVICE_PATH: &str = concat!("impl orion_server::Service", " for");
const RUN: &str = concat!("ACTION", "_RUN");

#[test]
fn nothing_answers_action_runs_but_node_logics_own_list() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../..");
    let mut files = Vec::new();
    sources(&root.join("crates"), &mut files);
    sources(&root.join("examples"), &mut files);
    assert!(files.len() > 100, "the workspace was not found at {root:?}");
    let mut answering: Vec<String> = files
        .iter()
        .filter(|path| {
            let text = std::fs::read_to_string(path).unwrap_or_default();
            (text.contains(SERVICE) || text.contains(SERVICE_PATH)) && text.contains(RUN)
        })
        .map(|path| {
            path.strip_prefix(&root)
                .unwrap_or(path)
                .to_string_lossy()
                .into_owned()
        })
        .collect();
    answering.sort();
    assert_eq!(
        answering,
        [
            "crates/comm/relay/src/lib.rs",
            "crates/testing/stubs/src/router.rs"
        ],
        "a service answers action runs by rules of its own: use orion_stubs::router::Router"
    );

    // And the stub's router is the list: it makes none of the list's messages itself.
    let router = include_str!("../src/router.rs");
    assert!(router.contains("orion_action_list::{Actions"));
    for own in [
        "Claim {",
        "Cancel {",
        "ActionResult {",
        "Progress {",
        "Started {",
        "CallerGone {",
    ] {
        assert!(
            !router.contains(own),
            "the stub's router makes `{own}` itself"
        );
    }
}
