//! `orionctl apply update`, `get updates` and `delete update` against a running node over the
//! local socket (`docs/update-recovery.md`, "Data model").

use orion::control_plane::{StateSnapshot, UpdateIntentView};

use support::{TestHarness, output_text, run_orionctl};

mod support;

fn snapshot_of(harness: &TestHarness) -> StateSnapshot {
    harness._app.state_snapshot()
}

#[tokio::test(flavor = "multi_thread")]
async fn orionctl_applies_lists_and_deletes_update_intents() {
    let harness = TestHarness::start("node.orionctl.updates").await;
    let socket = harness.ipc_socket.to_string_lossy().into_owned();
    let node = "node.orionctl.updates";

    // Writing an intent: the summary echoes it, generation 1 for a new intent.
    let applied = run_orionctl([
        "apply",
        "update",
        "--socket",
        &socket,
        "--node",
        node,
        "--component",
        "os",
        "--version",
        "2026.11",
        "--artifact",
        "artifact.os-2026.11",
        "--drain",
        "drain:5000",
        "--arg",
        "image_url=string:http://192.0.2.5/os.img",
    ]);
    assert!(applied.status.success(), "{}", output_text(&applied));
    let stdout = String::from_utf8_lossy(&applied.stdout);
    assert!(stdout.contains("apply update accepted intent=node.orionctl.updates/os"));
    assert!(stdout.contains("version=2026.11 generation=1"));

    let intent = snapshot_of(&harness)
        .state
        .desired
        .update_intents
        .values()
        .next()
        .cloned()
        .expect("the intent is in desired state");
    assert_eq!(intent.target_version, "2026.11");
    assert_eq!(intent.generation, 1);
    assert_eq!(
        intent.artifact.as_ref().map(|id| id.as_str()),
        Some("artifact.os-2026.11")
    );
    assert_eq!(intent.args.len(), 1);

    // Listing: one line per intent, with the node's status once it has reported one.
    let listed = run_orionctl(["get", "updates", "--socket", &socket]);
    assert!(listed.status.success(), "{}", output_text(&listed));
    let listed = String::from_utf8_lossy(&listed.stdout).into_owned();
    assert!(listed.starts_with("updates count=1"), "{listed}");
    let line = listed
        .lines()
        .find(|line| line.starts_with("update intent=node.orionctl.updates/os "))
        .unwrap_or_else(|| panic!("no intent line in {listed}"));
    for expected in [
        "node=node.orionctl.updates",
        "component=os",
        "version=2026.11",
        "generation=1",
        "handler=action:update@node/node.orionctl.updates",
        "drain=drain:5000",
        "paused=false",
    ] {
        assert!(
            line.contains(expected),
            "`{expected}` missing from `{line}`"
        );
    }

    // The same version again keeps the generation; --retry bumps it; a new version bumps it too.
    for (version, retry, generation) in [
        ("2026.11", false, 1),
        ("2026.11", true, 2),
        ("2026.12", false, 3),
    ] {
        let output = if retry {
            run_orionctl([
                "apply",
                "update",
                "--socket",
                &socket,
                "--node",
                node,
                "--component",
                "os",
                "--version",
                version,
                "--retry",
            ])
        } else {
            run_orionctl([
                "apply",
                "update",
                "--socket",
                &socket,
                "--node",
                node,
                "--component",
                "os",
                "--version",
                version,
            ])
        };
        assert!(output.status.success(), "{}", output_text(&output));
        let current = snapshot_of(&harness)
            .state
            .desired
            .update_intents
            .values()
            .next()
            .cloned()
            .expect("intent");
        assert_eq!(current.generation, generation);
    }

    // JSON carries the intent (and no status before the node reports one).
    let json = run_orionctl(["get", "updates", "--socket", &socket, "-o", "json"]);
    assert!(json.status.success(), "{}", output_text(&json));
    let views: Vec<UpdateIntentView> =
        serde_json::from_slice(&json.stdout).expect("updates json should parse");
    assert_eq!(views.len(), 1);
    assert_eq!(views[0].intent.target_version, "2026.12");
    assert_eq!(views[0].intent.generation, 3);

    // Filtering by target node.
    let other = run_orionctl([
        "get",
        "updates",
        "--socket",
        &socket,
        "--node",
        "node.elsewhere",
    ]);
    assert!(other.status.success(), "{}", output_text(&other));
    assert!(String::from_utf8_lossy(&other.stdout).starts_with("updates count=0"));

    // Invalid input is refused by the node and nothing is written.
    let invalid = run_orionctl([
        "apply",
        "update",
        "--socket",
        &socket,
        "--node",
        node,
        "--component",
        "os",
        "--version",
        "",
    ]);
    assert!(!invalid.status.success(), "{}", output_text(&invalid));

    // Deleting removes the intent; the node keeps what it runs.
    let deleted = run_orionctl([
        "delete",
        "update",
        "--socket",
        &socket,
        "--node",
        node,
        "--component",
        "os",
    ]);
    assert!(deleted.status.success(), "{}", output_text(&deleted));
    assert!(
        String::from_utf8_lossy(&deleted.stdout)
            .contains("delete update accepted intent=node.orionctl.updates/os")
    );
    assert!(
        snapshot_of(&harness)
            .state
            .desired
            .update_intents
            .is_empty()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn orionctl_dry_run_does_not_write() {
    let harness = TestHarness::start("node.orionctl.updates.dry").await;
    let socket = harness.ipc_socket.to_string_lossy().into_owned();
    let output = run_orionctl([
        "apply",
        "update",
        "--socket",
        &socket,
        "--node",
        "node.orionctl.updates.dry",
        "--component",
        "os",
        "--version",
        "1",
        "--dry-run",
        "-o",
        "json",
    ]);
    assert!(output.status.success(), "{}", output_text(&output));
    let report: serde_json::Value = serde_json::from_slice(&output.stdout).expect("json");
    assert_eq!(report["intent"]["target_version"], "1");
    assert!(
        snapshot_of(&harness)
            .state
            .desired
            .update_intents
            .is_empty()
    );
}
