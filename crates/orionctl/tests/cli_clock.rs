mod support;

use orion_node::{ClockStatusSource, KernelClockReading};
use support::{TestHarness, output_text, run_orionctl};

/// `STA_NANO`: offsets in nanoseconds.
const STA_NANO: i32 = 0x2000;

struct SyncedClock;

impl ClockStatusSource for SyncedClock {
    fn read(&self) -> Option<KernelClockReading> {
        Some(KernelClockReading {
            state: 0,
            status: STA_NANO,
            offset: -1_500,
            max_error_us: 16_000,
            estimated_error_us: 2,
        })
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn orionctl_reports_node_clock_facts() {
    let harness = TestHarness::start("node.orionctl.clock").await;
    assert!(harness._app.refresh_clock_facts_from(&SyncedClock));
    let socket = harness.ipc_socket.to_string_lossy().into_owned();

    // The node is only in observed state (no desired node record) and is still listed.
    let nodes = run_orionctl(["get", "nodes", "--socket", &socket]);
    assert!(nodes.status.success(), "{}", output_text(&nodes));
    let stdout = String::from_utf8_lossy(&nodes.stdout);
    assert!(stdout.contains("nodes count=1"), "{stdout}");
    assert!(
        stdout.contains(
            "node id=node.orionctl.clock health=unknown schedulable=true labels=- \
             clock_source=system clock_synced=true clock_offset_ns=-1500 \
             clock_max_error_ns=16000000 timebase=-"
        ),
        "{stdout}"
    );

    let json = run_orionctl(["get", "nodes", "--socket", &socket, "-o", "json"]);
    assert!(json.status.success(), "{}", output_text(&json));
    let value: serde_json::Value =
        serde_json::from_slice(&json.stdout).expect("json output should parse");
    let clock = &value[0]["clock"];
    assert_eq!(clock["source"], "System");
    assert_eq!(clock["synchronized"], true);
    assert_eq!(clock["offset_ns"], -1500);
    assert_eq!(clock["estimated_error_ns"], 2000);

    let describe = run_orionctl([
        "describe",
        "node",
        "--socket",
        &socket,
        "node.orionctl.clock",
    ]);
    assert!(describe.status.success(), "{}", output_text(&describe));
    let stdout = String::from_utf8_lossy(&describe.stdout);
    for line in [
        "node: node.orionctl.clock",
        "clock_source: system",
        "clock_synchronized: true",
        "clock_offset_ns: -1500",
        "clock_max_error_ns: 16000000",
        "clock_estimated_error_ns: 2000",
        "clock_stratum: -",
        "timebase: -",
    ] {
        assert!(stdout.contains(line), "missing `{line}` in:\n{stdout}");
    }

    let metrics = run_orionctl(["get", "observability", "--socket", &socket, "-o", "metrics"]);
    assert!(metrics.status.success(), "{}", output_text(&metrics));
    let stdout = String::from_utf8_lossy(&metrics.stdout);
    assert!(
        stdout.contains("orion_node_clock_synchronized{node_id=\"node.orionctl.clock\"} 1"),
        "{stdout}"
    );
    assert!(
        stdout.contains(
            "orion_node_clock_offset_seconds{node_id=\"node.orionctl.clock\"} -0.0000015"
        ),
        "{stdout}"
    );
}
