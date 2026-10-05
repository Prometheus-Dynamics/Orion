mod support;

use orion_control_plane::{HostFacts, HostMetricsSample, HostTemperature, NodeHostFacts};
use orion_node::HostFactsSource;
use support::{TestHarness, output_text, run_orionctl};

struct FixedHost;

impl HostFactsSource for FixedHost {
    fn sample(&self) -> HostFacts {
        HostFacts {
            identity: NodeHostFacts {
                hostname: Some("edge-1".into()),
                os_id: Some("debian".into()),
                os_version: Some("12".into()),
                image_name: Some("edge-image".into()),
                image_version: Some("2024.10.1".into()),
                kernel_release: Some("6.6.31".into()),
                architecture: Some("aarch64".into()),
                board_model: Some("Compute Module 5".into()),
                cpu_count: Some(4),
                ..NodeHostFacts::default()
            },
            metrics: HostMetricsSample {
                uptime_seconds: Some(3_600),
                load_1_milli: Some(250),
                temperatures: vec![HostTemperature::new("cpu-thermal", 48_500)],
                ..HostMetricsSample::default()
            },
            sampled_at_ms: 0,
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn orionctl_reports_host_facts_links_and_actions() {
    let harness = TestHarness::start("node.orionctl.host").await;
    assert!(harness._app.refresh_host_facts_from(&FixedHost));
    let socket = harness.ipc_socket.to_string_lossy().into_owned();

    let nodes = run_orionctl(["get", "nodes", "--socket", &socket]);
    assert!(nodes.status.success(), "{}", output_text(&nodes));
    let stdout = String::from_utf8_lossy(&nodes.stdout);
    assert!(
        stdout.contains(
            "hostname=edge-1 os=debian os_version=12 image=edge-image image_version=2024.10.1 \
             kernel=6.6.31 arch=aarch64"
        ),
        "{stdout}"
    );

    let describe = run_orionctl([
        "describe",
        "node",
        "--socket",
        &socket,
        "node.orionctl.host",
    ]);
    assert!(describe.status.success(), "{}", output_text(&describe));
    let stdout = String::from_utf8_lossy(&describe.stdout);
    for line in [
        "host_hostname: edge-1",
        "host_image_version: 2024.10.1",
        "host_cpu_count: 4",
        "host_uptime_seconds: 3600",
        "host_load: 0.25,-,-",
        "host_temperatures: cpu-thermal=48.5C",
    ] {
        assert!(stdout.contains(line), "missing `{line}` in:\n{stdout}");
    }

    let status = run_orionctl([
        "get",
        "status",
        "--socket",
        &socket,
        "--subject",
        "node/node.orionctl.host",
    ]);
    assert!(status.status.success(), "{}", output_text(&status));
    let stdout = String::from_utf8_lossy(&status.stdout);
    assert!(
        stdout.contains("key=host.temperature.cpu-thermal type=int value=48500"),
        "{stdout}"
    );

    let metrics = run_orionctl(["get", "observability", "--socket", &socket, "-o", "metrics"]);
    assert!(metrics.status.success(), "{}", output_text(&metrics));
    let stdout = String::from_utf8_lossy(&metrics.stdout);
    assert!(
        stdout.contains("orion_node_host_info{node_id=\"node.orionctl.host\""),
        "{stdout}"
    );

    let links = run_orionctl(["get", "links", "--socket", &socket]);
    assert!(links.status.success(), "{}", output_text(&links));
    assert_eq!(String::from_utf8_lossy(&links.stdout), "links count=0\n");

    // No handler is registered for node actions, so the action is rejected and `--wait` fails.
    let run = run_orionctl([
        "action",
        "run",
        "--socket",
        &socket,
        "--id",
        "reboot-1",
        "node/node.orionctl.host",
        "reboot",
        "--arg",
        "delay_ms=5000",
        "--wait",
    ]);
    assert!(!run.status.success(), "{}", output_text(&run));
    let stdout = String::from_utf8_lossy(&run.stdout);
    assert!(
        stdout.contains(
            "action id=reboot-1 target=node/node.orionctl.host name=reboot state=rejected"
        ),
        "{stdout}"
    );
    assert!(
        String::from_utf8_lossy(&run.stderr).contains("action reboot-1 rejected"),
        "{}",
        output_text(&run)
    );

    let actions = run_orionctl(["get", "actions", "--socket", &socket, "-o", "json"]);
    assert!(actions.status.success(), "{}", output_text(&actions));
    let value: serde_json::Value =
        serde_json::from_slice(&actions.stdout).expect("json output should parse");
    assert_eq!(value[0]["action_id"], "reboot-1");
    assert_eq!(value[0]["requested_by"], "local:orionctl");
}
