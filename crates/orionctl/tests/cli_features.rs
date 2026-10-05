//! Builds without the optional `http` / `yaml` / `toml` features reject those requests with an
//! error naming the missing feature (run with `cargo test -p orionctl --no-default-features`).
#![cfg(not(all(feature = "http", feature = "yaml", feature = "toml")))]

mod support;

#[cfg(not(all(feature = "yaml", feature = "toml")))]
use support::TestHarness;
use support::{output_text, run_orionctl};

fn assert_feature_disabled(output: &std::process::Output, feature: &str) {
    assert!(!output.status.success(), "{}", output_text(output));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains(&format!("built without the `{feature}` feature")),
        "{stderr}"
    );
}

#[cfg(not(feature = "http"))]
#[test]
fn http_targets_require_the_http_feature() {
    let health = run_orionctl(["get", "health", "--http", "http://127.0.0.1:9100"]);
    assert_feature_disabled(&health, "http");

    let workloads = run_orionctl(["get", "workloads", "--http", "http://127.0.0.1:9100"]);
    assert_feature_disabled(&workloads, "http");
}

#[cfg(not(feature = "yaml"))]
#[tokio::test(flavor = "multi_thread")]
async fn yaml_output_and_specs_require_the_yaml_feature() {
    let harness = TestHarness::start("node.orionctl.features.yaml").await;
    let socket = harness.ipc_socket.to_string_lossy().into_owned();

    let output = run_orionctl(["get", "snapshot", "--socket", &socket, "-o", "yaml"]);
    assert_feature_disabled(&output, "yaml");

    let spec_path = support::temp_spec_path("orionctl-features-spec.yaml");
    std::fs::write(&spec_path, "workload_id: workload.demo\n").expect("spec file should write");
    let apply = run_orionctl([
        "apply",
        "workload",
        "--socket",
        &socket,
        "--spec",
        &spec_path.to_string_lossy(),
    ]);
    let _ = std::fs::remove_file(&spec_path);
    assert_feature_disabled(&apply, "yaml");

    let json = run_orionctl(["get", "snapshot", "--socket", &socket, "-o", "json"]);
    assert!(json.status.success(), "{}", output_text(&json));
}

#[cfg(not(feature = "toml"))]
#[tokio::test(flavor = "multi_thread")]
async fn toml_output_and_specs_require_the_toml_feature() {
    let harness = TestHarness::start("node.orionctl.features.toml").await;
    let socket = harness.ipc_socket.to_string_lossy().into_owned();

    let output = run_orionctl(["peers", "list", "--socket", &socket, "-o", "toml"]);
    assert_feature_disabled(&output, "toml");

    let spec_path = support::temp_spec_path("orionctl-features-spec.toml");
    std::fs::write(&spec_path, "workload_id = \"workload.demo\"\n")
        .expect("spec file should write");
    let apply = run_orionctl([
        "apply",
        "workload",
        "--socket",
        &socket,
        "--spec",
        &spec_path.to_string_lossy(),
    ]);
    let _ = std::fs::remove_file(&spec_path);
    assert_feature_disabled(&apply, "toml");
}
