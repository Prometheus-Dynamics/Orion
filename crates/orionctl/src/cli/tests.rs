use std::{
    fs,
    time::{SystemTime, UNIX_EPOCH},
};

#[cfg(feature = "http")]
use super::{HttpTargetArgs, HttpTargetScheme, OutputFormat};
use super::{
    StructuredFormat, TypedConfigValue, parse_binding, parse_bool_config, parse_bytes_hex_config,
    parse_requirement, parse_string_config, parse_uint_config, preferred_runtime_path,
};
use crate::build_info;
#[cfg(feature = "http")]
use crate::transport::HttpTargetExt;

#[cfg(feature = "http")]
fn target(http: &str) -> HttpTargetArgs {
    HttpTargetArgs {
        http: http.to_owned(),
        ca_cert: None,
        client_cert: None,
        client_key: None,
        client_name: "orionctl.test".to_owned(),
        output: OutputFormat::Summary,
    }
}

#[cfg(feature = "http")]
#[test]
fn plain_http_targets_reject_tls_material_early() {
    let mut args = target("http://127.0.0.1:9100");
    args.ca_cert = Some("ca.pem".into());
    let error = args
        .validate_tls_args(HttpTargetScheme::Http)
        .expect_err("plain HTTP should reject TLS material");
    assert!(error.contains("plain http:// targets do not use TLS material"));
}

#[cfg(feature = "http")]
#[test]
fn client_identity_requires_matching_key() {
    let mut args = target("https://127.0.0.1:9100");
    args.client_cert = Some("client-cert.pem".into());
    let error = args
        .validate_tls_args(HttpTargetScheme::Https)
        .expect_err("partial client identity should fail");
    assert!(error.contains("requires both --client-cert and --client-key"));
}

#[test]
fn parse_requirement_accepts_type_and_count() {
    let requirement = parse_requirement("camera.stream:2").expect("requirement should parse");
    assert_eq!(requirement.resource_type.to_string(), "camera.stream");
    assert_eq!(requirement.count, 2);
}

#[test]
fn parse_binding_accepts_resource_and_node() {
    let binding = parse_binding("resource.front@node-a").expect("binding should parse");
    assert_eq!(binding.resource_id.to_string(), "resource.front");
    assert_eq!(binding.node_id.to_string(), "node-a");
}

#[test]
fn parse_typed_config_fields_accept_expected_shapes() {
    let enabled = parse_bool_config("enabled=true").expect("bool config should parse");
    assert_eq!(enabled.key, "enabled");
    assert_eq!(enabled.value, TypedConfigValue::Bool(true));

    let count = parse_uint_config("count=42").expect("uint config should parse");
    assert_eq!(count.key, "count");
    assert_eq!(count.value, TypedConfigValue::UInt(42));

    let label = parse_string_config("graph.kind=inline").expect("string config should parse");
    assert_eq!(label.key, "graph.kind");
    assert_eq!(label.value, TypedConfigValue::String("inline".to_owned()));

    let bytes = parse_bytes_hex_config("payload=6869").expect("hex config should parse");
    assert_eq!(bytes.key, "payload");
    assert_eq!(bytes.value, TypedConfigValue::Bytes(vec![0x68, 0x69]));
}

#[test]
fn structured_format_display_matches_cli_values() {
    assert_eq!(StructuredFormat::Json.to_string(), "json");
    assert_eq!(StructuredFormat::Yaml.to_string(), "yaml");
    assert_eq!(StructuredFormat::Toml.to_string(), "toml");
}

#[test]
fn preferred_runtime_path_uses_runtime_socket_when_present() {
    let runtime = unique_test_path("runtime.sock");
    fs::write(&runtime, b"socket").expect("test runtime socket placeholder should write");

    let selected = preferred_runtime_path(runtime.clone(), || unique_test_path("fallback.sock"));

    assert_eq!(selected, runtime);
    let _ = fs::remove_file(selected);
}

#[test]
fn preferred_runtime_path_falls_back_when_runtime_socket_is_missing() {
    let runtime = unique_test_path("missing.sock");
    let fallback = unique_test_path("fallback.sock");

    let selected = preferred_runtime_path(runtime, || fallback.clone());

    assert_eq!(selected, fallback);
}

#[test]
fn clap_version_strings_include_build_metadata() {
    use clap::CommandFactory;

    let command = super::Cli::command();
    assert_eq!(command.get_version(), Some(build_info::PKG_VERSION));
    assert_eq!(command.get_long_version(), Some(build_info::BUILD_VERSION));
    assert!(build_info::BUILD_VERSION.contains(build_info::PKG_VERSION));
    assert!(build_info::BUILD_VERSION.contains(env!("ORION_BUILD_COMMIT")));
}

fn unique_test_path(name: &str) -> std::path::PathBuf {
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("orionctl-{stamp}-{name}"))
}
