#![cfg(all(feature = "macros", feature = "runtime", feature = "control-plane"))]

use orion::{
    NodeId, OrionConfigDecode, OrionExecutor, OrionProvider, RuntimeType,
    control_plane::{ConfigDecodeError, ConfigMapRef, TypedConfigValue, WorkloadConfig},
    orion_resource_type, orion_runtime_type,
    runtime::{ExecutorDescriptor, ProviderDescriptor},
};
use std::collections::BTreeMap;

#[orion_runtime_type("graph.exec.v1")]
struct GraphExecV1;

#[orion_resource_type("imu.sample")]
struct ImuSample;

#[derive(OrionExecutor)]
#[orion_executor(id = "executor.engine", runtime_types = ["graph.exec.v1"])]
struct DerivedExecutor {
    node_id: NodeId,
}

#[derive(OrionProvider)]
#[orion_provider(id = "provider.peripherals", resource_types = ["imu.sample"])]
struct DerivedProvider {
    node_id: NodeId,
}

#[derive(OrionConfigDecode, Debug, PartialEq, Eq)]
struct DerivedConfig {
    width: u64,
    enabled: Option<bool>,
    #[orion(path = "graph.inline")]
    graph_inline: String,
}

#[derive(OrionConfigDecode, Debug, PartialEq, Eq)]
struct DerivedPluginConfig {
    name: String,
    version: Option<String>,
}

#[derive(OrionConfigDecode, Debug, PartialEq, Eq)]
struct DerivedPluginListConfig {
    #[orion(prefix = "plugin")]
    plugins: Vec<DerivedPluginConfig>,
}

#[derive(OrionConfigDecode, Debug, PartialEq, Eq)]
#[orion(tag = "action.kind")]
enum DerivedActionConfig {
    #[orion(tag = "gpio.read")]
    GpioRead,
    #[orion(tag = "gpio.write")]
    GpioWrite {
        #[orion(path = "arg.high")]
        high: bool,
    },
}

#[test]
fn facade_macros_expand_without_direct_workspace_imports() {
    let executor = DerivedExecutor {
        node_id: NodeId::new("node-a"),
    };
    let provider = DerivedProvider {
        node_id: NodeId::new("node-a"),
    };

    assert_eq!(
        GraphExecV1::runtime_type(),
        RuntimeType::of::<GraphExecV1>()
    );
    assert_eq!(executor.executor_record().runtime_types.len(), 1);
    assert_eq!(provider.provider_record().resource_types.len(), 1);
    assert_eq!(ImuSample::resource_type().as_str(), "imu.sample");
}

#[test]
fn facade_config_decode_macro_decodes_workload_config() {
    let config = WorkloadConfig::new("graph.workload.config.v1")
        .field("width", TypedConfigValue::UInt(1280))
        .field("enabled", TypedConfigValue::Bool(true))
        .field(
            "graph.inline",
            TypedConfigValue::String("{\"nodes\":[]}".into()),
        );

    let decoded = DerivedConfig::try_from(&config).expect("config should decode");
    assert_eq!(
        decoded,
        DerivedConfig {
            width: 1280,
            enabled: Some(true),
            graph_inline: "{\"nodes\":[]}".into(),
        }
    );
}

#[test]
fn facade_config_decode_macro_surfaces_field_errors() {
    let payload = BTreeMap::from([("width".to_string(), TypedConfigValue::String("wide".into()))]);
    let error = DerivedConfig::try_from(&payload).expect_err("type mismatch should fail");

    assert_eq!(
        error,
        ConfigDecodeError::InvalidType {
            field: "width".into(),
            expected: "uint",
            actual: "string",
        }
    );

    let view = ConfigMapRef::new(&payload);
    assert_eq!(
        view.required_string("missing").unwrap_err().to_string(),
        "missing string field 'missing'"
    );
}

#[test]
fn facade_config_decode_macro_decodes_tagged_enum() {
    let config = WorkloadConfig::new("resource.action.config.v1")
        .field("action.kind", TypedConfigValue::String("gpio.write".into()))
        .field("arg.high", TypedConfigValue::Bool(true));

    let decoded = DerivedActionConfig::try_from(&config).expect("enum config should decode");
    assert_eq!(decoded, DerivedActionConfig::GpioWrite { high: true });
}

#[test]
fn facade_config_decode_macro_reports_unknown_enum_tag() {
    let config = WorkloadConfig::new("resource.action.config.v1")
        .field("action.kind", TypedConfigValue::String("gpio.flip".into()));

    let error = DerivedActionConfig::try_from(&config).expect_err("unknown tag should fail");
    assert_eq!(
        error,
        ConfigDecodeError::InvalidValue {
            field: "action.kind".into(),
            message: "unsupported tag 'gpio.flip'".into(),
        }
    );
}

#[test]
fn facade_config_decode_macro_decodes_indexed_groups() {
    let config = WorkloadConfig::new("graph.workload.config.v1")
        .field("plugin.0.name", TypedConfigValue::String("cv".into()))
        .field("plugin.1.name", TypedConfigValue::String("slam".into()))
        .field("plugin.1.version", TypedConfigValue::String("1.0.0".into()));

    let decoded = DerivedPluginListConfig::try_from(&config).expect("indexed groups should decode");
    assert_eq!(
        decoded,
        DerivedPluginListConfig {
            plugins: vec![
                DerivedPluginConfig {
                    name: "cv".into(),
                    version: None,
                },
                DerivedPluginConfig {
                    name: "slam".into(),
                    version: Some("1.0.0".into()),
                },
            ],
        }
    );
}
