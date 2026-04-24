#![cfg(all(feature = "macros", feature = "runtime", feature = "control-plane"))]

use orion::{
    NodeId, OrionExecutor, OrionProvider, RuntimeType,
    control_plane::{ConfigDecodeError, ConfigMapRef, TypedConfigValue, WorkloadConfig},
    orion_resource_type, orion_runtime_type,
    runtime::{ExecutorDescriptor, ProviderDescriptor},
};
use serde::Deserialize;
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

#[derive(Deserialize, Debug, PartialEq, Eq)]
struct DerivedGraphConfig {
    inline: String,
}

#[derive(Deserialize, Debug, PartialEq, Eq)]
struct DerivedConfig {
    width: u64,
    enabled: Option<bool>,
    graph: DerivedGraphConfig,
}

#[derive(Deserialize, Debug, PartialEq, Eq)]
struct DerivedPluginConfig {
    name: String,
    version: Option<String>,
}

#[derive(Deserialize, Debug, PartialEq, Eq)]
struct DerivedPluginListConfig {
    plugin: Vec<DerivedPluginConfig>,
}

#[derive(Deserialize, Debug, PartialEq, Eq)]
struct DerivedBindingConfig {
    resource_id: String,
}

#[derive(Deserialize, Debug, PartialEq, Eq)]
struct DerivedBindingMapConfig {
    binding: BTreeMap<String, DerivedBindingConfig>,
}

#[derive(Deserialize, Debug, PartialEq, Eq)]
struct DerivedDefaultsConfig {
    #[serde(default = "default_tick_ms")]
    tick_ms: u64,
    #[serde(default)]
    enabled: bool,
}

#[derive(Deserialize, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct DerivedStrictConfig {
    width: u64,
}

#[derive(Deserialize, Debug, PartialEq, Eq)]
#[serde(tag = "kind")]
enum DerivedActionConfig {
    #[serde(rename = "gpio.read")]
    GpioRead,
    #[serde(rename = "gpio.write")]
    GpioWrite { high: bool },
}

#[derive(Deserialize, Debug, PartialEq, Eq)]
struct DerivedActionRoot {
    action: DerivedActionConfig,
}

fn default_tick_ms() -> u64 {
    1000
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

    let decoded: DerivedConfig =
        orion::control_plane::deserialize_config(&config.payload).expect("config should decode");
    assert_eq!(
        decoded,
        DerivedConfig {
            width: 1280,
            enabled: Some(true),
            graph: DerivedGraphConfig {
                inline: "{\"nodes\":[]}".into(),
            },
        }
    );
}

#[test]
fn facade_config_decode_macro_surfaces_field_errors() {
    let payload = BTreeMap::from([("width".to_string(), TypedConfigValue::String("wide".into()))]);
    let error = orion::control_plane::deserialize_config::<DerivedConfig>(&payload)
        .expect_err("type mismatch should fail");

    assert_eq!(
        error,
        ConfigDecodeError::Deserialize {
            field: "width".into(),
            message: "invalid type: string \"wide\", expected u64".into(),
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
        .field("action.high", TypedConfigValue::Bool(true));

    let decoded: DerivedActionRoot = orion::control_plane::deserialize_config(&config.payload)
        .expect("enum config should decode");
    assert_eq!(
        decoded,
        DerivedActionRoot {
            action: DerivedActionConfig::GpioWrite { high: true }
        }
    );
}

#[test]
fn facade_config_decode_macro_reports_unknown_enum_tag() {
    let config = WorkloadConfig::new("resource.action.config.v1")
        .field("action.kind", TypedConfigValue::String("gpio.flip".into()));

    let error = orion::control_plane::deserialize_config::<DerivedActionRoot>(&config.payload)
        .expect_err("unknown tag should fail");
    assert!(matches!(
        error,
        ConfigDecodeError::Deserialize { field, .. } if field == "action.kind"
    ));
}

#[test]
fn facade_config_decode_macro_decodes_indexed_groups() {
    let config = WorkloadConfig::new("graph.workload.config.v1")
        .field("plugin.0.name", TypedConfigValue::String("cv".into()))
        .field("plugin.1.name", TypedConfigValue::String("slam".into()))
        .field("plugin.1.version", TypedConfigValue::String("1.0.0".into()));

    let decoded: DerivedPluginListConfig =
        orion::control_plane::deserialize_config(&config.payload)
            .expect("indexed groups should decode");
    assert_eq!(
        decoded,
        DerivedPluginListConfig {
            plugin: vec![
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

#[test]
fn facade_config_decode_macro_decodes_named_groups() {
    let config = WorkloadConfig::new("graph.workload.config.v1")
        .field(
            "binding.camera.resource_id",
            TypedConfigValue::String("resource.camera".into()),
        )
        .field(
            "binding.depth.resource_id",
            TypedConfigValue::String("resource.depth".into()),
        );

    let decoded: DerivedBindingMapConfig =
        orion::control_plane::deserialize_config(&config.payload)
            .expect("named groups should decode");
    assert_eq!(
        decoded,
        DerivedBindingMapConfig {
            binding: BTreeMap::from([
                (
                    "camera".into(),
                    DerivedBindingConfig {
                        resource_id: "resource.camera".into(),
                    },
                ),
                (
                    "depth".into(),
                    DerivedBindingConfig {
                        resource_id: "resource.depth".into(),
                    },
                ),
            ]),
        }
    );
}

#[test]
fn facade_config_decode_macro_uses_serde_defaults() {
    let config = WorkloadConfig::new("runtime.tuning.v1");

    let decoded: DerivedDefaultsConfig =
        orion::control_plane::deserialize_config(&config.payload).expect("defaults should decode");
    assert_eq!(
        decoded,
        DerivedDefaultsConfig {
            tick_ms: 1000,
            enabled: false,
        }
    );
}

#[test]
fn facade_config_decode_macro_respects_serde_deny_unknown_fields() {
    let config = WorkloadConfig::new("strict.config.v1")
        .field("width", TypedConfigValue::UInt(1280))
        .field("extra", TypedConfigValue::Bool(true));

    let error = orion::control_plane::deserialize_config::<DerivedStrictConfig>(&config.payload)
        .expect_err("unknown fields should fail");
    assert!(matches!(
        error,
        ConfigDecodeError::Deserialize { field, .. } if field == "extra"
    ));
}
