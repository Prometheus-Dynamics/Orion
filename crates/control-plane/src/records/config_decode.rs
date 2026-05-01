use std::collections::{BTreeMap, BTreeSet};

use serde::de::DeserializeOwned;
use serde_json::{Map as JsonMap, Number as JsonNumber, Value as JsonValue};
use thiserror::Error;

use super::workloads::TypedConfigValue;

#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum ConfigDecodeError {
    #[error("missing {expected} field '{field}'")]
    MissingField {
        field: String,
        expected: &'static str,
    },
    #[error("field '{field}' expected {expected}, found {actual}")]
    InvalidType {
        field: String,
        expected: &'static str,
        actual: &'static str,
    },
    #[error("field '{field}' is invalid: {message}")]
    InvalidValue { field: String, message: String },
    #[error("config payload is structurally invalid at '{field}': {message}")]
    InvalidStructure { field: String, message: String },
    #[error("config payload failed to deserialize at '{field}': {message}")]
    Deserialize { field: String, message: String },
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum ConfigTree {
    Object(BTreeMap<String, ConfigTree>),
    Array(BTreeMap<usize, ConfigTree>),
    Scalar(TypedConfigValue),
}

impl ConfigTree {
    fn insert(
        &mut self,
        path: &[PathSegment<'_>],
        value: TypedConfigValue,
        full_field: &str,
    ) -> Result<(), ConfigDecodeError> {
        match path {
            [] => Err(ConfigDecodeError::InvalidStructure {
                field: full_field.to_owned(),
                message: "empty path".into(),
            }),
            [segment] => match (self, segment) {
                (Self::Object(entries), PathSegment::Key(key)) => {
                    if let Some(existing) = entries.get(*key) {
                        return Err(ConfigDecodeError::InvalidStructure {
                            field: full_field.to_owned(),
                            message: format!(
                                "field conflicts with existing {} entry",
                                existing.kind_name()
                            ),
                        });
                    }
                    entries.insert((*key).to_owned(), Self::Scalar(value));
                    Ok(())
                }
                (Self::Array(entries), PathSegment::Index(index)) => {
                    if let Some(existing) = entries.get(index) {
                        return Err(ConfigDecodeError::InvalidStructure {
                            field: full_field.to_owned(),
                            message: format!(
                                "field conflicts with existing {} entry",
                                existing.kind_name()
                            ),
                        });
                    }
                    entries.insert(*index, Self::Scalar(value));
                    Ok(())
                }
                (Self::Object(_), PathSegment::Index(_))
                | (Self::Array(_), PathSegment::Key(_)) => {
                    Err(ConfigDecodeError::InvalidStructure {
                        field: full_field.to_owned(),
                        message: "path mixes object and array segments".into(),
                    })
                }
                (Self::Scalar(_), _) => Err(ConfigDecodeError::InvalidStructure {
                    field: full_field.to_owned(),
                    message: "field collides with existing scalar value".into(),
                }),
            },
            [segment, rest @ ..] => match (self, segment) {
                (Self::Object(entries), PathSegment::Key(key)) => {
                    let child = entries.entry((*key).to_owned()).or_insert_with(|| {
                        next_container(rest.first().copied().expect("rest is non-empty"))
                    });
                    if !child.can_accept(rest.first().copied().expect("rest is non-empty")) {
                        return Err(ConfigDecodeError::InvalidStructure {
                            field: full_field.to_owned(),
                            message: format!(
                                "field conflicts with existing {} entry",
                                child.kind_name()
                            ),
                        });
                    }
                    child.insert(rest, value, full_field)
                }
                (Self::Array(entries), PathSegment::Index(index)) => {
                    let child = entries.entry(*index).or_insert_with(|| {
                        next_container(rest.first().copied().expect("rest is non-empty"))
                    });
                    if !child.can_accept(rest.first().copied().expect("rest is non-empty")) {
                        return Err(ConfigDecodeError::InvalidStructure {
                            field: full_field.to_owned(),
                            message: format!(
                                "field conflicts with existing {} entry",
                                child.kind_name()
                            ),
                        });
                    }
                    child.insert(rest, value, full_field)
                }
                (Self::Object(_), PathSegment::Index(_))
                | (Self::Array(_), PathSegment::Key(_)) => {
                    Err(ConfigDecodeError::InvalidStructure {
                        field: full_field.to_owned(),
                        message: "path mixes object and array segments".into(),
                    })
                }
                (Self::Scalar(_), _) => Err(ConfigDecodeError::InvalidStructure {
                    field: full_field.to_owned(),
                    message: "field collides with existing scalar value".into(),
                }),
            },
        }
    }

    fn kind_name(&self) -> &'static str {
        match self {
            Self::Object(_) => "object",
            Self::Array(_) => "array",
            Self::Scalar(_) => "scalar",
        }
    }

    fn can_accept(&self, next: PathSegment<'_>) -> bool {
        matches!(
            (self, next),
            (Self::Object(_), PathSegment::Key(_)) | (Self::Array(_), PathSegment::Index(_))
        )
    }

    fn into_json(self) -> JsonValue {
        match self {
            Self::Object(entries) => JsonValue::Object(
                entries
                    .into_iter()
                    .map(|(key, value)| (key, value.into_json()))
                    .collect::<JsonMap<String, JsonValue>>(),
            ),
            Self::Array(entries) => JsonValue::Array(
                entries
                    .into_values()
                    .map(ConfigTree::into_json)
                    .collect::<Vec<_>>(),
            ),
            Self::Scalar(value) => typed_value_to_json(value),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PathSegment<'a> {
    Key(&'a str),
    Index(usize),
}

fn next_container(next: PathSegment<'_>) -> ConfigTree {
    match next {
        PathSegment::Key(_) => ConfigTree::Object(BTreeMap::new()),
        PathSegment::Index(_) => ConfigTree::Array(BTreeMap::new()),
    }
}

fn parse_path(field: &str) -> Result<Vec<PathSegment<'_>>, ConfigDecodeError> {
    let mut segments = Vec::new();
    for part in field.split('.') {
        if part.is_empty() {
            return Err(ConfigDecodeError::InvalidStructure {
                field: field.to_owned(),
                message: "path contains an empty segment".into(),
            });
        }
        match part.parse::<usize>() {
            Ok(index) => segments.push(PathSegment::Index(index)),
            Err(_) => segments.push(PathSegment::Key(part)),
        }
    }
    Ok(segments)
}

fn typed_value_to_json(value: TypedConfigValue) -> JsonValue {
    match value {
        TypedConfigValue::Bool(value) => JsonValue::Bool(value),
        TypedConfigValue::Int(value) => JsonValue::Number(JsonNumber::from(value)),
        TypedConfigValue::UInt(value) => JsonValue::Number(JsonNumber::from(value)),
        TypedConfigValue::String(value) => JsonValue::String(value),
        TypedConfigValue::Bytes(value) => JsonValue::Array(
            value
                .into_iter()
                .map(|byte| JsonValue::Number(JsonNumber::from(byte)))
                .collect(),
        ),
    }
}

fn trim_serde_location_suffix(message: String) -> String {
    match message.split_once(" at line ") {
        Some((prefix, _)) => prefix.to_owned(),
        None => message,
    }
}

pub fn config_json_value(
    payload: &BTreeMap<String, TypedConfigValue>,
) -> Result<JsonValue, ConfigDecodeError> {
    let mut root = ConfigTree::Object(BTreeMap::new());
    for (field, value) in payload {
        let path = parse_path(field)?;
        root.insert(&path, value.clone(), field)?;
    }
    Ok(root.into_json())
}

pub fn deserialize_config<T>(
    payload: &BTreeMap<String, TypedConfigValue>,
) -> Result<T, ConfigDecodeError>
where
    T: DeserializeOwned,
{
    let value = config_json_value(payload)?;
    let encoded =
        serde_json::to_vec(&value).map_err(|error| ConfigDecodeError::InvalidStructure {
            field: "<root>".into(),
            message: format!("failed to encode expanded config tree: {error}"),
        })?;
    let mut deserializer = serde_json::Deserializer::from_slice(&encoded);
    serde_path_to_error::deserialize(&mut deserializer).map_err(|error| {
        let field = error.path().to_string();
        ConfigDecodeError::Deserialize {
            field: if field.is_empty() {
                "<root>".into()
            } else {
                field
            },
            message: trim_serde_location_suffix(error.into_inner().to_string()),
        }
    })
}

#[derive(Clone, Copy, Debug)]
pub struct ConfigMapRef<'a> {
    payload: &'a BTreeMap<String, TypedConfigValue>,
}

impl<'a> ConfigMapRef<'a> {
    pub fn new(payload: &'a BTreeMap<String, TypedConfigValue>) -> Self {
        Self { payload }
    }

    pub fn payload(&self) -> &'a BTreeMap<String, TypedConfigValue> {
        self.payload
    }

    pub fn value(&self, field: &str) -> Option<&'a TypedConfigValue> {
        self.payload.get(field)
    }

    pub fn required_value(&self, field: &str) -> Result<&'a TypedConfigValue, ConfigDecodeError> {
        self.value(field).ok_or(ConfigDecodeError::MissingField {
            field: field.to_string(),
            expected: "typed",
        })
    }

    pub fn optional_bool(&self, field: &str) -> Result<Option<bool>, ConfigDecodeError> {
        match self.value(field) {
            Some(TypedConfigValue::Bool(value)) => Ok(Some(*value)),
            Some(other) => Err(ConfigDecodeError::InvalidType {
                field: field.to_string(),
                expected: "bool",
                actual: other.kind_name(),
            }),
            None => Ok(None),
        }
    }

    pub fn required_bool(&self, field: &str) -> Result<bool, ConfigDecodeError> {
        self.optional_bool(field)?
            .ok_or(ConfigDecodeError::MissingField {
                field: field.to_string(),
                expected: "bool",
            })
    }

    pub fn optional_int(&self, field: &str) -> Result<Option<i64>, ConfigDecodeError> {
        match self.value(field) {
            Some(TypedConfigValue::Int(value)) => Ok(Some(*value)),
            Some(other) => Err(ConfigDecodeError::InvalidType {
                field: field.to_string(),
                expected: "int",
                actual: other.kind_name(),
            }),
            None => Ok(None),
        }
    }

    pub fn required_int(&self, field: &str) -> Result<i64, ConfigDecodeError> {
        self.optional_int(field)?
            .ok_or(ConfigDecodeError::MissingField {
                field: field.to_string(),
                expected: "int",
            })
    }

    pub fn optional_uint(&self, field: &str) -> Result<Option<u64>, ConfigDecodeError> {
        match self.value(field) {
            Some(TypedConfigValue::UInt(value)) => Ok(Some(*value)),
            Some(other) => Err(ConfigDecodeError::InvalidType {
                field: field.to_string(),
                expected: "uint",
                actual: other.kind_name(),
            }),
            None => Ok(None),
        }
    }

    pub fn required_uint(&self, field: &str) -> Result<u64, ConfigDecodeError> {
        self.optional_uint(field)?
            .ok_or(ConfigDecodeError::MissingField {
                field: field.to_string(),
                expected: "uint",
            })
    }

    pub fn optional_string(&self, field: &str) -> Result<Option<&'a str>, ConfigDecodeError> {
        match self.value(field) {
            Some(TypedConfigValue::String(value)) => Ok(Some(value.as_str())),
            Some(other) => Err(ConfigDecodeError::InvalidType {
                field: field.to_string(),
                expected: "string",
                actual: other.kind_name(),
            }),
            None => Ok(None),
        }
    }

    pub fn required_string(&self, field: &str) -> Result<&'a str, ConfigDecodeError> {
        self.optional_string(field)?
            .ok_or(ConfigDecodeError::MissingField {
                field: field.to_string(),
                expected: "string",
            })
    }

    pub fn optional_bytes(&self, field: &str) -> Result<Option<&'a [u8]>, ConfigDecodeError> {
        match self.value(field) {
            Some(TypedConfigValue::Bytes(value)) => Ok(Some(value.as_slice())),
            Some(other) => Err(ConfigDecodeError::InvalidType {
                field: field.to_string(),
                expected: "bytes",
                actual: other.kind_name(),
            }),
            None => Ok(None),
        }
    }

    pub fn required_bytes(&self, field: &str) -> Result<&'a [u8], ConfigDecodeError> {
        self.optional_bytes(field)?
            .ok_or(ConfigDecodeError::MissingField {
                field: field.to_string(),
                expected: "bytes",
            })
    }

    pub fn entries_with_prefix(
        &self,
        prefix: &str,
    ) -> impl Iterator<Item = (&'a str, &'a TypedConfigValue)> {
        self.payload
            .iter()
            .filter_map(move |(key, value)| key.strip_prefix(prefix).map(|suffix| (suffix, value)))
    }

    pub fn indexed_group_indices(&self, prefix: &str) -> BTreeSet<usize> {
        self.payload
            .keys()
            .filter_map(|key| key.strip_prefix(prefix))
            .filter_map(|suffix| suffix.strip_prefix('.'))
            .filter_map(|suffix| suffix.split_once('.').map(|(index, _)| index))
            .filter_map(|index| index.parse::<usize>().ok())
            .collect()
    }

    pub fn indexed_group_payload(
        &self,
        prefix: &str,
        index: usize,
    ) -> BTreeMap<String, TypedConfigValue> {
        let group_prefix = format!("{prefix}.{index}.");
        self.payload
            .iter()
            .filter_map(|(key, value)| {
                key.strip_prefix(&group_prefix)
                    .map(|suffix| (suffix.to_string(), value.clone()))
            })
            .collect()
    }

    pub fn named_group_keys(&self, prefix: &str) -> BTreeSet<String> {
        self.payload
            .keys()
            .filter_map(|key| key.strip_prefix(prefix))
            .filter_map(|suffix| suffix.strip_prefix('.'))
            .filter_map(|suffix| suffix.split_once('.').map(|(name, _)| name.to_string()))
            .collect()
    }

    pub fn named_group_payload(
        &self,
        prefix: &str,
        name: &str,
    ) -> BTreeMap<String, TypedConfigValue> {
        let group_prefix = format!("{prefix}.{name}.");
        self.payload
            .iter()
            .filter_map(|(key, value)| {
                key.strip_prefix(&group_prefix)
                    .map(|suffix| (suffix.to_string(), value.clone()))
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use serde::Deserialize;

    use super::*;

    #[derive(Debug, Deserialize, PartialEq, Eq)]
    struct GraphSection {
        kind: String,
        inline: String,
    }

    #[derive(Debug, Deserialize, PartialEq, Eq)]
    struct DerivedConfig {
        width: u64,
        enabled: Option<bool>,
        graph: GraphSection,
    }

    #[derive(Debug, Deserialize, PartialEq, Eq)]
    struct PluginConfig {
        name: String,
        version: Option<String>,
    }

    #[derive(Debug, Deserialize, PartialEq, Eq)]
    struct BindingConfig {
        resource_id: String,
    }

    #[test]
    fn config_map_ref_returns_required_string_and_uint() {
        let payload = BTreeMap::from([
            (
                "graph.kind".to_string(),
                TypedConfigValue::String("inline".into()),
            ),
            ("arg.len".to_string(), TypedConfigValue::UInt(64)),
        ]);
        let config = ConfigMapRef::new(&payload);

        assert_eq!(
            config
                .required_string("graph.kind")
                .expect("graph.kind should decode"),
            "inline"
        );
        assert_eq!(
            config
                .required_uint("arg.len")
                .expect("arg.len should decode"),
            64
        );
    }

    #[test]
    fn config_map_ref_reports_missing_and_invalid_types() {
        let payload = BTreeMap::from([(
            "arg.enabled".to_string(),
            TypedConfigValue::String("yes".into()),
        )]);
        let config = ConfigMapRef::new(&payload);

        assert_eq!(
            config.required_string("arg.missing"),
            Err(ConfigDecodeError::MissingField {
                field: "arg.missing".into(),
                expected: "string",
            })
        );
        assert_eq!(
            config.required_bool("arg.enabled"),
            Err(ConfigDecodeError::InvalidType {
                field: "arg.enabled".into(),
                expected: "bool",
                actual: "string",
            })
        );
    }

    #[test]
    fn config_json_value_builds_nested_hierarchy() {
        let payload = BTreeMap::from([
            ("width".to_string(), TypedConfigValue::UInt(1280)),
            ("enabled".to_string(), TypedConfigValue::Bool(true)),
            (
                "graph.kind".to_string(),
                TypedConfigValue::String("inline".into()),
            ),
            (
                "graph.inline".to_string(),
                TypedConfigValue::String("{\"nodes\":[]}".into()),
            ),
        ]);

        let decoded: DerivedConfig =
            deserialize_config(&payload).expect("nested config should deserialize");
        assert_eq!(
            decoded,
            DerivedConfig {
                width: 1280,
                enabled: Some(true),
                graph: GraphSection {
                    kind: "inline".into(),
                    inline: "{\"nodes\":[]}".into(),
                },
            }
        );
    }

    #[test]
    fn config_json_value_builds_arrays_and_named_maps() {
        #[derive(Debug, Deserialize, PartialEq, Eq)]
        struct Root {
            plugin: Vec<PluginConfig>,
            binding: BTreeMap<String, BindingConfig>,
        }

        let payload = BTreeMap::from([
            (
                "plugin.0.name".to_string(),
                TypedConfigValue::String("cv".into()),
            ),
            (
                "plugin.1.name".to_string(),
                TypedConfigValue::String("slam".into()),
            ),
            (
                "plugin.1.version".to_string(),
                TypedConfigValue::String("1.0.0".into()),
            ),
            (
                "binding.camera.resource_id".to_string(),
                TypedConfigValue::String("resource.camera".into()),
            ),
        ]);

        let decoded: Root = deserialize_config(&payload).expect("collections should deserialize");
        assert_eq!(decoded.plugin.len(), 2);
        assert_eq!(decoded.plugin[0].name, "cv");
        assert_eq!(
            decoded.binding.get("camera"),
            Some(&BindingConfig {
                resource_id: "resource.camera".into()
            })
        );
    }

    #[test]
    fn deserialize_config_reports_precise_field_path() {
        let payload = BTreeMap::from([
            ("width".to_string(), TypedConfigValue::UInt(1280)),
            (
                "graph.kind".to_string(),
                TypedConfigValue::String("inline".into()),
            ),
            ("graph.inline".to_string(), TypedConfigValue::Bool(true)),
        ]);

        assert_eq!(
            deserialize_config::<DerivedConfig>(&payload),
            Err(ConfigDecodeError::Deserialize {
                field: "graph.inline".into(),
                message: "invalid type: boolean `true`, expected a string".into(),
            })
        );
    }

    #[test]
    fn config_json_value_rejects_conflicting_paths() {
        let payload = BTreeMap::from([
            (
                "graph".to_string(),
                TypedConfigValue::String("inline".into()),
            ),
            (
                "graph.kind".to_string(),
                TypedConfigValue::String("inline".into()),
            ),
        ]);

        assert!(matches!(
            config_json_value(&payload),
            Err(ConfigDecodeError::InvalidStructure { field, .. }) if field == "graph.kind"
        ));
    }

    #[test]
    fn config_map_ref_collects_indexed_group_indices() {
        let payload = BTreeMap::from([
            (
                "plugin.0.name".to_string(),
                TypedConfigValue::String("cv".into()),
            ),
            (
                "plugin.2.name".to_string(),
                TypedConfigValue::String("slam".into()),
            ),
            (
                "plugin.2.version".to_string(),
                TypedConfigValue::String("1.0.0".into()),
            ),
            (
                "binding.input.resource_id".to_string(),
                TypedConfigValue::String("resource.camera".into()),
            ),
        ]);
        let config = ConfigMapRef::new(&payload);

        assert_eq!(
            config.indexed_group_indices("plugin"),
            BTreeSet::from([0, 2])
        );
        assert_eq!(
            config
                .entries_with_prefix("binding.")
                .map(|(suffix, _)| suffix)
                .collect::<Vec<_>>(),
            vec!["input.resource_id"]
        );
        assert_eq!(
            config.indexed_group_payload("plugin", 2),
            BTreeMap::from([
                ("name".to_string(), TypedConfigValue::String("slam".into())),
                (
                    "version".to_string(),
                    TypedConfigValue::String("1.0.0".into())
                ),
            ])
        );
        assert_eq!(
            config.named_group_keys("binding"),
            BTreeSet::from(["input".to_string()])
        );
        assert_eq!(
            config.named_group_payload("binding", "input"),
            BTreeMap::from([(
                "resource_id".to_string(),
                TypedConfigValue::String("resource.camera".into())
            )])
        );
    }
}
