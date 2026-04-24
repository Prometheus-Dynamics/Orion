use std::collections::{BTreeMap, BTreeSet};

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
}

#[cfg(test)]
mod tests {
    use super::*;

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
    }
}
