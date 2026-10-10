//! NT4 value types and their numeric type ids.

use serde::{Deserialize, Serialize};

/// Binary type id of a `boolean` value.
pub const BOOLEAN: u8 = 0;
/// Binary type id of a `double` value.
pub const DOUBLE: u8 = 1;
/// Binary type id of an `int` value (signed 64-bit).
pub const INT: u8 = 2;
/// Binary type id of a `float` value.
pub const FLOAT: u8 = 3;
/// Binary type id of a `string` value.
pub const STRING: u8 = 4;
/// Binary type id of a `raw` value. `msgpack`, `protobuf` and `struct:*` topics also carry raw bytes.
pub const RAW: u8 = 5;
/// Binary type id of a `boolean[]` value.
pub const BOOLEAN_ARRAY: u8 = 16;
/// Binary type id of a `double[]` value.
pub const DOUBLE_ARRAY: u8 = 17;
/// Binary type id of an `int[]` value.
pub const INT_ARRAY: u8 = 18;
/// Binary type id of a `float[]` value.
pub const FLOAT_ARRAY: u8 = 19;
/// Binary type id of a `string[]` value.
pub const STRING_ARRAY: u8 = 20;

/// A NetworkTables value.
///
/// The variant decides the binary type id. A topic's type string (for example `"double"`, or
/// `"struct:Pose2d"`) decides which variant a topic accepts; see [`type_id_for_name`]. Raw bytes
/// stay opaque: `msgpack`, `protobuf` and `struct:*` topics carry [`Value::Raw`] and the topic's
/// type string says how to decode it.
///
/// JSON (serde) uses `{"type": "...", "value": ...}` with the NT4 type string as `type`. Raw bytes
/// serialize as a JSON array of numbers, and non-finite floats serialize as `null`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "value")]
pub enum Value {
    /// `boolean`
    #[serde(rename = "boolean")]
    Boolean(bool),
    /// `double`
    #[serde(rename = "double")]
    Double(f64),
    /// `int`
    #[serde(rename = "int")]
    Int(i64),
    /// `float`
    #[serde(rename = "float")]
    Float(f32),
    /// `string`
    #[serde(rename = "string")]
    String(String),
    /// `raw`, `msgpack`, `protobuf`, `struct:*` (opaque bytes)
    #[serde(rename = "raw")]
    Raw(Vec<u8>),
    /// `boolean[]`
    #[serde(rename = "boolean[]")]
    BooleanArray(Vec<bool>),
    /// `double[]`
    #[serde(rename = "double[]")]
    DoubleArray(Vec<f64>),
    /// `int[]`
    #[serde(rename = "int[]")]
    IntArray(Vec<i64>),
    /// `float[]`
    #[serde(rename = "float[]")]
    FloatArray(Vec<f32>),
    /// `string[]`
    #[serde(rename = "string[]")]
    StringArray(Vec<String>),
}

impl Value {
    /// The binary type id of this value (`BOOLEAN`, `DOUBLE`, ... `STRING_ARRAY`).
    pub fn type_id(&self) -> u8 {
        match self {
            Self::Boolean(_) => BOOLEAN,
            Self::Double(_) => DOUBLE,
            Self::Int(_) => INT,
            Self::Float(_) => FLOAT,
            Self::String(_) => STRING,
            Self::Raw(_) => RAW,
            Self::BooleanArray(_) => BOOLEAN_ARRAY,
            Self::DoubleArray(_) => DOUBLE_ARRAY,
            Self::IntArray(_) => INT_ARRAY,
            Self::FloatArray(_) => FLOAT_ARRAY,
            Self::StringArray(_) => STRING_ARRAY,
        }
    }

    /// The type string for this value when no topic says otherwise (`raw` for [`Value::Raw`]).
    pub fn default_type_name(&self) -> &'static str {
        match self {
            Self::Boolean(_) => "boolean",
            Self::Double(_) => "double",
            Self::Int(_) => "int",
            Self::Float(_) => "float",
            Self::String(_) => "string",
            Self::Raw(_) => "raw",
            Self::BooleanArray(_) => "boolean[]",
            Self::DoubleArray(_) => "double[]",
            Self::IntArray(_) => "int[]",
            Self::FloatArray(_) => "float[]",
            Self::StringArray(_) => "string[]",
        }
    }

    /// Whether this value may be published on a topic of type `type_name`.
    pub fn fits_type(&self, type_name: &str) -> bool {
        type_id_for_name(type_name) == Some(self.type_id())
    }

    /// The boolean, if this is a `boolean`.
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Self::Boolean(v) => Some(*v),
            _ => None,
        }
    }

    /// The number, if this is a `double`, `float` or `int` (ints convert lossily above 2^53).
    #[allow(clippy::cast_precision_loss)]
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Self::Double(v) => Some(*v),
            Self::Float(v) => Some(f64::from(*v)),
            Self::Int(v) => Some(*v as f64),
            _ => None,
        }
    }

    /// The integer, if this is an `int`.
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Self::Int(v) => Some(*v),
            _ => None,
        }
    }

    /// The string, if this is a `string`.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::String(v) => Some(v),
            _ => None,
        }
    }
}

/// The binary type id for an NT4 type string, or `None` for a type this crate does not model.
///
/// `msgpack`, `protobuf`, `raw` and every `struct:*` / `proto:*` type map to [`RAW`]. Type strings
/// are matched exactly, so `"double[]"` is [`DOUBLE_ARRAY`].
pub fn type_id_for_name(type_name: &str) -> Option<u8> {
    Some(match type_name {
        "boolean" => BOOLEAN,
        "double" => DOUBLE,
        "int" => INT,
        "float" => FLOAT,
        "string" => STRING,
        "raw" | "msgpack" | "protobuf" => RAW,
        "boolean[]" => BOOLEAN_ARRAY,
        "double[]" => DOUBLE_ARRAY,
        "int[]" => INT_ARRAY,
        "float[]" => FLOAT_ARRAY,
        "string[]" => STRING_ARRAY,
        other if other.starts_with("struct:") || other.starts_with("proto:") => RAW,
        _ => return None,
    })
}

impl From<bool> for Value {
    fn from(v: bool) -> Self {
        Self::Boolean(v)
    }
}

impl From<f64> for Value {
    fn from(v: f64) -> Self {
        Self::Double(v)
    }
}

impl From<f32> for Value {
    fn from(v: f32) -> Self {
        Self::Float(v)
    }
}

impl From<i64> for Value {
    fn from(v: i64) -> Self {
        Self::Int(v)
    }
}

impl From<&str> for Value {
    fn from(v: &str) -> Self {
        Self::String(v.to_owned())
    }
}

impl From<String> for Value {
    fn from(v: String) -> Self {
        Self::String(v)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn type_names_map_to_ids() {
        assert_eq!(type_id_for_name("boolean"), Some(BOOLEAN));
        assert_eq!(type_id_for_name("double[]"), Some(DOUBLE_ARRAY));
        assert_eq!(type_id_for_name("msgpack"), Some(RAW));
        assert_eq!(type_id_for_name("struct:Pose2d[]"), Some(RAW));
        assert_eq!(type_id_for_name("proto:frc.Foo"), Some(RAW));
        assert_eq!(type_id_for_name("json"), None);
    }

    #[test]
    fn values_fit_their_type_names() {
        assert!(Value::Double(1.0).fits_type("double"));
        assert!(!Value::Double(1.0).fits_type("float"));
        assert!(Value::Raw(vec![1]).fits_type("struct:Pose2d"));
        assert!(!Value::Raw(vec![1]).fits_type("double"));
    }

    #[test]
    fn json_uses_nt4_type_strings() {
        let json = serde_json::to_string(&Value::IntArray(vec![1, 2])).unwrap();
        assert_eq!(json, r#"{"type":"int[]","value":[1,2]}"#);
        let back: Value = serde_json::from_str(&json).unwrap();
        assert_eq!(back, Value::IntArray(vec![1, 2]));
    }
}
