//! A value here as an NT4 value, and back. The field's type and the NT4 type string decide;
//! a pair that does not go together is refused, never guessed at.

use orion_msg_topics::FieldType;
use orion_nt4::Value as Nt;
use orion_nt4::value as nt;
use orion_values::Value;

/// A payload of type `ty`, as a value of the NT4 type `nt_type`. `None` if the payload is
/// not what its type says, or the two types do not go together.
///
/// | Here | NT4 |
/// | --- | --- |
/// | `Bool` | `boolean` |
/// | `Int` | `int`, `double`, `float` |
/// | `Float` | `double`, `float` |
/// | `Text` | `string`, `json` |
/// | `Bytes`, `Structured` | `raw` and every type that is bytes: `struct:…`, `proto:…`, `msgpack`, `protobuf` |
/// | `FloatArray` | `double[]`, `float[]` |
/// | `IntArray` | `int[]` |
#[must_use]
pub fn to_nt4(ty: FieldType, payload: &[u8], nt_type: &str) -> Option<Nt> {
    let id = orion_nt4::type_id_for_name(nt_type)?;
    Some(match (Value::read(ty, payload).ok()?, id) {
        (Value::Bool(value), nt::BOOLEAN) => Nt::Boolean(value),
        (Value::Int(value), nt::INT) => Nt::Int(value),
        (Value::Int(value), nt::DOUBLE) => Nt::Double(value as f64),
        (Value::Int(value), nt::FLOAT) => Nt::Float(value as f32),
        (Value::Float(value), nt::DOUBLE) => Nt::Double(value),
        (Value::Float(value), nt::FLOAT) => Nt::Float(value as f32),
        (Value::Text(value), nt::STRING) => Nt::String(value.to_owned()),
        (Value::Bytes(value) | Value::Structured(value), nt::RAW) => Nt::Raw(value.to_vec()),
        (Value::FloatArray(values), nt::DOUBLE_ARRAY) => Nt::DoubleArray(values.iter().collect()),
        (Value::FloatArray(values), nt::FLOAT_ARRAY) => {
            Nt::FloatArray(values.iter().map(|value| value as f32).collect())
        }
        (Value::IntArray(values), nt::INT_ARRAY) => Nt::IntArray(values.iter().collect()),
        _ => return None,
    })
}

/// An NT4 value as the payload of a field of type `ty`, written to `out`, which is cleared
/// first. `None` if the value is not one that type takes. A client may write a number of
/// another width than the mapping names; a whole number is not taken for a fraction's place
/// the other way round.
///
/// | NT4 value | Here |
/// | --- | --- |
/// | boolean | `Bool` |
/// | int | `Int`, `Float` |
/// | double, float | `Float` |
/// | string | `Text` |
/// | raw | `Bytes` |
/// | double[], float[] | `FloatArray` |
/// | int[] | `IntArray` |
#[must_use]
pub fn from_nt4(value: &Nt, ty: FieldType, out: &mut Vec<u8>) -> Option<()> {
    out.clear();
    match (value, ty) {
        (Nt::Boolean(value), FieldType::Bool) => out.push(u8::from(*value)),
        (Nt::Int(value), FieldType::Int) => out.extend_from_slice(&value.to_le_bytes()),
        (Nt::Int(value), FieldType::Float) => out.extend_from_slice(&(*value as f64).to_le_bytes()),
        (Nt::Double(value), FieldType::Float) => out.extend_from_slice(&value.to_le_bytes()),
        (Nt::Float(value), FieldType::Float) => {
            out.extend_from_slice(&f64::from(*value).to_le_bytes());
        }
        (Nt::String(value), FieldType::Text) => out.extend_from_slice(value.as_bytes()),
        (Nt::Raw(value), FieldType::Bytes) => out.extend_from_slice(value),
        (Nt::DoubleArray(values), FieldType::FloatArray) => {
            values
                .iter()
                .for_each(|value| out.extend_from_slice(&value.to_le_bytes()));
        }
        (Nt::FloatArray(values), FieldType::FloatArray) => values
            .iter()
            .for_each(|value| out.extend_from_slice(&f64::from(*value).to_le_bytes())),
        (Nt::IntArray(values), FieldType::IntArray) => {
            values
                .iter()
                .for_each(|value| out.extend_from_slice(&value.to_le_bytes()));
        }
        _ => return None,
    }
    Some(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_pair_that_goes_together_goes_there_and_back() {
        let floats: Vec<u8> = [1.5_f64, -2.0]
            .iter()
            .flat_map(|v| v.to_le_bytes())
            .collect();
        let ints: Vec<u8> = [7_i64, -9].iter().flat_map(|v| v.to_le_bytes()).collect();
        let cases: [(FieldType, &[u8], &str, Nt); 9] = [
            (FieldType::Bool, &[1], "boolean", Nt::Boolean(true)),
            (FieldType::Int, &(-5_i64).to_le_bytes(), "int", Nt::Int(-5)),
            (
                FieldType::Float,
                &2.25_f64.to_le_bytes(),
                "double",
                Nt::Double(2.25),
            ),
            (
                FieldType::Float,
                &2.25_f64.to_le_bytes(),
                "float",
                Nt::Float(2.25),
            ),
            (
                FieldType::Text,
                "héllo".as_bytes(),
                "string",
                Nt::String("héllo".into()),
            ),
            (
                FieldType::Bytes,
                &[1, 2, 3],
                "struct:Pose2d",
                Nt::Raw(vec![1, 2, 3]),
            ),
            (
                FieldType::FloatArray,
                &floats,
                "double[]",
                Nt::DoubleArray(vec![1.5, -2.0]),
            ),
            (
                FieldType::FloatArray,
                &floats,
                "float[]",
                Nt::FloatArray(vec![1.5, -2.0]),
            ),
            (
                FieldType::IntArray,
                &ints,
                "int[]",
                Nt::IntArray(vec![7, -9]),
            ),
        ];
        let mut back = Vec::new();
        for (ty, payload, nt_type, value) in cases {
            assert_eq!(
                to_nt4(ty, payload, nt_type),
                Some(value.clone()),
                "{nt_type}"
            );
            assert_eq!(from_nt4(&value, ty, &mut back), Some(()), "{nt_type}");
            assert_eq!(back, payload, "{nt_type}");
        }
        // A whole number may be shown as a fraction, and a structured value is its bytes.
        assert_eq!(
            to_nt4(FieldType::Int, &3_i64.to_le_bytes(), "double"),
            Some(Nt::Double(3.0))
        );
        assert_eq!(
            to_nt4(FieldType::Structured, &[9, 9], "raw"),
            Some(Nt::Raw(vec![9, 9]))
        );
        assert_eq!(from_nt4(&Nt::Int(4), FieldType::Float, &mut back), Some(()));
        assert_eq!(back, 4.0_f64.to_le_bytes());
    }

    #[test]
    fn what_does_not_go_together_is_refused() {
        let mut out = Vec::new();
        // The types do not match.
        assert_eq!(
            to_nt4(FieldType::Float, &1.0_f64.to_le_bytes(), "int"),
            None
        );
        assert_eq!(to_nt4(FieldType::Text, b"x", "double"), None);
        assert_eq!(to_nt4(FieldType::Bool, &[1], "boolean[]"), None);
        assert_eq!(to_nt4(FieldType::Buffers, &[0; 8], "raw"), None);
        // The payload is not what its type says.
        assert_eq!(to_nt4(FieldType::Float, &[0; 7], "double"), None);
        assert_eq!(to_nt4(FieldType::Bool, &[2], "boolean"), None);
        assert_eq!(to_nt4(FieldType::Text, &[0xFF], "string"), None);
        // Not an NT4 type at all.
        assert_eq!(
            to_nt4(FieldType::Float, &1.0_f64.to_le_bytes(), "number"),
            None
        );
        // A fraction is not taken for a whole number, and so on.
        assert_eq!(from_nt4(&Nt::Double(1.5), FieldType::Int, &mut out), None);
        assert_eq!(
            from_nt4(&Nt::String("x".into()), FieldType::Bytes, &mut out),
            None
        );
        assert_eq!(
            from_nt4(&Nt::BooleanArray(vec![true]), FieldType::Bool, &mut out),
            None
        );
        assert_eq!(
            from_nt4(&Nt::StringArray(vec![]), FieldType::Text, &mut out),
            None
        );
    }
}
