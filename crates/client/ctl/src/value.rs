//! Values as text: a sample shown by its field's type, and a typed value read from text.

use orion_msg_topics::FieldType;

/// A sample's bytes as a person reads them, by the type its field was declared with.
pub(crate) fn show(ty: FieldType, payload: &[u8]) -> String {
    let eight = |payload: &[u8]| <[u8; 8]>::try_from(payload).ok();
    match ty {
        FieldType::Bool => match payload {
            [0] => "false".into(),
            [1] => "true".into(),
            _ => hex(payload),
        },
        FieldType::Int => eight(payload).map_or_else(
            || hex(payload),
            |bytes| i64::from_le_bytes(bytes).to_string(),
        ),
        FieldType::Float => eight(payload).map_or_else(
            || hex(payload),
            |bytes| f64::from_le_bytes(bytes).to_string(),
        ),
        FieldType::Text => String::from_utf8_lossy(payload).into_owned(),
        FieldType::FloatArray if payload.len().is_multiple_of(8) => {
            let values: Vec<String> = payload
                .chunks(8)
                .filter_map(eight)
                .map(|bytes| f64::from_le_bytes(bytes).to_string())
                .collect();
            format!("[{}]", values.join(", "))
        }
        FieldType::IntArray if payload.len().is_multiple_of(8) => {
            let values: Vec<String> = payload
                .chunks(8)
                .filter_map(eight)
                .map(|bytes| i64::from_le_bytes(bytes).to_string())
                .collect();
            format!("[{}]", values.join(", "))
        }
        _ => hex(payload),
    }
}

/// Bytes as lowercase hex.
pub(crate) fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    bytes
        .iter()
        .fold(String::with_capacity(bytes.len() * 2), |mut out, byte| {
            let _ = write!(out, "{byte:02x}");
            out
        })
}

/// Hex as bytes.
pub(crate) fn unhex(text: &str) -> Option<Vec<u8>> {
    if !text.len().is_multiple_of(2) || !text.is_ascii() {
        return None;
    }
    text.as_bytes()
        .chunks(2)
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).ok()?, 16).ok())
        .collect()
}

/// A field to publish, written `name:type=value`: its name, its type and its value's bytes.
pub(crate) fn field(text: &str) -> Result<(String, FieldType, Vec<u8>), String> {
    let form = || format!("{text:?} is not name:type=value");
    let (declared, value) = text.split_once('=').ok_or_else(form)?;
    let (name, ty) = declared.split_once(':').ok_or_else(form)?;
    let bad = |what: &str| format!("{value:?} is not {what}");
    let (ty, bytes) = match ty {
        "bool" => (
            FieldType::Bool,
            vec![u8::from(
                value.parse::<bool>().map_err(|_| bad("true or false"))?,
            )],
        ),
        "int" => (
            FieldType::Int,
            value
                .parse::<i64>()
                .map_err(|_| bad("a whole number"))?
                .to_le_bytes()
                .to_vec(),
        ),
        "float" => (
            FieldType::Float,
            value
                .parse::<f64>()
                .map_err(|_| bad("a number"))?
                .to_le_bytes()
                .to_vec(),
        ),
        "text" => (FieldType::Text, value.as_bytes().to_vec()),
        "bytes" => (FieldType::Bytes, unhex(value).ok_or_else(|| bad("hex"))?),
        other => return Err(format!("{other:?} is not bool, int, float, text or bytes")),
    };
    if name.is_empty() {
        return Err(form());
    }
    Ok((name.to_owned(), ty, bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_value_is_shown_by_its_fields_type() {
        assert_eq!(show(FieldType::Bool, &[1]), "true");
        assert_eq!(show(FieldType::Int, &(-7_i64).to_le_bytes()), "-7");
        assert_eq!(show(FieldType::Float, &1.5_f64.to_le_bytes()), "1.5");
        assert_eq!(show(FieldType::Text, b"idle"), "idle");
        let array: Vec<u8> = [1.0_f64, 2.5]
            .iter()
            .flat_map(|value| value.to_le_bytes())
            .collect();
        assert_eq!(show(FieldType::FloatArray, &array), "[1, 2.5]");
        // Bytes, and anything that is not the size its type says, are shown as they are.
        assert_eq!(show(FieldType::Bytes, &[0xAB, 0x01]), "ab01");
        assert_eq!(show(FieldType::Int, &[1, 2]), "0102");
        assert_eq!(show(FieldType::Bool, &[7]), "07");
    }

    #[test]
    fn a_field_is_read_from_text() {
        assert_eq!(
            field("rpm:float=1200"),
            Ok((
                "rpm".into(),
                FieldType::Float,
                1200.0_f64.to_le_bytes().to_vec()
            ))
        );
        assert_eq!(
            field("on:bool=true"),
            Ok(("on".into(), FieldType::Bool, vec![1]))
        );
        assert_eq!(
            field("count:int=-3"),
            Ok((
                "count".into(),
                FieldType::Int,
                (-3_i64).to_le_bytes().to_vec()
            ))
        );
        assert_eq!(
            field("mode:text=a=b"),
            Ok(("mode".into(), FieldType::Text, b"a=b".to_vec()))
        );
        assert_eq!(
            field("raw:bytes=00ff"),
            Ok(("raw".into(), FieldType::Bytes, vec![0, 255]))
        );
        for wrong in [
            "rpm=1",
            "rpm:float",
            ":int=1",
            "rpm:speed=1",
            "rpm:float=fast",
            "raw:bytes=xyz",
            "on:bool=1",
        ] {
            assert!(field(wrong).is_err(), "{wrong}");
        }
        assert_eq!(
            (unhex("0a"), unhex("a"), hex(&[10])),
            (Some(vec![10]), None, "0a".into())
        );
    }
}
