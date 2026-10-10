//! Binary frames: `[topic id, timestamp (us), type id, value]` as MessagePack arrays.
//!
//! Topic id `-1` marks an RTT ping: the client sends `[-1, 0, INT, client_time]` and the server
//! answers `[-1, server_time, INT, client_time]`.

use crate::error::{Error, Result};
use crate::value::{self, Value};

/// One decoded binary frame.
#[derive(Clone, Debug, PartialEq)]
pub struct Frame {
    /// Topic id (server-assigned id from a server frame, publisher uid from a client frame, or -1).
    pub id: i32,
    /// Microseconds on the server's clock (or the sender's clock before sync).
    pub timestamp_us: i64,
    /// The value. Its variant gives the binary type id.
    pub value: Value,
}

/// Encodes one binary frame.
pub fn encode_frame(id: i32, timestamp_us: i64, value: &Value) -> Result<Vec<u8>> {
    let mut buf = Vec::with_capacity(16);
    enc(rmp::encode::write_array_len(&mut buf, 4))?;
    enc(rmp::encode::write_sint(&mut buf, i64::from(id)))?;
    enc(rmp::encode::write_sint(&mut buf, timestamp_us))?;
    enc(rmp::encode::write_sint(
        &mut buf,
        i64::from(value.type_id()),
    ))?;
    write_value(&mut buf, value)?;
    Ok(buf)
}

/// Decodes one binary frame. Trailing bytes are an error.
pub fn decode_frame(mut bytes: &[u8]) -> Result<Frame> {
    let top = rmpv::decode::read_value(&mut bytes).map_err(|e| dec(e.to_string()))?;
    if !bytes.is_empty() {
        return Err(dec("trailing bytes after frame".into()));
    }
    let rmpv::Value::Array(items) = top else {
        return Err(dec("frame is not an array".into()));
    };
    let [id, timestamp, type_id, value] = <[rmpv::Value; 4]>::try_from(items)
        .map_err(|_| dec("frame must have 4 elements".into()))?;
    let id = i32::try_from(int_of(&id)?).map_err(|_| dec("topic id out of range".into()))?;
    let timestamp_us = int_of(&timestamp)?;
    let type_id =
        u8::try_from(int_of(&type_id)?).map_err(|_| dec("type id out of range".into()))?;
    let value = value_from_msgpack(type_id, value)?;
    Ok(Frame {
        id,
        timestamp_us,
        value,
    })
}

fn dec(msg: String) -> Error {
    Error::Decode(msg)
}

fn enc<T, E: std::fmt::Display>(result: std::result::Result<T, E>) -> Result<T> {
    result.map_err(|e| Error::Encode(e.to_string()))
}

fn len_u32(len: usize) -> Result<u32> {
    u32::try_from(len).map_err(|_| Error::Encode("array longer than u32::MAX".into()))
}

fn write_value(buf: &mut Vec<u8>, value: &Value) -> Result<()> {
    match value {
        Value::Boolean(v) => enc(rmp::encode::write_bool(buf, *v)),
        Value::Double(v) => enc(rmp::encode::write_f64(buf, *v)),
        Value::Int(v) => enc(rmp::encode::write_sint(buf, *v).map(|_| ())),
        Value::Float(v) => enc(rmp::encode::write_f32(buf, *v)),
        Value::String(v) => enc(rmp::encode::write_str(buf, v)),
        Value::Raw(v) => enc(rmp::encode::write_bin(buf, v)),
        Value::BooleanArray(items) => {
            enc(rmp::encode::write_array_len(buf, len_u32(items.len())?))?;
            items
                .iter()
                .try_for_each(|v| enc(rmp::encode::write_bool(buf, *v)))
        }
        Value::DoubleArray(items) => {
            enc(rmp::encode::write_array_len(buf, len_u32(items.len())?))?;
            items
                .iter()
                .try_for_each(|v| enc(rmp::encode::write_f64(buf, *v)))
        }
        Value::IntArray(items) => {
            enc(rmp::encode::write_array_len(buf, len_u32(items.len())?))?;
            items
                .iter()
                .try_for_each(|v| enc(rmp::encode::write_sint(buf, *v).map(|_| ())))
        }
        Value::FloatArray(items) => {
            enc(rmp::encode::write_array_len(buf, len_u32(items.len())?))?;
            items
                .iter()
                .try_for_each(|v| enc(rmp::encode::write_f32(buf, *v)))
        }
        Value::StringArray(items) => {
            enc(rmp::encode::write_array_len(buf, len_u32(items.len())?))?;
            items
                .iter()
                .try_for_each(|v| enc(rmp::encode::write_str(buf, v)))
        }
    }
}

fn int_of(v: &rmpv::Value) -> Result<i64> {
    match v {
        rmpv::Value::Integer(i) => i
            .as_i64()
            .ok_or_else(|| dec("integer out of i64 range".into())),
        _ => Err(dec("expected an integer".into())),
    }
}

fn mismatch(type_id: u8) -> Error {
    dec(format!("value does not match binary type id {type_id}"))
}

fn value_from_msgpack(type_id: u8, v: rmpv::Value) -> Result<Value> {
    Ok(match (type_id, v) {
        (value::BOOLEAN, rmpv::Value::Boolean(b)) => Value::Boolean(b),
        (value::DOUBLE, rmpv::Value::F64(d)) => Value::Double(d),
        (value::DOUBLE, rmpv::Value::F32(d)) => Value::Double(f64::from(d)),
        (value::INT, rmpv::Value::Integer(i)) => Value::Int(
            i.as_i64()
                .ok_or_else(|| dec("int value out of i64 range".into()))?,
        ),
        (value::FLOAT, rmpv::Value::F32(f)) => Value::Float(f),
        (value::FLOAT, rmpv::Value::F64(f)) => Value::Float(f as f32),
        (value::STRING, rmpv::Value::String(s)) => Value::String(
            s.into_str()
                .ok_or_else(|| dec("string is not UTF-8".into()))?,
        ),
        (value::RAW, rmpv::Value::Binary(b)) => Value::Raw(b),
        (value::BOOLEAN_ARRAY, rmpv::Value::Array(items)) => Value::BooleanArray(
            items
                .into_iter()
                .map(|x| match x {
                    rmpv::Value::Boolean(b) => Ok(b),
                    _ => Err(mismatch(type_id)),
                })
                .collect::<Result<_>>()?,
        ),
        (value::DOUBLE_ARRAY, rmpv::Value::Array(items)) => Value::DoubleArray(
            items
                .into_iter()
                .map(|x| match x {
                    rmpv::Value::F64(d) => Ok(d),
                    rmpv::Value::F32(d) => Ok(f64::from(d)),
                    _ => Err(mismatch(type_id)),
                })
                .collect::<Result<_>>()?,
        ),
        (value::INT_ARRAY, rmpv::Value::Array(items)) => {
            Value::IntArray(items.iter().map(int_of).collect::<Result<_>>()?)
        }
        (value::FLOAT_ARRAY, rmpv::Value::Array(items)) => Value::FloatArray(
            items
                .into_iter()
                .map(|x| match x {
                    rmpv::Value::F32(f) => Ok(f),
                    rmpv::Value::F64(f) => Ok(f as f32),
                    _ => Err(mismatch(type_id)),
                })
                .collect::<Result<_>>()?,
        ),
        (value::STRING_ARRAY, rmpv::Value::Array(items)) => Value::StringArray(
            items
                .into_iter()
                .map(|x| match x {
                    rmpv::Value::String(s) => s
                        .into_str()
                        .ok_or_else(|| dec("string is not UTF-8".into())),
                    _ => Err(mismatch(type_id)),
                })
                .collect::<Result<_>>()?,
        ),
        (other, _) => return Err(mismatch(other)),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip(value: Value) {
        let bytes = encode_frame(7, 123_456, &value).unwrap();
        let frame = decode_frame(&bytes).unwrap();
        assert_eq!(
            frame,
            Frame {
                id: 7,
                timestamp_us: 123_456,
                value
            }
        );
    }

    #[test]
    fn every_type_round_trips() {
        round_trip(Value::Boolean(true));
        round_trip(Value::Double(-1.5e300));
        round_trip(Value::Int(i64::MIN));
        round_trip(Value::Int(i64::MAX));
        round_trip(Value::Float(0.25));
        round_trip(Value::String("héllo".into()));
        round_trip(Value::Raw(vec![0, 1, 2, 255]));
        round_trip(Value::BooleanArray(vec![true, false]));
        round_trip(Value::DoubleArray(vec![1.0, f64::INFINITY]));
        round_trip(Value::IntArray(vec![-1, 0, 1 << 40]));
        round_trip(Value::FloatArray(vec![3.5]));
        round_trip(Value::StringArray(vec!["a".into(), String::new()]));
        round_trip(Value::BooleanArray(Vec::new()));
    }

    #[test]
    fn double_frame_matches_the_spec_bytes() {
        // fixarray(4), id -1, timestamp 0, type 1, float64 1.0
        let bytes = encode_frame(-1, 0, &Value::Double(1.0)).unwrap();
        assert_eq!(
            bytes,
            [0x94, 0xff, 0x00, 0x01, 0xcb, 0x3f, 0xf0, 0, 0, 0, 0, 0, 0]
        );
    }

    #[test]
    fn integers_decode_from_any_msgpack_width() {
        // [-1, 0, 2, 5] with the int as uint8 (0xcc 0x05): accepted for the int type.
        let bytes = [0x94, 0xff, 0x00, 0x02, 0xcc, 0x05];
        assert_eq!(decode_frame(&bytes).unwrap().value, Value::Int(5));
    }

    #[test]
    fn rejects_mismatched_and_trailing_data() {
        // type id 1 (double) carrying a string
        let bytes = [0x94, 0x00, 0x00, 0x01, 0xa1, b'x'];
        assert!(decode_frame(&bytes).is_err());
        let mut trailing = encode_frame(1, 2, &Value::Boolean(false)).unwrap();
        trailing.push(0);
        assert!(decode_frame(&trailing).is_err());
        assert!(decode_frame(&[0x90]).is_err());
    }
}
