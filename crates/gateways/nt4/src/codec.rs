//! Binary frames: `[topic id, timestamp (us), type id, value]` as MessagePack arrays.
//!
//! Topic id `-1` marks an RTT ping: the client sends `[-1, 0, INT, client_time]` and the server
//! answers `[-1, server_time, INT, client_time]`.

use crate::error::{Error, Result};
use crate::msgpack as mp;
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

/// Runs `f` on a per-thread scratch buffer holding one encoded frame.
fn with_frame<T>(
    id: i32,
    timestamp_us: i64,
    value: &Value,
    f: impl FnOnce(&[u8]) -> T,
) -> Result<T> {
    thread_local! {
        static SCRATCH: std::cell::RefCell<Vec<u8>> = const { std::cell::RefCell::new(Vec::new()) };
    }
    SCRATCH.with(|scratch| {
        let mut buf = scratch.borrow_mut();
        buf.clear();
        encode_frame_into(&mut buf, id, timestamp_us, value)?;
        let out = f(&buf);
        // A large value's scratch is not kept around.
        if buf.capacity() > 4096 {
            *buf = Vec::new();
        }
        Ok(out)
    })
}

/// Encodes one binary frame (one message).
///
/// The buffer has exactly the frame's length: encoded in a reused per-thread scratch, then
/// copied once.
pub fn encode_frame(id: i32, timestamp_us: i64, value: &Value) -> Result<Vec<u8>> {
    with_frame(id, timestamp_us, value, <[u8]>::to_vec)
}

/// As [`encode_frame`], into bytes that many readers share: one allocation.
#[cfg(feature = "server")]
pub(crate) fn encode_frame_shared(
    id: i32,
    timestamp_us: i64,
    value: &Value,
) -> Result<std::sync::Arc<[u8]>> {
    with_frame(id, timestamp_us, value, |frame| std::sync::Arc::from(frame))
}

/// Appends one encoded message to `buf`. Several messages in one buffer make a batched binary
/// frame, which is what ntcore sends (see [`decode_frames`]).
pub fn encode_frame_into(
    buf: &mut Vec<u8>,
    id: i32,
    timestamp_us: i64,
    value: &Value,
) -> Result<()> {
    mp::array_len(buf, 4);
    mp::int(buf, i64::from(id));
    mp::int(buf, timestamp_us);
    mp::int(buf, i64::from(value.type_id()));
    write_value(buf, value)
}

/// Encodes several messages as one batched binary frame, to save WebSocket messages.
pub fn encode_frames<'a>(
    messages: impl IntoIterator<Item = (i32, i64, &'a Value)>,
) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    for (id, timestamp_us, value) in messages {
        encode_frame_into(&mut buf, id, timestamp_us, value)?;
    }
    Ok(buf)
}

/// Decodes every message in a binary frame. A frame is a sequence of concatenated MessagePack
/// arrays: ntcore batches several values into one WebSocket message. Returns the messages decoded
/// before any malformed one, plus the error that stopped decoding; the rest of the frame is
/// dropped, and the connection stays up.
pub fn decode_frames(bytes: &[u8]) -> (Vec<Frame>, Option<Error>) {
    let mut frames = Vec::new();
    let error = for_each_frame(bytes, |frame| frames.push(frame));
    (frames, error)
}

/// Decodes every message in a binary frame and hands each to `f`, as [`decode_frames`] does but
/// without collecting them. Header and scalar values (boolean, double, int, float) decode without
/// allocating. Returns the error that stopped decoding, if any.
pub fn for_each_frame(mut bytes: &[u8], mut f: impl FnMut(Frame)) -> Option<Error> {
    while !bytes.is_empty() {
        match decode_one(&mut bytes) {
            Ok(frame) => f(frame),
            Err(e) => return Some(e),
        }
    }
    None
}

fn rd(e: mp::Bad) -> Error {
    dec(e.to_owned())
}

/// An array's elements, each read by `item`. Nothing is set aside for more elements than
/// the bytes left could hold.
fn items<T>(
    bytes: &mut &[u8],
    item: impl Fn(&mut &[u8]) -> std::result::Result<T, mp::Bad>,
) -> Result<Vec<T>> {
    let len = mp::read_array_len(bytes).map_err(rd)?;
    let mut out = Vec::with_capacity(len.min(bytes.len()));
    for _ in 0..len {
        out.push(item(bytes).map_err(rd)?);
    }
    Ok(out)
}

/// One message from the front of `bytes`, which advances past it.
fn decode_one(bytes: &mut &[u8]) -> Result<Frame> {
    let len = mp::read_array_len(bytes).map_err(rd)?;
    if len != 4 {
        return Err(dec("frame must have 4 elements".into()));
    }
    let id = mp::read_int(bytes).map_err(rd)?;
    let id = i32::try_from(id).map_err(|_| dec("topic id out of range".into()))?;
    let timestamp_us = mp::read_int(bytes).map_err(rd)?;
    let type_id = mp::read_int(bytes).map_err(rd)?;
    let type_id = u8::try_from(type_id).map_err(|_| dec("type id out of range".into()))?;
    if bytes.is_empty() {
        return Err(dec("frame ends before its value".into()));
    }
    // A value of another kind than its type id says is the sender's mistake, whatever it is.
    let wrong = |_| mismatch(type_id);
    let value = match type_id {
        value::BOOLEAN => Value::Boolean(mp::read_bool(bytes).map_err(wrong)?),
        value::DOUBLE => Value::Double(mp::read_float(bytes).map_err(wrong)?),
        value::INT => Value::Int(mp::read_int(bytes).map_err(|e| {
            if e == "integer out of i64 range" {
                dec("int value out of i64 range".into())
            } else {
                mismatch(type_id)
            }
        })?),
        value::FLOAT => Value::Float(mp::read_f32(bytes).map_err(wrong)?),
        value::STRING => Value::String(mp::read_str(bytes).map_err(rd)?.to_owned()),
        value::RAW => Value::Raw(mp::read_bin(bytes).map_err(wrong)?.to_vec()),
        value::BOOLEAN_ARRAY => Value::BooleanArray(items(bytes, mp::read_bool)?),
        value::DOUBLE_ARRAY => Value::DoubleArray(items(bytes, mp::read_float)?),
        value::INT_ARRAY => Value::IntArray(items(bytes, mp::read_int)?),
        value::FLOAT_ARRAY => Value::FloatArray(items(bytes, mp::read_f32)?),
        value::STRING_ARRAY => Value::StringArray(items(bytes, |bytes| {
            mp::read_str(bytes).map(str::to_owned)
        })?),
        other => return Err(mismatch(other)),
    };
    Ok(Frame {
        id,
        timestamp_us,
        value,
    })
}

/// Decodes a buffer that must hold exactly one message.
pub fn decode_frame(mut bytes: &[u8]) -> Result<Frame> {
    let frame = decode_one(&mut bytes)?;
    if bytes.is_empty() {
        Ok(frame)
    } else {
        Err(dec("expected exactly one frame".into()))
    }
}

fn dec(msg: String) -> Error {
    Error::Decode(msg)
}

fn len_u32(len: usize) -> Result<u32> {
    u32::try_from(len).map_err(|_| Error::Encode("array longer than u32::MAX".into()))
}

fn too_long() -> Error {
    Error::Encode("value longer than u32::MAX".into())
}

fn write_value(buf: &mut Vec<u8>, value: &Value) -> Result<()> {
    match value {
        Value::Boolean(v) => mp::bool(buf, *v),
        Value::Double(v) => mp::f64(buf, *v),
        Value::Int(v) => mp::int(buf, *v),
        Value::Float(v) => mp::f32(buf, *v),
        Value::String(v) => mp::str(buf, v).ok_or_else(too_long)?,
        Value::Raw(v) => mp::bin(buf, v).ok_or_else(too_long)?,
        Value::BooleanArray(items) => {
            mp::array_len(buf, len_u32(items.len())?);
            items.iter().for_each(|v| mp::bool(buf, *v));
        }
        Value::DoubleArray(items) => {
            mp::array_len(buf, len_u32(items.len())?);
            items.iter().for_each(|v| mp::f64(buf, *v));
        }
        Value::IntArray(items) => {
            mp::array_len(buf, len_u32(items.len())?);
            items.iter().for_each(|v| mp::int(buf, *v));
        }
        Value::FloatArray(items) => {
            mp::array_len(buf, len_u32(items.len())?);
            items.iter().for_each(|v| mp::f32(buf, *v));
        }
        Value::StringArray(items) => {
            mp::array_len(buf, len_u32(items.len())?);
            for v in items {
                mp::str(buf, v).ok_or_else(too_long)?;
            }
        }
    }
    Ok(())
}

fn mismatch(type_id: u8) -> Error {
    dec(format!("value does not match binary type id {type_id}"))
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
    fn batched_frame_decodes_every_message() {
        let values = [
            Value::Double(1.5),
            Value::String("x".into()),
            Value::DoubleArray(vec![2.0]),
        ];
        let batch = encode_frames(
            values
                .iter()
                .enumerate()
                .map(|(i, v)| (i as i32, 10 + i as i64, v)),
        )
        .unwrap();
        let (frames, err) = decode_frames(&batch);
        assert!(err.is_none());
        assert_eq!(frames.len(), 3);
        for (i, (frame, value)) in frames.iter().zip(&values).enumerate() {
            assert_eq!(frame.id, i as i32);
            assert_eq!(frame.timestamp_us, 10 + i as i64);
            assert_eq!(&frame.value, value);
        }
    }

    #[test]
    fn batch_mixing_an_rtt_reply_and_a_value_decodes_both() {
        // What ntcore sends after a ping: the RTT reply (id -1) and a value in one frame.
        let mut batch = encode_frame(-1, 5_000, &Value::Int(4_000)).unwrap();
        batch.extend(encode_frame(3, 5_001, &Value::Boolean(true)).unwrap());
        let (frames, err) = decode_frames(&batch);
        assert!(err.is_none());
        assert_eq!(frames[0].id, -1);
        assert_eq!(frames[0].value, Value::Int(4_000));
        assert_eq!(
            frames[1],
            Frame {
                id: 3,
                timestamp_us: 5_001,
                value: Value::Boolean(true)
            }
        );
    }

    #[test]
    fn captured_style_batch_with_ntcore_widths() {
        // Hand-built to the layout ntcore uses: positive fixint ids, uint32 timestamps and
        // float64 doubles, then a string and a double array in the same message.
        // [0, 0xce 1000000, 1, 0xcb 2.5]
        let mut bytes = vec![0x94, 0x00, 0xce, 0x00, 0x0f, 0x42, 0x40, 0x01, 0xcb];
        bytes.extend_from_slice(&2.5f64.to_be_bytes());
        // [1, 1000001, 4, "hi"]
        bytes.extend_from_slice(&[
            0x94, 0x01, 0xce, 0x00, 0x0f, 0x42, 0x41, 0x04, 0xa2, b'h', b'i',
        ]);
        // [2, 1000002, 17, [1.0, 2.0]]
        bytes.extend_from_slice(&[0x94, 0x02, 0xce, 0x00, 0x0f, 0x42, 0x42, 0x11, 0x92, 0xcb]);
        bytes.extend_from_slice(&1.0f64.to_be_bytes());
        bytes.push(0xcb);
        bytes.extend_from_slice(&2.0f64.to_be_bytes());
        let (frames, err) = decode_frames(&bytes);
        assert!(err.is_none(), "{err:?}");
        assert_eq!(frames.len(), 3);
        assert_eq!(frames[0].value, Value::Double(2.5));
        assert_eq!(frames[1].value, Value::String("hi".into()));
        assert_eq!(frames[2].value, Value::DoubleArray(vec![1.0, 2.0]));
    }

    #[test]
    fn malformed_message_keeps_earlier_frames_and_drops_the_rest() {
        let mut batch = encode_frame(1, 1, &Value::Int(7)).unwrap();
        // A message whose type id (99) is unknown, then a good message that must be dropped.
        batch.extend_from_slice(&[0x94, 0x01, 0x01, 0x63, 0x01]);
        batch.extend(encode_frame(2, 2, &Value::Int(8)).unwrap());
        let (frames, err) = decode_frames(&batch);
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].value, Value::Int(7));
        assert!(err.is_some());
    }

    #[test]
    fn truncated_message_is_an_error_not_a_panic() {
        let bytes = encode_frame(1, 1, &Value::String("abcdef".into())).unwrap();
        let (frames, err) = decode_frames(&bytes[..bytes.len() - 2]);
        assert!(frames.is_empty());
        assert!(err.is_some());
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
