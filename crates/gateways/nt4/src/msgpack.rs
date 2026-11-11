//! The part of MessagePack that NT4 uses, written out: integers, floats, booleans, strings,
//! bytes and arrays of them. Writing picks the shortest form, as every other writer does.

/// What went wrong reading.
pub(crate) type Bad = &'static str;

const ENDS: Bad = "the frame ends in the middle of a value";

pub(crate) fn array_len(buf: &mut Vec<u8>, len: u32) {
    match len {
        0..=15 => buf.push(0x90 | len as u8),
        16..=0xFFFF => {
            buf.push(0xDC);
            buf.extend_from_slice(&(len as u16).to_be_bytes());
        }
        _ => {
            buf.push(0xDD);
            buf.extend_from_slice(&len.to_be_bytes());
        }
    }
}

pub(crate) fn int(buf: &mut Vec<u8>, value: i64) {
    match value {
        -32..=127 => buf.push(value as u8),
        128..=0xFF => buf.extend_from_slice(&[0xCC, value as u8]),
        0x100..=0xFFFF => {
            buf.push(0xCD);
            buf.extend_from_slice(&(value as u16).to_be_bytes());
        }
        0x1_0000..=0xFFFF_FFFF => {
            buf.push(0xCE);
            buf.extend_from_slice(&(value as u32).to_be_bytes());
        }
        0x1_0000_0000.. => {
            buf.push(0xCF);
            buf.extend_from_slice(&(value as u64).to_be_bytes());
        }
        -128..=-33 => buf.extend_from_slice(&[0xD0, value as u8]),
        -0x8000..=-129 => {
            buf.push(0xD1);
            buf.extend_from_slice(&(value as i16).to_be_bytes());
        }
        -0x8000_0000..=-0x8001 => {
            buf.push(0xD2);
            buf.extend_from_slice(&(value as i32).to_be_bytes());
        }
        _ => {
            buf.push(0xD3);
            buf.extend_from_slice(&value.to_be_bytes());
        }
    }
}

pub(crate) fn bool(buf: &mut Vec<u8>, value: bool) {
    buf.push(if value { 0xC3 } else { 0xC2 });
}

pub(crate) fn f64(buf: &mut Vec<u8>, value: f64) {
    buf.push(0xCB);
    buf.extend_from_slice(&value.to_be_bytes());
}

pub(crate) fn f32(buf: &mut Vec<u8>, value: f32) {
    buf.push(0xCA);
    buf.extend_from_slice(&value.to_be_bytes());
}

/// `None` if it is longer than MessagePack can say.
pub(crate) fn str(buf: &mut Vec<u8>, value: &str) -> Option<()> {
    match value.len() {
        len @ 0..=31 => buf.push(0xA0 | len as u8),
        len @ 32..=0xFF => buf.extend_from_slice(&[0xD9, len as u8]),
        len @ 0x100..=0xFFFF => {
            buf.push(0xDA);
            buf.extend_from_slice(&(len as u16).to_be_bytes());
        }
        len => {
            buf.push(0xDB);
            buf.extend_from_slice(&u32::try_from(len).ok()?.to_be_bytes());
        }
    }
    buf.extend_from_slice(value.as_bytes());
    Some(())
}

/// `None` if it is longer than MessagePack can say.
pub(crate) fn bin(buf: &mut Vec<u8>, value: &[u8]) -> Option<()> {
    match value.len() {
        len @ 0..=0xFF => buf.extend_from_slice(&[0xC4, len as u8]),
        len @ 0x100..=0xFFFF => {
            buf.push(0xC5);
            buf.extend_from_slice(&(len as u16).to_be_bytes());
        }
        len => {
            buf.push(0xC6);
            buf.extend_from_slice(&u32::try_from(len).ok()?.to_be_bytes());
        }
    }
    buf.extend_from_slice(value);
    Some(())
}

fn take<'a>(bytes: &mut &'a [u8], count: usize) -> Result<&'a [u8], Bad> {
    let (front, rest) = bytes.split_at_checked(count).ok_or(ENDS)?;
    *bytes = rest;
    Ok(front)
}

fn array<const N: usize>(bytes: &mut &[u8]) -> Result<[u8; N], Bad> {
    let mut out = [0_u8; N];
    out.copy_from_slice(take(bytes, N)?);
    Ok(out)
}

fn marker(bytes: &mut &[u8]) -> Result<u8, Bad> {
    Ok(take(bytes, 1)?[0])
}

/// Reads a length of `width` bytes, big end first.
fn length(bytes: &mut &[u8], width: usize) -> Result<usize, Bad> {
    Ok(take(bytes, width)?
        .iter()
        .fold(0_usize, |sum, byte| (sum << 8) | usize::from(*byte)))
}

pub(crate) fn read_array_len(bytes: &mut &[u8]) -> Result<usize, Bad> {
    match marker(bytes)? {
        short @ 0x90..=0x9F => Ok(usize::from(short & 0x0F)),
        0xDC => length(bytes, 2),
        0xDD => length(bytes, 4),
        _ => Err("expected an array"),
    }
}

/// Any integer form, if what it holds fits an `i64`.
pub(crate) fn read_int(bytes: &mut &[u8]) -> Result<i64, Bad> {
    Ok(match marker(bytes)? {
        positive @ 0x00..=0x7F => i64::from(positive),
        negative @ 0xE0..=0xFF => i64::from(negative as i8),
        0xCC => i64::from(marker(bytes)?),
        0xCD => i64::from(u16::from_be_bytes(array(bytes)?)),
        0xCE => i64::from(u32::from_be_bytes(array(bytes)?)),
        0xCF => i64::try_from(u64::from_be_bytes(array(bytes)?))
            .map_err(|_| "integer out of i64 range")?,
        0xD0 => i64::from(marker(bytes)? as i8),
        0xD1 => i64::from(i16::from_be_bytes(array(bytes)?)),
        0xD2 => i64::from(i32::from_be_bytes(array(bytes)?)),
        0xD3 => i64::from_be_bytes(array(bytes)?),
        _ => return Err("expected an integer"),
    })
}

pub(crate) fn read_bool(bytes: &mut &[u8]) -> Result<bool, Bad> {
    match marker(bytes)? {
        0xC2 => Ok(false),
        0xC3 => Ok(true),
        _ => Err("expected a boolean"),
    }
}

/// A float of either width, as the wider.
pub(crate) fn read_float(bytes: &mut &[u8]) -> Result<f64, Bad> {
    match marker(bytes)? {
        0xCA => Ok(f64::from(f32::from_be_bytes(array(bytes)?))),
        0xCB => Ok(f64::from_be_bytes(array(bytes)?)),
        _ => Err("expected a float"),
    }
}

/// A float of either width, as the narrower, bit for bit if that is how it came.
pub(crate) fn read_f32(bytes: &mut &[u8]) -> Result<f32, Bad> {
    match marker(bytes)? {
        0xCA => Ok(f32::from_be_bytes(array(bytes)?)),
        0xCB => Ok(f64::from_be_bytes(array(bytes)?) as f32),
        _ => Err("expected a float"),
    }
}

pub(crate) fn read_str<'a>(bytes: &mut &'a [u8]) -> Result<&'a str, Bad> {
    let len = match marker(bytes)? {
        short @ 0xA0..=0xBF => usize::from(short & 0x1F),
        0xD9 => length(bytes, 1)?,
        0xDA => length(bytes, 2)?,
        0xDB => length(bytes, 4)?,
        _ => return Err("expected a string"),
    };
    std::str::from_utf8(take(bytes, len)?).map_err(|_| "string is not UTF-8")
}

pub(crate) fn read_bin<'a>(bytes: &mut &'a [u8]) -> Result<&'a [u8], Bad> {
    let len = match marker(bytes)? {
        0xC4 => length(bytes, 1)?,
        0xC5 => length(bytes, 2)?,
        0xC6 => length(bytes, 4)?,
        _ => return Err("expected bytes"),
    };
    take(bytes, len)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn integers_take_their_shortest_form_and_read_back() {
        let cases: [(i64, &[u8]); 14] = [
            (0, &[0x00]),
            (127, &[0x7F]),
            (128, &[0xCC, 0x80]),
            (255, &[0xCC, 0xFF]),
            (256, &[0xCD, 0x01, 0x00]),
            (65_536, &[0xCE, 0x00, 0x01, 0x00, 0x00]),
            (1 << 32, &[0xCF, 0, 0, 0, 1, 0, 0, 0, 0]),
            (-1, &[0xFF]),
            (-32, &[0xE0]),
            (-33, &[0xD0, 0xDF]),
            (-128, &[0xD0, 0x80]),
            (-129, &[0xD1, 0xFF, 0x7F]),
            (-32_769, &[0xD2, 0xFF, 0xFF, 0x7F, 0xFF]),
            (i64::MIN, &[0xD3, 0x80, 0, 0, 0, 0, 0, 0, 0]),
        ];
        for (value, bytes) in cases {
            let mut buf = Vec::new();
            int(&mut buf, value);
            assert_eq!(buf, bytes, "{value}");
            assert_eq!(read_int(&mut buf.as_slice()), Ok(value));
        }
        // More than an `i64` holds is refused, not wrapped.
        assert!(read_int(&mut [0xCF, 0xFF, 0, 0, 0, 0, 0, 0, 0].as_slice()).is_err());
    }

    #[test]
    fn strings_bytes_and_arrays_say_their_length_in_the_shortest_form() {
        for len in [0, 31, 32, 255, 256, 65_535, 65_536] {
            let text = "x".repeat(len);
            let mut buf = Vec::new();
            str(&mut buf, &text).unwrap();
            assert_eq!(read_str(&mut buf.as_slice()), Ok(text.as_str()), "{len}");
            let mut buf = Vec::new();
            bin(&mut buf, text.as_bytes()).unwrap();
            assert_eq!(read_bin(&mut buf.as_slice()), Ok(text.as_bytes()), "{len}");
        }
        for len in [0, 15, 16, 65_535, 65_536] {
            let mut buf = Vec::new();
            array_len(&mut buf, len);
            assert_eq!(read_array_len(&mut buf.as_slice()), Ok(len as usize));
        }
        let mut buf = Vec::new();
        str(&mut buf, "abc").unwrap();
        assert_eq!(buf, [0xA3, b'a', b'b', b'c']);
    }

    #[test]
    fn a_value_cut_short_is_an_error_wherever_it_is_cut() {
        let mut whole = Vec::new();
        str(&mut whole, &"y".repeat(300)).unwrap();
        for cut in 0..whole.len() {
            assert!(read_str(&mut &whole[..cut]).is_err(), "{cut}");
        }
        assert!(read_float(&mut [0xCB, 0, 0].as_slice()).is_err());
        assert!(read_int(&mut [].as_slice()).is_err());
    }
}
