//! How a file's name, which on a board is any bytes, is carried as text.
//!
//! What a name may hold as it is: letters, digits, `. _ - : /`, and any character outside
//! ASCII that is not a control. Everything else is `%XX`, its byte in uppercase hex: an ASCII
//! byte outside that set (a space, a quote), `%` itself, a control, and a byte that is not
//! UTF-8. Nothing else is escaped, so each name has one form: a name escaped where it need not
//! be, or in lowercase hex, is not a [`crate::FileName`].

use core::fmt;

const HEX: &[u8; 16] = b"0123456789ABCDEF";

/// Whether `c` is carried as it is.
const fn kept(c: char) -> bool {
    if c.is_ascii() {
        c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-' | ':' | '/')
    } else {
        // `char::is_control` is not const: the C1 controls are the only ones outside ASCII.
        !matches!(c, '\u{80}'..='\u{9f}')
    }
}

/// Calls `out` with each piece of the escaped form of `raw`, in order.
fn escape(raw: &[u8], mut out: impl FnMut(&[u8]) -> fmt::Result) -> fmt::Result {
    let byte = |b: u8, out: &mut dyn FnMut(&[u8]) -> fmt::Result| {
        out(&[b'%', HEX[usize::from(b >> 4)], HEX[usize::from(b & 15)]])
    };
    for chunk in raw.utf8_chunks() {
        let valid = chunk.valid();
        let mut from = 0;
        for (at, c) in valid.char_indices() {
            if !kept(c) {
                out(&valid.as_bytes()[from..at])?;
                for &b in &valid.as_bytes()[at..at + c.len_utf8()] {
                    byte(b, &mut out)?;
                }
                from = at + c.len_utf8();
            }
        }
        out(&valid.as_bytes()[from..])?;
        for &b in chunk.invalid() {
            byte(b, &mut out)?;
        }
    }
    Ok(())
}

/// A name's bytes as text, escaped: `format!("{}", Escaped(raw))`.
#[derive(Clone, Copy, Debug)]
pub struct Escaped<'a>(pub &'a [u8]);

impl fmt::Display for Escaped<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Each piece is ASCII or whole characters of valid UTF-8.
        escape(self.0, |piece| {
            f.write_str(core::str::from_utf8(piece).map_err(|_| fmt::Error)?)
        })
    }
}

fn hex(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

/// The bytes an escaped name stands for. A `%` without two uppercase hex digits after it
/// stands for itself: a name checked with [`canonical`] has none.
pub fn unescape(escaped: &str) -> impl Iterator<Item = u8> + '_ {
    let bytes = escaped.as_bytes();
    let mut at = 0;
    core::iter::from_fn(move || {
        let b = *bytes.get(at)?;
        if b == b'%'
            && let (Some(high), Some(low)) = (
                bytes.get(at + 1).copied().and_then(hex),
                bytes.get(at + 2).copied().and_then(hex),
            )
        {
            at += 3;
            return Some(high << 4 | low);
        }
        at += 1;
        Some(b)
    })
}

/// Whether `escaped`, of at most `N` bytes, is a name's one escaped form.
pub(crate) fn canonical<const N: usize>(escaped: &str) -> bool {
    let bytes = escaped.as_bytes();
    if bytes.len() > N {
        return false;
    }
    // Every `%` begins an escape.
    let mut at = 0;
    while at < bytes.len() {
        if bytes[at] == b'%' {
            if bytes.get(at + 1).copied().and_then(hex).is_none()
                || bytes.get(at + 2).copied().and_then(hex).is_none()
            {
                return false;
            }
            at += 3;
        } else {
            at += 1;
        }
    }
    // The bytes it stands for, escaped again, give it back.
    let mut raw = [0_u8; N];
    let mut len = 0;
    for b in unescape(escaped) {
        raw[len] = b;
        len += 1;
    }
    let mut rest = bytes;
    escape(&raw[..len], |piece| match rest.strip_prefix(piece) {
        Some(after) => {
            rest = after;
            Ok(())
        }
        None => Err(fmt::Error),
    })
    .is_ok()
        && rest.is_empty()
}

#[cfg(test)]
mod tests {
    extern crate std;

    use std::string::{String, ToString};
    use std::vec::Vec;

    use super::{Escaped, canonical, unescape};
    use crate::FileName;

    fn escaped(raw: &[u8]) -> String {
        Escaped(raw).to_string()
    }

    #[test]
    fn a_name_is_escaped_only_where_it_must_be() {
        assert_eq!(escaped(b"slots/boot.img"), "slots/boot.img");
        assert_eq!(escaped("Отчёт 2.txt".as_bytes()), "Отчёт%202.txt");
        assert_eq!(escaped("写真.jpg".as_bytes()), "写真.jpg");
        assert_eq!(escaped(b"100%"), "100%25");
        assert_eq!(escaped(b"a\tb\x7f"), "a%09b%7F");
        assert_eq!(escaped(b"bad\xff\xc3"), "bad%FF%C3");
        assert_eq!(escaped("\u{85}".as_bytes()), "%C2%85");
        assert_eq!(escaped(b"it's \"x\""), "it%27s%20%22x%22");
    }

    #[test]
    fn only_the_one_escaped_form_is_a_file_name() {
        for name in [
            "roots/a/My Files",
            "a%2e",
            "a%2E",
            "a%c3%a9",
            "a%C3%A9",
            "a%4",
            "a%",
            "a%ZZ",
        ] {
            assert!(FileName::new(name).is_err(), "{name}");
        }
        for name in ["roots/a/My%20Files", "a%25", "é/写真", "bad%FF", "a%C3%FF"] {
            assert_eq!(
                FileName::new(name).map(|n| n.as_str().len()),
                Ok(name.len()),
                "{name}"
            );
        }
        // `%C3%A9` alone is é, which is carried as it is.
        assert!(FileName::new("a%C3%A9").is_err());
        // `..` escaped is still refused: `.` is never escaped.
        assert!(FileName::new("a/%2E%2E/b").is_err());
        assert!(FileName::new("a/../b").is_err());
    }

    #[test]
    fn the_limit_is_on_the_escaped_form_and_is_255() {
        assert_eq!(FileName::MAX, 255);
        let long = "é".repeat(127); // 254 bytes
        assert!(FileName::new(&long).is_ok());
        assert!(FileName::new(&(long.clone() + "a")).is_ok());
        assert!(FileName::new(&(long + "ab")).is_err());
        assert!(FileName::new(&"%20".repeat(85)).is_ok());
        assert!(FileName::new(&"%20".repeat(86)).is_err());
    }

    /// Bytes, for the property: drawn mostly from those that need thought.
    fn draw(seed: &mut u64) -> u8 {
        *seed = seed
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        let pick = (*seed >> 33) as u8;
        const EDGES: &[u8] = b"%/. \x00\x7f\x80\xbf\xc2\xc3\xe2\xe6\xf0\xff\x85\xa9aZ9";
        if pick & 1 == 0 {
            EDGES[usize::from(pick >> 1) % EDGES.len()]
        } else {
            pick
        }
    }

    #[test]
    fn every_name_escaped_comes_back_whole_and_is_its_one_form() {
        let mut seed = 7;
        for case in 0..50_000 {
            let len = usize::from(draw(&mut seed) % 24);
            let raw: Vec<u8> = (0..len).map(|_| draw(&mut seed)).collect();
            let text = escaped(&raw);
            let back: Vec<u8> = unescape(&text).collect();
            assert_eq!(back, raw, "case {case}: {text}");
            assert!(
                canonical::<255>(&text),
                "case {case}: {text} is not its own form"
            );
            // Any other spelling of the same bytes is not.
            if let Some(at) = text.find(|c: char| c.is_ascii_alphanumeric()) {
                let b = text.as_bytes()[at];
                let other = [&text[..at], &std::format!("%{b:02X}"), &text[at + 1..]].concat();
                assert!(!canonical::<255>(&other), "case {case}: {other}");
            }
        }
    }
}
