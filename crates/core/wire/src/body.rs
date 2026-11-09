//! Message bodies and the rules that keep them compatible.
//!
//! A body is postcard, with a fixed field order. [`body!`](crate::body!) declares one:
//!
//! - Fields before the `;` were in the body from the start and are required.
//! - Fields after the `;` were appended later. A shorter body from an older peer leaves them at
//!   their defaults.
//! - A longer body from a newer peer is accepted, and what isn't understood is ignored.
//!
//! Postcard doesn't do the last two by itself, so a body is decoded one field at a time.
//! A list element carries its own length ([`List`]), so elements can grow the same way.

use core::fmt;
use core::marker::PhantomData;

use orion_types::Error;
use postcard::ser_flavors::Size;
use serde::de::Deserialize;
use serde::ser::{Serialize, SerializeTuple, Serializer};

use crate::frame::{self, FrameHeader, FrameView};

/// Something that decodes from the bytes of a body, borrowing from them.
pub trait Decode<'de>: Sized {
    /// Decodes `bytes`.
    ///
    /// # Errors
    ///
    /// [`Error::Malformed`].
    fn decode(bytes: &'de [u8]) -> Result<Self, Error>;
}

/// A message body: something with a kind number.
pub trait Body: Serialize {
    /// Its number in [`crate::kind`].
    const KIND: u8;
}

/// Encodes `body` as a complete frame into `out`, returning the frame length.
///
/// # Errors
///
/// [`Error::BufferTooSmall`].
pub fn encode_body<B: Body>(body: &B, seq: u16, out: &mut [u8]) -> Result<usize, Error> {
    let used = encode_fields(body, frame::payload_area(out))?;
    frame::encode_in_place(FrameHeader::new(B::KIND, seq), used, out)
        .map_err(|_| Error::BufferTooSmall)
}

/// Encodes just the fields of `value` into `out`, returning the length.
///
/// # Errors
///
/// [`Error::BufferTooSmall`].
pub fn encode_fields<T: Serialize>(value: &T, out: &mut [u8]) -> Result<usize, Error> {
    postcard::to_slice(value, out)
        .map(|used| used.len())
        .map_err(|_| Error::BufferTooSmall)
}

/// Decodes the body of `frame` as a `B`.
///
/// # Errors
///
/// [`Error::Malformed`], including when the frame is another kind.
pub fn decode_body<'a, B: Body + Decode<'a>>(frame: &FrameView<'a>) -> Result<B, Error> {
    if frame.kind() != B::KIND {
        return Err(Error::Malformed);
    }
    B::decode(frame.payload())
}

#[doc(hidden)]
pub fn take<'de, T: Deserialize<'de>>(bytes: &'de [u8]) -> Result<(T, &'de [u8]), Error> {
    postcard::take_from_bytes(bytes).map_err(|_| Error::Malformed)
}

/// Declares a body, or a list element if it has no `= KIND`. See [the module docs](self).
///
/// ```ignore
/// body! {
///     /// A sample.
///     pub struct Hello<'a> = kind::HELLO {
///         pub name: &'a str,
///         ;
///         pub added_later: u16,
///     }
/// }
/// ```
#[macro_export]
macro_rules! body {
    (
        $(#[$meta:meta])*
        pub struct $name:ident $(<$lt:lifetime>)? $(= $kind:path)? {
            $($(#[$fmeta:meta])* pub $field:ident : $ty:ty,)*
            $(; $($(#[$ameta:meta])* pub $afield:ident : $aty:ty,)*)?
        }
    ) => {
        $(#[$meta])*
        #[derive(Clone, Debug, PartialEq, serde::Serialize)]
        pub struct $name $(<$lt>)? {
            $($(#[$fmeta])* pub $field: $ty,)*
            $($($(#[$ameta])* pub $afield: $aty,)*)?
        }

        $crate::__body_kind!([$($lt)?] $name [$($kind)?]);
        $crate::__body_decode!(
            [$($lt)?] $name
            [$($field : $ty,)*]
            [$($($afield : $aty,)*)?]
        );
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __body_kind {
    ([] $name:ident [$kind:path]) => {
        impl $crate::Body for $name {
            const KIND: u8 = $kind;
        }
    };
    ([$lt:lifetime] $name:ident [$kind:path]) => {
        impl<$lt> $crate::Body for $name<$lt> {
            const KIND: u8 = $kind;
        }
    };
    ([$($lt:lifetime)?] $name:ident []) => {};
}

#[doc(hidden)]
#[macro_export]
macro_rules! __body_decode {
    ([] $name:ident [$($field:ident : $ty:ty,)*] [$($afield:ident : $aty:ty,)*]) => {
        impl<'de> $crate::Decode<'de> for $name {
            $crate::__body_decode_fn!('de [$($field : $ty,)*] [$($afield : $aty,)*]);
        }
    };
    ([$lt:lifetime] $name:ident [$($field:ident : $ty:ty,)*] [$($afield:ident : $aty:ty,)*]) => {
        impl<$lt> $crate::Decode<$lt> for $name<$lt> {
            $crate::__body_decode_fn!($lt [$($field : $ty,)*] [$($afield : $aty,)*]);
        }
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __body_decode_fn {
    ($lt:lifetime [$($field:ident : $ty:ty,)*] [$($afield:ident : $aty:ty,)*]) => {
        #[allow(unused_mut, unused_variables, unused_assignments)]
        fn decode(bytes: &$lt [u8]) -> Result<Self, $crate::types::Error> {
            let mut rest = bytes;
            $(
                let ($field, tail) = $crate::body::take::<$ty>(rest)?;
                rest = tail;
            )*
            $(
                let $afield = if rest.is_empty() {
                    <$aty as Default>::default()
                } else {
                    let (value, tail) = $crate::body::take::<$aty>(rest)?;
                    rest = tail;
                    value
                };
            )*
            Ok(Self { $($field,)* $($afield,)* })
        }
    };
}

/// Declares an enum that stays readable when a newer peer sends a value this build doesn't have:
/// it arrives as `Unknown(n)` instead of failing the whole body.
#[macro_export]
macro_rules! open_enum {
    (
        $(#[$meta:meta])*
        pub enum $name:ident : $repr:ty {
            $($(#[$vmeta:meta])* $variant:ident = $value:literal,)*
        }
    ) => {
        $(#[$meta])*
        #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
        pub enum $name {
            $($(#[$vmeta])* $variant,)*
            /// A value this build doesn't know.
            Unknown($repr),
        }

        impl $name {
            /// The number on the wire.
            #[must_use]
            pub const fn code(self) -> $repr {
                match self {
                    $(Self::$variant => $value,)*
                    Self::Unknown(code) => code,
                }
            }

            /// The value for a number on the wire.
            #[must_use]
            pub const fn from_code(code: $repr) -> Self {
                match code {
                    $($value => Self::$variant,)*
                    other => Self::Unknown(other),
                }
            }
        }

        impl $crate::__serde::Serialize for $name {
            fn serialize<S: $crate::__serde::Serializer>(
                &self,
                serializer: S,
            ) -> Result<S::Ok, S::Error> {
                $crate::__serde::Serialize::serialize(&self.code(), serializer)
            }
        }

        impl<'de> $crate::__serde::Deserialize<'de> for $name {
            fn deserialize<D: $crate::__serde::Deserializer<'de>>(
                deserializer: D,
            ) -> Result<Self, D::Error> {
                <$repr as $crate::__serde::Deserialize<'de>>::deserialize(deserializer)
                    .map(Self::from_code)
            }
        }
    };
}

/// Bytes the wire doesn't look inside, borrowed from the receive buffer.
#[derive(Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct Bytes<'a>(pub &'a [u8]);

impl fmt::Debug for Bytes<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Bytes({})", self.0.len())
    }
}

impl core::ops::Deref for Bytes<'_> {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        self.0
    }
}

impl Serialize for Bytes<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_bytes(self.0)
    }
}

impl<'de> Deserialize<'de> for Bytes<'de> {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        <&'de [u8]>::deserialize(deserializer).map(Self)
    }
}

/// How many bytes postcard's varint for `value` takes.
const fn varint_len(mut value: usize) -> usize {
    let mut bytes = 1;
    while value >= 0x80 {
        value >>= 7;
        bytes += 1;
    }
    bytes
}

/// Reads a varint from the front of `bytes`.
fn read_varint(bytes: &[u8]) -> Option<(usize, &[u8])> {
    let mut value = 0_usize;
    for (index, &byte) in bytes.iter().take(5).enumerate() {
        value |= usize::from(byte & 0x7F).checked_shl(7 * index as u32)?;
        if byte & 0x80 == 0 {
            return Some((value, bytes.get(index + 1..)?));
        }
    }
    None
}

#[derive(Clone, Copy, Debug)]
enum Repr<'a, T> {
    Encoded { bytes: &'a [u8], count: u32 },
    Items(&'a [T]),
}

/// A list. Sends from a slice and arrives as a view over the receive buffer, decoding each element
/// when it's read, so it needs no allocator on either side.
///
/// On the wire: a total length, then each element with its own length in front. The lengths are
/// what let an element gain fields later, and let a reader skip what it doesn't understand.
#[derive(Clone, Copy, Debug)]
pub struct List<'a, T>(Repr<'a, T>);

impl<'a, T> List<'a, T> {
    /// A list of `items`, for sending.
    #[must_use]
    pub const fn new(items: &'a [T]) -> Self {
        Self(Repr::Items(items))
    }

    /// How many elements it has.
    #[must_use]
    pub const fn len(&self) -> usize {
        match self.0 {
            Repr::Encoded { count, .. } => count as usize,
            Repr::Items(items) => items.len(),
        }
    }

    /// Whether it has none.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl<T> Default for List<'_, T> {
    fn default() -> Self {
        Self::new(&[])
    }
}

impl<'a, T: Decode<'a> + Clone> List<'a, T> {
    /// The elements. An element that doesn't decode yields an error and the rest still follow.
    #[must_use]
    pub fn iter(&self) -> Iter<'a, T> {
        Iter(match self.0 {
            Repr::Encoded { bytes, .. } => IterRepr::Encoded(bytes, PhantomData),
            Repr::Items(items) => IterRepr::Items(items.iter()),
        })
    }
}

impl<'a, T: Decode<'a> + Clone + PartialEq> PartialEq for List<'a, T> {
    fn eq(&self, other: &Self) -> bool {
        self.iter().eq(other.iter())
    }
}

enum IterRepr<'a, T> {
    Encoded(&'a [u8], PhantomData<T>),
    Items(core::slice::Iter<'a, T>),
}

/// See [`List::iter`].
pub struct Iter<'a, T>(IterRepr<'a, T>);

impl<'a, T: Decode<'a> + Clone> Iterator for Iter<'a, T> {
    type Item = Result<T, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.0 {
            IterRepr::Items(items) => items.next().cloned().map(Ok),
            IterRepr::Encoded(bytes, _) => {
                let (len, rest) = read_varint(bytes)?;
                let element = rest.get(..len)?;
                *bytes = rest.get(len..)?;
                Some(T::decode(element))
            }
        }
    }
}

impl<T: Serialize> Serialize for List<'_, T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let items = match self.0 {
            Repr::Encoded { bytes, .. } => return serializer.serialize_bytes(bytes),
            Repr::Items(items) => items,
        };
        let size = |item: &T| {
            postcard::serialize_with_flavor::<T, Size, usize>(item, Size::default())
                .map_err(serde::ser::Error::custom)
        };
        let mut total = 0_usize;
        for item in items {
            let len = size(item)?;
            total += varint_len(len) + len;
        }
        let mut tuple = serializer.serialize_tuple(1 + 2 * items.len())?;
        tuple.serialize_element(&total)?;
        for item in items {
            tuple.serialize_element(&size(item)?)?;
            tuple.serialize_element(item)?;
        }
        tuple.end()
    }
}

impl<'de, T> Deserialize<'de> for List<'de, T> {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let bytes = <&'de [u8]>::deserialize(deserializer)?;
        let bad = || serde::de::Error::custom("malformed list");
        let mut rest = bytes;
        let mut count = 0_u32;
        while !rest.is_empty() {
            let (len, tail) = read_varint(rest).ok_or_else(bad)?;
            rest = tail.get(len..).ok_or_else(bad)?;
            count += 1;
        }
        Ok(Self(Repr::Encoded { bytes, count }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kind;

    open_enum! {
        /// A sample.
        pub enum Color: u8 {
            Red = 0,
            Blue = 1,
        }
    }

    body! {
        /// Version one.
        pub struct Old<'a> = kind::HELLO {
            pub name: &'a str,
            pub color: Color,
        }
    }

    body! {
        /// Version two: one field appended.
        pub struct New<'a> = kind::HELLO {
            pub name: &'a str,
            pub color: Color,
            ;
            pub extra: u32,
            pub note: Option<&'a str>,
        }
    }

    body! {
        /// An element.
        pub struct Item {
            pub a: u16,
            ;
            pub b: u16,
        }
    }

    body! {
        /// A body holding a list.
        pub struct Holder<'a> = kind::PUT {
            #[serde(borrow)]
            pub items: List<'a, Item>,
            pub after: u8,
        }
    }

    fn encode<T: Serialize>(value: &T, buf: &mut [u8; 128]) -> usize {
        encode_fields(value, buf).unwrap()
    }

    #[test]
    fn a_shorter_body_takes_defaults() {
        let mut buf = [0; 128];
        let len = encode(
            &Old {
                name: "n",
                color: Color::Blue,
            },
            &mut buf,
        );
        let new = New::decode(&buf[..len]).unwrap();
        assert_eq!(
            (new.name, new.color, new.extra, new.note),
            ("n", Color::Blue, 0, None)
        );
    }

    #[test]
    fn a_longer_body_is_accepted() {
        let mut buf = [0; 128];
        let value = New {
            name: "n",
            color: Color::Red,
            extra: 7,
            note: Some("x"),
        };
        let len = encode(&value, &mut buf);
        let old = Old::decode(&buf[..len]).unwrap();
        assert_eq!((old.name, old.color), ("n", Color::Red));
        assert_eq!(New::decode(&buf[..len]).unwrap(), value);
    }

    #[test]
    fn a_body_missing_a_required_field_is_refused() {
        let mut buf = [0; 128];
        let len = encode(
            &Old {
                name: "n",
                color: Color::Red,
            },
            &mut buf,
        );
        assert_eq!(Old::decode(&buf[..len - 1]).unwrap_err(), Error::Malformed);
    }

    #[test]
    fn an_unknown_enum_value_survives() {
        let mut buf = [0; 128];
        let len = encode(
            &Old {
                name: "n",
                color: Color::Unknown(9),
            },
            &mut buf,
        );
        assert_eq!(Old::decode(&buf[..len]).unwrap().color, Color::Unknown(9));
    }

    #[test]
    fn lists_round_trip_and_keep_what_follows_in_step() {
        let items = [
            Item { a: 1, b: 2 },
            Item { a: 300, b: 0 },
            Item { a: 3, b: 70_00 },
        ];
        let mut buf = [0; 128];
        let value = Holder {
            items: List::new(&items),
            after: 9,
        };
        let len = encode(&value, &mut buf);
        let decoded = Holder::decode(&buf[..len]).unwrap();
        assert_eq!(decoded.after, 9);
        assert_eq!(decoded.items.len(), 3);
        assert_eq!(decoded, value);
        let mut again = [0; 128];
        let again_len = encode(&decoded, &mut again);
        assert_eq!(&buf[..len], &again[..again_len]);
    }

    #[test]
    fn a_list_element_can_be_shorter_or_longer() {
        // Total length 2: one element of one byte, holding only `a`. Then `after`.
        let short = [2, 1, 5, 1];
        let holder = Holder::decode(&short);
        assert!(holder.is_ok());
        let holder = holder.unwrap();
        let item = holder.items.iter().next().unwrap().unwrap();
        assert_eq!((item.a, item.b), (5, 0));
        // An element with an extra trailing byte.
        let long = [4, 3, 5, 6, 7, 1];
        let holder = Holder::decode(&long).unwrap();
        let item = holder.items.iter().next().unwrap().unwrap();
        assert_eq!((item.a, item.b), (5, 6));
    }

    #[test]
    fn a_list_claiming_more_than_it_holds_is_refused() {
        assert_eq!(Holder::decode(&[2, 9, 1, 0]).unwrap_err(), Error::Malformed);
        assert_eq!(Holder::decode(&[9, 1]).unwrap_err(), Error::Malformed);
    }

    #[test]
    fn varint_lengths_agree_with_postcard() {
        for value in [0, 1, 127, 128, 16_383, 16_384, 2_097_151, 2_097_152] {
            let mut buf = [0; 8];
            let used = postcard::to_slice(&value, &mut buf).unwrap().len();
            assert_eq!(varint_len(value), used, "{value}");
            assert_eq!(read_varint(&buf[..used]).unwrap().0, value);
        }
    }

    #[test]
    fn frames_carry_a_body_and_refuse_the_wrong_kind() {
        let mut buf = [0; 128];
        let value = Old {
            name: "n",
            color: Color::Blue,
        };
        let len = encode_body(&value, 4, &mut buf).unwrap();
        let frame = frame::decode(&buf[..len]).unwrap();
        assert_eq!((frame.kind(), frame.seq()), (kind::HELLO, 4));
        assert_eq!(decode_body::<Old>(&frame).unwrap(), value);
        assert_eq!(decode_body::<Holder>(&frame).unwrap_err(), Error::Malformed);
        assert_eq!(
            encode_body(&value, 4, &mut [0; 8]).unwrap_err(),
            Error::BufferTooSmall
        );
    }
}
