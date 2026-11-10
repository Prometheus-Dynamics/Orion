//! Values with a fixed size and layout.

use core::marker::PhantomData;

use orion_types::Error;

/// A value that is always the same number of bytes, laid out little-endian with no padding.
pub trait Fixed: Sized {
    /// How many bytes it is.
    const SIZE: usize;

    /// Writes it to `out`, which is exactly [`Fixed::SIZE`] bytes.
    fn put(&self, out: &mut [u8]);

    /// Reads it from `bytes`, which is exactly [`Fixed::SIZE`] bytes. Every bit pattern reads as
    /// some value: there is nothing to validate.
    fn get(bytes: &[u8]) -> Self;
}

/// A structured value: a [`Fixed`] with a type name that the topic's declaration pins. Made with
/// [`structured!`](crate::structured).
pub trait Structured: Fixed {
    /// The pinned type name.
    const TYPE_NAME: &'static str;
    /// The members in order: name and size in bytes.
    const MEMBERS: &'static [(&'static str, usize)];
}

macro_rules! numbers {
    ($($ty:ty),*) => {$(
        impl Fixed for $ty {
            const SIZE: usize = size_of::<$ty>();

            fn put(&self, out: &mut [u8]) {
                out.copy_from_slice(&self.to_le_bytes());
            }

            fn get(bytes: &[u8]) -> Self {
                let mut le = [0_u8; size_of::<$ty>()];
                le.copy_from_slice(bytes);
                <$ty>::from_le_bytes(le)
            }
        }
    )*};
}

numbers!(u8, i8, u16, i16, u32, i32, u64, i64, f32, f64);

impl Fixed for bool {
    const SIZE: usize = 1;

    fn put(&self, out: &mut [u8]) {
        out[0] = u8::from(*self);
    }

    fn get(bytes: &[u8]) -> Self {
        bytes[0] != 0
    }
}

impl<T: Fixed, const N: usize> Fixed for [T; N] {
    const SIZE: usize = T::SIZE * N;

    fn put(&self, out: &mut [u8]) {
        for (item, out) in self.iter().zip(out.chunks_exact_mut(T::SIZE.max(1))) {
            item.put(&mut out[..T::SIZE]);
        }
    }

    fn get(bytes: &[u8]) -> Self {
        core::array::from_fn(|index| T::get(&bytes[index * T::SIZE..(index + 1) * T::SIZE]))
    }
}

/// Writes `value` at the start of `out` and returns how many bytes it took.
///
/// # Errors
///
/// [`Error::BufferTooSmall`] if `out` is shorter than the value.
pub fn encode<T: Fixed>(value: &T, out: &mut [u8]) -> Result<usize, Error> {
    let out = out.get_mut(..T::SIZE).ok_or(Error::BufferTooSmall)?;
    value.put(out);
    Ok(T::SIZE)
}

/// Reads a `T` from the start of `payload`. Bytes after it are ignored, which is what lets a
/// structured type grow at the end.
///
/// # Errors
///
/// [`Error::Malformed`] if `payload` is shorter than the value.
pub fn decode<T: Fixed>(payload: &[u8]) -> Result<T, Error> {
    payload.get(..T::SIZE).map(T::get).ok_or(Error::Malformed)
}

/// Writes `items` one after another at the start of `out` and returns how many bytes they took.
///
/// # Errors
///
/// [`Error::BufferTooSmall`] if `out` is too short.
pub fn encode_array<T: Fixed>(items: &[T], out: &mut [u8]) -> Result<usize, Error> {
    let len = items
        .len()
        .checked_mul(T::SIZE)
        .ok_or(Error::BufferTooSmall)?;
    let out = out.get_mut(..len).ok_or(Error::BufferTooSmall)?;
    for (index, item) in items.iter().enumerate() {
        item.put(&mut out[index * T::SIZE..(index + 1) * T::SIZE]);
    }
    Ok(len)
}

/// An array payload, read in place. Elements are read from their offsets on demand, so the
/// payload needs no alignment and nothing is copied.
#[derive(Debug, PartialEq, Eq)]
pub struct Array<'a, T> {
    bytes: &'a [u8],
    of: PhantomData<T>,
}

impl<T> Clone for Array<'_, T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T> Copy for Array<'_, T> {}

impl<'a, T: Fixed> Array<'a, T> {
    /// Reads `payload` as an array of `T`.
    ///
    /// # Errors
    ///
    /// [`Error::Malformed`] if the payload isn't a whole number of elements.
    pub fn new(payload: &'a [u8]) -> Result<Self, Error> {
        if T::SIZE == 0 || !payload.len().is_multiple_of(T::SIZE) {
            return Err(Error::Malformed);
        }
        Ok(Self {
            bytes: payload,
            of: PhantomData,
        })
    }

    /// How many elements there are.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.bytes.len() / T::SIZE
    }

    /// Whether there are none.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    /// Element `index`.
    #[must_use]
    pub fn get(&self, index: usize) -> Option<T> {
        let at = index.checked_mul(T::SIZE)?;
        self.bytes.get(at..at.checked_add(T::SIZE)?).map(T::get)
    }

    /// The elements in order.
    pub fn iter(&self) -> impl ExactSizeIterator<Item = T> + use<'a, T> {
        self.bytes.chunks_exact(T::SIZE).map(T::get)
    }

    /// The payload.
    #[must_use]
    pub const fn as_bytes(&self) -> &'a [u8] {
        self.bytes
    }
}

/// Where member `name` starts in a structured value, from [`Structured::MEMBERS`]. A reader that
/// wants one number out of a large value reads it from here.
#[must_use]
pub const fn offset_of(members: &[(&str, usize)], name: &str) -> Option<usize> {
    let (mut index, mut offset) = (0, 0);
    while index < members.len() {
        if same(members[index].0.as_bytes(), name.as_bytes()) {
            return Some(offset);
        }
        offset += members[index].1;
        index += 1;
    }
    None
}

const fn same(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut index = 0;
    while index < a.len() {
        if a[index] != b[index] {
            return false;
        }
        index += 1;
    }
    true
}

/// Declares a structured value: a plain struct whose members are [`Fixed`], with the type name a
/// topic's declaration pins.
///
/// ```
/// orion_values::structured! {
///     /// Where something is.
///     pub struct Pose = "pose3d" {
///         /// Metres.
///         pub position: [f64; 3],
///         /// A unit quaternion.
///         pub rotation: [f64; 4],
///     }
/// }
/// let pose = Pose { position: [1.0, 2.0, 3.0], rotation: [1.0, 0.0, 0.0, 0.0] };
/// let mut payload = [0_u8; 56];
/// assert_eq!(orion_values::encode(&pose, &mut payload), Ok(56));
/// assert_eq!(orion_values::decode::<Pose>(&payload), Ok(pose));
/// ```
///
/// Members are laid out in the order written, with no padding. Only ever add members at the
/// end: a reader built before then still reads the part it knows.
#[macro_export]
macro_rules! structured {
    (
        $(#[$meta:meta])*
        pub struct $name:ident = $type_name:literal {
            $($(#[$member_meta:meta])* pub $member:ident : $ty:ty,)+
        }
    ) => {
        $(#[$meta])*
        #[derive(Clone, Copy, Debug, PartialEq)]
        pub struct $name {
            $($(#[$member_meta])* pub $member: $ty,)+
        }

        impl $crate::Fixed for $name {
            const SIZE: usize = 0 $(+ <$ty as $crate::Fixed>::SIZE)+;

            fn put(&self, out: &mut [u8]) {
                let mut at = 0;
                $(
                    let size = <$ty as $crate::Fixed>::SIZE;
                    $crate::Fixed::put(&self.$member, &mut out[at..at + size]);
                    at += size;
                )+
                let _ = at;
            }

            fn get(bytes: &[u8]) -> Self {
                let mut at = 0;
                $(
                    let size = <$ty as $crate::Fixed>::SIZE;
                    let $member = <$ty as $crate::Fixed>::get(&bytes[at..at + size]);
                    at += size;
                )+
                let _ = at;
                Self { $($member,)+ }
            }
        }

        impl $crate::Structured for $name {
            const TYPE_NAME: &'static str = $type_name;
            const MEMBERS: &'static [(&'static str, usize)] =
                &[$((stringify!($member), <$ty as $crate::Fixed>::SIZE),)+];
        }
    };
}
