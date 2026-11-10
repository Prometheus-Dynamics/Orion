//! A payload read by its declared type, for a program that learns the types at run time: a
//! gateway, a tool that prints any topic.

use orion_msg_topics::FieldType;
use orion_types::Error;

use crate::fixed::{Array, Fixed, decode, encode};

/// One sample's value. Text, bytes and arrays borrow the payload.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Value<'a> {
    /// `true` or `false`.
    Bool(bool),
    /// A 64-bit signed integer.
    Int(i64),
    /// A 64-bit float.
    Float(f64),
    /// UTF-8 text.
    Text(&'a str),
    /// Bytes.
    Bytes(&'a [u8]),
    /// 64-bit floats.
    FloatArray(Array<'a, f64>),
    /// 64-bit signed integers.
    IntArray(Array<'a, i64>),
    /// A structured value, as its bytes. The field's pinned type name says which
    /// [`Structured`](crate::Structured) reads it.
    Structured(&'a [u8]),
}

impl<'a> Value<'a> {
    /// Reads `payload` as a value of type `ty`.
    ///
    /// # Errors
    ///
    /// [`Error::Malformed`] if the payload doesn't fit the type; [`Error::Unsupported`] for a
    /// type that isn't a value (buffers, or one this build doesn't know).
    pub fn read(ty: FieldType, payload: &'a [u8]) -> Result<Self, Error> {
        let exactly = |len: usize| {
            if payload.len() == len {
                Ok(())
            } else {
                Err(Error::Malformed)
            }
        };
        Ok(match ty {
            FieldType::Bool => {
                exactly(1)?;
                match payload[0] {
                    0 => Self::Bool(false),
                    1 => Self::Bool(true),
                    _ => return Err(Error::Malformed),
                }
            }
            FieldType::Int => {
                exactly(8)?;
                Self::Int(decode(payload)?)
            }
            FieldType::Float => {
                exactly(8)?;
                Self::Float(decode(payload)?)
            }
            FieldType::Text => {
                Self::Text(core::str::from_utf8(payload).map_err(|_| Error::Malformed)?)
            }
            FieldType::Bytes => Self::Bytes(payload),
            FieldType::FloatArray => Self::FloatArray(Array::new(payload)?),
            FieldType::IntArray => Self::IntArray(Array::new(payload)?),
            FieldType::Structured => Self::Structured(payload),
            FieldType::Buffers | FieldType::Unknown(_) => return Err(Error::Unsupported),
        })
    }

    /// The type a field must be declared as to carry this value.
    #[must_use]
    pub const fn ty(&self) -> FieldType {
        match self {
            Self::Bool(_) => FieldType::Bool,
            Self::Int(_) => FieldType::Int,
            Self::Float(_) => FieldType::Float,
            Self::Text(_) => FieldType::Text,
            Self::Bytes(_) => FieldType::Bytes,
            Self::FloatArray(_) => FieldType::FloatArray,
            Self::IntArray(_) => FieldType::IntArray,
            Self::Structured(_) => FieldType::Structured,
        }
    }

    /// Writes the value as a payload at the start of `out` and returns its length.
    ///
    /// # Errors
    ///
    /// [`Error::BufferTooSmall`] if `out` is too short.
    pub fn write(&self, out: &mut [u8]) -> Result<usize, Error> {
        let copy = |bytes: &[u8], out: &mut [u8]| {
            out.get_mut(..bytes.len())
                .ok_or(Error::BufferTooSmall)?
                .copy_from_slice(bytes);
            Ok(bytes.len())
        };
        match self {
            Self::Bool(value) => encode(value, out),
            Self::Int(value) => encode(value, out),
            Self::Float(value) => encode(value, out),
            Self::Text(text) => copy(text.as_bytes(), out),
            Self::Bytes(bytes) | Self::Structured(bytes) => copy(bytes, out),
            Self::FloatArray(array) => copy(array.as_bytes(), out),
            Self::IntArray(array) => copy(array.as_bytes(), out),
        }
    }

    /// The value as a float, if it is a number: what a graph or a gauge wants.
    #[must_use]
    pub fn as_f64(&self) -> Option<f64> {
        match *self {
            Self::Bool(value) => Some(f64::from(u8::from(value))),
            Self::Int(value) => Some(value as f64),
            Self::Float(value) => Some(value),
            _ => None,
        }
    }

    /// Reads a structured value as `T`, checking nothing about the name: the caller compared the
    /// field's pinned type name with [`Structured::TYPE_NAME`](crate::Structured::TYPE_NAME) when
    /// it subscribed.
    ///
    /// # Errors
    ///
    /// [`Error::Malformed`] if this isn't a structured value or is shorter than `T`.
    pub fn structured<T: Fixed>(&self) -> Result<T, Error> {
        match self {
            Self::Structured(bytes) => decode(bytes),
            _ => Err(Error::Malformed),
        }
    }
}
