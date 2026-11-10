//! What a topic's payload holds.
//!
//! A sample's payload is plain bytes; the field's type in the declaration says how to read them.
//! Every layout here is fixed and little-endian, so a number is read straight from its offset
//! with no decoding step and no allocation:
//!
//! | Type | Payload |
//! | --- | --- |
//! | `Bool` | 1 byte, 0 or 1 |
//! | `Int` | 8 bytes, `i64` |
//! | `Float` | 8 bytes, `f64` |
//! | `Text` | UTF-8, the whole payload |
//! | `Bytes` | the whole payload |
//! | `FloatArray`, `IntArray` | 8 bytes per element |
//! | `Structured` | the members one after another, no padding; the declaration pins the type name |
//!
//! [`Fixed`] is a value with a fixed size: the numbers, arrays of them, and structured values
//! made with [`structured!`]. [`Value`] reads any payload by its declared type, for a program
//! that learns the types at run time.
//!
//! A structured type may only ever grow at the end. A reader built with a shorter one reads the
//! part it knows, so the type name can stay the same.

#![no_std]

mod fixed;
mod value;

pub use fixed::{Array, Fixed, Structured, decode, encode, encode_array, offset_of};
pub use orion_msg_topics::FieldType;
pub use orion_types::Error;
pub use value::Value;
