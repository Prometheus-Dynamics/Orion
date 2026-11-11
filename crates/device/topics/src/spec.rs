//! What a topic is, as constants a firmware keeps in flash, and its declaration as the
//! host reads it.

use orion_device_core::codec::Writer;
use orion_msg_topics::FieldType;

/// One field of a topic.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Field {
    /// Its name.
    pub name: &'static str,
    /// Its type: how its payload is read.
    pub ty: FieldType,
    /// Its unit. Empty for none.
    pub unit: &'static str,
    /// The pinned type name of a structured value. Empty otherwise.
    pub type_name: &'static str,
}

impl Field {
    /// A field that is not structured.
    #[must_use]
    pub const fn new(name: &'static str, ty: FieldType, unit: &'static str) -> Self {
        Self {
            name,
            ty,
            unit,
            type_name: "",
        }
    }

    /// A structured field whose type is pinned as `type_name`.
    #[must_use]
    pub const fn structured(name: &'static str, type_name: &'static str) -> Self {
        Self {
            name,
            ty: FieldType::Structured,
            unit: "",
            type_name,
        }
    }

    /// How many bytes it takes inside a declaration, without its length.
    const fn encoded_len(&self) -> usize {
        str_len(self.name) + 1 + str_len(self.unit) + str_len(self.type_name)
    }
}

/// One topic.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TopicSpec {
    /// Its name: letters, digits, `-`, `_` and `.`, up to 63 bytes. The host refuses a
    /// declaration whose name is not one.
    pub name: &'static str,
    /// Its fields, in the order their numbers name.
    pub fields: &'static [Field],
    /// How many samples Orion keeps. A latest-value topic is 2.
    pub history: u32,
    /// How long a value stays valid. Zero for always.
    pub expiry_ms: u32,
    /// The largest sample payload.
    pub max_payload: u32,
}

impl TopicSpec {
    /// A latest-value topic of numbers: two kept, never expiring, payloads up to 8 bytes.
    #[must_use]
    pub const fn latest(name: &'static str, fields: &'static [Field]) -> Self {
        Self {
            name,
            fields,
            history: 2,
            expiry_ms: 0,
            max_payload: 8,
        }
    }

    /// Writes the declaration's payload into `out` and says how long it is, or `None` if
    /// it does not fit.
    pub(crate) fn write_declare(&self, out: &mut [u8]) -> Option<usize> {
        let mut w = Writer::new(out);
        w.bytes(self.name.as_bytes());
        // A list: its whole length, then each element with its own length first.
        let mut total = 0;
        for field in self.fields {
            let len = field.encoded_len();
            total += varint_len(len) + len;
        }
        w.varint(total as u64);
        for field in self.fields {
            w.varint(field.encoded_len() as u64)
                .bytes(field.name.as_bytes())
                .u8(field.ty.code())
                .bytes(field.unit.as_bytes())
                .bytes(field.type_name.as_bytes());
        }
        w.varint(u64::from(self.history))
            .varint(u64::from(self.expiry_ms))
            .varint(u64::from(self.max_payload))
            .finish()
    }
}

/// How many bytes a varint of `value` takes.
const fn varint_len(mut value: usize) -> usize {
    let mut len = 1;
    while value >= 0x80 {
        value >>= 7;
        len += 1;
    }
    len
}

/// How many bytes a string takes: its length, then it.
const fn str_len(text: &str) -> usize {
    varint_len(text.len()) + text.len()
}
