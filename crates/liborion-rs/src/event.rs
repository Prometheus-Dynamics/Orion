//! One event of a link session, with its bytes.

use crate::sys::{self, OrionEvent};

/// What an [`Event`] is (`ORION_EVENT_*`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum EventKind {
    /// A status entry set: [`Event::value`] holds it.
    Status,
    /// A status entry expired or removed.
    Expired,
    /// The end of an answer (a query's, or one change set or batch of a watch).
    Answered,
    /// The node refused the request: [`Event::error_code`], and the reason in [`Event::data`].
    RequestError,
    /// A state of an action: target id, action id, the result as JSON.
    Action,
    /// A record set or replaced: its id, and the record as JSON.
    Record,
    /// A record gone from a watch's set.
    RecordRemoved,
    /// One series of a stream's catalog: its name (key) and unit (data).
    StreamSeries,
    /// Samples of one series: [`Event::samples`].
    StreamSamples,
    /// The subscription's position no longer holds.
    StreamReset,
    /// A kind this wrapper does not know (a newer library).
    Other(u32),
}

impl EventKind {
    fn of(kind: u32) -> Self {
        match kind {
            sys::ORION_EVENT_STATUS => Self::Status,
            sys::ORION_EVENT_EXPIRED => Self::Expired,
            sys::ORION_EVENT_ANSWERED => Self::Answered,
            sys::ORION_EVENT_REQUEST_ERROR => Self::RequestError,
            sys::ORION_EVENT_ACTION => Self::Action,
            sys::ORION_EVENT_RECORD => Self::Record,
            sys::ORION_EVENT_RECORD_REMOVED => Self::RecordRemoved,
            sys::ORION_EVENT_STREAM_SERIES => Self::StreamSeries,
            sys::ORION_EVENT_STREAM_SAMPLES => Self::StreamSamples,
            sys::ORION_EVENT_STREAM_RESET => Self::StreamReset,
            other => Self::Other(other),
        }
    }
}

/// A status value.
#[derive(Debug, Clone, PartialEq)]
pub enum Value<'a> {
    Bool(bool),
    Int(i64),
    UInt(u64),
    F64(f64),
    String(&'a str),
    Bytes(&'a [u8]),
}

/// One event: the C struct (`raw`, docs/capi.md says what each field holds per kind) and its
/// subject, key and data bytes.
#[derive(Debug, Clone, PartialEq)]
pub struct Event {
    /// The event as the library filled it.
    pub raw: OrionEvent,
    bytes: Vec<u8>,
}

impl Event {
    pub(crate) fn new(raw: OrionEvent, bytes: &[u8]) -> Self {
        Self {
            raw,
            bytes: bytes.to_vec(),
        }
    }

    /// What the event is.
    pub fn kind(&self) -> EventKind {
        EventKind::of(self.raw.kind)
    }

    /// The request it answers.
    pub fn req(&self) -> u32 {
        self.raw.req
    }

    /// Part of a bootstrap (the whole current set) or of a stream's backfill.
    pub fn bootstrap(&self) -> bool {
        self.raw.flags & sys::ORION_EVENT_BOOTSTRAP != 0
    }

    /// The subject id (a status entry's subject, an action's target, a record's id).
    pub fn subject(&self) -> &[u8] {
        &self.bytes[..self.raw.subject_len as usize]
    }

    /// The key (a status key, an action id, a series name).
    pub fn key(&self) -> &[u8] {
        let at = self.raw.subject_len as usize;
        &self.bytes[at..at + self.raw.key_len as usize]
    }

    /// The data (a string or bytes value, an error's reason, JSON, a unit, packed samples).
    pub fn data(&self) -> &[u8] {
        &self.bytes[(self.raw.subject_len + self.raw.key_len) as usize..]
    }

    /// [`Self::subject`] as text (lossy).
    pub fn subject_str(&self) -> std::borrow::Cow<'_, str> {
        String::from_utf8_lossy(self.subject())
    }

    /// [`Self::key`] as text (lossy).
    pub fn key_str(&self) -> std::borrow::Cow<'_, str> {
        String::from_utf8_lossy(self.key())
    }

    /// A status entry's value; `None` for other kinds.
    pub fn value(&self) -> Option<Value<'_>> {
        if self.kind() != EventKind::Status {
            return None;
        }
        Some(match self.raw.value_type {
            sys::ORION_VALUE_BOOL => Value::Bool(self.raw.int_value != 0),
            sys::ORION_VALUE_INT => Value::Int(self.raw.int_value),
            sys::ORION_VALUE_UINT => Value::UInt(self.raw.uint_value),
            sys::ORION_VALUE_F64 => Value::F64(self.raw.float_value),
            sys::ORION_VALUE_STRING => Value::String(std::str::from_utf8(self.data()).ok()?),
            _ => Value::Bytes(self.data()),
        })
    }

    /// A refusal's link error code.
    pub fn error_code(&self) -> Option<u16> {
        (self.kind() == EventKind::RequestError).then_some(self.raw.uint_value as u16)
    }

    /// `ORION_ACTION_*` of an action state.
    pub fn action_state(&self) -> Option<u32> {
        (self.kind() == EventKind::Action).then_some(self.raw.uint_value as u32)
    }

    /// An action state that will not change again.
    pub fn action_final(&self) -> bool {
        self.action_state().is_some_and(|state| {
            !matches!(
                state,
                sys::ORION_ACTION_ACCEPTED | sys::ORION_ACTION_RUNNING
            )
        })
    }

    /// The series index of a catalog or samples event.
    pub fn series(&self) -> Option<u16> {
        matches!(
            self.kind(),
            EventKind::StreamSeries | EventKind::StreamSamples
        )
        .then(|| self.raw.int_value as u16)
    }

    /// The `(t_us, value)` samples of a samples event, oldest first; empty for other kinds.
    pub fn samples(&self) -> impl Iterator<Item = (u64, f64)> + '_ {
        let data = if self.kind() == EventKind::StreamSamples {
            self.data()
        } else {
            &[]
        };
        data.as_chunks::<16>().0.iter().map(|pair| {
            let (t, v) = pair.split_at(8);
            (
                u64::from_ne_bytes(t.try_into().unwrap_or_default()),
                f64::from_ne_bytes(v.try_into().unwrap_or_default()),
            )
        })
    }
}
