//! Latest values: the newest sample of each field of a topic, with the declaration's expiry.
//!
//! This is how slow facts are read (a temperature, a mode, a version): the reader asks for a
//! field's value now, not for a history. Finding it is one memory read whatever the number of
//! fields, because each field's lane in the ring knows its own newest slot. A field that is
//! published a thousand times a second never pushes out its quiet neighbours.
//!
//! A declaration can give its values an expiry: a publisher that stops refreshing a value is
//! saying nothing about it any more, and readers should not go on believing the last one. A
//! value is expired when the reader's clock is more than the expiry past the time the sample
//! carries. Both clocks must be the same one; Orion's time sync is what makes that true across
//! boards.
//!
//! [`Latest`] also remembers what it last returned for each field, so a reader that looks at
//! many fields over and over can skip the ones that have not changed without copying them.

use orion_ring::{FieldId, Mem, ReadMap, Reader, Sample, SampleTime};

/// What a field holds now.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Value<'a> {
    /// A value within its expiry.
    Fresh(Sample<'a>),
    /// A value past its expiry. It is still handed over: how stale is too stale to show is the
    /// reader's call, but it must not be treated as current.
    Expired(Sample<'a>),
    /// The field has never been published, isn't in the declaration, or its newest sample was
    /// overwritten four times while it was being copied.
    Nothing,
}

impl<'a> Value<'a> {
    /// The sample, if it is fresh.
    #[must_use]
    pub const fn fresh(self) -> Option<Sample<'a>> {
        match self {
            Self::Fresh(sample) => Some(sample),
            _ => None,
        }
    }
}

/// Whether a sample that belongs to `time` is past `expiry_ms` at `now`. An expiry of zero never
/// expires. A sample from the future (a clock that stepped back) is not expired.
#[must_use]
pub const fn is_expired(time: SampleTime, now: SampleTime, expiry_ms: u32) -> bool {
    expiry_ms != 0 && now.saturating_since(time) > expiry_ms as u64 * 1_000_000
}

/// The latest values of one topic.
#[derive(Debug)]
pub struct Latest<M: Mem = ReadMap> {
    reader: Reader<M>,
    /// For each field, how many samples it had when it was last returned by [`Latest::changed`].
    returned: Box<[u64]>,
}

impl<M: Mem> Latest<M> {
    /// Reads latest values through `reader`.
    #[must_use]
    pub fn new(reader: Reader<M>) -> Self {
        let fields = reader.geometry().fields() as usize;
        Self {
            reader,
            returned: vec![0; fields].into_boxed_slice(),
        }
    }

    /// How many fields the topic has.
    #[must_use]
    pub fn fields(&self) -> u32 {
        self.reader.geometry().fields()
    }

    /// The declaration's expiry in milliseconds. Zero for none.
    #[must_use]
    pub fn expiry_ms(&self) -> u32 {
        self.reader.geometry().expiry_ms()
    }

    /// The value of `field` at `now`. Constant time in the number of fields.
    pub fn get(&mut self, field: FieldId, now: SampleTime) -> Value<'_> {
        let expiry_ms = self.expiry_ms();
        match self.reader.latest(field) {
            Some(sample) if is_expired(sample.time, now, expiry_ms) => Value::Expired(sample),
            Some(sample) => Value::Fresh(sample),
            None => Value::Nothing,
        }
    }

    /// The value of `field` if it was published since this was last called for it. Otherwise
    /// `None`, at the cost of one memory read and no copy. The first call returns the current
    /// value, if there is one.
    pub fn changed(&mut self, field: FieldId) -> Option<Sample<'_>> {
        let returned = self.returned.get_mut(usize::from(field.0))?;
        let head = self.reader.lane_head(field);
        if head == *returned {
            return None;
        }
        // Read exactly the sample that head names, so the note of what was returned is right
        // even if the writer has moved on by the time the copy is done.
        let sample = self.reader.at(field, head.wrapping_sub(1))?;
        *returned = head;
        Some(sample)
    }

    /// The reader underneath, for following the topic in order as well.
    pub fn reader(&mut self) -> &mut Reader<M> {
        &mut self.reader
    }
}
