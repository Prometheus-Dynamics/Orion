//! A reader of a ring. Everything it reads from the ring is untrusted.

use std::os::fd::OwnedFd;
use std::sync::atomic::Ordering;

use crate::layout::{Geometry, SLOT_HEADER_WORDS, W_HEAD, stamp};
use crate::{FieldId, Flags, Mem, ReadMap, RingError, SampleTime};

/// How many samples one [`Reader::next`] call steps over looking for one that is still there.
const PROBES: u32 = 64;
/// How many times [`Reader::latest`] looks again when the writer overwrites what it was copying.
const LATEST_TRIES: u32 = 4;

/// One sample, copied out of the ring and checked.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Sample<'a> {
    /// Its number in the order of publication, across all fields.
    pub seq: u64,
    /// The field it belongs to.
    pub field: FieldId,
    /// The time it belongs to.
    pub time: SampleTime,
    /// Its flags.
    pub flags: Flags,
    /// The value.
    pub payload: &'a [u8],
}

/// What [`Reader::next`] found.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Next<'a> {
    /// The next sample in the order of publication.
    Sample(Sample<'a>),
    /// Nothing new.
    Empty,
    /// This many samples were overwritten before the reader got to them. Call again for the
    /// samples after them.
    Skipped(u64),
}

/// What a slot held, once the copy is known to be whole.
#[derive(Clone, Copy)]
struct Found {
    seq: u64,
    field: u16,
    time: u64,
    flags: u8,
    len: usize,
}

/// A read end of a ring. It never writes to the ring and holds its own copy of the shape, so
/// nothing the writer does can send it out of bounds or keep it in a loop.
#[derive(Debug)]
pub struct Reader<M: Mem = ReadMap> {
    mem: M,
    geometry: Geometry,
    cursor: u64,
    missed: u64,
    scratch: Box<[u8]>,
}

impl Reader<ReadMap> {
    /// Attaches to the ring behind `fd`, a read end handed over by the ring's owner. The first
    /// [`Reader::next`] returns the first sample published after this call.
    ///
    /// # Errors
    ///
    /// [`RingError`] if `fd` isn't sealed, can't be mapped or doesn't hold a ring.
    pub fn attach(fd: OwnedFd) -> Result<Self, RingError> {
        Self::over(ReadMap::new(fd)?)
    }
}

impl<M: Mem> Reader<M> {
    /// Attaches to the ring in `mem`.
    ///
    /// # Errors
    ///
    /// [`RingError::Malformed`] if `mem` doesn't hold a ring.
    pub fn over(mem: M) -> Result<Self, RingError> {
        let geometry = Geometry::read(&mem)?;
        let cursor = mem.load(W_HEAD, Ordering::Acquire);
        Ok(Self {
            scratch: vec![0; geometry.max_payload as usize].into_boxed_slice(),
            mem,
            geometry,
            cursor,
            missed: 0,
        })
    }

    /// The ring's shape, as it was when this reader attached.
    #[must_use]
    pub const fn geometry(&self) -> &Geometry {
        &self.geometry
    }

    /// How many samples this reader has been told it missed, in total.
    #[must_use]
    pub const fn missed(&self) -> u64 {
        self.missed
    }

    /// How many samples have been published that [`Reader::next`] has not yet returned or
    /// counted as lost. One memory read; nothing is consumed.
    #[must_use]
    pub fn pending(&self) -> u64 {
        self.mem
            .load(W_HEAD, Ordering::Acquire)
            .saturating_sub(self.cursor)
    }

    /// Moves back to the oldest sample whose place in the order is still known.
    pub fn rewind(&mut self) {
        let head = self.mem.load(W_HEAD, Ordering::Acquire);
        self.cursor = head.saturating_sub(self.geometry.window());
    }

    /// The next sample in the order of publication, or how many were lost on the way to it.
    // Not an iterator: a sample borrows the reader's copy of it.
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Next<'_> {
        let head = self.mem.load(W_HEAD, Ordering::Acquire);
        if head < self.cursor {
            // The count went backwards, which no honest writer does. Start again from it.
            self.cursor = head;
        }
        let behind = head - self.cursor;
        let window = self.geometry.window();
        if behind > window {
            let lost = behind - window;
            self.cursor += lost;
            self.missed = self.missed.saturating_add(lost);
            return Next::Skipped(lost);
        }
        let mut lost = 0_u64;
        let mut found = None;
        for _ in 0..PROBES {
            if self.cursor == head {
                break;
            }
            found = self.read_in_order(self.cursor);
            if found.is_some() {
                break;
            }
            self.cursor += 1;
            lost += 1;
        }
        if lost > 0 {
            // Whatever was found is read again by the next call.
            self.missed = self.missed.saturating_add(lost);
            return Next::Skipped(lost);
        }
        match found {
            Some(found) => {
                self.cursor += 1;
                Next::Sample(self.sample(found))
            }
            None => Next::Empty,
        }
    }

    /// The newest sample of `field`, or `None` if it has none (or the field isn't declared).
    ///
    /// Successive calls never go backwards. A writer that overwrites the newest sample faster
    /// than this reader can copy it, four times running, also gives `None`: a field published
    /// that fast with large payloads wants a deeper history.
    pub fn latest(&mut self, field: FieldId) -> Option<Sample<'_>> {
        let mut found = None;
        for _ in 0..LATEST_TRIES {
            let head = self.lane_head(field);
            if head == 0 {
                return None;
            }
            found = self.read_lane(field, head - 1);
            if found.is_some() {
                break;
            }
        }
        found.map(|found| self.sample(found))
    }

    /// How many samples `field` has ever had. Its newest is number `lane_head - 1`, and the
    /// oldest still kept is `history` before that.
    #[must_use]
    pub fn lane_head(&self, field: FieldId) -> u64 {
        if u32::from(field.0) >= self.geometry.fields {
            return 0;
        }
        self.mem.load(
            self.geometry.lane_heads_at + usize::from(field.0),
            Ordering::Acquire,
        )
    }

    /// Sample number `lane_seq` of `field`, counting that field's samples from zero. `None` if
    /// it was overwritten, isn't published yet or is being written right now.
    pub fn at(&mut self, field: FieldId, lane_seq: u64) -> Option<Sample<'_>> {
        let head = self.lane_head(field);
        if lane_seq >= head || head - lane_seq > u64::from(self.geometry.history) {
            return None;
        }
        self.read_lane(field, lane_seq)
            .map(|found| self.sample(found))
    }

    /// The time of sample number `lane_seq` of `field`, without copying its payload: for
    /// searching a field's history by time. `None` as for [`Reader::at`].
    #[must_use]
    pub fn time_at(&self, field: FieldId, lane_seq: u64) -> Option<SampleTime> {
        let head = self.lane_head(field);
        if lane_seq >= head || head - lane_seq > u64::from(self.geometry.history) {
            return None;
        }
        let lane_slot = lane_seq as usize & (self.geometry.history as usize - 1);
        let at = self.geometry.slot_at(usize::from(field.0), lane_slot);
        let before = self.mem.load(at, Ordering::Acquire);
        if before == 0 || before & 1 == 1 {
            return None;
        }
        let time = self.mem.load(at + 1, Ordering::Relaxed);
        let holds = self.mem.load(at + 3, Ordering::Relaxed);
        M::fence(Ordering::Acquire);
        if holds != lane_seq || self.mem.load(at, Ordering::Relaxed) != before {
            return None;
        }
        Some(SampleTime::from_nanos(time))
    }

    fn sample(&self, found: Found) -> Sample<'_> {
        Sample {
            seq: found.seq,
            field: FieldId(found.field),
            time: SampleTime::from_nanos(found.time),
            flags: Flags(found.flags),
            payload: &self.scratch[..found.len],
        }
    }

    /// Reads the sample with sequence `seq` through the index.
    fn read_in_order(&mut self, seq: u64) -> Option<Found> {
        let geometry = &self.geometry;
        let index_at = geometry.index_at + (seq as usize & (geometry.index_slots - 1));
        let entry = self.mem.load(index_at, Ordering::Relaxed);
        let (field, lane_slot) = (entry & 0xffff, entry >> 16);
        if field >= u64::from(geometry.fields) || lane_slot >= u64::from(geometry.history) {
            return None;
        }
        self.read_slot(field as u16, lane_slot as usize, Some(stamp(seq)), None)
    }

    fn read_lane(&mut self, field: FieldId, lane_seq: u64) -> Option<Found> {
        if u32::from(field.0) >= self.geometry.fields {
            return None;
        }
        let lane_slot = lane_seq as usize & (self.geometry.history as usize - 1);
        self.read_slot(field.0, lane_slot, None, Some(lane_seq))
    }

    /// Copies one slot out and checks the copy is whole. `field` and `lane_slot` must be in
    /// range; they come from this reader's shape, never from the ring unchecked.
    ///
    /// A slot is reused, so the caller says which sample it wants: by its stamp when following
    /// the order, by its number within the field when reading a lane. A slot holding any other
    /// sample, even a newer one, is not it.
    fn read_slot(
        &mut self,
        field: u16,
        lane_slot: usize,
        stamp: Option<u64>,
        lane_seq: Option<u64>,
    ) -> Option<Found> {
        let at = self.geometry.slot_at(usize::from(field), lane_slot);
        let before = self.mem.load(at, Ordering::Acquire);
        if before == 0 || before & 1 == 1 || stamp.is_some_and(|stamp| stamp != before) {
            return None;
        }
        let time = self.mem.load(at + 1, Ordering::Relaxed);
        let meta = self.mem.load(at + 2, Ordering::Relaxed);
        let len = (meta & 0xffff_ffff) as usize;
        if len > self.geometry.max_payload as usize {
            return None;
        }
        if lane_seq.is_some_and(|wanted| wanted != self.mem.load(at + 3, Ordering::Relaxed)) {
            return None;
        }
        let mut word_at = at + SLOT_HEADER_WORDS;
        let (words, rest) = self.scratch[..len].as_chunks_mut::<8>();
        for word in words {
            *word = self.mem.load(word_at, Ordering::Relaxed).to_le_bytes();
            word_at += 1;
        }
        if !rest.is_empty() {
            let bytes = self.mem.load(word_at, Ordering::Relaxed).to_le_bytes();
            rest.copy_from_slice(&bytes[..rest.len()]);
        }
        // If any word above came from a newer write, this fence makes the stamp below show it.
        M::fence(Ordering::Acquire);
        if self.mem.load(at, Ordering::Relaxed) != before {
            return None;
        }
        Some(Found {
            seq: (before >> 1).wrapping_sub(1),
            field,
            time,
            flags: (meta >> 48) as u8,
            len,
        })
    }
}
