//! The one writer of a ring.

use std::os::fd::OwnedFd;
use std::sync::atomic::Ordering;

use crate::layout::{Geometry, SLOT_HEADER_WORDS, W_HEAD, W_SUBSCRIBERS, busy, stamp};
use crate::{FieldId, Flags, MemMut, PublishError, RingError, SampleTime, WriteMap};

/// The write end of a ring. There must be exactly one per ring: two would corrupt that topic, and
/// only that topic.
#[derive(Debug)]
pub struct Writer<M: MemMut = WriteMap> {
    mem: M,
    geometry: Geometry,
    head: u64,
}

impl Writer<WriteMap> {
    /// Attaches to the ring behind `fd`, the write end handed over by the ring's owner.
    ///
    /// # Errors
    ///
    /// [`RingError`] if `fd` can't be mapped for writing or doesn't hold a ring.
    pub fn attach(fd: OwnedFd) -> Result<Self, RingError> {
        Self::over(WriteMap::new(fd)?)
    }
}

impl<M: MemMut> Writer<M> {
    /// Attaches to the ring in `mem`, carrying on from where the last writer stopped.
    ///
    /// # Errors
    ///
    /// [`RingError::Malformed`] if `mem` doesn't hold a ring.
    pub fn over(mem: M) -> Result<Self, RingError> {
        let geometry = Geometry::read(&mem)?;
        let head = mem.load(W_HEAD, Ordering::Relaxed);
        Ok(Self {
            mem,
            geometry,
            head,
        })
    }

    /// The ring's shape.
    #[must_use]
    pub const fn geometry(&self) -> &Geometry {
        &self.geometry
    }

    /// How many samples were ever published.
    #[must_use]
    pub const fn published(&self) -> u64 {
        self.head
    }

    /// How many subscribers the topic has: one memory read, so a publisher can skip making a
    /// sample nobody wants.
    #[must_use]
    pub fn subscribers(&self) -> u32 {
        self.mem.load(W_SUBSCRIBERS, Ordering::Relaxed) as u32
    }

    /// Publishes one sample and returns its sequence number. Memory writes only: no system call,
    /// no allocation, no waiting on any reader.
    ///
    /// # Errors
    ///
    /// [`PublishError`] if the field isn't declared or the payload is too large. Nothing is
    /// written.
    pub fn publish(
        &mut self,
        field: FieldId,
        time: SampleTime,
        flags: Flags,
        payload: &[u8],
    ) -> Result<u64, PublishError> {
        let geometry = &self.geometry;
        let lane = usize::from(field.0);
        if lane >= geometry.fields as usize {
            return Err(PublishError::NoSuchField);
        }
        if payload.len() > geometry.max_payload as usize {
            return Err(PublishError::TooLarge);
        }
        let seq = self.head;
        let lane_head_at = geometry.lane_heads_at + lane;
        let lane_head = self.mem.load(lane_head_at, Ordering::Relaxed);
        let lane_slot = (lane_head as usize) & (geometry.history as usize - 1);
        let at = geometry.slot_at(lane, lane_slot);

        // Mark the slot busy before touching it. The fence orders this store before the data
        // stores for any reader that sees one of them (docs/ring.md).
        self.mem.store(at, busy(seq), Ordering::Relaxed);
        M::fence(Ordering::Release);
        self.mem.store(at + 1, time.as_nanos(), Ordering::Relaxed);
        let meta = payload.len() as u64 | u64::from(field.0) << 32 | u64::from(flags.0) << 48;
        self.mem.store(at + 2, meta, Ordering::Relaxed);
        self.mem.store(at + 3, lane_head, Ordering::Relaxed);
        let mut word_at = at + SLOT_HEADER_WORDS;
        let (words, rest) = payload.as_chunks::<8>();
        for word in words {
            self.mem
                .store(word_at, u64::from_le_bytes(*word), Ordering::Relaxed);
            word_at += 1;
        }
        if !rest.is_empty() {
            let mut bytes = [0_u8; 8];
            bytes[..rest.len()].copy_from_slice(rest);
            self.mem
                .store(word_at, u64::from_le_bytes(bytes), Ordering::Relaxed);
        }
        self.mem.store(at, stamp(seq), Ordering::Release);

        // Where this sample is, for readers following the order of publication.
        let index_at = geometry.index_at + (seq as usize & (geometry.index_slots - 1));
        self.mem.store(
            index_at,
            u64::from(field.0) | (lane_slot as u64) << 16,
            Ordering::Relaxed,
        );
        self.mem
            .store(lane_head_at, lane_head.wrapping_add(1), Ordering::Release);
        self.head = seq.wrapping_add(1);
        self.mem.store(W_HEAD, self.head, Ordering::Release);
        Ok(seq)
    }
}
