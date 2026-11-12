//! The publisher of a buffer topic.

use orion_ring::{FieldId, Flags, MemMut, SampleTime, WriteMap, Writer};

use crate::{BufferError, BufferIndex, Dropped, Holders, MAX_PER_FRAME, Pool, entry, page};

/// Publishes frames of a buffer topic and takes their buffers back.
///
/// A buffer is either **free** (the publisher may fill it, or give it to a device to fill) or
/// **published** (a ring entry names it and readers may hold it). [`BufferWriter::publish`]
/// moves buffers to published; [`BufferWriter::reclaim`] and [`BufferWriter::take`] move them
/// back, and only when no reader holds them. The publisher never waits for a buffer: when none
/// can be taken back, the frame is dropped.
#[derive(Debug)]
pub struct BufferWriter<M: MemMut = WriteMap> {
    writer: Writer<M>,
    field: FieldId,
    pool: Pool,
    /// For each buffer, the sequence of the frame it is published in, or `None` if it is free.
    published: Box<[Option<u64>]>,
    /// The newest frame's sequence. Its buffers are not taken back by [`BufferWriter::take`].
    newest: Option<u64>,
    scratch: Box<[u8]>,
}

impl<M: MemMut> BufferWriter<M> {
    /// Publishes `pool`'s buffers on `field` of `writer`'s ring. Every buffer starts free.
    #[must_use]
    pub fn new(writer: Writer<M>, field: FieldId, pool: Pool) -> Self {
        let scratch = vec![0; writer.geometry().max_payload() as usize].into_boxed_slice();
        Self {
            published: vec![None; pool.buffers().len()].into_boxed_slice(),
            writer,
            field,
            pool,
            newest: None,
            scratch,
        }
    }

    /// The pool.
    #[must_use]
    pub const fn pool(&self) -> &Pool {
        &self.pool
    }

    /// How many subscribers the topic has. With none, a publisher can skip making frames.
    #[must_use]
    pub fn subscribers(&self) -> u32 {
        self.writer.subscribers()
    }

    /// Replaces the pool, for a camera that changed mode. The new pool's generation must be
    /// higher. Frames of the old pool that readers still hold stay valid for them: they keep
    /// the old buffers mapped until they let go.
    pub fn replace_pool(&mut self, pool: Pool) -> Pool {
        self.published = vec![None; pool.buffers().len()].into_boxed_slice();
        self.newest = None;
        core::mem::replace(&mut self.pool, pool)
    }

    /// Publishes one frame: the buffers that hold it, its time, and metadata passed on unread.
    /// Returns the frame's sequence number. Memory writes only.
    ///
    /// # Errors
    ///
    /// [`BufferError`] if a buffer isn't free or isn't in the pool, or the entry doesn't fit.
    /// Nothing is published.
    pub fn publish(
        &mut self,
        buffers: &[BufferIndex],
        time: SampleTime,
        meta: &[u8],
    ) -> Result<u64, BufferError> {
        if buffers.is_empty() || buffers.len() > MAX_PER_FRAME {
            return Err(BufferError::Count);
        }
        let mut named = [(BufferIndex(0), 0); MAX_PER_FRAME];
        for (at, buffer) in buffers.iter().enumerate() {
            match self.published.get(buffer.0 as usize) {
                None => return Err(BufferError::NoSuchBuffer(*buffer)),
                Some(Some(_)) => return Err(BufferError::NotFree(*buffer)),
                Some(None) if buffers[..at].contains(buffer) => {
                    return Err(BufferError::NotFree(*buffer));
                }
                Some(None) => named[at] = (*buffer, page::ticket(self.pool.page(), *buffer)),
            }
        }
        let len = entry::write(
            &mut self.scratch,
            self.pool.generation(),
            &named[..buffers.len()],
            meta,
        )
        .ok_or(BufferError::TooLarge)?;
        let seq = self
            .writer
            .publish(self.field, time, Flags::NONE, &self.scratch[..len])
            .map_err(|_| BufferError::TooLarge)?;
        for buffer in buffers {
            self.published[buffer.0 as usize] = Some(seq);
        }
        self.newest = Some(seq);
        Ok(seq)
    }

    /// Takes `buffer` back if no reader holds it, and says whether it is now free. From this
    /// call on no reader can newly hold the frame that was in it, even if this returns `false`
    /// because one already does; call again later for that one.
    pub fn reclaim(&mut self, buffer: BufferIndex, holders: &impl Holders) -> bool {
        match self.published.get(buffer.0 as usize) {
            None => false,
            Some(None) => true,
            Some(Some(_)) => {
                page::retire(self.pool.page(), buffer);
                if holders.held(buffer) {
                    return false;
                }
                page::release(self.pool.page(), buffer);
                self.published[buffer.0 as usize] = None;
                true
            }
        }
    }

    /// A free buffer to put the next frame in: one that was never published, or the one
    /// published longest ago that no reader holds. The newest frame is left alone, so there is
    /// always a frame for a reader to find.
    ///
    /// # Errors
    ///
    /// [`Dropped`] if readers hold everything else. Drop the frame; a later call will succeed
    /// once a reader lets go or is cut off.
    pub fn take(&mut self, holders: &impl Holders) -> Result<BufferIndex, Dropped> {
        if let Some(free) = self.published.iter().position(Option::is_none) {
            return Ok(BufferIndex(free as u32));
        }
        // Oldest first. Each pass over the pool finds the next oldest above `after`.
        let mut after = None;
        loop {
            let oldest = self
                .published
                .iter()
                .enumerate()
                .filter_map(|(index, seq)| Some((index, (*seq)?)))
                .filter(|(_, seq)| Some(*seq) != self.newest && after.is_none_or(|a| *seq > a))
                .min_by_key(|(_, seq)| *seq);
            let Some((_, seq)) = oldest else {
                return Err(Dropped);
            };
            // A frame of several buffers comes back together.
            let mut first = None;
            for index in 0..self.published.len() {
                if self.published[index] == Some(seq)
                    && self.reclaim(BufferIndex(index as u32), holders)
                {
                    first.get_or_insert(BufferIndex(index as u32));
                }
            }
            if let Some(buffer) = first {
                return Ok(buffer);
            }
            after = Some(seq);
        }
    }

    /// The bytes of `buffer`, to fill, if it is free and this process fills the pool
    /// ([`Pool::in_memory`]).
    pub fn buffer_mut(&mut self, buffer: BufferIndex) -> Option<&mut [u8]> {
        match self.published.get(buffer.0 as usize) {
            Some(None) => self.pool.bytes_mut(buffer),
            _ => None,
        }
    }
}
