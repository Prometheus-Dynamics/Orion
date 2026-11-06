//! Answers to client requests split into parts that fit the session's frame size.

use super::*;
use crate::frame::FRAME_OVERHEAD;
use core::ops::Range;

/// Upper bound of a part's body without its records: the request id (5), two flags, and two
/// sequence lengths (5 each).
const PART_HEADER: usize = 17;

fn encoded_len<T: Serialize>(value: &T, scratch: &mut [u8]) -> usize {
    postcard::to_slice(value, scratch).map_or(usize::MAX, |used| used.len())
}

/// Packs records of the given lengths into consecutive ranges whose total fits `budget`. At least
/// one (empty) range, so an empty answer is still sent.
pub(crate) fn pack(lens: &[usize], budget: usize) -> Result<Vec<Range<usize>>, RecordTooLarge> {
    let mut parts = Vec::new();
    let (mut start, mut used) = (0, 0usize);
    for (index, &len) in lens.iter().enumerate() {
        if len > budget {
            return Err(RecordTooLarge {
                record_len: len,
                max_record: budget,
            });
        }
        if used + len > budget {
            parts.push(start..index);
            start = index;
            used = 0;
        }
        used += len;
    }
    parts.push(start..lens.len());
    Ok(parts)
}

fn budget(max_frame: usize) -> usize {
    max_frame.saturating_sub(FRAME_OVERHEAD + PART_HEADER)
}

/// A status answer encoded as frame bodies' inputs: call [`StatusParts::encode`] per part.
pub(crate) struct StatusParts {
    req: u32,
    bootstrap: bool,
    updated: Vec<StatusRecordWire>,
    expired: Vec<StatusKeyWire>,
    /// Ranges over `updated` then `expired`, as one sequence.
    parts: Vec<Range<usize>>,
}

impl StatusParts {
    pub(crate) fn new(
        req: u32,
        bootstrap: bool,
        updated: &[StatusEntry],
        expired: &[StatusKey],
        max_frame: usize,
    ) -> Result<Self, RecordTooLarge> {
        let updated: Vec<StatusRecordWire> = updated.iter().map(Into::into).collect();
        let expired: Vec<StatusKeyWire> = expired.iter().map(Into::into).collect();
        let mut scratch = alloc::vec![0u8; max_frame.max(FRAME_OVERHEAD)];
        let mut lens: Vec<usize> = updated
            .iter()
            .map(|record| encoded_len(record, &mut scratch))
            .collect();
        lens.extend(expired.iter().map(|key| encoded_len(key, &mut scratch)));
        let parts = pack(&lens, budget(max_frame))?;
        Ok(Self {
            req,
            bootstrap,
            updated,
            expired,
            parts,
        })
    }

    pub(crate) fn len(&self) -> usize {
        self.parts.len()
    }

    pub(crate) fn encode(
        &self,
        part: usize,
        seq: u16,
        buf: &mut [u8],
    ) -> Result<usize, MessageError> {
        let range = self.parts.get(part).cloned().unwrap_or(0..0);
        let split = self.updated.len();
        let updated = self
            .updated
            .get(range.start.min(split)..range.end.min(split))
            .unwrap_or_default();
        let expired = self
            .expired
            .get(range.start.saturating_sub(split)..range.end.saturating_sub(split))
            .unwrap_or_default();
        crate::message::codec::encode_with(
            kind::STATUS_CHANGES,
            seq,
            &StatusChangesRef {
                req: self.req,
                bootstrap: self.bootstrap,
                more: part + 1 < self.parts.len(),
                updated,
                expired,
            },
            buf,
        )
    }
}

/// An action answer split into parts; see [`StatusParts`].
pub(crate) struct ActionParts {
    req: u32,
    records: Vec<ActionRecordWire>,
    parts: Vec<Range<usize>>,
}

impl ActionParts {
    pub(crate) fn new(
        req: u32,
        results: &[ActionResult],
        max_frame: usize,
    ) -> Result<Self, RecordTooLarge> {
        let records: Vec<ActionRecordWire> = results.iter().map(Into::into).collect();
        let mut scratch = alloc::vec![0u8; max_frame.max(FRAME_OVERHEAD)];
        let lens: Vec<usize> = records
            .iter()
            .map(|record| encoded_len(record, &mut scratch))
            .collect();
        let parts = pack(&lens, budget(max_frame))?;
        Ok(Self {
            req,
            records,
            parts,
        })
    }

    pub(crate) fn len(&self) -> usize {
        self.parts.len()
    }

    pub(crate) fn encode(
        &self,
        part: usize,
        seq: u16,
        buf: &mut [u8],
    ) -> Result<usize, MessageError> {
        let range = self.parts.get(part).cloned().unwrap_or(0..0);
        crate::message::codec::encode_with(
            kind::ACTION_RESULTS,
            seq,
            &ActionResultsRef {
                req: self.req,
                more: part + 1 < self.parts.len(),
                results: self.records.get(range).unwrap_or_default(),
            },
            buf,
        )
    }
}
