//! The ring a subscriber keeps of its own, when no ring can be shared with it: the samples
//! that arrive as messages are written to it, and the subscriber reads it as any other.
//!
//! On Linux it is a ring in a memfd, the same kind the comm core shares, so a subscriber reads
//! one type of ring either way. Elsewhere it is on the heap, shared by its one writer and its
//! one reader in this process.

use std::sync::Arc;

use shm_ring::{Heap, RingError, Spec, Writer};

/// What a subscriber reads: its mapping of a ring on Linux, the heap elsewhere.
#[cfg(target_os = "linux")]
pub type RingReader = shm_ring::Reader;
/// What a subscriber reads: its mapping of a ring on Linux, the heap elsewhere.
#[cfg(not(target_os = "linux"))]
pub type RingReader = shm_ring::Reader<std::sync::Arc<shm_ring::Heap>>;

/// A subscriber's own ring and its writer.
pub(crate) struct Mirror {
    #[cfg(target_os = "linux")]
    _ring: shm_ring::Ring,
    #[cfg(target_os = "linux")]
    pub(crate) writer: Writer,
    #[cfg(not(target_os = "linux"))]
    pub(crate) writer: Writer<std::sync::Arc<shm_ring::Heap>>,
}

impl Mirror {
    /// A ring of shape `spec`, and a reader at its start.
    #[cfg(target_os = "linux")]
    pub(crate) fn create(spec: &Spec) -> Result<(Self, RingReader), RingError> {
        let ring = shm_ring::Ring::create(spec)?;
        let reader = ring.reader()?;
        let writer = ring.writer()?;
        Ok((
            Self {
                _ring: ring,
                writer,
            },
            reader,
        ))
    }

    /// A ring of shape `spec`, and a reader at its start.
    #[cfg(not(target_os = "linux"))]
    pub(crate) fn create(spec: &Spec) -> Result<(Self, RingReader), RingError> {
        let (writer, reader) = on_heap(spec)?;
        Ok((Self { writer }, reader))
    }
}

/// A ring of shape `spec` on the heap: its writer, and a reader at its start.
// Built everywhere, so that Linux's tests run it too.
#[cfg_attr(target_os = "linux", allow(dead_code))]
pub(crate) fn on_heap(spec: &Spec) -> Result<(HeapWriter, HeapReader), RingError> {
    let words = shm_ring::Geometry::for_spec(spec)?.bytes().div_ceil(8);
    let heap = Arc::new(Heap::new(words));
    shm_ring::format(&*heap, spec)?;
    let reader = shm_ring::Reader::over(Arc::clone(&heap))?;
    let writer = Writer::over(heap)?;
    Ok((writer, reader))
}

/// The writer of a ring on the heap.
pub(crate) type HeapWriter = Writer<Arc<Heap>>;
/// A reader of a ring on the heap.
pub(crate) type HeapReader = shm_ring::Reader<Arc<Heap>>;

#[cfg(test)]
mod tests {
    use shm_ring::{FieldId, Flags, Next, SampleTime, Spec};
    use shm_ring_time::Lookup;

    use super::on_heap;

    #[test]
    fn a_ring_on_the_heap_is_read_as_it_was_written() {
        let spec = Spec {
            fields: 2,
            history: 4,
            max_payload: 8,
            expiry_ms: 0,
        };
        let (mut writer, mut reader) = on_heap(&spec).expect("a ring");
        let at = SampleTime::from_nanos;
        for (field, nanos, value) in [(0, 10, 1_u8), (1, 20, 2), (0, 30, 3)] {
            writer
                .publish(FieldId(field), at(nanos), Flags::NONE, &[value])
                .expect("publish");
        }
        // In order, every one.
        let mut seen = Vec::new();
        while let Next::Sample(sample) = reader.next() {
            seen.push((sample.field.0, sample.payload[0]));
        }
        assert_eq!(seen, [(0, 1), (1, 2), (0, 3)]);
        // The newest of a field, and the one belonging to a time.
        assert_eq!(reader.latest(FieldId(0)).expect("latest").payload, [3]);
        match shm_ring_time::at_or_before(&mut reader, FieldId(0), at(25)) {
            Lookup::Lagged { sample, lag_ns } => {
                assert_eq!((sample.payload, lag_ns), (&[1][..], 15));
            }
            other => panic!("expected the first, got {other:?}"),
        }
        // A sample the shape has no room for is refused.
        assert!(
            writer
                .publish(FieldId(2), at(40), Flags::NONE, &[0])
                .is_err()
        );
        assert!(
            writer
                .publish(FieldId(0), at(40), Flags::NONE, &[0; 9])
                .is_err()
        );
    }
}
