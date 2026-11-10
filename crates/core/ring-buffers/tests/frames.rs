//! Buffer topics in one process: frames read in place, buffers reused, pools replaced.

#![allow(unsafe_code)]

use std::os::fd::{FromRawFd, OwnedFd};

use orion_measure::CountingAlloc;
use orion_measure::harness::assert_steady_no_allocs;
use orion_ring::{FieldId, FrameTime, Ring, RingError, Spec};
use orion_ring_buffers::{
    BufferDesc, BufferError, BufferIndex, BufferReader, BufferWriter, Dropped, Frame, Holders,
    NextFrame, NoHolders, NoMarks, Pool, PoolView,
};

#[global_allocator]
static GLOBAL: CountingAlloc = CountingAlloc::system();

const FRAME: usize = 4096;

fn ring() -> Ring {
    Ring::create(&Spec {
        fields: 1,
        history: 8,
        max_payload: 128,
        expiry_ms: 0,
    })
    .expect("ring")
}

fn topic(buffers: u32) -> (Ring, BufferWriter, BufferReader) {
    let ring = ring();
    let pool = Pool::in_memory(1, buffers, FRAME).expect("pool");
    let view = pool.view().expect("view");
    let writer = BufferWriter::new(ring.writer().expect("writer"), FieldId(0), pool);
    let reader = BufferReader::new(ring.reader().expect("reader"), FieldId(0), view, NoMarks);
    (ring, writer, reader)
}

/// Fills a free buffer with `fill` and publishes it as a frame.
fn send(
    writer: &mut BufferWriter,
    holders: &impl Holders,
    fill: u8,
    time: u64,
) -> Result<BufferIndex, Dropped> {
    let buffer = writer.take(holders)?;
    writer.buffer_mut(buffer).expect("free").fill(fill);
    writer
        .publish(&[buffer], FrameTime::from_nanos(time), &[fill, fill])
        .expect("publish");
    Ok(buffer)
}

fn frame<'a>(next: NextFrame<'a>) -> Frame<'a> {
    match next {
        NextFrame::Frame(frame) => frame,
        other => panic!("{other:?}"),
    }
}

/// Some readers hold some buffers.
struct Holding(Vec<BufferIndex>);

impl Holders for Holding {
    fn held(&self, buffer: BufferIndex) -> bool {
        self.0.contains(&buffer)
    }
}

#[test]
fn a_frame_is_read_in_place_with_its_time_and_metadata() {
    let (_ring, mut writer, mut reader) = topic(4);
    assert!(matches!(reader.next(), NextFrame::Empty));
    let buffer = send(&mut writer, &NoHolders, 0xAB, 777).expect("frame");
    let frame = frame(reader.next());
    assert_eq!(
        (frame.seq, frame.time, frame.meta),
        (0, FrameTime::from_nanos(777), &[0xAB, 0xAB][..])
    );
    assert_eq!((frame.held.len(), frame.held.buffer(0)), (1, Some(buffer)));
    let bytes = frame.held.bytes(0).expect("bytes");
    assert_eq!(bytes.len(), FRAME);
    assert!(bytes.iter().all(|byte| *byte == 0xAB));
    assert_eq!(frame.held.bytes(1), None);
    let (_, desc) = frame.held.descriptor(0).expect("descriptor");
    assert_eq!(
        (desc.offset, desc.size),
        (u64::from(buffer.0) * FRAME as u64, FRAME as u64)
    );
}

#[test]
fn the_reader_sees_the_publishers_memory_not_a_copy() {
    let (_ring, mut writer, mut reader) = topic(2);
    let buffer = send(&mut writer, &NoHolders, 1, 0).expect("frame");
    let held = frame(reader.next()).held;
    assert!(held.bytes(0).expect("bytes").iter().all(|byte| *byte == 1));
    // Nothing holds the buffer here (`NoMarks`), so the publisher can take it back and write it
    // again. The reader's slice shows the new bytes: it is the same memory.
    assert!(writer.reclaim(buffer, &NoHolders));
    writer.buffer_mut(buffer).expect("free").fill(2);
    assert!(held.bytes(0).expect("bytes").iter().all(|byte| *byte == 2));
}

#[test]
fn a_frame_whose_buffer_was_taken_back_is_skipped_not_misread() {
    let (_ring, mut writer, mut reader) = topic(2);
    // Three frames through two buffers: the first frame's buffer is reused for the third.
    for fill in 1..=3 {
        send(&mut writer, &NoHolders, fill, u64::from(fill)).expect("frame");
    }
    assert!(matches!(reader.next(), NextFrame::Skipped(1)));
    for fill in 2..=3 {
        let frame = frame(reader.next());
        assert_eq!(frame.meta, [fill, fill]);
        assert!(
            frame
                .held
                .bytes(0)
                .expect("bytes")
                .iter()
                .all(|byte| *byte == fill)
        );
    }
    assert!(matches!(reader.next(), NextFrame::Empty));
    assert_eq!(reader.missed(), 1);
}

#[test]
fn the_publisher_drops_frames_instead_of_waiting_and_keeps_the_newest_frame() {
    let (_ring, mut writer, mut reader) = topic(3);
    let first = send(&mut writer, &NoHolders, 1, 1).expect("frame");
    let second = send(&mut writer, &NoHolders, 2, 2).expect("frame");
    let third = send(&mut writer, &NoHolders, 3, 3).expect("frame");
    // Readers hold the two oldest. The newest is never taken back, so there is nowhere to put
    // a frame: it is dropped, at once.
    let holding = Holding(vec![first, second]);
    assert_eq!(send(&mut writer, &holding, 4, 4), Err(Dropped));
    assert_eq!(writer.buffer_mut(first), None);
    // The newest frame is still there for a reader.
    for _ in 0..2 {
        assert!(matches!(
            reader.next(),
            NextFrame::Skipped(_) | NextFrame::Frame(_)
        ));
    }
    // One reader lets go: that buffer, and only that one, comes back.
    let holding = Holding(vec![first]);
    assert_eq!(send(&mut writer, &holding, 5, 5), Ok(second));
    assert_ne!(third, second);
    assert!(!writer.reclaim(first, &holding));
    assert!(writer.reclaim(first, &NoHolders));
}

#[test]
fn a_frame_of_several_buffers_travels_and_comes_back_together() {
    let (_ring, mut writer, mut reader) = topic(8);
    let levels = [BufferIndex(5), BufferIndex(0), BufferIndex(3)];
    for (level, buffer) in levels.iter().enumerate() {
        writer
            .buffer_mut(*buffer)
            .expect("free")
            .fill(level as u8 + 10);
    }
    writer
        .publish(&levels, FrameTime::from_nanos(9), b"pyramid")
        .expect("publish");
    let frame = frame(reader.next());
    assert_eq!((frame.held.len(), frame.meta), (3, &b"pyramid"[..]));
    for (level, buffer) in levels.iter().enumerate() {
        assert_eq!(frame.held.buffer(level), Some(*buffer));
        assert_eq!(frame.held.bytes(level).expect("bytes")[0], level as u8 + 10);
    }
    drop(frame);
    // Four more frames use the five buffers left; the fifth needs the pyramid's back.
    for fill in 0..5 {
        send(&mut writer, &NoHolders, fill, 0).expect("frame");
    }
    assert_eq!(
        writer.buffer_mut(BufferIndex(5)).map(|bytes| bytes.len()),
        None
    );
    send(&mut writer, &NoHolders, 9, 0).expect("frame");
    assert!(writer.buffer_mut(BufferIndex(3)).is_some());
}

#[test]
fn publishes_that_break_the_rules_are_refused() {
    let (_ring, mut writer, _reader) = topic(2);
    let t = FrameTime::ZERO;
    assert_eq!(writer.publish(&[], t, b""), Err(BufferError::Count));
    assert_eq!(
        writer.publish(&[BufferIndex(0); 9], t, b""),
        Err(BufferError::Count)
    );
    assert_eq!(
        writer.publish(&[BufferIndex(2)], t, b""),
        Err(BufferError::NoSuchBuffer(BufferIndex(2)))
    );
    assert_eq!(
        writer.publish(&[BufferIndex(0), BufferIndex(0)], t, b""),
        Err(BufferError::NotFree(BufferIndex(0)))
    );
    assert_eq!(
        writer.publish(&[BufferIndex(0)], t, &[0; 200]),
        Err(BufferError::TooLarge)
    );
    assert_eq!(writer.publish(&[BufferIndex(0)], t, b""), Ok(0));
    assert_eq!(
        writer.publish(&[BufferIndex(0)], t, b""),
        Err(BufferError::NotFree(BufferIndex(0)))
    );
    assert!(!writer.reclaim(BufferIndex(7), &NoHolders));
}

#[test]
fn a_replaced_pool_is_announced_and_old_frames_stay_readable_while_held() {
    let (_ring, mut writer, mut reader) = topic(2);
    send(&mut writer, &NoHolders, 1, 1).expect("frame");
    let old = frame(reader.next()).held;

    let bigger = Pool::in_memory(2, 3, 2 * FRAME).expect("pool");
    let view = bigger.view().expect("view");
    drop(writer.replace_pool(bigger));
    send(&mut writer, &NoHolders, 7, 2).expect("frame");
    send(&mut writer, &NoHolders, 8, 3).expect("frame");

    // The reader is told, once, and loses the frame it could not read.
    assert!(matches!(
        reader.next(),
        NextFrame::PoolChanged { generation: 2 }
    ));
    reader.set_pool(view, NoMarks);
    let new = frame(reader.next());
    assert_eq!(
        (new.meta, new.held.bytes(0).expect("bytes").len()),
        (&[8, 8][..], 2 * FRAME)
    );
    // The frame of the old pool is still mapped and still what it was, though its pool is gone.
    assert!(old.bytes(0).expect("bytes").iter().all(|byte| *byte == 1));
    assert_eq!(reader.missed(), 1);
}

fn memfd(bytes: usize, sealed: bool) -> OwnedFd {
    // SAFETY: a literal name and valid flags; the descriptor is owned by nothing else.
    unsafe {
        let raw = libc::memfd_create(
            c"test".as_ptr(),
            libc::MFD_CLOEXEC | libc::MFD_ALLOW_SEALING,
        );
        assert!(raw >= 0);
        assert_eq!(libc::ftruncate(raw, bytes as libc::off_t), 0);
        if sealed {
            assert_eq!(libc::fcntl(raw, libc::F_ADD_SEALS, libc::F_SEAL_SHRINK), 0);
        }
        OwnedFd::from_raw_fd(raw)
    }
}

#[test]
fn a_pool_that_does_not_add_up_is_refused_by_the_subscriber() {
    let pool = Pool::in_memory(1, 2, FRAME).expect("pool");
    let one = |offset, size| {
        [BufferDesc {
            descriptor: 0,
            offset,
            size,
        }]
    };
    let attach = |fd: OwnedFd, buffers: &[BufferDesc]| {
        PoolView::attach(pool.page_fd().expect("page"), vec![fd], buffers)
    };
    let two = |a: [BufferDesc; 1], b: [BufferDesc; 1]| [a[0], b[0]];

    // A descriptor that could be truncated under the reader.
    let buffers = two(one(0, 64), one(64, 64));
    assert!(matches!(
        attach(memfd(4096, false), &buffers),
        Err(RingError::Unsealed)
    ));
    // A buffer past the end of its descriptor, or whose end overflows.
    for bad in [one(4000, 200), one(u64::MAX, 2)] {
        let buffers = two(one(0, 64), bad);
        assert!(matches!(
            attach(memfd(4096, true), &buffers),
            Err(RingError::Malformed(_))
        ));
    }
    // A buffer in a descriptor that wasn't sent.
    let mut buffers = two(one(0, 64), one(64, 64));
    buffers[1].descriptor = 3;
    assert!(matches!(
        attach(memfd(4096, true), &buffers),
        Err(RingError::Malformed(_))
    ));
    // A buffer list that isn't the one the pool page is for.
    assert!(matches!(
        attach(memfd(4096, true), &one(0, 64)),
        Err(RingError::Malformed(_))
    ));
    // A page that isn't a pool page.
    let not_a_page = memfd(4096, true);
    let buffers = two(one(0, 64), one(64, 64));
    assert!(matches!(
        PoolView::attach(not_a_page, vec![memfd(4096, true)], &buffers),
        Err(RingError::Malformed(_))
    ));
    // And one that adds up.
    let view = attach(memfd(4096, true), &buffers).expect("view");
    assert_eq!(
        (view.len(), view.generation(), view.is_empty()),
        (2, 1, false)
    );
    assert!(view.descriptor(BufferIndex(1)).is_some());
}

#[test]
fn an_entry_naming_buffers_outside_the_pool_is_not_followed() {
    let ring = ring();
    let pool = Pool::in_memory(1, 2, FRAME).expect("pool");
    let view = pool.view().expect("view");
    let mut raw = ring.writer().expect("writer");
    let mut reader = BufferReader::new(ring.reader().expect("reader"), FieldId(0), view, NoMarks);
    let mut payload = [0_u8; 64];
    let bad: [&[(BufferIndex, u64)]; 2] = [&[(BufferIndex(2), 0)], &[(BufferIndex(u32::MAX), 0)]];
    for buffers in bad {
        let len = orion_ring_buffers::entry::write(&mut payload, 1, buffers, b"").expect("entry");
        raw.publish(
            FieldId(0),
            FrameTime::ZERO,
            orion_ring::Flags::NONE,
            &payload[..len],
        )
        .expect("publish");
    }
    // Garbage where an entry should be, and an entry cut short.
    raw.publish(
        FieldId(0),
        FrameTime::ZERO,
        orion_ring::Flags::NONE,
        &[0xFF; 40],
    )
    .expect("publish");
    raw.publish(
        FieldId(0),
        FrameTime::ZERO,
        orion_ring::Flags::NONE,
        &[1, 0, 0, 0, 2, 0, 0, 0, 0],
    )
    .expect("publish");
    assert!(matches!(reader.next(), NextFrame::Skipped(4)));
    assert!(matches!(reader.next(), NextFrame::Empty));
}

#[test]
fn publishing_and_reading_frames_allocates_nothing() {
    let (_ring, mut writer, mut reader) = topic(4);
    let mut read = 0_u64;
    let mut fill = 0_u8;
    assert_steady_no_allocs("frames", 16, 10_000, || {
        fill = fill.wrapping_add(1);
        send(&mut writer, &NoHolders, fill, u64::from(fill)).expect("frame");
        if let NextFrame::Frame(frame) = reader.next() {
            read += u64::from(frame.held.bytes(0).expect("bytes")[100] == fill);
        }
    });
    assert_eq!(read, 10_016);
}
