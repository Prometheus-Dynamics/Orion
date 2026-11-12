//! The ring in a real memfd, shared between mappings and threads.

#![allow(unsafe_code)]

use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::sync::atomic::{AtomicBool, Ordering};

use orion_ring::{FieldId, Flags, Next, Reader, Ring, RingError, SampleTime, Spec, Writer};

fn spec(fields: u32, history: u32, max_payload: u32) -> Spec {
    Spec {
        fields,
        history,
        max_payload,
        expiry_ms: 250,
    }
}

/// A memfd of `bytes` zeroed bytes with seals `seals` (0 for none).
fn memfd(bytes: usize, seals: libc::c_int) -> OwnedFd {
    // SAFETY: a literal name and valid flags; the descriptor is owned by nothing else.
    let fd = unsafe {
        let raw = libc::memfd_create(
            c"test".as_ptr(),
            libc::MFD_CLOEXEC | libc::MFD_ALLOW_SEALING,
        );
        assert!(raw >= 0);
        OwnedFd::from_raw_fd(raw)
    };
    // SAFETY: `fd` is open.
    unsafe {
        assert_eq!(libc::ftruncate(fd.as_raw_fd(), bytes as libc::off_t), 0);
        if seals != 0 {
            assert_eq!(libc::fcntl(fd.as_raw_fd(), libc::F_ADD_SEALS, seals), 0);
        }
    }
    fd
}

#[test]
fn a_sample_crosses_from_the_write_end_to_a_read_end() {
    let ring = Ring::create(&spec(2, 4, 100)).expect("ring");
    assert_eq!(ring.mapped_bytes() % 4096, 0);
    assert_eq!(ring.geometry().expiry_ms(), 250);
    let mut writer = Writer::attach(ring.write_fd().expect("write end")).expect("writer");
    let mut reader = Reader::attach(ring.read_fd().expect("read end")).expect("reader");
    writer
        .publish(
            FieldId(1),
            SampleTime::from_nanos(42),
            Flags::OTHERS_DEPEND,
            b"hello",
        )
        .expect("publish");
    match reader.next() {
        Next::Sample(sample) => {
            assert_eq!((sample.seq, sample.field), (0, FieldId(1)));
            assert_eq!(sample.time, SampleTime::from_nanos(42));
            assert!(sample.flags.has(Flags::OTHERS_DEPEND));
            assert_eq!(sample.payload, b"hello");
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn the_publisher_sees_the_subscriber_count() {
    let ring = Ring::create(&spec(1, 2, 8)).expect("ring");
    let writer = ring.writer().expect("writer");
    assert_eq!(writer.subscribers(), 0);
    ring.set_subscribers(3);
    assert_eq!((writer.subscribers(), ring.subscribers()), (3, 3));
}

#[test]
fn a_read_end_cannot_be_mapped_for_writing() {
    let ring = Ring::create(&spec(1, 2, 8)).expect("ring");
    let read_end = ring.read_fd().expect("read end");
    assert!(matches!(Writer::attach(read_end), Err(RingError::Io(_))));
}

#[test]
fn a_ring_can_never_be_resized() {
    let ring = Ring::create(&spec(1, 2, 8)).expect("ring");
    let fd = ring.write_fd().expect("write end");
    // SAFETY: `fd` is open.
    unsafe {
        assert_eq!(libc::ftruncate(fd.as_raw_fd(), 0), -1);
        assert_eq!(libc::ftruncate(fd.as_raw_fd(), 1 << 20), -1);
        assert_eq!(
            libc::fcntl(fd.as_raw_fd(), libc::F_ADD_SEALS, libc::F_SEAL_WRITE),
            -1
        );
    }
}

#[test]
fn memory_that_could_shrink_or_is_not_a_ring_is_refused() {
    // Not sealed: its writer could truncate it under a reader.
    assert!(matches!(
        Reader::attach(memfd(4096, 0)),
        Err(RingError::Unsealed)
    ));
    // Not a memfd at all.
    let file = std::fs::File::open("/proc/self/exe").expect("file");
    assert!(matches!(
        Reader::attach(OwnedFd::from(file)),
        Err(RingError::Unsealed)
    ));
    // Sealed, but empty, too short, or zeroes.
    for bytes in [0, 4, 64, 4096] {
        let fd = memfd(bytes, libc::F_SEAL_SHRINK);
        assert!(
            matches!(Reader::attach(fd), Err(RingError::Malformed(_))),
            "{bytes}"
        );
    }
    // A real header in a file cut short of the ring it describes.
    let ring = Ring::create(&spec(4, 64, 256)).expect("ring");
    let short = memfd(4096, 0);
    let mut header = [0_u8; 128];
    // SAFETY: both descriptors are open and the buffer is 128 bytes.
    unsafe {
        assert_eq!(
            libc::pread(
                ring.write_fd().expect("fd").as_raw_fd(),
                header.as_mut_ptr().cast(),
                128,
                0
            ),
            128
        );
        assert_eq!(
            libc::pwrite(short.as_raw_fd(), header.as_ptr().cast(), 128, 0),
            128
        );
        assert_eq!(
            libc::fcntl(short.as_raw_fd(), libc::F_ADD_SEALS, libc::F_SEAL_SHRINK),
            0
        );
    }
    assert!(matches!(
        Reader::attach(short),
        Err(RingError::Malformed("memory is smaller than the ring"))
    ));
}

/// Every byte of a payload says which sample it belongs to, so a torn copy would show.
fn fill(buffer: &mut [u8], seq: u64) -> &[u8] {
    let len = 1 + (seq % buffer.len() as u64) as usize;
    for (i, byte) in buffer[..len].iter_mut().enumerate() {
        *byte = (seq as u8).wrapping_mul(31).wrapping_add(i as u8);
    }
    &buffer[..len]
}

#[test]
fn readers_racing_the_writer_never_see_a_torn_sample() {
    const SAMPLES: u64 = 300_000;
    let ring = Ring::create(&spec(2, 4, 96)).expect("ring");
    let done = AtomicBool::new(false);
    std::thread::scope(|scope| {
        let mut readers = Vec::new();
        for id in 0..3 {
            let mut reader = ring.reader().expect("reader");
            let done = &done;
            readers.push(scope.spawn(move || {
                let mut expected = [0_u8; 96];
                let (mut seen, mut last, mut idle) = (0_u64, None, 0_u32);
                loop {
                    let finished = done.load(Ordering::Acquire);
                    let sample = if id == 2 {
                        // One reader only ever asks for the newest of a field.
                        reader.latest(FieldId(1)).or_else(|| {
                            idle += 1;
                            None
                        })
                    } else {
                        match reader.next() {
                            Next::Sample(sample) => Some(sample),
                            Next::Skipped(_) => continue,
                            Next::Empty => None,
                        }
                    };
                    match sample {
                        Some(sample) => {
                            assert_eq!(sample.payload, fill(&mut expected, sample.seq));
                            assert_eq!(sample.time.as_nanos(), sample.seq * 7);
                            assert_eq!(u64::from(sample.field.0), sample.seq % 2);
                            if id != 2 {
                                assert!(last.is_none_or(|last| sample.seq > last), "order");
                                last = Some(sample.seq);
                            }
                            seen += 1;
                        }
                        None if finished => break,
                        None => std::hint::spin_loop(),
                    }
                    if id == 2 && finished {
                        break;
                    }
                }
                if id != 2 {
                    assert_eq!(
                        seen + reader.missed(),
                        SAMPLES,
                        "every sample read or counted"
                    );
                }
                seen
            }));
        }
        let mut writer = ring.writer().expect("writer");
        let mut buffer = [0_u8; 96];
        for seq in 0..SAMPLES {
            let payload = fill(&mut buffer, seq);
            let field = FieldId((seq % 2) as u16);
            writer
                .publish(field, SampleTime::from_nanos(seq * 7), Flags::NONE, payload)
                .expect("publish");
        }
        done.store(true, Ordering::Release);
        for reader in readers {
            assert!(reader.join().expect("reader") > 0);
        }
    });
}
