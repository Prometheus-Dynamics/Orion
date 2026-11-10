//! Publishing and reading allocate nothing.

use orion_measure::CountingAlloc;
use orion_measure::harness::assert_steady_no_allocs;
use orion_ring::{FieldId, Flags, FrameTime, Next, Ring, Spec};

#[global_allocator]
static GLOBAL: CountingAlloc = CountingAlloc::system();

#[test]
fn publish_next_and_latest_allocate_nothing() {
    let ring = Ring::create(&Spec {
        fields: 4,
        history: 8,
        max_payload: 256,
        expiry_ms: 0,
    })
    .expect("ring");
    let mut writer = ring.writer().expect("writer");
    let mut slow = ring.reader().expect("reader");
    let payload = [7_u8; 200];
    let mut seq = 0_u64;

    assert_steady_no_allocs("publish", 16, 10_000, || {
        let field = FieldId((seq % 4) as u16);
        writer
            .publish(field, FrameTime::from_nanos(seq), Flags::NONE, &payload)
            .expect("publish");
        seq += 1;
    });

    let mut reader = ring.reader().expect("reader");
    let mut read = 0_u64;
    assert_steady_no_allocs("publish, next and latest", 16, 10_000, || {
        let field = FieldId((seq % 4) as u16);
        writer
            .publish(field, FrameTime::from_nanos(seq), Flags::NONE, &payload)
            .expect("publish");
        seq += 1;
        while let Next::Sample(sample) = reader.next() {
            read += sample.payload.len() as u64;
        }
        read += reader
            .latest(field)
            .map_or(0, |sample| sample.payload.len() as u64);
    });
    assert_eq!(read, 2 * 200 * 10_016);

    // A reader that fell behind skips ahead without allocating either.
    let mut skipped = 0_u64;
    assert_steady_no_allocs("a slow reader skipping", 0, 100, || {
        if let Next::Skipped(count) = slow.next() {
            skipped += count;
        }
    });
    assert!(skipped > 0);
}
