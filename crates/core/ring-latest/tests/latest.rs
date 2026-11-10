use orion_measure::CountingAlloc;
use orion_measure::harness::assert_steady_no_allocs;
use orion_ring::{FieldId, Flags, FrameTime, Ring, Spec};
use orion_ring_latest::{Latest, Value, is_expired};

#[global_allocator]
static GLOBAL: CountingAlloc = CountingAlloc::system();

const MS: u64 = 1_000_000;

fn at(ms: u64) -> FrameTime {
    FrameTime::from_nanos(ms * MS)
}

fn ring(fields: u32, expiry_ms: u32) -> Ring {
    Ring::create(&Spec {
        fields,
        history: 2,
        max_payload: 16,
        expiry_ms,
    })
    .expect("ring")
}

#[test]
fn a_value_is_fresh_until_its_expiry_and_expired_after() {
    let ring = ring(2, 250);
    let mut writer = ring.writer().expect("writer");
    let mut latest = Latest::new(ring.reader().expect("reader"));
    assert_eq!(latest.expiry_ms(), 250);
    assert_eq!(latest.get(FieldId(0), at(0)), Value::Nothing);

    writer
        .publish(FieldId(0), at(1000), Flags::NONE, b"21.5")
        .expect("publish");
    // At the instant, at the edge, and one nanosecond past it.
    for now in [at(1000), at(1250)] {
        let value = latest.get(FieldId(0), now);
        assert!(
            matches!(value, Value::Fresh(sample) if sample.payload == b"21.5"),
            "{value:?}"
        );
    }
    let past = FrameTime::from_nanos(1250 * MS + 1);
    let value = latest.get(FieldId(0), past);
    assert!(
        matches!(value, Value::Expired(sample) if sample.payload == b"21.5"),
        "{value:?}"
    );
    assert_eq!(value.fresh(), None);
    // A reader whose clock is behind the publisher's does not call the value expired.
    assert!(latest.get(FieldId(0), at(10)).fresh().is_some());

    // Refreshing it makes it fresh again, and the other field is untouched by all of this.
    writer
        .publish(FieldId(0), at(1300), Flags::NONE, b"21.6")
        .expect("publish");
    let value = latest.get(FieldId(0), past);
    assert!(matches!(value, Value::Fresh(sample) if sample.payload == b"21.6"));
    assert_eq!(latest.get(FieldId(1), past), Value::Nothing);
    assert_eq!(latest.get(FieldId(2), past), Value::Nothing);
}

#[test]
fn without_an_expiry_a_value_stays_fresh() {
    let ring = ring(1, 0);
    let mut writer = ring.writer().expect("writer");
    let mut latest = Latest::new(ring.reader().expect("reader"));
    writer
        .publish(FieldId(0), at(1), Flags::NONE, b"v1")
        .expect("publish");
    assert!(
        latest
            .get(FieldId(0), FrameTime::from_nanos(u64::MAX))
            .fresh()
            .is_some()
    );
    assert!(!is_expired(at(1), FrameTime::from_nanos(u64::MAX), 0));
    assert!(is_expired(at(1), at(3), 1));
    assert!(!is_expired(at(1), at(2), 1));
    // The largest expiry does not overflow.
    assert!(!is_expired(at(0), at(u64::from(u32::MAX)), u32::MAX));
}

#[test]
fn a_many_field_declaration_keeps_every_field_whatever_the_others_do() {
    const FIELDS: u16 = 500;
    let ring = ring(u32::from(FIELDS), 0);
    let mut writer = ring.writer().expect("writer");
    let mut latest = Latest::new(ring.reader().expect("reader"));
    assert_eq!(latest.fields(), 500);
    // Every field once.
    for field in 0..FIELDS {
        writer
            .publish(FieldId(field), at(1), Flags::NONE, &field.to_le_bytes())
            .expect("publish");
    }
    // Then one field hammered, far past the size of the whole ring.
    for round in 0..20_000_u32 {
        writer
            .publish(FieldId(7), at(2), Flags::NONE, &round.to_le_bytes())
            .expect("publish");
    }
    for field in 0..FIELDS {
        let value = latest.get(FieldId(field), at(3)).fresh().expect("kept");
        if field == 7 {
            assert_eq!(value.payload, 19_999_u32.to_le_bytes());
        } else {
            assert_eq!(value.payload, field.to_le_bytes(), "field {field}");
        }
    }
}

#[test]
fn changed_returns_each_new_value_once() {
    let ring = ring(3, 0);
    let mut writer = ring.writer().expect("writer");
    writer
        .publish(FieldId(1), at(1), Flags::NONE, b"before")
        .expect("publish");
    let mut latest = Latest::new(ring.reader().expect("reader"));
    // What was there before the reader came is returned once.
    assert!(matches!(latest.changed(FieldId(1)), Some(sample) if sample.payload == b"before"));
    assert_eq!(latest.changed(FieldId(1)), None);
    assert_eq!(latest.changed(FieldId(0)), None);
    assert_eq!(latest.changed(FieldId(3)), None);
    writer
        .publish(FieldId(1), at(2), Flags::NONE, b"a")
        .expect("publish");
    writer
        .publish(FieldId(1), at(3), Flags::NONE, b"b")
        .expect("publish");
    // Two publishes between looks: the newest, once.
    assert!(matches!(latest.changed(FieldId(1)), Some(sample) if sample.payload == b"b"));
    assert_eq!(latest.changed(FieldId(1)), None);
    assert_eq!(latest.changed(FieldId(2)), None);
}

#[test]
fn reading_latest_values_allocates_nothing_and_does_not_grow_with_the_fields() {
    let ring = ring(2000, 100);
    let mut writer = ring.writer().expect("writer");
    let mut latest = Latest::new(ring.reader().expect("reader"));
    for field in 0..2000_u16 {
        writer
            .publish(FieldId(field), at(1), Flags::NONE, &[1; 16])
            .expect("publish");
    }
    let mut fresh = 0_u32;
    let mut round = 0_u16;
    assert_steady_no_allocs("latest", 8, 20_000, || {
        let field = FieldId(round % 2000);
        round = round.wrapping_add(1);
        fresh += u32::from(latest.get(field, at(50)).fresh().is_some());
        fresh += u32::from(latest.changed(field).is_some());
    });
    // Every `get` was fresh, and each field changed once.
    assert_eq!(fresh, 20_008 + 2000);
}
