use std::time::{Duration, Instant};

use orion_measure::CountingAlloc;
use orion_measure::harness::assert_steady_no_allocs;
use orion_ring::{FieldId, Flags, Reader, Ring, SampleTime, Spec, Writer};
use orion_ring_doorbell::Doorbell;
use orion_ring_time::{Lookup, at_or_before, wait_at_or_before};

#[global_allocator]
static GLOBAL: CountingAlloc = CountingAlloc::system();

const FIELD: FieldId = FieldId(0);

fn at(time: u64) -> SampleTime {
    SampleTime::from_nanos(time)
}

fn topic(history: u32) -> (Ring, Writer, Reader) {
    let ring = Ring::create(&Spec {
        fields: 2,
        history,
        max_payload: 16,
        expiry_ms: 0,
    })
    .expect("ring");
    let (writer, reader) = (
        ring.writer().expect("writer"),
        ring.reader().expect("reader"),
    );
    (ring, writer, reader)
}

fn publish(writer: &mut Writer, time: u64) {
    writer
        .publish(FIELD, at(time), Flags::NONE, &time.to_le_bytes())
        .expect("publish");
}

/// What a lookup found, as (time of the sample, lag), with the payload checked against the time.
fn outcome(lookup: Lookup<'_>) -> Option<(u64, u64)> {
    let (sample, lag) = match lookup {
        Lookup::Hit(sample) => (sample, 0),
        Lookup::Lagged { sample, lag_ns } => (sample, lag_ns),
        Lookup::Missing => return None,
    };
    assert_eq!(sample.payload, sample.time.as_nanos().to_le_bytes());
    assert_eq!(lookup.is_hit(), lag == 0);
    Some((sample.time.as_nanos(), lag))
}

#[test]
fn hit_lagged_and_missing() {
    let (_ring, mut writer, mut reader) = topic(8);
    assert_eq!(at_or_before(&mut reader, FIELD, at(100)), Lookup::Missing);
    for time in [100, 200, 300] {
        publish(&mut writer, time);
    }
    assert_eq!(
        outcome(at_or_before(&mut reader, FIELD, at(200))),
        Some((200, 0))
    );
    assert_eq!(
        outcome(at_or_before(&mut reader, FIELD, at(299))),
        Some((200, 99))
    );
    assert_eq!(
        outcome(at_or_before(&mut reader, FIELD, at(300))),
        Some((300, 0))
    );
    assert_eq!(
        outcome(at_or_before(&mut reader, FIELD, at(u64::MAX))),
        Some((300, u64::MAX - 300))
    );
    // Before everything the field has.
    assert_eq!(at_or_before(&mut reader, FIELD, at(99)), Lookup::Missing);
    // Another field, and one that isn't declared.
    assert_eq!(
        at_or_before(&mut reader, FieldId(1), at(300)),
        Lookup::Missing
    );
    assert_eq!(
        at_or_before(&mut reader, FieldId(9), at(300)),
        Lookup::Missing
    );
    assert!(at_or_before(&mut reader, FIELD, at(100)).sample().is_some());
}

#[test]
fn every_time_finds_the_newest_sample_at_or_before_it_at_any_depth() {
    // Histories around the point where the one-by-one check hands over to the search, full
    // and part full, wrapped and not.
    for history in [2, 4, 8, 64] {
        for published in [1, 3, 5, 9, 64, 70, 200_u64] {
            let (_ring, mut writer, mut reader) = topic(history);
            // Samples at 10, 20, 30, ...
            for index in 1..=published {
                publish(&mut writer, index * 10);
            }
            let kept = published.min(u64::from(history));
            let oldest = (published - kept + 1) * 10;
            for time in 0..=published * 10 + 15 {
                let found = outcome(at_or_before(&mut reader, FIELD, at(time)));
                let expected = if time < oldest {
                    None
                } else {
                    let sample = (time / 10).min(published) * 10;
                    Some((sample, time - sample))
                };
                assert_eq!(
                    found, expected,
                    "history {history}, {published} published, time {time}"
                );
            }
        }
    }
}

#[test]
fn equal_times_and_times_out_of_order_still_give_a_real_sample() {
    let (_ring, mut writer, mut reader) = topic(16);
    for time in [10, 20, 20, 20, 30, 25, 40, 40] {
        publish(&mut writer, time);
    }
    assert_eq!(
        outcome(at_or_before(&mut reader, FIELD, at(20))),
        Some((20, 0))
    );
    assert_eq!(
        outcome(at_or_before(&mut reader, FIELD, at(40))),
        Some((40, 0))
    );
    // 25 was published after 30. The answer is a sample at or before the time, whichever.
    for time in [26, 31, 35] {
        let (sample, lag) = outcome(at_or_before(&mut reader, FIELD, at(time))).expect("a sample");
        assert!(sample <= time && lag == time - sample);
    }
}

/// The pairing rule the frame pipelines on `dev` apply, over a one-deep slot per producer: the
/// consumer of the frame at `frame` gets the producer's latest value, aligned if it is for that
/// frame, lagged (by `frame - latest`) if not, missing if the producer has published nothing.
fn dev_pairing(latest: Option<u64>, frame: u64) -> Option<(u64, i128)> {
    latest.map(|latest| (latest, i128::from(frame) - i128::from(latest)))
}

#[test]
fn the_outcomes_are_devs_pairing_on_recorded_sequences() {
    // Each step is a producer tick (`P`, the frame it finished) or a consumer tick (`C`, the
    // frame it is on). These are the shapes the pipelines produce: in step, the producer a
    // frame behind, the producer skipping frames, the producer stalled, and not started.
    let sequences: [&[(char, u64)]; 5] = [
        &[
            ('P', 10),
            ('C', 10),
            ('P', 20),
            ('C', 20),
            ('P', 30),
            ('C', 30),
        ],
        &[
            ('C', 10),
            ('P', 10),
            ('C', 20),
            ('P', 20),
            ('C', 30),
            ('P', 30),
            ('C', 40),
        ],
        &[
            ('P', 10),
            ('C', 10),
            ('C', 20),
            ('P', 30),
            ('C', 30),
            ('C', 40),
            ('P', 50),
            ('C', 50),
        ],
        &[
            ('P', 10),
            ('C', 10),
            ('C', 20),
            ('C', 30),
            ('C', 40),
            ('C', 50),
        ],
        &[('C', 10), ('C', 20), ('P', 20), ('C', 30)],
    ];
    let mut compared = 0;
    for sequence in sequences {
        let (_ring, mut writer, mut reader) = topic(8);
        let mut latest = None;
        for (who, frame) in sequence {
            if *who == 'P' {
                publish(&mut writer, *frame);
                latest = Some(*frame);
                continue;
            }
            let ours = outcome(at_or_before(&mut reader, FIELD, at(*frame)));
            let theirs = dev_pairing(latest, *frame).map(|(sample, lag)| (sample, lag as u64));
            assert_eq!(ours, theirs, "{sequence:?} at consumer frame {frame}");
            compared += 1;
        }
    }
    assert_eq!(compared, 20);

    // The one place the answers differ, on purpose: a consumer behind the producer. A one-deep
    // slot only has the producer's newest value, for a later frame than the consumer's (dev
    // reports that as lagged, with a negative lag). The ring still has the consumer's frame.
    let (_ring, mut writer, mut reader) = topic(8);
    for frame in [10, 20, 30] {
        publish(&mut writer, frame);
    }
    assert_eq!(dev_pairing(Some(30), 20), Some((30, -10)));
    assert_eq!(
        outcome(at_or_before(&mut reader, FIELD, at(20))),
        Some((20, 0))
    );
}

#[test]
fn a_reader_waits_for_a_publisher_that_is_a_frame_behind_and_no_longer() {
    let (_ring, mut writer, mut reader) = topic(8);
    let mut doorbell = Doorbell::create().expect("doorbell");
    let mut bell = doorbell.bell().expect("bell");
    publish(&mut writer, 100);

    // Already there: no wait at all.
    let started = Instant::now();
    let found = wait_at_or_before(
        &mut reader,
        &mut doorbell,
        FIELD,
        at(100),
        Duration::from_secs(20),
    );
    assert_eq!(outcome(found.expect("wait")), Some((100, 0)));
    assert!(started.elapsed() < Duration::from_secs(5));

    // The publisher gets to frame 200 a little later: the reader waits for it and gets a hit.
    std::thread::scope(|scope| {
        scope.spawn(|| {
            std::thread::sleep(Duration::from_millis(50));
            publish(&mut writer, 200);
            bell.ring();
        });
        let started = Instant::now();
        let found = wait_at_or_before(
            &mut reader,
            &mut doorbell,
            FIELD,
            at(200),
            Duration::from_secs(20),
        );
        assert_eq!(outcome(found.expect("wait")), Some((200, 0)));
        assert!(
            started.elapsed() >= Duration::from_millis(40),
            "it did not wait"
        );
    });

    // The publisher never gets to frame 300: the reader waits its limit, then takes the
    // newest there is, lagged.
    let started = Instant::now();
    let found = wait_at_or_before(
        &mut reader,
        &mut doorbell,
        FIELD,
        at(300),
        Duration::from_millis(60),
    );
    assert_eq!(outcome(found.expect("wait")), Some((200, 100)));
    let waited = started.elapsed();
    assert!(
        waited >= Duration::from_millis(60),
        "gave up early: {waited:?}"
    );

    // A publisher that skipped the frame and is already past it is not waited for: the frame
    // is not coming.
    publish(&mut writer, 500);
    let started = Instant::now();
    let found = wait_at_or_before(
        &mut reader,
        &mut doorbell,
        FIELD,
        at(400),
        Duration::from_secs(20),
    );
    assert_eq!(outcome(found.expect("wait")), Some((200, 200)));
    assert!(started.elapsed() < Duration::from_secs(5));

    // No wait allowed, and nothing published on the field: missing, at once.
    let found = wait_at_or_before(
        &mut reader,
        &mut doorbell,
        FieldId(1),
        at(400),
        Duration::ZERO,
    );
    assert_eq!(found.expect("wait"), Lookup::Missing);
}

#[test]
fn a_ring_from_another_topic_does_not_end_the_wait_early() {
    let (_ring, mut writer, mut reader) = topic(8);
    let mut doorbell = Doorbell::create().expect("doorbell");
    let mut other_topic = doorbell.bell().expect("bell");
    let mut bell = doorbell.bell().expect("bell");
    std::thread::scope(|scope| {
        scope.spawn(|| {
            // Rings for something else, then the sample.
            for _ in 0..5 {
                std::thread::sleep(Duration::from_millis(10));
                other_topic.ring();
            }
            publish(&mut writer, 100);
            bell.ring();
        });
        let found = wait_at_or_before(
            &mut reader,
            &mut doorbell,
            FIELD,
            at(100),
            Duration::from_secs(20),
        );
        assert_eq!(outcome(found.expect("wait")), Some((100, 0)));
    });
}

#[test]
fn a_lookup_allocates_nothing() {
    let (_ring, mut writer, mut reader) = topic(256);
    for index in 1..=1000_u64 {
        publish(&mut writer, index * 10);
    }
    let mut time = 7450_u64;
    let mut hits = 0_u64;
    assert_steady_no_allocs("lookup", 8, 20_000, || {
        time = 7450 + (time * 31 + 7) % 2550;
        hits += u64::from(
            at_or_before(&mut reader, FIELD, at(time))
                .sample()
                .is_some(),
        );
    });
    assert_eq!(hits, 20_008);
}
