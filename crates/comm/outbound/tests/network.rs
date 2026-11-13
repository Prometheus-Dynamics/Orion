//! With the real client: a subscriber across the network reads what a program on the board
//! publishes, with the same code as a subscriber on the board.

use std::time::{Duration, Instant};

use orion::topics::{FieldId, Next, OnSubscribe, SampleTime, Status, Subscriber, TopicDecl};
use orion::{Address, Orion, fields};
use orion_stubs::Board;

/// Everything a subscriber does, wherever it is: wait for samples and take them in order.
fn read(subscriber: &mut Subscriber, want: usize) -> Vec<(u64, f64)> {
    let mut seen = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(10);
    while seen.len() < want {
        assert!(
            Instant::now() < deadline,
            "only {} of {want} samples",
            seen.len()
        );
        subscriber.wait(Duration::from_millis(50)).expect("wait");
        loop {
            match subscriber.next() {
                Next::Sample(sample) => seen.push((
                    sample.time.as_nanos(),
                    f64::from_le_bytes(sample.payload.try_into().expect("a float")),
                )),
                Next::Empty => break,
                Next::Skipped(lost) => panic!("lost {lost}"),
            }
        }
    }
    seen
}

#[test]
fn a_subscriber_across_the_network_reads_as_one_on_the_board_does() {
    let board = Board::start("outbound-network");
    let on_board = Address::Local(board.socket().to_owned());
    let across: Address = format!("orion+tcp://{}", board.network())
        .parse()
        .expect("address");

    let sensor = Orion::connect(&on_board, "sensor").expect("connect");
    let mut level = sensor
        .declare(&TopicDecl {
            name: "tank.level",
            fields: &fields![litres: Float "l"],
            history: 256,
            expiry_ms: 0,
            max_payload: 8,
            on_subscribe: Default::default(),
        })
        .expect("declare");

    let near = Orion::connect(&on_board, "panel").expect("connect");
    let far = Orion::connect(&across, "dashboard").expect("connect over the network");
    let mut near_reading = near.subscribe("tank.level").expect("subscribe");
    let mut far_reading = far.subscribe("tank.level").expect("subscribe");
    assert_eq!(far_reading.status(), Status::Live);

    // The publisher knows of both, and does nothing different for either.
    let before = Instant::now();
    while level.subscribers() < 2 {
        assert!(before.elapsed() < Duration::from_secs(5));
        level.poll().expect("poll");
        std::thread::sleep(Duration::from_millis(1));
    }
    let expected: Vec<(u64, f64)> = (1..=200_u64)
        .map(|step| (step, step as f64 * 0.5))
        .collect();
    for (time, litres) in &expected {
        level
            .publish(
                FieldId(0),
                SampleTime::from_nanos(*time),
                &litres.to_le_bytes(),
            )
            .expect("publish");
        if time % 20 == 0 {
            // A publisher that does other things between samples.
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    assert_eq!(read(&mut near_reading, 200), expected);
    assert_eq!(read(&mut far_reading, 200), expected);
    // The newest value is there to ask for, on both.
    for reading in [&mut near_reading, &mut far_reading] {
        let latest = reading.latest(FieldId(0)).expect("a latest value");
        assert_eq!(latest.time.as_nanos(), 200);
    }

    // It goes on: what is published later arrives later.
    level
        .publish(
            FieldId(0),
            SampleTime::from_nanos(201),
            &100.5_f64.to_le_bytes(),
        )
        .expect("publish");
    assert_eq!(read(&mut far_reading, 1), [(201, 100.5)]);
}

#[test]
fn a_subscriber_across_the_network_is_given_the_newest_of_each_field_when_the_topic_asks() {
    let board = Board::start("outbound-latest");
    let on_board = Address::Local(board.socket().to_owned());
    let across: Address = format!("orion+tcp://{}", board.network())
        .parse()
        .expect("address");
    let sensor = Orion::connect(&on_board, "sensor").expect("connect");
    let declare = |name, on_subscribe| TopicDecl {
        name,
        fields: &fields![left: Float "l", right: Float "l", spare: Float "l"],
        history: 16,
        expiry_ms: 60_000,
        max_payload: 8,
        on_subscribe,
    };
    let mut latest = sensor
        .declare(&declare("tank.latest", OnSubscribe::Latest))
        .expect("declare");
    let mut from_now = sensor
        .declare(&declare("tank.now", OnSubscribe::FromNow))
        .expect("declare");
    let now = SampleTime::now().as_nanos();
    for publisher in [&mut latest, &mut from_now] {
        for (field, time, value) in [
            (0, now - 3, 1.0_f64),
            (0, now - 2, 2.0),
            (1, now - 1, 3.0),
            // Long expired: not given.
            (2, 1, 9.0),
        ] {
            publisher
                .publish(
                    FieldId(field),
                    SampleTime::from_nanos(time),
                    &value.to_le_bytes(),
                )
                .expect("publish");
        }
    }

    let far = Orion::connect(&across, "dashboard").expect("connect over the network");
    let mut given = far.subscribe("tank.latest").expect("subscribe");
    assert_eq!(read(&mut given, 2), [(now - 2, 2.0), (now - 1, 3.0)]);
    let mut nothing = far.subscribe("tank.now").expect("subscribe");
    std::thread::sleep(Duration::from_millis(200));
    for reading in [&mut given, &mut nothing] {
        reading.wait(Duration::from_millis(50)).expect("wait");
        assert!(matches!(reading.next(), Next::Empty), "nothing more");
    }

    // Then it goes on as any subscription does.
    latest
        .publish(
            FieldId(1),
            SampleTime::from_nanos(now),
            &4.0_f64.to_le_bytes(),
        )
        .expect("publish");
    from_now
        .publish(
            FieldId(1),
            SampleTime::from_nanos(now),
            &4.0_f64.to_le_bytes(),
        )
        .expect("publish");
    assert_eq!(read(&mut given, 1), [(now, 4.0)]);
    assert_eq!(read(&mut nothing, 1), [(now, 4.0)]);
}
