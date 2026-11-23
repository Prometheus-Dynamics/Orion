//! A publisher can tell whether anyone is listening, wherever the listener is: so a program
//! that reads a device to publish it need not read it while nobody does.

use std::time::{Duration, Instant};

use orion::topics::{FieldId, OnSubscribe, Publisher, SampleTime, TopicDecl};
use orion::{Address, Orion, fields};
use orion_test_board::{Options, TestBoard};

/// How many subscribers the publisher has been told of.
fn listening(values: &mut Publisher) -> u32 {
    values.poll().expect("poll");
    values.subscribers()
}

fn until(what: &str, mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !done() {
        assert!(Instant::now() < deadline, "{what}");
        std::thread::sleep(Duration::from_millis(5));
    }
}

#[test]
fn a_publisher_is_told_when_a_subscriber_on_the_network_comes_and_when_it_goes() {
    let board = TestBoard::start_with(
        "watched",
        Options {
            name: "watched".to_owned(),
            network: true,
            ..Options::default()
        },
    );
    // A program on the board that publishes a device's values, kept for a late listener.
    let bridge = board.connect("bridge").expect("connect");
    let mut values = bridge
        .declare(&TopicDecl {
            name: "device.values",
            fields: &fields![volts: Float "V"],
            history: 2,
            expiry_ms: 0,
            max_payload: 8,
            on_subscribe: OnSubscribe::Latest,
        })
        .expect("declare");
    assert_eq!(listening(&mut values), 0, "nobody listens yet");

    // A listener on the network: it cannot map the ring, and is sent samples as messages.
    let network = Address::Network {
        host: board.network().expect("network way in").to_string(),
        key: None,
    };
    let dashboard = Orion::connect(&network, "dashboard").expect("connect");
    let mut reading = dashboard.subscribe("device.values").expect("subscribe");
    until("the publisher is told of the listener", || {
        listening(&mut values) == 1
    });

    // Told, the publisher reads its device at once and publishes: the listener has it.
    values
        .publish(FieldId(0), SampleTime::now(), &3.3_f64.to_le_bytes())
        .expect("publish");
    until("the listener has the value", || {
        reading.wait(Duration::from_millis(10)).expect("wait");
        reading
            .latest(FieldId(0))
            .is_some_and(|sample| sample.payload == 3.3_f64.to_le_bytes())
    });

    // A second listener, on the board: two. Each going is told.
    let panel = board.connect("panel").expect("connect");
    let near = panel.subscribe("device.values").expect("subscribe");
    until("both are counted", || listening(&mut values) == 2);
    drop(reading);
    drop(dashboard);
    until("the network listener went", || listening(&mut values) == 1);
    drop(near);
    drop(panel);
    until("nobody listens", || listening(&mut values) == 0);
}
