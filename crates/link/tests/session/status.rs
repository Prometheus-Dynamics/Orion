//! Device status (kind `STATUS`): fire-and-forget, newest batch wins, rate-limited.

use orion_link::host::HostEvent;
use orion_link::message::{StatusEntry, TypedConfigValue, kind};

use crate::sim::{StreamPair, host_config};

fn connected_pair(seed: u64) -> StreamPair {
    let mut pair = StreamPair::new("imu-board", host_config(), seed);
    assert!(pair.run_until(2_000, StreamPair::connected), "handshake");
    pair
}

fn status_batches(pair: &StreamPair) -> Vec<Vec<StatusEntry>> {
    pair.host_events
        .iter()
        .filter_map(|(_, event)| match event {
            HostEvent::Status { entries, .. } => Some(entries.clone()),
            _ => None,
        })
        .collect()
}

fn temperature(value: i64) -> Vec<StatusEntry> {
    vec![StatusEntry::new("temperature", TypedConfigValue::Int(value)).with_ttl_ms(2_000)]
}

fn acks(pair: &StreamPair) -> usize {
    pair.down
        .sent_kinds
        .iter()
        .filter(|&&k| k == kind::ACK)
        .count()
}

#[test]
fn status_is_fire_and_forget_and_rate_limited() {
    let mut pair = connected_pair(40);
    let acks_before = acks(&pair);
    // Five publishes within one rate-limit window: the first goes out at once, the rest
    // collapse into the newest batch, which is sent once the window has passed.
    for value in 0..5 {
        pair.device.publish_status(&temperature(value)).unwrap();
        pair.run(10);
    }
    assert!(pair.run_until(1_000, |p| !p.device.status_pending()));
    pair.run(200);
    assert_eq!(status_batches(&pair), vec![temperature(0), temperature(4)]);
    let stats = pair.device.stats();
    assert_eq!(stats.status_sent, 2);
    assert_eq!(stats.status_replaced, 3);
    assert_eq!(acks(&pair), acks_before, "status is never acknowledged");
    assert_eq!(pair.host.stats().status_received, 2);
}

#[test]
fn status_published_while_disconnected_is_sent_after_connect() {
    let mut pair = StreamPair::new("imu-board", host_config(), 41);
    pair.device.publish_status(&temperature(7)).unwrap();
    assert!(pair.run_until(2_000, |p| !p.device.status_pending()));
    pair.run(50);
    assert_eq!(status_batches(&pair), vec![temperature(7)]);
}

#[test]
fn lost_status_is_not_retransmitted() {
    let mut pair = connected_pair(42);
    pair.up.drop_kinds = vec![kind::STATUS];
    pair.device.publish_status(&temperature(1)).unwrap();
    pair.run(2_000);
    assert!(status_batches(&pair).is_empty());
    assert_eq!(pair.device.stats().status_sent, 1);
}

#[test]
fn oversized_status_is_refused_at_publish() {
    let mut pair = connected_pair(43);
    let big = vec![StatusEntry::new(
        "blob",
        TypedConfigValue::Bytes(vec![0; 1_024]),
    )];
    assert!(pair.device.publish_status(&big).is_err());
    assert!(!pair.device.status_pending());
}
