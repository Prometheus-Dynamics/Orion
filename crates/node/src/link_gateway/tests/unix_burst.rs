//! Bursts on a Unix link with a sub-second heartbeat timeout (300 ms here): past the frame budget,
//! reads wait for their turn and the session stays up.

use super::serial::{WAIT, start};
use super::unix::{socket_path, unix_link};
use super::unix_client::RawClient;
use super::*;
use orion::ProviderId;
use orion::control_plane::{StatusQuery, StatusSubject, TypedConfigValue};
use orion_link::message::{
    Message, ProviderState, Roles, StatusEntry as LinkStatus, StatusRequest,
};
use std::time::Instant;

const PROVIDER: &str = "provider.imu-board";

/// A provider session sends `count` status values back to back; a client watching them must see
/// the last one, with no session lost. Returns how long that took.
async fn burst(name: &str, query: &str, count: i64) -> Duration {
    let app = test_app(name);
    let path = socket_path(name);
    let gateway = start(&app, vec![unix_link(&path, query)]);
    let publisher = RawClient::connect_as("imu-board", &path, Roles::PROVIDER).await;
    publisher.send(Message::ProviderState(ProviderState {
        provider: device_provider(PROVIDER),
        resources: vec![device_resource("imu-board.imu-0", PROVIDER, "unix")],
    }));
    assert!(wait_until(WAIT, || has_provider(&app, PROVIDER)).await);
    let value = |v: i64| Message::Status(vec![LinkStatus::new("count", TypedConfigValue::Int(v))]);
    publisher.send(value(-1));
    let filter = StatusQuery::subject(StatusSubject::Provider(ProviderId::new(PROVIDER)));
    assert!(wait_until(WAIT, || !app.query_status(&filter).is_empty()).await);
    let mut client = RawClient::connect("console", &path).await;
    client.send(Message::StatusWatch(StatusRequest { req: 1, filter }));
    assert!(matches!(client.next().await, Message::StatusChanges(c) if c.bootstrap));

    let started = Instant::now();
    for v in 0..count {
        publisher.send(value(v));
    }
    let last = TypedConfigValue::Int(count - 1);
    loop {
        match client.next().await {
            Message::StatusChanges(c) if c.updated.iter().any(|e| e.value == last) => break,
            Message::StatusChanges(_) => {}
            other => panic!("unexpected {other:?}"),
        }
    }
    let took = started.elapsed();
    let status = &app.link_status()[0];
    assert_eq!(status.device_timeouts, 0, "{name}: a session timed out");
    assert!(has_provider(&app, PROVIDER));
    gateway.shutdown().await;
    took
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_burst_past_the_old_cap_goes_through_at_once() {
    // 2,000 frames: eight times the old 256 a second, inside the default budget's burst.
    let took = burst("burst-default", "", 2_000).await;
    assert!(took < Duration::from_secs(1), "took {took:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_burst_past_the_budget_waits_without_losing_the_session() {
    // 200 frames a second, 600 sent at once: about two seconds of backpressure, more than six
    // heartbeat timeouts.
    let took = burst("burst-held", "&max_frames_per_s=200", 600).await;
    assert!(took >= Duration::from_millis(1_500), "throttled: {took:?}");
}
