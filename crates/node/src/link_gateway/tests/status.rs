//! Device status (link kind `STATUS`) lands in the node's volatile status lane.

use super::serial::{SimDevice, WAIT, open_pty, serial_link, start};
use super::*;
use orion::control_plane::{StatusQuery, StatusSubject, TypedConfigValue};
use orion_link::message::StatusEntry as LinkStatusEntry;

fn device_status(app: &NodeApp) -> Vec<orion::control_plane::StatusEntry> {
    app.query_status(&StatusQuery::subject(StatusSubject::Provider(
        orion::ProviderId::new("provider.status-board"),
    )))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn device_status_is_filed_under_its_provider() {
    let app = test_app("serial-status");
    let pty = open_pty();
    let gateway = start(&app, vec![serial_link(&pty, "")]);
    let device = SimDevice::start("status-board", pty);

    // Status before the first accepted snapshot has no provider to file it under.
    device
        .device
        .lock()
        .expect("device lock")
        .publish_status(&[LinkStatusEntry::new(
            "temperature_mc",
            TypedConfigValue::Int(1),
        )])
        .expect("status fits");
    assert!(
        wait_until(WAIT, || app
            .link_status()
            .first()
            .is_some_and(|status| status.status_rejects >= 1))
        .await,
        "early status should be refused: {:?}",
        app.link_status()
    );
    assert!(device_status(&app).is_empty());

    device.publish(
        &device_provider("provider.status-board"),
        &[device_resource(
            "status-board.imu-0",
            "provider.status-board",
            "rate=100hz",
        )],
    );
    assert!(wait_until(WAIT, || has_provider(&app, "provider.status-board")).await);
    device
        .device
        .lock()
        .expect("device lock")
        .publish_status(&[
            LinkStatusEntry::new("temperature_mc", TypedConfigValue::Int(41_250))
                .with_ttl_ms(10_000),
            LinkStatusEntry::new("mode", TypedConfigValue::String("streaming".into())),
        ])
        .expect("status fits");
    assert!(
        wait_until(WAIT, || device_status(&app).len() == 2).await,
        "device status did not reach the lane: {:?}",
        app.link_status()
    );
    let entries = device_status(&app);
    let temperature = entries
        .iter()
        .find(|entry| entry.key == "temperature_mc")
        .expect("temperature entry");
    assert_eq!(temperature.value, TypedConfigValue::Int(41_250));
    assert_eq!(temperature.ttl_ms, 10_000);
    assert!(app.link_status()[0].status_batches >= 1);
    let usage = app.observability_snapshot().resource_usage.status_lane;
    assert_eq!(usage.publishers, 1);

    gateway.shutdown().await;
}
