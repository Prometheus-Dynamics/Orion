//! Latency over a Unix link through the node, in-process: a provider session publishes, a client
//! session receives. Each sample waits for the previous one to arrive, so this measures one
//! value's trip, not throughput.
//!
//! Ignored by default (timing depends on the machine). Run it in release:
//! `cargo test -p orion-node --release --features link-gateway --lib unix_link_latency -- --ignored --nocapture`

use super::serial::{WAIT, start};
use super::unix::{socket_path, unix_link};
use super::unix_client::RawClient;
use super::*;
use orion::ProviderId;
use orion::control_plane::{
    Backfill, SampleChunkBuilder, SeriesDecl, SeriesKind, StatusQuery, StatusSubject, StreamRef,
    StreamSubscription,
};
use orion_link::message::{
    Message, ProviderState, Roles, StatusEntry as LinkStatus, StatusRequest, StreamDeclare,
    StreamPublish, StreamSubscribe,
};
use std::time::Instant;

const PROVIDER: &str = "provider.imu-board";
const SAMPLES: usize = 2_000;

fn report(what: &str, mut micros: Vec<u64>) {
    micros.sort_unstable();
    let at = |p: f64| micros[((micros.len() - 1) as f64 * p) as usize];
    println!(
        "{what}: n={} p50={} us p90={} us p99={} us max={} us",
        micros.len(),
        at(0.50),
        at(0.90),
        at(0.99),
        micros[micros.len() - 1]
    );
}

async fn provider_session(app: &NodeApp, path: &std::path::Path) -> RawClient {
    let publisher = RawClient::connect_as("imu-board", path, Roles::PROVIDER).await;
    publisher.send(Message::ProviderState(ProviderState {
        provider: device_provider(PROVIDER),
        resources: vec![device_resource("imu-board.imu-0", PROVIDER, "unix")],
    }));
    assert!(wait_until(WAIT, || has_provider(app, PROVIDER)).await);
    publisher
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "latency measurement; run in release with --ignored --nocapture"]
async fn unix_link_latency() {
    let app = test_app("unix-latency");
    let path = socket_path("latency");
    let gateway = start(&app, vec![unix_link(&path, "")]);
    let mut publisher = provider_session(&app, &path).await;

    // Stream: one sample per batch, publish to StreamData.
    publisher.send(Message::StreamDeclare(StreamDeclare {
        req: 1,
        id: "imu".into(),
        series: vec![SeriesDecl::new("imu.accel_x", SeriesKind::F32, 1_000)],
    }));
    assert!(matches!(publisher.next().await, Message::StreamDemand(_)));
    let mut client = RawClient::connect("console", &path).await;
    client.send(Message::StreamSubscribe(Box::new(StreamSubscribe {
        req: 2,
        subscription: StreamSubscription::all(StreamRef {
            owner: StatusSubject::Provider(ProviderId::new(PROVIDER)),
            id: "imu".into(),
        })
        .with_backfill(Backfill::None),
    })));
    assert!(matches!(client.next().await, Message::StreamCatalog(_)));
    let mut stream = Vec::with_capacity(SAMPLES);
    let t0 = 1_700_000_000_000_000u64;
    for i in 0..SAMPLES as u64 + 100 {
        let mut builder = SampleChunkBuilder::new(0, SeriesKind::F32);
        builder.push(t0 + i * 1_000, i as f64);
        let sent = Instant::now();
        publisher.send(Message::StreamPublish(StreamPublish {
            id: "imu".into(),
            chunks: vec![builder.take()],
        }));
        loop {
            match client.next().await {
                Message::StreamData(data) if !data.backfill => break,
                Message::StreamData(_) => {}
                other => panic!("unexpected at {i}: {other:?}"),
            }
        }
        if i >= 100 {
            stream.push(sent.elapsed().as_micros() as u64);
        }
    }
    report("stream publish -> StreamData", stream);

    // Status: one value per Status frame, to StatusChanges of a watch.
    publisher.send(Message::Status(vec![LinkStatus::new(
        "temperature_mc",
        orion::control_plane::TypedConfigValue::Int(-1),
    )]));
    let filter = StatusQuery::subject(StatusSubject::Provider(ProviderId::new(PROVIDER)));
    assert!(wait_until(WAIT, || !app.query_status(&filter).is_empty()).await);
    client.send(Message::StatusWatch(StatusRequest { req: 3, filter }));
    assert!(matches!(client.next().await, Message::StatusChanges(c) if c.bootstrap));
    let mut status = Vec::with_capacity(SAMPLES);
    for i in 0..SAMPLES as i64 + 100 {
        let sent = Instant::now();
        publisher.send(Message::Status(vec![LinkStatus::new(
            "temperature_mc",
            orion::control_plane::TypedConfigValue::Int(i),
        )]));
        loop {
            match client.next().await {
                Message::StatusChanges(c)
                    if c.updated
                        .iter()
                        .any(|e| e.value == orion::control_plane::TypedConfigValue::Int(i)) =>
                {
                    break;
                }
                Message::StatusChanges(_) => {}
                other => panic!("unexpected at {i}: {other:?}"),
            }
        }
        if i >= 100 {
            status.push(sent.elapsed().as_micros() as u64);
        }
    }
    report("status publish -> StatusChanges", status);
    gateway.shutdown().await;
}
