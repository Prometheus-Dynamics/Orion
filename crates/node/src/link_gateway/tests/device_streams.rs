//! Streams of serial and CAN devices (interface revision 2): a no_std device session declares a
//! stream and publishes batches; a client session on the node's Unix link subscribes to it, and
//! the device hears of it.

use super::can::{BusDevice, MemoryBus, can_config, provider_for};
use super::serial::{SimDevice, WAIT, open_pty, serial_link, start};
use super::unix::{socket_path, unix_link};
use super::unix_client::RawClient;
use super::*;
use crate::link_gateway::LinkConfig;
use crate::link_gateway::can_link;
use crate::link_gateway::events::LinkContext;
use orion::ProviderId;
use orion::control_plane::{
    Backfill, SeriesKind as Kind, StatusSubject, StreamRef, StreamSubscription,
};
use orion_link::device::DeviceEvent;
use orion_link::message::{Message, StreamSubscribe};
use orion_link::wire::{
    HistoryView, SampleWriter, Sampling, SeriesKind, SeriesView, StreamDemandView,
};
use orion_link::{CanLinkIds, Packet};
use std::io;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

const SERIES: [SeriesView<'static>; 1] = [SeriesView {
    name: "accel_x",
    unit: Some("m/s2"),
    kind: SeriesKind::F32,
    period_us: 1_000,
    history: HistoryView {
        full_ms: 60_000,
        reduced_ms: 0,
        reduced_period_ms: 0,
    },
    sampling: Sampling::WhileSubscribed,
}];

fn now_us() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock after 1970")
        .as_micros() as u64
}

fn subscribe(req: u32, provider: &str) -> Message {
    Message::StreamSubscribe(Box::new(StreamSubscribe {
        req,
        subscription: StreamSubscription::all(StreamRef {
            owner: StatusSubject::Provider(ProviderId::new(provider)),
            id: "imu".into(),
        })
        .with_backfill(Backfill::None),
    }))
}

/// Ten samples 1 ms apart from now, valued `0..10`.
fn ten_samples(buf: &mut [u8]) -> SampleWriter<'_> {
    let t0 = now_us();
    let mut writer = SampleWriter::new(buf);
    for i in 0..10u64 {
        assert!(writer.push_f32(t0 + i * 1_000, i as f32));
    }
    writer
}

/// The subscriber's catalog, then samples of the batch the device publishes through `publish`
/// (retried until the device's previous batch is out).
async fn follow(client: &mut RawClient, req: u32, mut publish: impl FnMut() -> bool) {
    assert!(matches!(
        client.next().await,
        Message::StreamCatalog(c) if c.req == req && c.live && c.series.len() == 1
            && c.series[0].unit.as_deref() == Some("m/s2")
    ));
    assert!(
        wait_until(WAIT, &mut publish).await,
        "the device's batch was not taken"
    );
    // The (empty) backfill answer comes first.
    loop {
        match client.next().await {
            Message::StreamData(data) if data.backfill => assert!(data.chunks.is_empty()),
            Message::StreamData(data) => {
                assert_eq!(data.req, req);
                let samples = data.chunks[0].samples(Kind::F32).expect("decodes");
                let values: Vec<f64> = samples.iter().map(|(_, v)| *v).collect();
                assert_eq!(values, (0..10).map(f64::from).collect::<Vec<_>>());
                break;
            }
            other => panic!("expected samples, got {other:?}"),
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_serial_device_streams_to_a_unix_subscriber() {
    let app = test_app("serial-streams");
    let pty = open_pty();
    let path = socket_path("serial-streams");
    let gateway = start(
        &app,
        vec![serial_link(&pty, "&allow=imu-board"), unix_link(&path, "")],
    );
    let device = SimDevice::start("imu-board", pty);
    device.publish(
        &device_provider("provider.imu-board"),
        &[device_resource(
            "imu-board.imu-0",
            "provider.imu-board",
            "serial",
        )],
    );
    device
        .device
        .lock()
        .expect("device lock")
        .declare_stream("imu", &SERIES)
        .expect("fits");
    let demand = |device: &SimDevice| device.device.lock().expect("device lock").stream_demand();
    assert!(
        wait_until(WAIT, || demand(&device).is_some()).await,
        "the declaration was not accepted: {:?}",
        device.events.lock().unwrap()
    );
    assert_eq!(demand(&device), Some(StreamDemandView::default()));

    let mut client = RawClient::connect("console", &path).await;
    client.send(subscribe(3, "provider.imu-board"));
    let mut buf = [0u8; 128];
    let publish = || {
        let samples = ten_samples(&mut buf);
        let mut device = device.device.lock().expect("device lock");
        device.stream_demand().is_some_and(|d| d.wants(0))
            && device.publish_stream(&[samples.chunk(0)]).is_ok()
    };
    follow(&mut client, 3, publish).await;
    assert_eq!(
        demand(&device),
        Some(StreamDemandView {
            subscribers: 1,
            series: 1,
        })
    );

    // The device goes: its stream is no longer live, and a new session declares it again.
    device.unplugged.store(true, Ordering::SeqCst);
    assert!(
        wait_until(WAIT, || !device.is_connected()).await,
        "the device kept its session"
    );
    device.unplugged.store(false, Ordering::SeqCst);
    assert!(
        wait_until(WAIT, || demand(&device).is_some_and(|d| d.subscribers == 1)).await,
        "the stream was not declared again"
    );
    assert!(device.saw(|e| *e == DeviceEvent::Disconnected));
    drop(client);
    assert!(
        wait_until(WAIT, || demand(&device).is_some_and(|d| d.subscribers == 0)).await,
        "the device did not hear the subscriber leave"
    );
    gateway.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_can_device_streams_to_a_unix_subscriber() {
    let app = test_app("can-streams");
    let path = socket_path("can-streams");
    let gateway = start(&app, vec![unix_link(&path, "")]);
    let link = LinkConfig::parse(&format!(
        "can:mem0?device_base=0x100&host_base=0x180&addresses=1-8&heartbeat_ms={HEARTBEAT_MS}"
    ))
    .expect("link should parse");
    let config = can_config(&link);
    let bus = Arc::new(MemoryBus::default());
    let gateway_port = Mutex::new(Some(bus.port()));
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let host = link.host_config(orion::NodeId::new(NODE), 1);
    let ctx = LinkContext::new(app.clone(), link.name.clone());
    let opener = move || {
        gateway_port
            .lock()
            .expect("port lock")
            .take()
            .ok_or_else(|| io::Error::other("memory port already taken"))
    };
    let can = tokio::spawn(can_link::run(
        ctx,
        config.clone(),
        host,
        opener,
        shutdown_rx,
    ));

    let ids = CanLinkIds::for_address(config.base_ids(), 3).expect("ids");
    let device = BusDevice::start("dev-3", ids, Packet::CLASSIC, bus.port());
    let (provider, resources) = provider_for(3);
    device.publish(&provider, &resources);
    device
        .device
        .lock()
        .expect("device lock")
        .declare_stream("imu", &SERIES)
        .expect("fits");
    assert!(
        wait_until(WAIT, || device
            .device
            .lock()
            .expect("device lock")
            .stream_accepted())
        .await,
        "the declaration was not accepted"
    );

    let mut client = RawClient::connect("console", &path).await;
    client.send(subscribe(4, "provider.dev-3"));
    let mut buf = [0u8; 128];
    let publish = || {
        let samples = ten_samples(&mut buf);
        let mut device = device.device.lock().expect("device lock");
        device.stream_demand().is_some_and(|d| d.wants(0))
            && device.publish_stream(&[samples.chunk(0)]).is_ok()
    };
    follow(&mut client, 4, publish).await;

    let _ = shutdown_tx.send(true);
    can.await.expect("CAN task");
    drop(client);
    gateway.shutdown().await;
}
