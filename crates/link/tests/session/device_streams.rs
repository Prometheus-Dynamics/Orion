//! A device's stream (feature `streams`) end to end: the no_std session's declaration, demand and
//! batches against the real `HostSession` over a lossy stream and against `HostBus` on CAN. The
//! host must see exactly the records the views stand for.

use orion_control_plane as cp;
use orion_link::device::{DeviceConfig, DeviceEvent, StreamDevice, StreamError};
use orion_link::host::{HostConfig, HostEvent};
use orion_link::message::ErrorCode;
use orion_link::wire::{
    ChunkView, HistoryView, ProviderView, ResourceView, SampleWriter, Sampling, SeriesKind,
    SeriesView, StreamDemandView,
};
use orion_link::{Packet, Stream};

use crate::sim::{CanBus, Faults, StreamPair, host_config};

const HISTORY: HistoryView = HistoryView {
    full_ms: 2_000,
    reduced_ms: 60_000,
    reduced_period_ms: 100,
};

const SERIES: [SeriesView<'static>; 2] = [
    SeriesView {
        name: "accel_x",
        unit: Some("m/s2"),
        kind: SeriesKind::F32,
        period_us: 1_000,
        history: HISTORY,
        sampling: Sampling::Always,
    },
    SeriesView {
        name: "steps",
        unit: None,
        kind: SeriesKind::I64,
        period_us: 10_000,
        history: HISTORY,
        sampling: Sampling::WhileSubscribed,
    },
];

fn series_records() -> Vec<cp::SeriesDecl> {
    let history = cp::HistoryPolicy {
        full_ms: 2_000,
        reduced_ms: 60_000,
        reduced_period_ms: 100,
    };
    vec![
        cp::SeriesDecl::new("accel_x", cp::SeriesKind::F32, 1_000)
            .with_unit("m/s2")
            .with_history(history),
        cp::SeriesDecl::new("steps", cp::SeriesKind::I64, 10_000)
            .with_history(history)
            .with_sampling(cp::Sampling::WhileSubscribed),
    ]
}

fn streaming() -> HostConfig {
    let mut config = host_config();
    config.serve_streams = true;
    config
}

fn publish_snapshot(device: &mut StreamDevice<512, 512, String>) {
    device
        .publish_provider_state(
            &ProviderView::new("provider.imu-board", "node-a"),
            &[ResourceView::new(
                "imu-board.imu-0",
                "imu.sample_source",
                "provider.imu-board",
            )],
        )
        .unwrap();
}

/// The declarations the host reported, with their request ids.
fn declarations(pair: &StreamPair) -> Vec<(u32, String, Vec<cp::SeriesDecl>)> {
    pair.host_events
        .iter()
        .filter_map(|(_, event)| match event {
            HostEvent::StreamDeclare {
                req, id, series, ..
            } => Some((*req, id.clone(), series.clone())),
            _ => None,
        })
        .collect()
}

fn batches(events: impl Iterator<Item = HostEvent>) -> Vec<(String, Vec<cp::SampleChunk>)> {
    events
        .filter_map(|event| match event {
            HostEvent::StreamPublish { id, chunks, .. } => Some((id.to_string(), chunks)),
            _ => None,
        })
        .collect()
}

#[test]
fn a_device_declares_its_stream_gets_demand_and_publishes() {
    for seed in 0..4u64 {
        let mut pair = StreamPair::new("imu-board", streaming(), 700 + seed);
        pair.up.faults = Faults::lossy(5);
        pair.down.faults = Faults::lossy(5);
        publish_snapshot(&mut pair.device);
        pair.device.declare_stream("imu", &SERIES).unwrap();
        assert_eq!(
            pair.device.publish_stream(&[]),
            Err(StreamError::NotAccepted)
        );
        // Repeated until answered: the host answers the first it sees.
        assert!(
            pair.run_until(10_000, |p| !declarations(p).is_empty()),
            "seed {seed}: no declaration"
        );
        let (_, id, series) = declarations(&pair).remove(0);
        assert_eq!((id.as_str(), series), ("imu", series_records()));
        assert!(
            pair.connected() && !pair.device.state_pending(),
            "sent after the snapshot's ack"
        );
        pair.host.send_stream_demand("imu", 2, &[0, 1]).unwrap();
        assert!(
            pair.run_until(10_000, |p| p.device.stream_accepted()),
            "seed {seed}: demand lost"
        );
        assert_eq!(
            pair.device.stream_demand(),
            Some(StreamDemandView {
                subscribers: 2,
                series: 0b11,
            })
        );
        assert!(pair.device.stream_demand().unwrap().wants(1));
        assert_eq!(pair.count_device(|e| *e == DeviceEvent::StreamDemand), 1);
        let declared = declarations(&pair).len();
        pair.run(3_000);
        assert_eq!(
            declarations(&pair).len(),
            declared,
            "repeated after the demand"
        );

        // Batches: one waits at a time.
        pair.up.faults = Faults::default();
        let mut accel = [0u8; 64];
        let mut steps = [0u8; 64];
        let mut sent = Vec::new();
        for batch in 0..5u64 {
            let mut a = SampleWriter::new(&mut accel);
            let mut s = SampleWriter::new(&mut steps);
            for i in 0..4u64 {
                let t = 1_000_000 + batch * 4_000 + i * 1_000;
                assert!(a.push_f32(t, (batch * 4 + i) as f32 * 0.5));
                assert!(s.push_i64(t, -(i as i64)));
            }
            pair.device
                .publish_stream(&[a.chunk(0), s.chunk(1)])
                .unwrap();
            assert_eq!(
                pair.device.publish_stream(&[a.chunk(0)]),
                Err(StreamError::Pending)
            );
            let mut accel_ref = cp::SampleChunkBuilder::new(0, cp::SeriesKind::F32);
            let mut steps_ref = cp::SampleChunkBuilder::new(1, cp::SeriesKind::I64);
            for i in 0..4u64 {
                let t = 1_000_000 + batch * 4_000 + i * 1_000;
                accel_ref.push(t, ((batch * 4 + i) as f32 * 0.5).into());
                steps_ref.push(t, -(i as f64));
            }
            sent.push(("imu".to_owned(), vec![accel_ref.take(), steps_ref.take()]));
            assert!(pair.run_until(1_000, |p| !p.device.stream_pending()));
        }
        pair.run(50);
        let got = batches(pair.host_events.drain(..).map(|(_, e)| e));
        assert_eq!(got, sent, "seed {seed}");
    }
}

#[test]
fn a_refused_stream_is_declared_again_in_the_next_session() {
    let mut pair = StreamPair::new("imu-board", streaming(), 720);
    publish_snapshot(&mut pair.device);
    pair.device.declare_stream("imu", &SERIES).unwrap();
    assert!(pair.run_until(5_000, |p| !declarations(p).is_empty()));
    let (req, ..) = declarations(&pair).remove(0);
    pair.host
        .send_request_error(req, ErrorCode::NOT_AUTHORIZED, "no")
        .unwrap();
    assert!(pair.run_until(2_000, |p| {
        p.count_device(|e| matches!(e, DeviceEvent::StreamRefused { .. })) > 0
    }));
    assert_eq!(
        pair.count_device(|e| *e == DeviceEvent::StreamRefused { code: 2 }),
        1
    );
    assert_eq!(
        pair.device.publish_stream(&[]),
        Err(StreamError::NotAccepted)
    );
    let declared = declarations(&pair).len();
    pair.run(3_000);
    assert_eq!(
        declarations(&pair).len(),
        declared,
        "repeated after the refusal"
    );

    // A new session (the host lost the device): the declaration goes again.
    pair.device_frozen = true;
    pair.run(5_000);
    pair.device_frozen = false;
    assert!(pair.run_until(10_000, |p| declarations(p).len() > declared));
}

#[test]
fn a_host_without_streams_refuses_the_declaration() {
    let mut pair = StreamPair::new("imu-board", host_config(), 730);
    publish_snapshot(&mut pair.device);
    pair.device.declare_stream("imu", &SERIES).unwrap();
    assert!(pair.run_until(5_000, |p| {
        p.count_device(|e| matches!(e, DeviceEvent::StreamRefused { .. })) > 0
    }));
    assert_eq!(
        pair.count_device(|e| *e == DeviceEvent::StreamRefused { code: 1 }),
        1
    );
    assert!(declarations(&pair).is_empty());
}

#[test]
fn a_device_of_an_earlier_revision_never_declares() {
    let mut config = DeviceConfig::provider("imu-board".to_owned());
    config.revision = 1;
    let mut pair = StreamPair::with_device(StreamDevice::new(config, Stream), streaming(), 740);
    publish_snapshot(&mut pair.device);
    pair.device.declare_stream("imu", &SERIES).unwrap();
    assert!(pair.run_until(5_000, StreamPair::connected));
    pair.run(1_000);
    assert_eq!(
        pair.count_device(|e| *e == DeviceEvent::StreamRefused { code: 1 }),
        1
    );
    assert!(declarations(&pair).is_empty());
}

#[test]
fn declarations_are_checked() {
    let mut pair = StreamPair::new("imu-board", streaming(), 750);
    assert_eq!(
        pair.device.declare_stream("imu", &[]),
        Err(StreamError::Series)
    );
    assert_eq!(
        pair.device.publish_stream(&[]),
        Err(StreamError::NotDeclared)
    );
    let many = [SERIES[0]; 65];
    assert_eq!(
        pair.device.declare_stream("imu", &many),
        Err(StreamError::Series)
    );
    let long = "x".repeat(600);
    assert!(matches!(
        pair.device.declare_stream(&long, &SERIES),
        Err(StreamError::Publish(_))
    ));
}

#[test]
fn sample_writer_stops_at_the_end_of_its_buffer() {
    let mut buf = [0u8; 10];
    let mut w = SampleWriter::new(&mut buf);
    assert!(w.push_f32(10, 1.0)); // delta 0 + 4 bytes
    assert!(!w.push_f64(11, 2.0)); // 1 + 8 bytes do not fit
    assert!(w.push_u32(5, 7)); // clamped to t = 10: delta 0 + 4 bytes
    assert_eq!((w.count(), w.len()), (2, 10));
    let chunk: ChunkView<'_> = w.chunk(3);
    assert_eq!((chunk.series, chunk.t0_us, chunk.count), (3, 10, 2));
}

#[test]
fn a_can_device_streams_through_the_bus() {
    for transport in [Packet::CLASSIC, Packet::FD] {
        let mut bus = CanBus::new(streaming(), transport, &[5], 760);
        {
            let node = bus.node(5);
            node.device
                .publish_provider_state(
                    &ProviderView::new("provider.dev-5", "node-a"),
                    &[ResourceView::new(
                        "dev-5.imu",
                        "imu.sample_source",
                        "provider.dev-5",
                    )],
                )
                .unwrap();
            node.device.declare_stream("imu", &SERIES).unwrap();
        }
        assert!(bus.run_until(10_000, |b| {
            b.host_events
                .iter()
                .any(|(_, _, e)| matches!(e, HostEvent::StreamDeclare { .. }))
        }));
        bus.host
            .session_mut(5)
            .expect("a session")
            .send_stream_demand("imu", 1, &[0])
            .unwrap();
        assert!(bus.run_until(2_000, |b| b.nodes[0].device.stream_accepted()));
        let mut buf = [0u8; 64];
        let mut w = SampleWriter::new(&mut buf);
        for i in 0..8u64 {
            assert!(w.push_f32(500 + i * 1_000, i as f32));
        }
        bus.node(5).device.publish_stream(&[w.chunk(0)]).unwrap();
        bus.run(200);
        let got = batches(bus.host_events.drain(..).map(|(_, _, e)| e));
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].1[0].count, 8);
        assert_eq!(
            got[0].1[0].samples(cp::SeriesKind::F32).unwrap()[7],
            (7_500, 7.0)
        );
    }
}
