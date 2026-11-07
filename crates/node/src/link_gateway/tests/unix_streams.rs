//! Streams on Unix links (interface revision 2): a provider session declares and publishes a
//! stream, client sessions subscribe to it.

use super::serial::{WAIT, start};
use super::unix::{socket_path, unix_link};
use super::unix_client::RawClient;
use super::*;
use orion::ProviderId;
use orion::control_plane::{
    Backfill, HistoryPolicy, SampleChunk, SampleChunkBuilder, SeriesDecl, SeriesKind,
    StatusSubject, StreamRef, StreamSubscription,
};
use orion_link::message::{
    ErrorCode, Message, ProviderState, Roles, StreamData, StreamDeclare, StreamPublish,
    StreamSubscribe,
};

const PROVIDER: &str = "provider.imu-board";

pub(super) fn now_us() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock after 1970")
        .as_micros() as u64
}

/// `count` samples 1 ms apart from `t0_us`.
pub(super) fn chunk(t0_us: u64, count: u64) -> SampleChunk {
    let mut builder = SampleChunkBuilder::new(0, SeriesKind::F32);
    for i in 0..count {
        builder.push(t0_us + i * 1_000, i as f64);
    }
    builder.take()
}

fn stream() -> StreamRef {
    StreamRef {
        owner: StatusSubject::Provider(ProviderId::new(PROVIDER)),
        id: "imu".into(),
    }
}

fn subscribe(req: u32, backfill: Backfill) -> Message {
    Message::StreamSubscribe(Box::new(StreamSubscribe {
        req,
        subscription: StreamSubscription::all(stream()).with_backfill(backfill),
    }))
}

/// The next message, which must be stream data.
async fn next_samples(client: &mut RawClient) -> StreamData {
    match client.next().await {
        Message::StreamData(data) => data,
        other => panic!("expected stream data, got {other:?}"),
    }
}

/// Samples in the answer that ends with the first part without `more`.
async fn whole_answer(client: &mut RawClient) -> (Vec<StreamData>, usize) {
    let mut parts = Vec::new();
    loop {
        let part = next_samples(client).await;
        let done = !part.more;
        parts.push(part);
        if done {
            break;
        }
    }
    let samples = parts
        .iter()
        .flat_map(|part| &part.chunks)
        .map(|chunk| chunk.samples(SeriesKind::F32).expect("decodes").len())
        .sum();
    (parts, samples)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_provider_session_publishes_a_stream_that_client_sessions_follow() {
    let app = test_app("unix-streams");
    let path = socket_path("streams");
    let gateway = start(&app, vec![unix_link(&path, "")]);

    let mut publisher = RawClient::connect_as("imu-board", &path, Roles::PROVIDER).await;
    let series = vec![
        SeriesDecl::new("imu.accel_x", SeriesKind::F32, 1_000)
            .with_history(HistoryPolicy::full(60_000)),
    ];
    // Before its provider is known, the session's declaration is refused.
    publisher.send(Message::StreamDeclare(StreamDeclare {
        req: 1,
        id: "imu".into(),
        series: series.clone(),
    }));
    assert!(matches!(
        publisher.next().await,
        Message::RequestError(e) if e.req == 1 && e.code == ErrorCode::NOT_AUTHORIZED
    ));
    publisher.send(Message::ProviderState(ProviderState {
        provider: device_provider(PROVIDER),
        resources: vec![device_resource("imu-board.imu-0", PROVIDER, "unix")],
    }));
    assert!(wait_until(WAIT, || has_provider(&app, PROVIDER)).await);
    publisher.send(Message::StreamDeclare(StreamDeclare {
        req: 2,
        id: "imu".into(),
        series,
    }));
    assert!(matches!(
        publisher.next().await,
        Message::StreamDemand(d) if d.id == "imu" && d.subscribers == 0
    ));

    // Four batches of 500 samples: more history than one frame holds.
    let t0 = now_us() - 5_000_000;
    for batch in 0..4u64 {
        publisher.send(Message::StreamPublish(StreamPublish {
            id: "imu".into(),
            chunks: vec![chunk(t0 + batch * 500_000, 500)],
        }));
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut client = RawClient::connect("console", &path).await;
    client.send(subscribe(5, Backfill::LastMs(60_000)));
    assert!(matches!(
        client.next().await,
        Message::StreamCatalog(c) if c.req == 5 && c.live && c.series.len() == 1
    ));
    let (parts, samples) = whole_answer(&mut client).await;
    assert!(parts.len() > 1, "2000 samples do not fit one frame");
    assert!(parts.iter().all(|part| part.req == 5 && part.backfill));
    assert_eq!(samples, 2_000);
    let backfill_cursor = parts[0].cursor;
    assert!(matches!(
        publisher.next().await,
        Message::StreamDemand(d) if d.subscribers == 1 && d.series == [0]
    ));

    // A live batch follows the backfill.
    publisher.send(Message::StreamPublish(StreamPublish {
        id: "imu".into(),
        chunks: vec![chunk(t0 + 2_000_000, 10)],
    }));
    let live = next_samples(&mut client).await;
    assert!(!live.backfill && !live.more && live.req == 5);
    assert_eq!(live.cursor.seq, backfill_cursor.seq + 1);
    assert_eq!(live.prev_seq, backfill_cursor.seq);

    // One subscription per stream per session.
    client.send(subscribe(6, Backfill::None));
    assert!(matches!(
        client.next().await,
        Message::RequestError(e) if e.req == 6 && e.code == ErrorCode::LIMIT
    ));

    // Ended, the subscription sends nothing more and the publisher hears of it.
    client.send(Message::RequestEnd { req: 5 });
    assert!(matches!(
        publisher.next().await,
        Message::StreamDemand(d) if d.subscribers == 0
    ));
    publisher.send(Message::StreamPublish(StreamPublish {
        id: "imu".into(),
        chunks: vec![chunk(t0 + 2_100_000, 10)],
    }));
    assert!(client.silent_for(Duration::from_millis(400)).await);

    // A batch for a stream the session did not declare is dropped.
    publisher.send(Message::StreamPublish(StreamPublish {
        id: "other".into(),
        chunks: vec![chunk(t0, 1)],
    }));
    // The publisher's end leaves the history; a new subscriber sees the stream not live.
    drop(publisher);
    tokio::time::sleep(Duration::from_millis(200)).await;
    client.send(subscribe(7, Backfill::None));
    assert!(matches!(
        client.next().await,
        Message::StreamCatalog(c) if c.req == 7 && !c.live
    ));
    gateway.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_whole_batch_subscriber_and_a_thinned_one_each_get_every_batch() {
    let app = test_app("unix-streams-mixed");
    let path = socket_path("streams-mixed");
    let gateway = start(&app, vec![unix_link(&path, "")]);
    let mut publisher = RawClient::connect_as("imu-board", &path, Roles::PROVIDER).await;
    publisher.send(Message::ProviderState(ProviderState {
        provider: device_provider(PROVIDER),
        resources: vec![device_resource("imu-board.imu-0", PROVIDER, "unix")],
    }));
    assert!(wait_until(WAIT, || has_provider(&app, PROVIDER)).await);
    publisher.send(Message::StreamDeclare(StreamDeclare {
        req: 1,
        id: "imu".into(),
        series: vec![SeriesDecl::new("imu.accel_x", SeriesKind::F32, 1_000)],
    }));
    assert!(matches!(publisher.next().await, Message::StreamDemand(_)));

    // `a` sorts before `b`: the whole-batch subscriber is served first, the thinned one after it.
    let mut whole = RawClient::connect("console-a", &path).await;
    whole.send(subscribe(1, Backfill::None));
    let mut thinned = RawClient::connect("console-b", &path).await;
    let mut sub = StreamSubscription::all(stream()).with_backfill(Backfill::None);
    sub.min_interval_us = 5_000;
    thinned.send(Message::StreamSubscribe(Box::new(StreamSubscribe {
        req: 2,
        subscription: sub,
    })));
    for client in [&mut whole, &mut thinned] {
        assert!(matches!(client.next().await, Message::StreamCatalog(_)));
        // An empty backfill answer ends the (empty) history.
        assert!(next_samples(client).await.backfill);
    }

    let t0 = now_us();
    for batch in 0..3u64 {
        publisher.send(Message::StreamPublish(StreamPublish {
            id: "imu".into(),
            chunks: vec![chunk(t0 + batch * 10_000, 10)],
        }));
        let samples = |data: &StreamData| -> usize {
            data.chunks
                .iter()
                .map(|c| c.samples(SeriesKind::F32).expect("decodes").len())
                .sum()
        };
        let got = next_samples(&mut whole).await;
        assert_eq!(samples(&got), 10, "batch {batch}");
        // 10 samples 1 ms apart, at most one per 5 ms.
        let got = next_samples(&mut thinned).await;
        assert_eq!(samples(&got), 2, "batch {batch}");
    }
    gateway.shutdown().await;
}
