//! Live streams (`docs/streams.md`) end to end: a local publisher on the node, a remote
//! operator subscribed over a push session on a real `orion+tcp` listener.

use super::remote_operator::{OperatorNode, enrolled_operator, operator_node};
use super::*;
use orion::control_plane::{
    Backfill, ClientEventKind, ClientEventPoll, ClientHello, ClientRole, HistoryPolicy,
    OperatorPolicy, SampleChunkBuilder, SeriesDecl, SeriesKind, StatusSubject, StreamCatalog,
    StreamCursor, StreamData, StreamDecl, StreamPublish, StreamRef, StreamResetReason,
    StreamSubscription,
};
use orion_client::remote::{OperatorIdentity, RemoteSession};
use orion_core::ClientName;
use std::time::Duration;

const PUBLISHER: &str = "board-agent";
const T0: u64 = 1_800_000_000_000_000;

fn stream_ref() -> StreamRef {
    StreamRef {
        owner: StatusSubject::Node(NodeId::new("node-a")),
        id: "board.live".into(),
    }
}

/// Three series: a 10 Hz gauge, a 100 Hz IMU channel (30 s, then 5 min at 10 Hz) and a colour.
fn declaration() -> StreamDecl {
    StreamDecl {
        owner: stream_ref().owner,
        id: stream_ref().id,
        series: vec![
            SeriesDecl::new("cpu-thermal.temp", SeriesKind::F32, 100_000)
                .with_unit("°C")
                .with_history(HistoryPolicy::full(300_000)),
            SeriesDecl::new("imu.accel_x", SeriesKind::F32, 10_000)
                .with_unit("m/s²")
                .with_history(HistoryPolicy::full(30_000).with_reduced(300_000, 100)),
            SeriesDecl::new("led.0", SeriesKind::U32, 0).with_history(HistoryPolicy::full(60_000)),
        ],
    }
}

fn local(app: &NodeApp, source: &str, message: ControlMessage) -> ControlMessage {
    app.apply_local_control_message(&LocalAddress::new(source), message)
        .expect("local control message")
}

fn connect_publisher(app: &NodeApp) {
    local(
        app,
        PUBLISHER,
        ControlMessage::ClientHello(ClientHello {
            client_name: ClientName::new(PUBLISHER),
            role: ClientRole::Executor,
        }),
    );
    assert_eq!(
        local(app, PUBLISHER, ControlMessage::DeclareStream(declaration())),
        ControlMessage::Accepted
    );
}

/// Batch `n` (50 ms each): five gauge samples' worth of time, the IMU at 100 Hz.
fn publish_batch(app: &NodeApp, n: u64) {
    let start = T0 + n * 50_000;
    let mut temp = SampleChunkBuilder::new(0, SeriesKind::F32);
    if n.is_multiple_of(2) {
        temp.push(start, 50.0 + n as f64 * 0.01);
    }
    let mut imu = SampleChunkBuilder::new(1, SeriesKind::F32);
    for i in 0..5 {
        imu.push(start + i * 10_000, (n * 5 + i) as f64);
    }
    let chunks = [temp, imu]
        .into_iter()
        .filter(|b| !b.is_empty())
        .map(|mut b| b.take())
        .collect();
    assert_eq!(
        local(
            app,
            PUBLISHER,
            ControlMessage::PublishSamples(StreamPublish {
                stream: stream_ref(),
                chunks,
            })
        ),
        ControlMessage::Accepted
    );
}

async fn next_stream_event(session: &mut RemoteSession) -> ClientEventKind {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let event = session.next_event().await.expect("the session stays open");
            if matches!(
                event.event,
                ClientEventKind::StreamCatalog(_)
                    | ClientEventKind::StreamData(_)
                    | ClientEventKind::StreamReset(_)
            ) {
                return event.event;
            }
        }
    })
    .await
    .expect("a stream event arrives in time")
}

async fn catalog(session: &mut RemoteSession) -> StreamCatalog {
    match next_stream_event(session).await {
        ClientEventKind::StreamCatalog(catalog) => catalog,
        other => panic!("expected a catalog, got {other:?}"),
    }
}

async fn data(session: &mut RemoteSession) -> StreamData {
    match next_stream_event(session).await {
        ClientEventKind::StreamData(data) => data,
        other => panic!("expected stream data, got {other:?}"),
    }
}

fn samples(data: &StreamData, series: u16) -> Vec<(u64, f64)> {
    let kind = declaration().series[usize::from(series)].kind;
    data.chunks
        .iter()
        .filter(|c| c.series == series)
        .flat_map(|c| c.samples(kind).expect("chunk decodes"))
        .collect()
}

async fn subscribed_node() -> (OperatorNode, RemoteSession) {
    let node = operator_node("node-a", None).await;
    let operator = enrolled_operator(
        &node,
        OperatorIdentity::generate("atlas").expect("identity"),
        OperatorPolicy::default(),
    )
    .await;
    let session = operator.open_session("atlas").await.expect("session opens");
    (node, session)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_subscriber_gets_the_history_in_one_backfill_then_every_batch() {
    let (node, mut session) = subscribed_node().await;
    connect_publisher(&node.app);
    for n in 0..100 {
        publish_batch(&node.app, n); // 5 s of samples before anyone looks
    }

    session
        .subscribe_stream(
            StreamSubscription::all(stream_ref()).with_backfill(Backfill::LastMs(60_000)),
        )
        .await
        .expect("an operator with read access may subscribe");
    let catalog = catalog(&mut session).await;
    assert_eq!(catalog.series, declaration().series);
    assert!(catalog.live);
    let backfill = data(&mut session).await;
    assert!(backfill.backfill);
    assert_eq!(backfill.cursor.seq, 100);
    assert_eq!(
        samples(&backfill, 1).len(),
        500,
        "every IMU sample of the 5 s"
    );
    assert_eq!(samples(&backfill, 0).len(), 50);
    assert!(samples(&backfill, 2).is_empty());

    // Live from here: each batch as it is published, numbered on from the backfill.
    publish_batch(&node.app, 100);
    let live = data(&mut session).await;
    assert!(!live.backfill);
    assert_eq!((live.prev_seq, live.cursor.seq), (100, 101));
    assert_eq!(samples(&live, 1).len(), 5);
    assert_eq!(live.cursor.epoch, catalog.epoch);

    let usage = node.app.observability_snapshot().resource_usage.streams;
    assert_eq!((usage.streams, usage.series, usage.subscribers), (1, 3, 1));
    assert!(usage.budget_bytes > 0 && usage.budget_bytes <= usage.max_budget_bytes);
    assert_eq!(usage.batches_total, 101);
    node.server.shutdown().await.expect("listener stops");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_resumed_subscription_gets_only_what_it_missed() {
    let (node, mut session) = subscribed_node().await;
    connect_publisher(&node.app);
    for n in 0..40 {
        publish_batch(&node.app, n);
    }
    session
        .subscribe_stream(
            StreamSubscription::all(stream_ref())
                .with_series(["imu.*"])
                .with_backfill(Backfill::LastMs(60_000)),
        )
        .await
        .expect("subscribes");
    let epoch = catalog(&mut session).await.epoch;
    let backfill = data(&mut session).await;
    assert_eq!(samples(&backfill, 1).len(), 200);
    assert!(
        samples(&backfill, 0).is_empty(),
        "only the series asked for"
    );

    // Ten more batches while "disconnected", then resume from batch 40.
    session
        .unsubscribe_stream(stream_ref())
        .await
        .expect("unsubscribes");
    for n in 40..50 {
        publish_batch(&node.app, n);
    }
    session
        .subscribe_stream(
            StreamSubscription::all(stream_ref())
                .with_series(["imu.*"])
                .with_backfill(Backfill::Since(StreamCursor { epoch, seq: 40 })),
        )
        .await
        .expect("resubscribes");
    assert_eq!(catalog(&mut session).await.epoch, epoch);
    let missed = data(&mut session).await;
    let got = samples(&missed, 1);
    assert_eq!(got.len(), 50, "just the ten missed batches");
    assert_eq!(got[0].1, 200.0, "starting right after batch 40");

    // A cursor from another epoch gets a reset and the whole history.
    session
        .subscribe_stream(
            StreamSubscription::all(stream_ref()).with_backfill(Backfill::Since(StreamCursor {
                epoch: epoch + 1,
                seq: 3,
            })),
        )
        .await
        .expect("resubscribes");
    match next_stream_event(&mut session).await {
        ClientEventKind::StreamReset(reset) => {
            assert_eq!(reset.reason, StreamResetReason::NewEpoch)
        }
        other => panic!("expected a reset, got {other:?}"),
    }
    catalog(&mut session).await;
    assert_eq!(samples(&data(&mut session).await, 1).len(), 250);
    node.server.shutdown().await.expect("listener stops");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn subscriptions_wait_for_their_stream_and_the_publisher_hears_the_demand() {
    let (node, mut session) = subscribed_node().await;
    // Atlas subscribes before board-agent has declared anything.
    session
        .subscribe_stream(StreamSubscription::all(stream_ref()).with_series(["led.*"]))
        .await
        .expect("a subscription may wait for its stream");
    assert_eq!(
        node.app
            .observability_snapshot()
            .resource_usage
            .streams
            .subscribers,
        1
    );
    connect_publisher(&node.app);
    assert!(catalog(&mut session).await.live);
    assert!(data(&mut session).await.backfill);

    // The publisher learns which series someone watches.
    let events = match local(
        &node.app,
        PUBLISHER,
        ControlMessage::PollClientEvents(ClientEventPoll {
            after_sequence: 0,
            max_events: 64,
        }),
    ) {
        ControlMessage::ClientEvents(events) => events,
        other => panic!("expected events, got {other:?}"),
    };
    let demand = events
        .iter()
        .rev()
        .find_map(|e| match &e.event {
            ClientEventKind::StreamDemand(d) => Some(d.clone()),
            _ => None,
        })
        .expect("the publisher gets the demand");
    assert_eq!((demand.subscribers, demand.series), (1, vec![2]));

    // The publisher goes away: the stream stays, not live, its history still served.
    drop(session);
    node.server.shutdown().await.expect("listener stops");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_new_declaration_starts_a_new_epoch_and_only_the_publisher_may_publish() {
    let (node, mut session) = subscribed_node().await;
    connect_publisher(&node.app);
    publish_batch(&node.app, 0);
    session
        .subscribe_stream(
            StreamSubscription::all(stream_ref()).with_backfill(Backfill::LastMs(60_000)),
        )
        .await
        .expect("subscribes");
    let first = catalog(&mut session).await.epoch;
    data(&mut session).await;

    // Someone else may neither publish nor take the stream over.
    local(
        &node.app,
        "intruder",
        ControlMessage::ClientHello(ClientHello {
            client_name: ClientName::new("intruder"),
            role: ClientRole::Executor,
        }),
    );
    assert!(
        node.app
            .apply_local_control_message(
                &LocalAddress::new("intruder"),
                ControlMessage::DeclareStream(declaration())
            )
            .is_err()
    );
    let mut batch = SampleChunkBuilder::new(0, SeriesKind::F32);
    batch.push(T0, 1.0);
    assert!(
        node.app
            .apply_local_control_message(
                &LocalAddress::new("intruder"),
                ControlMessage::PublishSamples(StreamPublish {
                    stream: stream_ref(),
                    chunks: vec![batch.take()],
                }),
            )
            .is_err()
    );

    // The publisher declares other series: subscribers are reset onto the new epoch.
    let mut changed = declaration();
    changed.series.pop();
    assert_eq!(
        local(
            &node.app,
            PUBLISHER,
            ControlMessage::DeclareStream(changed.clone())
        ),
        ControlMessage::Accepted
    );
    match next_stream_event(&mut session).await {
        ClientEventKind::StreamReset(reset) => {
            assert_eq!(reset.reason, StreamResetReason::NewEpoch)
        }
        other => panic!("expected a reset, got {other:?}"),
    }
    let second = catalog(&mut session).await;
    assert_ne!(second.epoch, first);
    assert_eq!(second.series, changed.series);
    node.server.shutdown().await.expect("listener stops");
}
