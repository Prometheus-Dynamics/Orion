//! Live streams (`docs/streams.md`) through real IPC servers: `orion_client::StreamPublisher`
//! declares and publishes, a `ControlPlaneEventStream` subscribes, and the publisher hears the
//! demand.

#![cfg(unix)]

use orion::client::{ControlPlaneEventStream, StreamPublisher};
use orion::control_plane::{
    Backfill, ClientEventKind, HistoryPolicy, SampleChunkBuilder, SeriesDecl, SeriesKind,
    StatusSubject, StreamDecl, StreamRef, StreamSubscription,
};
use orion_node::{NodeApp, NodeConfig, NodeId};
use std::{path::PathBuf, time::Duration};

fn temp_socket(label: &str) -> PathBuf {
    std::env::temp_dir().join(format!("orion-streams-{label}-{}.sock", std::process::id()))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_publisher_streams_to_a_local_subscriber_and_hears_the_demand() {
    let socket = temp_socket("unary");
    let stream_socket = temp_socket("stream");
    let app = NodeApp::try_new(
        NodeConfig::for_local_node(NodeId::new("node-a")).with_ipc_socket_path(socket.clone()),
    )
    .expect("node app builds");
    let (_, unary) = app
        .start_ipc_server_graceful(&socket)
        .await
        .expect("ipc server");
    let (_, streams) = app
        .start_ipc_stream_server_graceful(&stream_socket)
        .await
        .expect("ipc stream server");

    let stream = StreamRef {
        owner: StatusSubject::Node(NodeId::new("node-a")),
        id: "board.live".into(),
    };
    let mut publisher = StreamPublisher::connect_at(&stream_socket, "board-agent")
        .await
        .expect("publisher connects");
    assert_eq!(publisher.node_id(), &NodeId::new("node-a"));
    publisher
        .declare(StreamDecl {
            owner: stream.owner.clone(),
            id: stream.id.clone(),
            series: vec![
                SeriesDecl::new("cpu", SeriesKind::F32, 100_000)
                    .with_unit("%")
                    .with_history(HistoryPolicy::full(60_000)),
            ],
        })
        .await
        .expect("declares");
    for n in 0..20u64 {
        let mut chunk = SampleChunkBuilder::new(0, SeriesKind::F32);
        chunk.push(1_800_000_000_000_000 + n * 100_000, n as f64);
        publisher
            .publish(&stream, vec![chunk.take()])
            .await
            .expect("publishes");
    }

    let mut subscriber = ControlPlaneEventStream::connect_at(&stream_socket, "console-local")
        .await
        .expect("subscriber connects");
    subscriber
        .subscribe_stream(
            StreamSubscription::all(stream.clone()).with_backfill(Backfill::LastMs(60_000)),
        )
        .await
        .expect("subscribes");
    let mut backfilled = 0usize;
    tokio::time::timeout(Duration::from_secs(5), async {
        while backfilled == 0 {
            for event in subscriber.next_events().await.expect("events") {
                if let ClientEventKind::StreamData(data) = event.event {
                    assert!(data.backfill);
                    backfilled = data.chunks.iter().map(|c| c.count as usize).sum();
                }
            }
        }
    })
    .await
    .expect("the backfill arrives");
    assert_eq!(backfilled, 20);

    let demand = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let demand = publisher.next_demand().await.expect("demand");
            if demand.subscribers > 0 {
                return demand;
            }
        }
    })
    .await
    .expect("the publisher hears about the subscriber");
    assert_eq!(demand.series, vec![0]);

    drop((unary, streams));
    let _ = std::fs::remove_file(socket);
    let _ = std::fs::remove_file(stream_socket);
}
