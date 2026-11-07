//! `ORION_NODE_OPERATOR_OPEN_READ` and `ORION_NODE_OPERATOR_AUTO_ENROLL`
//! (`docs/remote-operator.md`, "Open read and auto-enrollment") over a real `orion+tcp`
//! listener: what operators the node has not enrolled may read, and networks whose operators
//! are enrolled on their hello.

use super::remote_operator::OperatorNode;
use super::*;
use crate::{OperatorAutoEnroll, OperatorOpenRead};
use orion::control_plane::{
    ActionRequest, ActionTarget, StateWatch, StatusQuery, StatusSubject, StreamRef,
    StreamSubscription,
};
use orion_client::remote::{NodeTrust, OperatorIdentity, RemoteError, RemoteOperator};

async fn node_with(open: OperatorOpenRead, auto_enroll: Vec<OperatorAutoEnroll>) -> OperatorNode {
    node_configured(open, auto_enroll, |config| config).await
}

async fn node_configured(
    open: OperatorOpenRead,
    auto_enroll: Vec<OperatorAutoEnroll>,
    adjust: impl FnOnce(crate::NodeConfig) -> crate::NodeConfig,
) -> OperatorNode {
    let mut config = test_node_config_with(
        "node-a",
        "node-a-operator",
        None,
        Vec::new(),
        crate::PeerAuthenticationMode::Required,
    );
    config.runtime_tuning.actions = config
        .runtime_tuning
        .actions
        .clone()
        .with_operator_open_read(open)
        .with_operator_auto_enroll(auto_enroll);
    let app = NodeApp::builder()
        .config(adjust(config))
        .try_build()
        .expect("node app should build");
    let (addr, server) = app
        .start_peer_tcp_server("127.0.0.1:0".parse().expect("address should parse"))
        .await
        .expect("peer TCP listener should start");
    OperatorNode { app, addr, server }
}

async fn stranger(node: &OperatorNode, name: &str) -> RemoteOperator {
    RemoteOperator::connect(
        &node.url(),
        OperatorIdentity::generate(name).expect("identity"),
        NodeTrust::FirstUse,
    )
    .await
    .expect("connects")
}

fn board_live() -> StreamRef {
    StreamRef {
        owner: StatusSubject::Node(NodeId::new("node-a")),
        id: "board.live".into(),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn open_read_lets_unenrolled_operators_watch_what_is_listed_and_nothing_else() {
    let node = node_with(
        OperatorOpenRead {
            status: true,
            streams: true,
            state: false,
        },
        Vec::new(),
    )
    .await;
    let operator = stranger(&node, "operator-console").await;
    assert!(!operator.is_enrolled());
    operator
        .status(StatusQuery::default())
        .await
        .expect("the status lane is open");

    let session = operator
        .open_session("operator-console")
        .await
        .expect("a session opens");
    session
        .watch_status(StatusQuery::all())
        .await
        .expect("status watches are open");
    session
        .subscribe_stream(StreamSubscription::all(board_live()))
        .await
        .expect("streams are open");
    let refused = session
        .watch_state(StateWatch::desired(orion::Revision::ZERO))
        .await
        .expect_err("state is not listed");
    assert!(matches!(refused, RemoteError::Rejected(_)), "{refused}");
    let refused = operator
        .run_action(ActionRequest::new(
            "r-1",
            ActionTarget::Node("node-a".into()),
            "reboot",
        ))
        .await
        .expect_err("open read never runs actions");
    assert!(
        refused.is_not_enrolled() || matches!(refused, RemoteError::Rejected(_)),
        "{refused}"
    );
    node.server.shutdown().await.expect("listener stops");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn without_open_read_an_unenrolled_operator_gets_no_session() {
    let node = node_with(OperatorOpenRead::default(), Vec::new()).await;
    let refused = stranger(&node, "operator-console")
        .await
        .open_session("operator-console")
        .await
        .expect_err("closed by default");
    assert!(matches!(refused, RemoteError::Rejected(_)), "{refused}");
    node.server.shutdown().await.expect("listener stops");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn operators_from_an_auto_enroll_network_are_enrolled_on_their_hello() {
    let rules = OperatorAutoEnroll::parse_all("127.0.0.0/8=reboot,usb-boot").expect("rule parses");
    let node = node_with(OperatorOpenRead::default(), rules).await;
    let identity = OperatorIdentity::generate("operator-console").expect("identity");
    let operator = RemoteOperator::connect(&node.url(), identity.clone(), NodeTrust::FirstUse)
        .await
        .expect("connects");
    assert!(operator.is_enrolled(), "enrolled from 127.0.0.1");
    let welcome = operator.welcome();
    assert!(welcome.read);
    assert_eq!(
        welcome.allowed_actions,
        vec!["reboot".to_owned(), "usb-boot".to_owned()]
    );
    let record = node
        .app
        .query_operators()
        .operators
        .into_iter()
        .find(|r| r.operator_id == *identity.operator_id())
        .expect("listed");
    assert_eq!(
        record.method,
        Some(orion::control_plane::OperatorEnrollmentMethod::Network)
    );

    // An administrator's removal sticks: the network does not enroll it again.
    node.app
        .remove_operator(identity.operator_id())
        .expect("removes");
    let refused = RemoteOperator::connect(&node.url(), identity, NodeTrust::FirstUse)
        .await
        .expect_err("a removed operator stays removed");
    assert!(refused.to_string().contains("was removed"), "{refused}");
    node.server.shutdown().await.expect("listener stops");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn operators_from_other_networks_are_not_auto_enrolled() {
    let rules = OperatorAutoEnroll::parse_all("10.0.0.0/8=reboot").expect("rule parses");
    let node = node_with(OperatorOpenRead::default(), rules).await;
    assert!(!stranger(&node, "operator-console").await.is_enrolled());
    node.server.shutdown().await.expect("listener stops");
}

#[test]
fn the_settings_parse_and_match() {
    let rules = OperatorAutoEnroll::parse_all(" 172.31.0.0/16=reboot, usb-boot ; fd00::/8 ")
        .expect("parses");
    assert_eq!(rules.len(), 2);
    assert_eq!(
        rules[0].actions,
        vec!["reboot".to_owned(), "usb-boot".to_owned()]
    );
    assert!(rules[1].actions.is_empty(), "no actions: read access only");
    assert!(rules[0].contains("172.31.250.9".parse().expect("ip")));
    assert!(
        rules[0].contains("::ffff:172.31.0.1".parse().expect("ip")),
        "IPv4-mapped counts"
    );
    assert!(!rules[0].contains("172.32.0.1".parse().expect("ip")));
    assert!(rules[1].contains("fd12::1".parse().expect("ip")));
    for bad in [
        "172.31.0.0=reboot",
        "nope/8",
        "10.0.0.0/33",
        "10.0.0.0/8=re*boot",
    ] {
        assert!(OperatorAutoEnroll::parse_all(bad).is_err(), "{bad}");
    }
    assert_eq!(
        OperatorOpenRead::parse("status, streams").expect("parses"),
        OperatorOpenRead {
            status: true,
            streams: true,
            state: false
        }
    );
    assert!(OperatorOpenRead::parse("everything").is_err());
}

/// A local publisher declares a node-owned stream on the IPC stream socket; an unenrolled operator
/// follows it over `orion+tcp` with series patterns and a backfill, then gets live samples.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_open_read_operator_follows_a_local_publishers_stream_by_pattern() {
    use orion::control_plane::{
        Backfill, ClientEventKind, HistoryPolicy, SampleChunkBuilder, SeriesDecl, SeriesKind,
        StreamDecl,
    };
    // The release heartbeat (unit tests use 50 ms): the publisher reads its socket only per
    // request.
    let node = node_configured(
        OperatorOpenRead {
            status: true,
            streams: true,
            state: false,
        },
        Vec::new(),
        |mut config| {
            config.ipc_stream_heartbeat_interval = Duration::from_secs(5);
            config.ipc_stream_heartbeat_timeout = Duration::from_secs(15);
            config
        },
    )
    .await;
    let stream_socket = crate::test_temp::unique("open-read-publisher", ".sock");
    let (_, streams) = node
        .app
        .start_ipc_stream_server_graceful(&stream_socket)
        .await
        .expect("stream server starts");
    let stream = StreamRef {
        owner: StatusSubject::Node(NodeId::new("node-a")),
        id: "board.hardware".into(),
    };
    let mut publisher =
        orion_client::StreamPublisher::connect_at(&stream_socket, "board-agent.live")
            .await
            .expect("publisher connects");
    let series = |name: &str, kind| {
        SeriesDecl::new(name, kind, 10_000).with_history(HistoryPolicy::full(30_000))
    };
    publisher
        .declare(StreamDecl {
            owner: stream.owner.clone(),
            id: stream.id.clone(),
            series: vec![
                series("imu.acceleration.x", SeriesKind::F32),
                series("imu.acceleration.y", SeriesKind::F32),
                series("status-ring.led.0", SeriesKind::U32),
            ],
        })
        .await
        .expect("declared");

    let operator = stranger(&node, "operator-console").await;
    let mut session = operator
        .open_session("operator-console")
        .await
        .expect("a session opens");
    session
        .subscribe_stream(
            StreamSubscription::all(stream.clone())
                .with_series(["imu.*", "lens.*"])
                .with_backfill(Backfill::LastMs(5_000)),
        )
        .await
        .expect("subscribed");
    let next = async |session: &mut orion_client::remote::RemoteSession| {
        tokio::time::timeout(Duration::from_secs(5), session.next_event())
            .await
            .expect("an event within 5 s")
            .expect("session open")
            .event
    };
    let catalog = next(&mut session).await;
    assert!(
        matches!(&catalog, ClientEventKind::StreamCatalog(c) if c.stream == stream),
        "{catalog:?}"
    );
    tokio::time::sleep(Duration::from_millis(100)).await;
    let t = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_micros() as u64;
    let mut x = SampleChunkBuilder::new(0, SeriesKind::F32);
    let mut y = SampleChunkBuilder::new(1, SeriesKind::F32);
    let mut led = SampleChunkBuilder::new(2, SeriesKind::U32);
    for n in 0..5u64 {
        x.push(t + n * 10_000, 0.5);
        y.push(t + n * 10_000, -9.8);
        led.push(t + n * 10_000, 255.0);
    }
    publisher
        .publish(&stream, vec![x.take(), y.take(), led.take()])
        .await
        .expect("published");
    loop {
        match next(&mut session).await {
            ClientEventKind::StreamData(data) if !data.backfill => {
                let series: Vec<u16> = data.chunks.iter().map(|c| c.series).collect();
                assert_eq!(series, [0, 1], "only the imu series");
                break;
            }
            ClientEventKind::StreamData(_) => {}
            other => panic!("unexpected {other:?}"),
        }
    }
    drop(streams);
    node.server.shutdown().await.expect("listener stops");
}
