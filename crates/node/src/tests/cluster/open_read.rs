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
        .config(config)
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
