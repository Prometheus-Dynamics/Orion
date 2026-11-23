//! Two boards with security off, and a node record on one that says where the other is: what
//! links them today, pinned as it is.

use std::time::{Duration, Instant};

use orion::Orion;
use orion::node::SyncState;
use orion::records::{Bytes, RecordClass, RecordEntry};
use orion::types::HlcTimestamp;
use orion::workloads::{RestartPolicy, Workload};
use orion_msg_records::NodeRecord;
use orion_test_board::{Options, TestBoard};
use orion_wire::List;
use orion_wire::body::encode_fields;

fn board(name: &str) -> TestBoard {
    TestBoard::start_with(
        name,
        Options {
            name: name.to_owned(),
            network: true,
            ..Options::default()
        },
    )
}

/// Tells the board behind `orion` where `node` is.
fn point(orion: &Orion, node: &str, address: &str) {
    let mut value = vec![0_u8; 128];
    let len = encode_fields(
        &NodeRecord {
            name: node.parse().expect("a name"),
            address,
            labels: List::new(&[]),
        },
        &mut value,
    )
    .expect("encode");
    orion
        .records()
        .put(&[RecordEntry {
            class: RecordClass::Node,
            key: node,
            stamp: HlcTimestamp::ZERO,
            deleted: false,
            value: Bytes(&value[..len]),
        }])
        .expect("put");
}

fn workloads(orion: &Orion) -> Vec<String> {
    orion
        .records()
        .get(RecordClass::Workload, "")
        .expect("get")
        .into_iter()
        .map(|record| record.key)
        .collect()
}

/// Pinned as it is today, not as it should be: whether boards with security off should sync
/// is not decided. The left board dials the right one, which takes none of what it says:
/// with security off the one who dialled is nobody in particular, and sync is taken from
/// peers. So the left board says `syncing` for good, and no record crosses either way.
#[test]
fn with_security_off_a_board_dials_the_one_its_node_record_names_and_no_record_crosses() {
    let left = board("left");
    let right = board("right");
    let (on_left, on_right) = (
        left.connect("tool").expect("connect"),
        right.connect("tool").expect("connect"),
    );
    on_left
        .workloads()
        .put(&Workload {
            name: "pipeline.front",
            kind: "graph",
            spec: b"spec",
            node: None,
            restart: RestartPolicy::Never,
        })
        .expect("put");
    point(
        &on_left,
        "right",
        &right.network().expect("a port").to_string(),
    );

    // The left board dials: the link is up, and the two have not agreed.
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let peers = on_left.sync_peers().expect("peers");
        if let [peer] = peers.as_slice()
            && peer.state == SyncState::Syncing
        {
            assert_eq!(peer.node, "right");
            break;
        }
        assert!(Instant::now() < deadline, "never dialled: {peers:?}");
        std::thread::sleep(Duration::from_millis(20));
    }

    // And it stays so. With security on and the keys pinned, the two agree well inside this.
    let until = Instant::now() + Duration::from_secs(3);
    while Instant::now() < until {
        let peers = on_left.sync_peers().expect("peers");
        assert_eq!(peers.len(), 1, "{peers:?}");
        assert_eq!(peers[0].state, SyncState::Syncing, "{peers:?}");
        assert_eq!(peers[0].agreed_ms, None);
        assert_eq!(
            workloads(&on_right),
            Vec::<String>::new(),
            "a record crossed"
        );
        assert_eq!(on_right.sync_peers().expect("peers"), Vec::new());
        assert_eq!(
            on_right
                .records()
                .get(RecordClass::Node, "")
                .expect("get")
                .len(),
            0,
            "the right board heard of a node"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
    assert_eq!(workloads(&on_left), ["pipeline.front"]);
}
