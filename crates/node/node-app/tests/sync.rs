//! Peer sync across two nodes, each a real comm core with its own node logic: what is put on
//! one is on the other, a node that only the other can reach syncs too, and what changed
//! while one was away arrives when it is back.

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use orion::records::{Bytes, RecordClass, RecordEntry};
use orion::types::{Error, HlcTimestamp};
use orion::{Address, ClientError, Identity, Orion};
use orion_msg_records::NodeRecord;
use orion_node_app::{Node, NodeSettings};
use orion_stubs::{Board, Security};
use orion_wire::List;
use orion_wire::body::encode_fields;
use tokio::sync::oneshot;

fn dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("orion-node-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(dir.join("state")).expect("directory");
    dir
}

fn settings(dir: &Path, name: &str) -> NodeSettings {
    let mut source = orion_config::Source::parse("", []).expect("no settings");
    let mut settings = NodeSettings::from_source(&mut source).expect("defaults");
    settings.config.socket = dir.join("orion.sock");
    settings.config.state_dir = dir.join("state");
    settings.config.keys_dir = dir.join("state");
    settings.config.security = Security::On;
    settings.files_dir = dir.join("files");
    settings.audit = Some(orion_audit::AuditConfig::new(&dir.join("state/audit.log")));
    settings.config.node_name = name.parse().expect("name");
    settings.save_after = Duration::from_millis(10);
    settings
}

/// Node logic in a thread of its own, until dropped.
struct Logic {
    kill: Option<oneshot::Sender<()>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Logic {
    fn start(settings: NodeSettings) -> Self {
        let (kill, killed) = oneshot::channel::<()>();
        let thread = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("runtime");
            runtime.block_on(async move {
                let mut node = Node::load(settings).expect("node logic starts");
                tokio::select! {
                    () = node.run(std::future::pending()) => {}
                    _ = killed => {}
                }
            });
        });
        Self {
            kill: Some(kill),
            thread: Some(thread),
        }
    }
}

impl Drop for Logic {
    fn drop(&mut self) {
        if let (Some(kill), Some(thread)) = (self.kill.take(), self.thread.take()) {
            let _ = kill.send(());
            let _ = thread.join();
        }
    }
}

/// A node: its comm core, its node logic, and a program to put and read records with.
struct Side {
    caller: Orion,
    _logic: Logic,
    board: Board,
}

impl Side {
    /// Starts `name` in `dir`, on `port` if it is to come back where it was, trusting `peer`
    /// with `key`.
    fn start(dir: &Path, name: &str, port: Option<u16>, peer: &str, key: [u8; 32]) -> Self {
        let board = Board::start_named(dir, Security::On, name, port);
        // Each knows the other's key: it is a peer, and the one it is pinned to.
        let peer = peer.parse().expect("name");
        let trusted = board.keys().lock().trust_peer(&peer, key).expect("trust");
        assert!(trusted || board.keys().lock().peer_key(&peer) == Some(&key));
        let logic = Logic::start(settings(dir, name));
        let caller = Orion::connect(&Address::Local(board.socket().to_owned()), "dashboard")
            .expect("connect");
        Self {
            caller,
            _logic: logic,
            board,
        }
    }

    /// Puts a node record for `name` at `address`, once node logic is there to take it.
    fn put_node(&self, name: &str, address: &str) {
        let mut value = vec![0_u8; 128];
        let len = encode_fields(
            &NodeRecord {
                name: name.parse().expect("name"),
                address,
                labels: List::new(&[]),
            },
            &mut value,
        )
        .expect("encode");
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let put = self.caller.records().put(&[RecordEntry {
                class: RecordClass::Node,
                key: name,
                stamp: HlcTimestamp::ZERO,
                deleted: false,
                value: Bytes(&value[..len]),
            }]);
            match put {
                Ok(_) => return,
                Err(ClientError::Refused(Error::Unavailable)) if Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(10));
                }
                Err(error) => panic!("put: {error}"),
            }
        }
    }

    /// Whether a node record for `name` is here.
    fn has_node(&self, name: &str) -> bool {
        self.caller
            .records()
            .get(RecordClass::Node, name)
            .is_ok_and(|records| {
                records
                    .iter()
                    .any(|record| record.key == name && !record.deleted)
            })
    }
}

fn until(what: &str, mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(15);
    while !done() {
        assert!(Instant::now() < deadline, "{what}");
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn key(dir: &Path) -> [u8; 32] {
    Identity::load_or_create(&dir.join("state/node-identity.ed25519"))
        .expect("key")
        .public()
}

#[test]
fn records_reach_the_other_node_both_ways_and_after_it_was_away() {
    let (left_dir, right_dir) = (dir("sync-left"), dir("sync-right"));
    let (left_key, right_key) = (key(&left_dir), key(&right_dir));
    let right = Side::start(&right_dir, "right", None, "left", left_key);
    let port = right.board.network().port();
    let left = Side::start(&left_dir, "left", None, "right", right_key);

    // Only the left node knows where the right one is. The right one never connects to the
    // left: what it has goes back on the session the left made.
    left.put_node("right", &format!("127.0.0.1:{port}"));
    left.put_node("spare", "");
    until("the left node's record is on the right", || {
        right.has_node("spare")
    });
    right.put_node("other", "");
    until("the right node's record is on the left", || {
        left.has_node("other")
    });

    // The right node goes away. What is put on the left meanwhile arrives when it is back,
    // on the same port, with what it kept.
    let mut right = right;
    right.board.stop();
    drop(right);
    left.put_node("while-away", "");
    let right = Side::start(&right_dir, "right", Some(port), "left", left_key);
    until("what was put while it was away is on the right", || {
        right.has_node("while-away")
    });
    assert!(right.has_node("other"), "what it had is still there");
    // When it was last agreed with is kept on both sides.
    until("the left node keeps when it agreed with the right", || {
        std::fs::read_to_string(left_dir.join("state/sync-agreed"))
            .is_ok_and(|text| text.starts_with("right "))
    });
}

#[test]
fn a_node_not_pinned_is_not_synced_with() {
    let (left_dir, right_dir) = (dir("unpinned-left"), dir("unpinned-right"));
    let left_key = key(&left_dir);
    let right = Side::start(&right_dir, "right", None, "left", left_key);
    let port = right.board.network().port();
    // The left node pins another key for the right one.
    let wrong = Identity::load_or_create(&left_dir.join("wrong.ed25519"))
        .expect("key")
        .public();
    let left = Side::start(&left_dir, "left", None, "right", wrong);
    left.put_node("right", &format!("127.0.0.1:{port}"));
    left.put_node("spare", "");
    std::thread::sleep(Duration::from_secs(2));
    assert!(
        !right.has_node("spare"),
        "a node that is not the one pinned got the records"
    );
}
