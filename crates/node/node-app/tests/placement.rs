//! Placement across two nodes, each a real comm core with its own node logic: a workload put
//! on a node with no executor for it runs on the node that has one, and moves when that one
//! no longer can and another can.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use orion::records::{Bytes, RecordClass, RecordEntry};
use orion::types::{Error, HlcTimestamp};
use orion::workloads::{RestartPolicy, Workload, WorkloadState};
use orion::{Address, ClientError, Identity, Orion};
use orion_msg_records::NodeRecord;
use orion_node_app::{Node, NodeSettings};
use orion_sample_executor::{Sample, run};
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
    settings.placement_grace = Duration::from_secs(1);
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

/// The sample executor in a thread of its own. Dropping it ends it and every command it ran.
struct Executor {
    stop: Arc<AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Executor {
    fn start(socket: &Path) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let (socket, stopping) = (socket.to_owned(), stop.clone());
        let thread = std::thread::spawn(move || {
            let mut sample = Sample::new(vec!["process".parse().expect("kind")]);
            run(&socket, &mut sample, || !stopping.load(Ordering::Relaxed)).expect("run");
        });
        Self {
            stop,
            thread: Some(thread),
        }
    }
}

impl Drop for Executor {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

/// A node: its comm core, its node logic, and a program to put and read with.
struct Side {
    caller: Orion,
    _logic: Logic,
    board: Board,
}

impl Side {
    fn start(dir: &Path, name: &str, peer: &str, key: [u8; 32]) -> Self {
        let board = Board::start_named(dir, Security::On, name, None);
        let trusted = board
            .keys()
            .lock()
            .trust_peer(&peer.parse().expect("name"), key)
            .expect("trust");
        assert!(trusted);
        let logic = Logic::start(settings(dir, name));
        let caller = Orion::connect(&Address::Local(board.socket().to_owned()), "operator")
            .expect("connect");
        Self {
            caller,
            _logic: logic,
            board,
        }
    }

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

    /// Whether this node runs `workload` and says it is in `state`. Putting it again as it is
    /// changes nothing and is how to ask.
    fn runs(&self, workload: &Workload<'_>, me: &str, state: WorkloadState) -> bool {
        if self.caller.workloads().put(workload).is_err() {
            return false;
        }
        self.caller
            .workloads()
            .wait(workload.name, Duration::from_millis(200), |tracked| {
                tracked.state == state && tracked.node.as_deref() == Some(me)
            })
            .ok()
            .flatten()
            .is_some()
    }
}

fn until(what: &str, mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(20);
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

fn alive(pid: u32) -> bool {
    std::fs::read_to_string(format!("/proc/{pid}/stat")).is_ok_and(|stat| {
        stat.rsplit(')')
            .next()
            .is_some_and(|rest| !rest.trim_start().starts_with('Z'))
    })
}

fn starts(file: &Path) -> Vec<u32> {
    std::fs::read_to_string(file)
        .unwrap_or_default()
        .lines()
        .filter_map(|line| line.trim().parse().ok())
        .collect()
}

#[test]
fn a_workload_runs_where_an_executor_is_and_moves_when_it_goes() {
    let (left_dir, right_dir) = (dir("place-left"), dir("place-right"));
    let (left_key, right_key) = (key(&left_dir), key(&right_dir));
    let right = Side::start(&right_dir, "right", "left", left_key);
    let port = right.board.network().port();
    let left = Side::start(&left_dir, "left", "right", right_key);
    left.put_node("right", &format!("127.0.0.1:{port}"));

    // Only the right node has an executor for the kind.
    let right_executor = Executor::start(right.board.socket());
    let file = left_dir.join("pids");
    let spec = format!("echo $$ >> {}; exec sleep 120", file.display());
    let workload = Workload {
        name: "sleeper",
        kind: "process",
        spec: spec.as_bytes(),
        node: None,
        restart: RestartPolicy::Always,
    };
    // Put on the left node, which cannot run it: it runs on the right one.
    until("the left node takes the workload", || {
        left.caller.workloads().put(&workload).is_ok()
    });
    until("it runs on the right node", || {
        right.runs(&workload, "right", WorkloadState::Running)
    });
    until("it has started once", || starts(&file).len() == 1);
    assert!(
        !left.runs(&workload, "left", WorkloadState::Running),
        "it does not run on the left node too"
    );

    // The right node's executor goes, and the left node gets one: after the grace period the
    // left node takes the workload over.
    drop(right_executor);
    let _left_executor = Executor::start(left.board.socket());
    until("it runs on the left node", || {
        left.runs(&workload, "left", WorkloadState::Running)
    });
    until("it has started a second time", || starts(&file).len() == 2);
    let pids = starts(&file);
    assert!(
        !alive(pids[0]),
        "the run on the right node ended with its executor"
    );
    assert!(alive(pids[1]));
    // And it stays there: the record is still there, and it is not started again.
    std::thread::sleep(Duration::from_secs(2));
    assert_eq!(starts(&file).len(), 2);
    assert_eq!(
        left.caller
            .records()
            .get(RecordClass::Workload, "sleeper")
            .expect("records")
            .len(),
        1
    );
}
