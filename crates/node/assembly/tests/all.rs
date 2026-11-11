//! Every part of node logic at once, from one settings file, against one comm core: records,
//! resources, actions, files in all three ways, workloads and the audit log, and then all of
//! it again after node logic has been stopped and started.

use std::io::{BufRead, BufReader, Cursor, Write};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use orion::actions::{ActionCall, Admits, Outcome, Policy, Request, Target};
use orion::files::{FileState, Handed, Pull, digest};
use orion::provider::ResourceState;
use orion::records::{Bytes, RecordClass, RecordEntry};
use orion::topics::{Field, FieldType};
use orion::types::{Error, HlcTimestamp};
use orion::workloads::{RestartPolicy, Workload, WorkloadState};
use orion::{Address, ClientError, Options, Orion, Roles};
use orion_assembly::{run_until, settings};
use orion_config::Source;
use orion_msg_records::LeaseRecord;
use orion_sample_executor::{Sample, run};
use orion_stubs::Board;
use orion_wire::body::encode_fields;
use tokio::sync::oneshot;

fn dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("orion-assembly-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("directory");
    dir
}

/// The settings file, as it would be on a board: the comm core's keys are in it too.
fn file(dir: &Path) -> String {
    format!(
        "node.name = board\n\
         socket = {dir}/orion.sock\n\
         state.dir = {dir}/state\n\
         # The comm core's, which node logic has no use for.\n\
         comm.heartbeat_ms = 2000\n\
         topics.max = 64\n\
         records.save_after_ms = 20\n\
         files.dir = {dir}/files\n\
         audit.file = {dir}/state/audit.log\n\
         workloads.stop_grace_ms = 500\n",
        dir = dir.display()
    )
}

/// Node logic, run as the program runs it, until it is asked to stop.
struct Logic {
    stop: Option<oneshot::Sender<()>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Logic {
    fn start(text: &str) -> Self {
        let mut source = Source::parse(text, []).expect("settings");
        let (settings, unknown) = settings(&mut source).expect("settings");
        assert!(unknown.is_empty(), "{unknown:?}");
        let (stop, stopped) = oneshot::channel::<()>();
        let thread = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("runtime");
            runtime
                .block_on(run_until(settings, async {
                    let _ = stopped.await;
                }))
                .expect("node logic runs");
        });
        Self {
            stop: Some(stop),
            thread: Some(thread),
        }
    }
}

impl Drop for Logic {
    fn drop(&mut self) {
        if let (Some(stop), Some(thread)) = (self.stop.take(), self.thread.take()) {
            let _ = stop.send(());
            let _ = thread.join();
        }
    }
}

fn until(what: &str, mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(15);
    while !done() {
        assert!(Instant::now() < deadline, "{what}");
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// Does `ask` until node logic is there to answer.
fn answered<T>(mut ask: impl FnMut() -> Result<T, ClientError>) -> T {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        match ask() {
            Ok(value) => return value,
            Err(ClientError::Refused(Error::Unavailable)) if Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(10));
            }
            Err(error) => panic!("{error}"),
        }
    }
}

/// A background program, stopped when dropped.
struct Program {
    stop: Arc<AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Program {
    fn start(body: impl FnOnce(&AtomicBool) + Send + 'static) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let stopping = stop.clone();
        Self {
            stop,
            thread: Some(std::thread::spawn(move || body(&stopping))),
        }
    }
}

impl Drop for Program {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

/// A server that answers every request with `file`.
fn serve(file: Vec<u8>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("listen");
    let address = listener.local_addr().expect("address");
    std::thread::spawn(move || {
        for mut stream in listener.incoming().flatten() {
            let mut reader = BufReader::new(stream.try_clone().expect("clone"));
            let mut line = String::new();
            while reader.read_line(&mut line).unwrap_or(0) != 0 && line != "\r\n" {
                line.clear();
            }
            let _ = write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                file.len()
            );
            let _ = stream.write_all(&file);
        }
    });
    format!("http://{address}/calibration.bin")
}

fn pids(file: &Path) -> Vec<u32> {
    std::fs::read_to_string(file)
        .unwrap_or_default()
        .lines()
        .filter_map(|line| line.trim().parse().ok())
        .collect()
}

#[test]
fn every_part_of_node_logic_runs_together_and_again_after_a_restart() {
    let dir = dir("all");
    let text = file(&dir);
    let board = Board::start_for_node(&dir);
    let address = Address::Local(board.socket().to_owned());
    let logic = Logic::start(&text);
    let operator = Orion::connect(&address, "operator").expect("connect");
    let records = operator.records();

    // --- Records ---
    let mut watch = records
        .watch(RecordClass::Node, "", HlcTimestamp::ZERO)
        .expect("watch");
    let note = RecordEntry {
        class: RecordClass::Node,
        key: "board",
        stamp: HlcTimestamp::ZERO,
        deleted: false,
        value: Bytes(b"here"),
    };
    let stored = answered(|| records.put(std::slice::from_ref(&note)));
    assert_eq!(
        watch.wait(Duration::from_secs(5)).expect("watch"),
        Some(stored[0].clone())
    );

    // --- Resources and actions: a camera with a topic and an action ---
    let driver = Orion::connect_with(
        &address,
        "camera-driver",
        Options {
            roles: Roles::CLIENT.and(Roles::PROVIDER),
            ..Options::default()
        },
    )
    .expect("connect");
    let mut describing = driver
        .provider()
        .resource("cam.front", "camera")
        .expect("name")
        .label("lens", "wide");
    let _frames = describing
        .topic(
            "frames",
            &[Field {
                name: "count",
                ty: FieldType::Int,
                unit: "",
                type_name: "",
            }],
            8,
            8,
        )
        .expect("topic");
    describing
        .action("focus", "Focuses the lens", Policy::new(Admits::LOCAL))
        .expect("action");
    let mut camera = describing.publish(ResourceState::Ready).expect("publish");
    let found = operator.resources("cam.").expect("list");
    assert_eq!(found.len(), 1);
    assert_eq!((found[0].topics.len(), found[0].actions.len()), (1, 1));

    // It is bound to one holder.
    let mut lease = vec![0_u8; 128];
    let len = encode_fields(
        &LeaseRecord {
            resource: "cam.front".parse().expect("name"),
            holder: "board/detector",
            expires_ms: 0,
        },
        &mut lease,
    )
    .expect("encode");
    records
        .put(&[RecordEntry {
            class: RecordClass::Lease,
            key: "cam.front",
            stamp: HlcTimestamp::ZERO,
            deleted: false,
            value: Bytes(&lease[..len]),
        }])
        .expect("bind");

    // Its action runs through the check, and the provider does it.
    let focus = |operator: &Orion, camera: &mut orion::provider::Resource, args: &[u8]| {
        let mut run = operator
            .run(&ActionCall {
                name: "cam.front.focus",
                target: Target::Resource("cam.front".parse().expect("name")),
                args,
                deadline_ms: 0,
                detached: false,
            })
            .expect("run");
        let requests = camera.requests().expect("the camera has an action");
        let Some(Request::Run { call, args, .. }) =
            requests.wait(Duration::from_secs(5)).expect("wait")
        else {
            panic!("the run never reached the provider");
        };
        requests
            .finish(call, Outcome::Succeeded, &args, "")
            .expect("finish");
        run.finish(Duration::from_secs(5)).expect("the run ends")
    };
    assert_eq!(focus(&operator, &mut camera, b"near").output, b"near");

    // --- Files: pushed, fetched, and handed to a receiver ---
    let model = b"the weights the detector needs".to_vec();
    let (model_sum, _) = digest(&mut Cursor::new(model.clone())).expect("digest");
    let pushed = operator
        .files()
        .push(
            "models/detector.bin",
            &mut Cursor::new(model.clone()),
            |_, _| {},
        )
        .expect("push");
    assert_eq!(pushed.state, FileState::Committed);
    assert_eq!(
        std::fs::read(dir.join("files/models/detector.bin")).expect("file"),
        model
    );

    let calibration = vec![7_u8; 50_000];
    let (calibration_sum, size) = digest(&mut Cursor::new(calibration.clone())).expect("digest");
    let url = serve(calibration.clone());
    let pulled = operator
        .files()
        .pull(
            &Pull {
                transfer: 77,
                url: &url,
                destination: "calibration.bin",
                size,
                digest: calibration_sum,
            },
            Duration::from_secs(20),
            |_, _| {},
        )
        .expect("pull");
    assert_eq!(pulled.state, FileState::Committed, "{}", pulled.detail);
    assert_eq!(
        std::fs::read(dir.join("files/calibration.bin")).expect("file"),
        calibration
    );

    let handed = Arc::new(AtomicU64::new(0));
    let (counted, socket) = (handed.clone(), board.socket().to_owned());
    let (ready, is_ready) = std::sync::mpsc::channel();
    let _manager = Program::start(move |stop| {
        let orion = Orion::connect(&Address::Local(socket), "board-manager").expect("connect");
        let mut sink = orion.files().sink("slots/").expect("sink");
        ready.send(()).expect("ready");
        let mut received = 0_u64;
        while !stop.load(Ordering::Relaxed) {
            match sink.next(Duration::from_millis(20)) {
                Ok(Some(Handed::Begin { transfer, .. })) => {
                    received = 0;
                    let _ = sink.status(transfer, 0, FileState::Receiving, "");
                }
                Ok(Some(Handed::Chunk { transfer, data, .. })) => {
                    received += data.len() as u64;
                    counted.fetch_add(data.len() as u64, Ordering::Relaxed);
                    let _ = sink.status(transfer, received, FileState::Receiving, "");
                }
                Ok(Some(Handed::Commit { transfer })) => {
                    let _ = sink.status(transfer, received, FileState::Committed, "");
                }
                Ok(None) => {}
                Err(_) => return,
            }
        }
    });
    is_ready
        .recv_timeout(Duration::from_secs(10))
        .expect("the receiver is there");
    let image = vec![3_u8; 200_000];
    let slot = operator
        .files()
        .push("slots/b.img", &mut Cursor::new(image.clone()), |_, _| {})
        .expect("push");
    assert_eq!(slot.state, FileState::Committed);
    assert_eq!(handed.load(Ordering::Relaxed), image.len() as u64);
    assert!(
        !dir.join("files/slots").exists(),
        "the node kept none of it"
    );

    // --- Workloads: one that needs the model, on the sample executor ---
    let socket = board.socket().to_owned();
    let _executor = Program::start(move |stop| {
        let mut sample = Sample::new(vec!["process".parse().expect("kind")]);
        run(&socket, &mut sample, || !stop.load(Ordering::Relaxed)).expect("run");
    });
    let pid_file = dir.join("pids");
    let spec = format!("echo $$ >> {}; exec sleep 120", pid_file.display());
    let detector = Workload {
        name: "detector",
        kind: "process",
        spec: spec.as_bytes(),
        node: None,
        restart: RestartPolicy::Always,
    };
    let needs = [("models/detector.bin", model_sum)];
    let running = |operator: &Orion| {
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            operator
                .workloads()
                .put_needing(&detector, &needs)
                .expect("put");
            let reached = operator
                .workloads()
                .wait("detector", Duration::from_millis(200), |tracked| {
                    tracked.state == WorkloadState::Running
                })
                .expect("connected");
            if reached.is_some() {
                return;
            }
            assert!(Instant::now() < deadline, "the detector never ran");
        }
    };
    running(&operator);
    until("the detector's command started", || {
        pids(&pid_file).len() == 1
    });

    // --- All of it again after node logic stops and starts ---
    drop(logic);
    // Stopped as the service manager stops it: what it had is saved.
    let log = std::fs::read_to_string(dir.join("state/audit.log")).expect("the audit log");
    for did in ["put", "register", "run", "file", "commit", "workload"] {
        assert!(
            log.contains(&format!("\"did\":\"{did}\"")),
            "nothing in the audit log about {did}:\n{log}"
        );
    }
    let _logic = Logic::start(&text);

    // The records are back, the lease among them.
    assert_eq!(answered(|| records.get(RecordClass::Node, "")), stored);
    assert_eq!(
        records.get(RecordClass::Lease, "").expect("leases").len(),
        1
    );
    // The camera's action is still on the list, though its driver did nothing.
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let mut run = operator
            .run(&ActionCall {
                name: "cam.front.focus",
                target: Target::Resource("cam.front".parse().expect("name")),
                args: b"far",
                deadline_ms: 0,
                detached: false,
            })
            .expect("run");
        let requests = camera.requests().expect("the camera has an action");
        if let Some(Request::Run { call, args, .. }) =
            requests.wait(Duration::from_millis(300)).expect("wait")
        {
            requests
                .finish(call, Outcome::Succeeded, &args, "")
                .expect("finish");
            assert_eq!(
                run.finish(Duration::from_secs(5)).expect("ends").output,
                b"far"
            );
            break;
        }
        assert!(Instant::now() < deadline, "the action was not passed again");
    }
    // The detector was adopted: the same process, with its file still known.
    running(&operator);
    std::thread::sleep(Duration::from_millis(300));
    assert_eq!(pids(&pid_file).len(), 1, "it was not started a second time");
    // The receiver still receives.
    let again = operator
        .files()
        .push("slots/c.img", &mut Cursor::new(image.clone()), |_, _| {})
        .expect("push");
    assert_eq!(again.state, FileState::Committed);
    assert_eq!(handed.load(Ordering::Relaxed), 2 * image.len() as u64);

    operator
        .workloads()
        .remove("detector", true)
        .expect("remove");
}

#[test]
fn only_a_key_of_node_logics_own_that_nobody_read_is_reported() {
    let text = "node.name = board\n\
                comm.heartbeat_ms = 2000\n\
                links.serial = a=/dev/ttyS0@115200\n\
                records.sav_after_ms = 20\n\
                files.max_size = 1000\n\
                audit.file = off\n";
    let mut source = Source::parse(text, []).expect("settings");
    let (settings, unknown) = settings(&mut source).expect("settings");
    let keys: Vec<&str> = unknown.iter().map(|warning| warning.key.as_str()).collect();
    assert_eq!(keys, ["records.sav_after_ms"]);
    assert_eq!(settings.files_max_size, 1000);
    assert!(settings.audit.is_none());
    assert_eq!(settings.save_after, Duration::from_millis(500));
    // A value that is not one is an error that names its key.
    let mut bad = Source::parse("files.max_size = lots\n", []).expect("settings");
    let error = orion_assembly::settings(&mut bad).expect_err("not a number");
    assert_eq!(error.key, "files.max_size");
}
