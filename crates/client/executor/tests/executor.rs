//! A sample executor against a tracker that stands in for node logic's.

#![cfg(target_os = "linux")]

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, mpsc};
use std::time::{Duration, Instant};

use orion_connect::{Address, Client, Options, Roles, Session};
use orion_executor::{ExecError, Executor, Serving, WorkloadKind, WorkloadName, WorkloadState};
use orion_msg_workloads::{
    ExecutorInventory, ExecutorKill, ExecutorOffer, ExecutorReport, ExecutorStart, ExecutorStop,
};
use orion_server::{
    Admits, Checked, Comm, Cx, Delivery, Error, Incoming, Limits, Policy, Security, Service,
    SessionId, Sessions, What,
};
use orion_unix::UnixCarrier;
use orion_wire::{Bytes, FrameView, kind};

/// What the tracker has heard, for the test to read from outside the board's thread.
#[derive(Default)]
struct Heard {
    kinds: Vec<String>,
    inventories: Vec<Vec<(String, WorkloadState)>>,
    reports: Vec<(String, WorkloadState, String)>,
}

/// What the test asks the tracker to tell the executor.
enum Tell {
    Start(&'static str, &'static [u8]),
    Stop(&'static str),
    Kill(&'static str),
}

struct Tracker {
    heard: Arc<Mutex<Heard>>,
    executor: std::rc::Rc<std::cell::Cell<Option<SessionId>>>,
}

impl Service for Tracker {
    fn kinds(&self) -> &'static [u8] {
        &[
            kind::EXECUTOR_OFFER,
            kind::EXECUTOR_REPORT,
            kind::EXECUTOR_INVENTORY,
        ]
    }

    fn asks<'a>(&self, _frame: &FrameView<'a>) -> Result<(What<'a>, Policy), Error> {
        Ok((
            What::Write("workloads"),
            Policy::new(Admits::LOCAL).without_grant(),
        ))
    }

    fn handle(&mut self, request: Checked<Incoming<'_>>, cx: &mut Cx<'_>) -> Result<(), Error> {
        let incoming = request.get();
        let mut heard = self.heard.lock().expect("lock");
        match incoming.kind() {
            kind::EXECUTOR_OFFER => {
                let offer: ExecutorOffer<'_> = incoming.body()?;
                heard.kinds = offer
                    .kinds
                    .iter()
                    .flatten()
                    .map(|offered| offered.kind.to_string())
                    .collect();
                self.executor.set(Some(cx.from()));
            }
            kind::EXECUTOR_INVENTORY => {
                let inventory: ExecutorInventory<'_> = incoming.body()?;
                heard.inventories.push(
                    inventory
                        .workloads
                        .iter()
                        .flatten()
                        .map(|item| (item.name.to_string(), item.state))
                        .collect(),
                );
            }
            _ => {
                let report: ExecutorReport<'_> = incoming.body()?;
                heard.reports.push((
                    report.name.to_string(),
                    report.state,
                    report.detail.to_owned(),
                ));
            }
        }
        Ok(())
    }

    fn closed(&mut self, session: SessionId, _sessions: &mut Sessions) {
        if self.executor.get() == Some(session) {
            self.executor.set(None);
        }
    }
}

struct Board {
    socket: PathBuf,
    heard: Arc<Mutex<Heard>>,
    tell: tokio::sync::mpsc::UnboundedSender<Tell>,
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Board {
    fn start(dir: &std::path::Path) -> Self {
        let socket = dir.join("orion.sock");
        let heard = Arc::new(Mutex::new(Heard::default()));
        let (ready, started) = mpsc::channel();
        let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
        let (tell, mut told) = tokio::sync::mpsc::unbounded_channel::<Tell>();
        let (path, shared) = (socket.clone(), heard.clone());
        let thread = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("runtime");
            tokio::task::LocalSet::new().block_on(&runtime, async move {
                let comm = Comm::new(
                    "board".parse().expect("name"),
                    Security::Off,
                    None,
                    Limits::default(),
                );
                let executor = std::rc::Rc::new(std::cell::Cell::new(None));
                comm.add_service(Tracker {
                    heard: shared,
                    executor: executor.clone(),
                });
                let unix = UnixCarrier::bind(&path, Some(0o600), &[]).expect("socket");
                let serving = comm.clone();
                tokio::task::spawn_local(async move { serving.serve(unix).await });
                // What the test says the tracker wants is sent to the executor.
                tokio::task::spawn_local(async move {
                    while let Some(tell) = told.recv().await {
                        let Some(to) = executor.get() else { continue };
                        let name = |name: &str| name.parse::<WorkloadName>().expect("name");
                        comm.with(|sessions, _| match tell {
                            Tell::Start(workload, spec) => sessions.send(
                                to,
                                &ExecutorStart {
                                    name: name(workload),
                                    kind: "graph".parse().expect("kind"),
                                    spec: Bytes(spec),
                                    cpus: Bytes(&[0b1100]),
                                },
                                Delivery::Must,
                            ),
                            Tell::Stop(workload) => sessions.send(
                                to,
                                &ExecutorStop {
                                    name: name(workload),
                                },
                                Delivery::Must,
                            ),
                            Tell::Kill(workload) => sessions.send(
                                to,
                                &ExecutorKill {
                                    name: name(workload),
                                },
                                Delivery::Must,
                            ),
                        })
                        .expect("sent");
                    }
                });
                ready.send(()).expect("ready");
                let _ = stopped.await;
            });
        });
        started.recv().expect("the board starts");
        Self {
            socket,
            heard,
            tell,
            stop: Some(stop),
            thread: Some(thread),
        }
    }

    fn stop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }

    fn reports(&self) -> Vec<(String, WorkloadState, String)> {
        self.heard.lock().expect("lock").reports.clone()
    }
}

impl Drop for Board {
    fn drop(&mut self) {
        self.stop();
    }
}

/// A sample executor: workloads are entries in a map that come up one step per look.
struct Sample {
    kinds: Vec<WorkloadKind>,
    running: BTreeMap<String, (WorkloadState, u32)>,
    cpus_seen: Vec<u8>,
}

impl Executor for Sample {
    fn kinds(&self) -> &[WorkloadKind] {
        &self.kinds
    }

    fn start(&mut self, name: &WorkloadName, spec: &[u8]) -> Result<(), ExecError> {
        if spec == b"broken" {
            return Err(ExecError("the graph does not load".into()));
        }
        if spec == b"done" {
            // Ends, well, before anyone looks.
            self.running.remove(name.as_str());
            return Ok(());
        }
        self.running
            .insert(name.to_string(), (WorkloadState::Starting, 0));
        Ok(())
    }

    fn start_on(
        &mut self,
        name: &WorkloadName,
        kind: &WorkloadKind,
        spec: &[u8],
        cpus: &[u8],
    ) -> Result<(), ExecError> {
        assert_eq!(kind.as_str(), "graph");
        self.cpus_seen = cpus.to_vec();
        self.start(name, spec)
    }

    fn stop(&mut self, name: &WorkloadName) {
        if let Some(run) = self.running.get_mut(name.as_str()) {
            *run = (WorkloadState::Stopping, 0);
        }
    }

    fn kill(&mut self, name: &WorkloadName) {
        self.running.remove(name.as_str());
    }

    fn inventory(&self) -> Vec<(WorkloadName, WorkloadState)> {
        self.running
            .iter()
            .map(|(name, (state, _))| (name.parse().expect("name"), *state))
            .collect()
    }
}

impl Sample {
    /// What a real executor's workloads do by themselves: come up, wind down, exit.
    fn live(&mut self) {
        let mut gone = Vec::new();
        for (name, (state, looks)) in &mut self.running {
            *looks += 1;
            match state {
                WorkloadState::Starting if *looks >= 2 => *state = WorkloadState::Running,
                WorkloadState::Stopping if *looks >= 2 => gone.push(name.clone()),
                _ => {}
            }
        }
        for name in gone {
            self.running.remove(&name);
        }
    }
}

fn serve_until(
    serving: &mut Serving,
    sample: &mut Sample,
    what: &str,
    mut done: impl FnMut(&Sample) -> bool,
) {
    let started = Instant::now();
    loop {
        serving
            .turn(sample, Duration::from_millis(10))
            .expect("turn");
        if done(sample) {
            return;
        }
        sample.live();
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "never happened: {what}"
        );
    }
}

fn connect(socket: &std::path::Path) -> Session {
    let options = Options {
        roles: Roles::EXECUTOR,
        ..Options::default()
    };
    Session::connect(
        &Address::Local(socket.to_owned()),
        "sample-executor",
        options,
    )
    .expect("connect")
}

#[test]
fn a_sample_executor_starts_stops_and_reports_what_is_true() {
    let dir = std::env::temp_dir().join(format!("orion-executor-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("dir");
    let mut board = Board::start(&dir);

    let client = Client::new(connect(&board.socket));
    let mut serving = Serving::new(&client);
    let mut sample = Sample {
        kinds: vec!["graph".parse().expect("kind")],
        running: BTreeMap::new(),
        cpus_seen: Vec::new(),
    };
    serving.start(&sample).expect("start");
    let reported = |board: &Board, name: &str| -> Vec<WorkloadState> {
        board
            .reports()
            .iter()
            .filter(|(of, ..)| of == name)
            .map(|(_, state, _)| *state)
            .collect()
    };
    // It says what it runs, and that it has nothing.
    serve_until(&mut serving, &mut sample, "the offer is heard", |_| {
        !board.heard.lock().expect("lock").inventories.is_empty()
    });
    assert_eq!(board.heard.lock().expect("lock").kinds, ["graph"]);
    assert_eq!(board.heard.lock().expect("lock").inventories, [vec![]]);

    // Told to start: it starts, and each state the workload goes through is reported once,
    // without the executor ever saying "report this".
    board
        .tell
        .send(Tell::Start("vision", b"pipeline"))
        .expect("tell");
    serve_until(&mut serving, &mut sample, "vision runs", |_| {
        reported(&board, "vision").last() == Some(&WorkloadState::Running)
    });
    assert_eq!(
        reported(&board, "vision"),
        [WorkloadState::Starting, WorkloadState::Running]
    );
    assert_eq!(
        sample.cpus_seen,
        [0b1100],
        "the CPUs it may use reach the executor"
    );

    // One that cannot start is reported failed, with why. It is not in the inventory.
    board
        .tell
        .send(Tell::Start("broken", b"broken"))
        .expect("tell");
    serve_until(&mut serving, &mut sample, "the failure is heard", |_| {
        !reported(&board, "broken").is_empty()
    });
    assert_eq!(
        board.reports().iter().find(|(name, ..)| name == "broken"),
        Some(&(
            "broken".to_owned(),
            WorkloadState::Failed,
            "the graph does not load".to_owned()
        ))
    );
    assert!(!sample.running.contains_key("broken"));

    // One that ends well before it is ever in an inventory is gone, not left starting.
    board
        .tell
        .send(Tell::Start("blink", b"done"))
        .expect("tell");
    serve_until(&mut serving, &mut sample, "blink is gone", |_| {
        reported(&board, "blink").last() == Some(&WorkloadState::Gone)
    });
    assert_eq!(reported(&board, "blink"), [WorkloadState::Gone]);

    // A second workload, then a stop: stopping, then gone when it really is.
    board
        .tell
        .send(Tell::Start("mapping", b"pipeline"))
        .expect("tell");
    serve_until(&mut serving, &mut sample, "mapping runs", |_| {
        reported(&board, "mapping").last() == Some(&WorkloadState::Running)
    });
    board.tell.send(Tell::Stop("vision")).expect("tell");
    serve_until(&mut serving, &mut sample, "vision is gone", |_| {
        reported(&board, "vision").last() == Some(&WorkloadState::Gone)
    });
    assert_eq!(
        reported(&board, "vision"),
        [
            WorkloadState::Starting,
            WorkloadState::Running,
            WorkloadState::Stopping,
            WorkloadState::Gone
        ]
    );
    // Nothing is reported while nothing changes.
    let before = board.reports().len();
    for _ in 0..20 {
        serving
            .turn(&mut sample, Duration::from_millis(5))
            .expect("turn");
    }
    assert_eq!(board.reports().len(), before);

    // Orion restarts. The run carries on, and when the executor is back it says everything
    // it has, so Orion can take the run back by name.
    board.stop();
    let lost = loop {
        match serving.turn(&mut sample, Duration::from_millis(20)) {
            Ok(()) => {}
            Err(error) => break error,
        }
    };
    assert!(lost.is_lost(), "{lost}");
    assert_eq!(
        sample.running.get("mapping").map(|run| run.0),
        Some(WorkloadState::Running)
    );
    board = Board::start(&dir);
    client
        .session(|session| session.reconnect(|_| true))
        .expect("back");
    serving.start(&sample).expect("start");
    serve_until(&mut serving, &mut sample, "the inventory is heard", |_| {
        !board.heard.lock().expect("lock").inventories.is_empty()
    });
    assert_eq!(
        board.heard.lock().expect("lock").inventories,
        [vec![("mapping".to_owned(), WorkloadState::Running)]]
    );
    assert!(
        board.reports().is_empty(),
        "the inventory said it all: nothing changed since"
    );

    // A kill ends it now.
    board.tell.send(Tell::Kill("mapping")).expect("tell");
    serve_until(&mut serving, &mut sample, "mapping is gone", |_| {
        reported(&board, "mapping").last() == Some(&WorkloadState::Gone)
    });
    assert!(sample.running.is_empty());
    drop(board);
    let _ = std::fs::remove_dir_all(dir);
}
