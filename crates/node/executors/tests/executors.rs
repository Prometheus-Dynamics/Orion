//! Workloads on the wire, without a wire: what each message does, to whom.

use orion_executors::{RestartConfig, Workloads};
use orion_msg_records::RecordClass;
use orion_msg_workloads::{
    ExecutorAsk, ExecutorInventory, ExecutorKill, ExecutorOffer, ExecutorReport, ExecutorStart,
    ExecutorStop, InventoryItem, OfferedKind, RestartPolicy, WorkloadPut, WorkloadRemove,
    WorkloadState, WorkloadStatus,
};
use orion_reply::{Asked, Out};
use orion_store::{Store, StoreConfig};
use orion_types::{Error, NodeName, WorkloadName};
use orion_wire::body::encode_fields;
use orion_wire::{Body, Bytes, Decode, List, kind};

const OPERATOR: u32 = 10;
const EXECUTOR: u32 = 20;
const OTHER: u32 = 30;
const WALL: u64 = 1_700_000_000_000;

#[derive(Default)]
struct Sent {
    frames: Vec<(u32, u8, Vec<u8>)>,
    scratch: Vec<u8>,
}

impl Out for Sent {
    fn frame(&mut self, session: u32, kind: u8, payload: &[u8], _may_drop: bool) {
        self.frames.push((session, kind, payload.to_vec()));
    }

    fn scratch(&mut self) -> &mut Vec<u8> {
        if self.scratch.is_empty() {
            self.scratch = vec![0; 4096];
        }
        &mut self.scratch
    }
}

fn me() -> NodeName {
    "board".parse().expect("name")
}

fn name(text: &str) -> WorkloadName {
    text.parse().expect("name")
}

struct Bench {
    workloads: Workloads,
    store: Store,
    sent: Sent,
    now: u64,
}

impl Bench {
    fn new() -> Self {
        let store = Store::new(me(), StoreConfig::default());
        Self {
            workloads: Workloads::restored(me(), RestartConfig::default(), &store, 0),
            store,
            sent: Sent::default(),
            now: 0,
        }
    }

    fn ask<B: Body>(&mut self, from: u32, body: &B) -> Result<(), Error> {
        let mut payload = vec![0_u8; 4096];
        let len = encode_fields(body, &mut payload).expect("encode");
        let asked = Asked {
            session: from,
            seq: 1,
            kind: B::KIND,
            class: 4,
            uid: 0,
            caller: "",
            payload: &payload[..len],
        };
        self.workloads
            .request(&asked, &mut self.store, self.now, WALL, &mut self.sent)
    }

    fn put(&mut self, from: u32, workload: &str, node: Option<&str>) -> Result<(), Error> {
        self.ask(
            from,
            &WorkloadPut {
                name: name(workload),
                kind: "graph".parse().expect("kind"),
                spec: Bytes(b"spec"),
                files: List::new(&[]),
                node: node.map(|node| node.parse().expect("node")),
                labels: List::new(&[]),
                cpus: Bytes(&[0b11]),
                restart: RestartPolicy::Always,
            },
        )
    }

    /// An executor for `graph` offers and says what it has.
    fn executor(&mut self, session: u32, has: &[(&str, WorkloadState)]) -> Result<(), Error> {
        let kinds = [OfferedKind {
            kind: "graph".parse().expect("kind"),
        }];
        self.ask(
            session,
            &ExecutorOffer {
                kinds: List::new(&kinds),
            },
        )?;
        let items: Vec<InventoryItem<'_>> = has
            .iter()
            .map(|(workload, state)| InventoryItem {
                name: name(workload),
                state: *state,
                detail: "",
            })
            .collect();
        self.ask(
            session,
            &ExecutorInventory {
                workloads: List::new(&items),
            },
        )
    }

    fn report(&mut self, from: u32, workload: &str, state: WorkloadState) -> Result<(), Error> {
        self.ask(
            from,
            &ExecutorReport {
                name: name(workload),
                state,
                detail: "",
            },
        )
    }

    /// What was sent of `kind`, to whom, in order, taken out of what was sent.
    fn took(&mut self, kind: u8) -> Vec<(u32, Vec<u8>)> {
        let (these, rest): (Vec<_>, Vec<_>) = std::mem::take(&mut self.sent.frames)
            .into_iter()
            .partition(|(_, sent, _)| *sent == kind);
        self.sent.frames = rest;
        these
            .into_iter()
            .map(|(to, _, payload)| (to, payload))
            .collect()
    }

    /// The states `session` was told of a workload, in order.
    fn told(&mut self, session: u32) -> Vec<WorkloadState> {
        self.took(kind::WORKLOAD_STATUS)
            .into_iter()
            .filter(|(to, _)| *to == session)
            .map(|(_, payload)| WorkloadStatus::decode(&payload).expect("status").state)
            .collect()
    }

    fn kept(&self) -> usize {
        self.store.list(RecordClass::Workload, "").count()
    }
}

#[test]
fn a_put_is_kept_started_and_told() {
    let mut bench = Bench::new();
    bench.put(OPERATOR, "a", None).expect("put");
    assert_eq!(bench.told(OPERATOR), [WorkloadState::Pending]);
    assert_eq!(bench.kept(), 1);
    assert!(bench.took(kind::EXECUTOR_START).is_empty());

    // An executor offers the kind and is asked what it has. When it has said, the workload
    // is started on it, with its kind, spec and CPUs.
    bench.executor(EXECUTOR, &[]).expect("executor");
    let asked = bench.took(kind::EXECUTOR_ASK);
    assert_eq!(asked.len(), 1);
    assert_eq!(asked[0].0, EXECUTOR);
    assert_eq!(ExecutorAsk::decode(&asked[0].1).expect("ask").kinds, 1);
    let started = bench.took(kind::EXECUTOR_START);
    assert_eq!(started.len(), 1);
    let start = ExecutorStart::decode(&started[0].1).expect("start");
    assert_eq!(started[0].0, EXECUTOR);
    assert_eq!((start.name, start.kind.as_str()), (name("a"), "graph"));
    assert_eq!((start.spec.0, start.cpus.0), (&b"spec"[..], &[0b11][..]));
    assert_eq!(bench.told(OPERATOR), [WorkloadState::Starting]);

    bench
        .report(EXECUTOR, "a", WorkloadState::Running)
        .expect("report");
    assert_eq!(bench.told(OPERATOR), [WorkloadState::Running]);
    // Put again as it is: nothing changes, and the asker hears where it is.
    bench.put(OTHER, "a", None).expect("put");
    assert_eq!(bench.told(OTHER), [WorkloadState::Running]);
    assert!(bench.took(kind::EXECUTOR_START).is_empty());
    // From now on both are told.
    bench
        .report(EXECUTOR, "a", WorkloadState::Stopping)
        .expect("report");
    assert_eq!(bench.told(OPERATOR), [WorkloadState::Stopping]);
}

#[test]
fn a_remove_stops_it_and_the_record_goes_when_the_run_has() {
    let mut bench = Bench::new();
    bench.executor(EXECUTOR, &[]).expect("executor");
    bench.put(OPERATOR, "a", None).expect("put");
    bench
        .report(EXECUTOR, "a", WorkloadState::Running)
        .expect("report");
    bench.sent.frames.clear();

    bench
        .ask(
            OTHER,
            &WorkloadRemove {
                name: name("a"),
                force: false,
            },
        )
        .expect("remove");
    let stopped = bench.took(kind::EXECUTOR_STOP);
    assert_eq!(stopped.len(), 1);
    assert_eq!(
        ExecutorStop::decode(&stopped[0].1).expect("stop").name,
        name("a")
    );
    assert_eq!(bench.told(OTHER), [WorkloadState::Stopping]);
    assert_eq!(bench.kept(), 1, "the record stays");
    bench
        .report(EXECUTOR, "a", WorkloadState::Gone)
        .expect("report");
    assert_eq!(bench.kept(), 0);
    // Whoever removed it hears that it is gone, and so does whoever put it.
    assert_eq!(bench.told(OPERATOR), [WorkloadState::Gone]);
    bench.sent.frames.clear();
    assert_eq!(
        bench.ask(
            OTHER,
            &WorkloadRemove {
                name: name("a"),
                force: false
            }
        ),
        Err(Error::NotFound)
    );
}

#[test]
fn a_forced_remove_drops_the_record_and_has_the_run_killed() {
    let mut bench = Bench::new();
    bench.executor(EXECUTOR, &[]).expect("executor");
    bench.put(OPERATOR, "a", None).expect("put");
    bench
        .report(EXECUTOR, "a", WorkloadState::Running)
        .expect("report");
    bench.sent.frames.clear();
    bench
        .ask(
            OPERATOR,
            &WorkloadRemove {
                name: name("a"),
                force: true,
            },
        )
        .expect("remove");
    let killed = bench.took(kind::EXECUTOR_KILL);
    assert_eq!(killed.len(), 1);
    assert_eq!(
        ExecutorKill::decode(&killed[0].1).expect("kill").name,
        name("a")
    );
    assert_eq!(bench.kept(), 0);
    assert_eq!(bench.told(OPERATOR), [WorkloadState::Gone]);
}

#[test]
fn what_an_executor_has_is_adopted_and_what_orion_does_not_want_is_stopped() {
    let mut bench = Bench::new();
    bench.put(OPERATOR, "wanted", None).expect("put");
    bench.sent.frames.clear();
    bench
        .executor(
            EXECUTOR,
            &[
                ("wanted", WorkloadState::Running),
                ("stray", WorkloadState::Running),
            ],
        )
        .expect("executor");
    assert!(
        bench.took(kind::EXECUTOR_START).is_empty(),
        "not started twice"
    );
    assert_eq!(bench.told(OPERATOR), [WorkloadState::Running]);
    let stopped = bench.took(kind::EXECUTOR_STOP);
    assert_eq!(stopped.len(), 1);
    assert_eq!(
        ExecutorStop::decode(&stopped[0].1).expect("stop").name,
        name("stray")
    );
    // A report of something Orion does not want is answered the same way.
    bench
        .report(EXECUTOR, "another", WorkloadState::Running)
        .expect("report");
    assert_eq!(bench.took(kind::EXECUTOR_STOP).len(), 1);
}

#[test]
fn a_kind_has_one_executor_and_only_it_speaks_for_its_workloads() {
    let mut bench = Bench::new();
    bench.executor(EXECUTOR, &[]).expect("executor");
    assert_eq!(bench.executor(OTHER, &[]), Err(Error::AlreadyExists));
    bench.put(OPERATOR, "a", None).expect("put");
    // A program that is no executor cannot report or say what it has.
    assert_eq!(
        bench.report(OTHER, "a", WorkloadState::Running),
        Err(Error::Denied)
    );
    assert_eq!(
        bench.ask(
            OTHER,
            &ExecutorInventory {
                workloads: List::new(&[])
            }
        ),
        Err(Error::Denied)
    );
    assert_eq!(bench.workloads.executors(), 1);
    // The executor goes: its kind is free, and its run is lost.
    bench.sent.frames.clear();
    bench
        .workloads
        .gone(EXECUTOR, &mut bench.store, bench.now, WALL, &mut bench.sent);
    assert_eq!(bench.told(OPERATOR), [WorkloadState::Lost]);
    bench.executor(OTHER, &[]).expect("the kind is free");
    assert_eq!(bench.told(OPERATOR), [WorkloadState::Starting]);
}

#[test]
fn a_workload_for_another_node_is_kept_and_not_run_here() {
    let mut bench = Bench::new();
    bench.executor(EXECUTOR, &[]).expect("executor");
    bench.sent.frames.clear();
    bench
        .put(OPERATOR, "elsewhere", Some("other"))
        .expect("put");
    assert_eq!(bench.kept(), 1);
    assert!(bench.took(kind::EXECUTOR_START).is_empty());
    let told = bench.took(kind::WORKLOAD_STATUS);
    let status = WorkloadStatus::decode(&told[0].1).expect("status");
    assert_eq!(status.state, WorkloadState::Pending);
    assert_eq!(status.detail, "it is for node other");
    assert!(bench.workloads.tracker().is_empty());
    // Named for this node, it runs here.
    bench.put(OPERATOR, "here", Some("board")).expect("put");
    assert_eq!(bench.took(kind::EXECUTOR_START).len(), 1);
    // Moved to another node, what ran here is stopped.
    bench
        .report(EXECUTOR, "here", WorkloadState::Running)
        .expect("report");
    bench.put(OPERATOR, "here", Some("other")).expect("put");
    assert_eq!(bench.took(kind::EXECUTOR_STOP).len(), 1);
    // Its record can be removed from here.
    bench
        .ask(
            OPERATOR,
            &WorkloadRemove {
                name: name("elsewhere"),
                force: false,
            },
        )
        .expect("remove");
}

#[test]
fn what_should_run_is_known_again_after_a_restart() {
    let mut bench = Bench::new();
    bench.put(OPERATOR, "a", None).expect("put");
    bench
        .put(OPERATOR, "elsewhere", Some("other"))
        .expect("put");
    // Node logic starts again over the same store.
    let mut again = Workloads::restored(me(), RestartConfig::default(), &bench.store, 0);
    assert_eq!(again.tracker().len(), 1);
    assert_eq!(
        again.tracker().get(&name("a")).expect("a").state,
        WorkloadState::Pending
    );
    // The executor that was there all along says what it has: the run is adopted.
    let mut sent = Sent::default();
    let mut ask = |workloads: &mut Workloads, store: &mut Store, kind: u8, payload: &[u8]| {
        let asked = Asked {
            session: EXECUTOR,
            seq: 1,
            kind,
            class: 4,
            uid: 0,
            caller: "",
            payload,
        };
        workloads.request(&asked, store, 0, WALL, &mut sent)
    };
    let kinds = [OfferedKind {
        kind: "graph".parse().expect("kind"),
    }];
    let mut payload = vec![0_u8; 256];
    let len = encode_fields(
        &ExecutorOffer {
            kinds: List::new(&kinds),
        },
        &mut payload,
    )
    .expect("e");
    ask(
        &mut again,
        &mut bench.store,
        kind::EXECUTOR_OFFER,
        &payload[..len],
    )
    .expect("offer");
    let items = [InventoryItem {
        name: name("a"),
        state: WorkloadState::Running,
        detail: "",
    }];
    let len = encode_fields(
        &ExecutorInventory {
            workloads: List::new(&items),
        },
        &mut payload,
    )
    .expect("e");
    ask(
        &mut again,
        &mut bench.store,
        kind::EXECUTOR_INVENTORY,
        &payload[..len],
    )
    .expect("has");
    assert_eq!(
        again.tracker().get(&name("a")).expect("a").state,
        WorkloadState::Running
    );
    assert!(
        sent.frames
            .iter()
            .all(|(_, sent, _)| *sent != kind::EXECUTOR_START),
        "it was not started again"
    );
    again.forget_sessions(0);
    assert_eq!(
        again.tracker().get(&name("a")).expect("a").state,
        WorkloadState::Lost
    );
}
