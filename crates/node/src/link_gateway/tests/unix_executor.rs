//! The executor role and record reads on Unix links (interface revision 3): an executor session
//! gets the workloads assigned to it and reports their state; a client reads and watches records.

use super::serial::{WAIT, start};
use super::unix::{socket_path, unix_link};
use super::unix_client::RawClient;
use super::*;
use orion::control_plane::{DesiredState, ExecutorRecord, WorkloadObservedState, WorkloadRecord};
use orion::{ArtifactId, ExecutorId, NodeId, ProviderId, RuntimeType, WorkloadId};
use orion_link::message::{
    ExecutorState, Message, ObservedWorkload, ProviderState, Record, RecordFilter, RecordKind,
    RecordRequest, Roles, Workloads,
};

const RUNTIME: &str = "graph.exec.v1";

fn executor_state(observed: Vec<ObservedWorkload>) -> Message {
    Message::ExecutorState(Box::new(ExecutorState {
        // The node id is the gateway's to set.
        executor: ExecutorRecord::builder(ExecutorId::new("executor.graph"), "elsewhere")
            .runtime_type(RuntimeType::new(RUNTIME))
            .build(),
        workloads: observed,
        resources: Vec::new(),
    }))
}

fn put_workload(app: &NodeApp, id: &str) {
    let mut desired = app.state_snapshot().state.desired;
    desired.put_workload(
        WorkloadRecord::builder(
            WorkloadId::new(id),
            RuntimeType::new(RUNTIME),
            ArtifactId::new("artifact.graph"),
        )
        .desired_state(DesiredState::Running)
        .assigned_to(NodeId::new(NODE))
        .build(),
    );
    app.replace_desired(desired);
}

async fn next_workloads(client: &mut RawClient) -> Workloads {
    match client.next().await {
        Message::Workloads(workloads) => workloads,
        other => panic!("unexpected {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_executor_session_gets_its_workloads_and_reports_their_state() {
    let name = "unix-executor";
    let app = test_app(name);
    let path = socket_path(name);
    let gateway = start(&app, vec![unix_link(&path, "")]);
    let mut executor = RawClient::connect_as("graph-exec", &path, Roles::EXECUTOR).await;

    executor.send(executor_state(Vec::new()));
    let first = next_workloads(&mut executor).await;
    assert!(first.bootstrap && first.workloads.is_empty(), "{first:?}");
    let record = app
        .state_snapshot()
        .state
        .desired
        .executors
        .get(&ExecutorId::new("executor.graph"))
        .cloned()
        .expect("the executor is registered");
    assert_eq!(record.node_id.as_str(), NODE, "the gateway sets the node");

    put_workload(&app, "workload.a");
    let assigned = next_workloads(&mut executor).await;
    assert!(!assigned.bootstrap);
    assert_eq!(
        assigned
            .workloads
            .iter()
            .map(|w| w.workload_id.as_str())
            .collect::<Vec<_>>(),
        ["workload.a"]
    );

    executor.send(executor_state(vec![ObservedWorkload {
        workload_id: WorkloadId::new("workload.a"),
        state: WorkloadObservedState::Running,
    }]));
    assert!(
        wait_until(WAIT, || {
            app.state_snapshot()
                .state
                .observed
                .workloads
                .get(&WorkloadId::new("workload.a"))
                .is_some_and(|w| w.observed_state == WorkloadObservedState::Running)
        })
        .await,
        "the reported state reaches the node"
    );

    // Unassigned: the executor hears the id.
    let mut desired = app.state_snapshot().state.desired;
    desired.workloads.remove(&WorkloadId::new("workload.a"));
    app.replace_desired(desired);
    loop {
        let change = next_workloads(&mut executor).await;
        if change.removed.iter().any(|id| id.as_str() == "workload.a") {
            break;
        }
    }
    gateway.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_client_reads_and_watches_records() {
    let name = "unix-records";
    let app = test_app(name);
    let path = socket_path(name);
    let gateway = start(&app, vec![unix_link(&path, "")]);
    let provider_id = "provider.imu-board";
    let publisher = RawClient::connect_as("imu-board", &path, Roles::PROVIDER).await;
    publisher.send(Message::ProviderState(ProviderState {
        provider: device_provider(provider_id),
        resources: vec![device_resource("imu-board.imu-0", provider_id, "unix")],
    }));
    assert!(wait_until(WAIT, || has_provider(&app, provider_id)).await);

    let mut client = RawClient::connect("console", &path).await;
    client.send(Message::RecordQuery(RecordRequest {
        req: 1,
        filter: RecordFilter {
            kind: RecordKind::Provider,
            id: None,
            node: Some(NODE.into()),
        },
    }));
    match client.next().await {
        Message::Records(records) => {
            assert!(records.bootstrap && !records.more);
            assert!(matches!(
                &records.updated[..],
                [Record::Provider(p)] if p.provider_id == ProviderId::new(provider_id)
            ));
        }
        other => panic!("unexpected {other:?}"),
    }

    client.send(Message::RecordWatch(RecordRequest {
        req: 2,
        filter: RecordFilter::all(RecordKind::Resource),
    }));
    match client.next().await {
        Message::Records(records) => {
            assert!(records.bootstrap);
            assert_eq!(records.updated.len(), 1, "{records:?}");
        }
        other => panic!("unexpected {other:?}"),
    }
    // One more resource: the change carries it alone.
    publisher.send(Message::ProviderState(ProviderState {
        provider: device_provider(provider_id),
        resources: vec![
            device_resource("imu-board.imu-0", provider_id, "unix"),
            device_resource("imu-board.imu-1", provider_id, "unix"),
        ],
    }));
    loop {
        match client.next().await {
            Message::Records(records) if records.req == 2 => {
                assert!(!records.bootstrap);
                let ids: Vec<String> = records
                    .updated
                    .iter()
                    .map(|record| record.key().id)
                    .collect();
                if ids.iter().any(|id| id == "imu-board.imu-1") {
                    assert!(!ids.iter().any(|id| id == "imu-board.imu-0"), "{ids:?}");
                    break;
                }
            }
            other => panic!("unexpected {other:?}"),
        }
    }
    gateway.shutdown().await;
}
