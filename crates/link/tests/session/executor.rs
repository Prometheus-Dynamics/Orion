//! The executor role and record reads (revision 3) on the host session.

use orion_control_plane::{ExecutorRecord, WorkloadObservedState, WorkloadRecord};
use orion_core::{ArtifactId, ExecutorId, NodeId, RuntimeType, WorkloadId};
use orion_link::host::{ActionSendError, ClientRequest, HostConfig, HostEvent};
use orion_link::message::{
    ErrorCode, ExecutorState, Message, ObservedWorkload, Record, RecordFilter, RecordKind,
    RecordRequest, RejectReason, Roles, Workloads,
};

use crate::client::{Client, serving};

fn serving_executors() -> HostConfig {
    let mut config = serving();
    config.serve_executors = true;
    config
}

fn state(observed: WorkloadObservedState) -> ExecutorState {
    ExecutorState {
        executor: ExecutorRecord::builder(ExecutorId::new("executor.graph"), NodeId::new("x"))
            .runtime_type(RuntimeType::new("graph.exec.v1"))
            .build(),
        workloads: vec![ObservedWorkload {
            workload_id: WorkloadId::new("workload.a"),
            state: observed,
        }],
        resources: Vec::new(),
    }
}

fn workload(index: usize) -> WorkloadRecord {
    WorkloadRecord::builder(
        WorkloadId::new(format!("workload.{index:03}")),
        RuntimeType::new("graph.exec.v1"),
        ArtifactId::new(format!("artifact.{index:03}-{}", "x".repeat(40))),
    )
    .assigned_to(NodeId::new("node-a"))
    .build()
}

#[test]
fn the_executor_role_needs_a_host_that_serves_it_and_revision_3() {
    let mut client = Client::new(serving());
    client.hello(Roles::EXECUTOR, 3, 4096);
    assert!(matches!(
        &client.received()[..],
        [Message::Reject(RejectReason::UnsupportedRoles)]
    ));

    let mut client = Client::new(serving_executors());
    client.hello(Roles::EXECUTOR, 2, 4096);
    assert!(matches!(
        &client.received()[..],
        [Message::Reject(RejectReason::UnsupportedRoles)]
    ));

    let mut client = Client::new(serving_executors());
    client.hello(Roles::EXECUTOR.union(Roles::CLIENT), 3, 4096);
    assert!(matches!(&client.received()[..], [Message::Welcome(w)] if w.revision == 3));
    assert_eq!(
        client.host.roles(),
        Some(Roles::EXECUTOR.union(Roles::CLIENT))
    );
}

#[test]
fn executor_state_is_acknowledged_and_reported_once_per_change() {
    let mut client = Client::new(serving_executors());
    client.hello(Roles::EXECUTOR, 3, 4096);
    client.received();
    client.events();

    let running = Message::ExecutorState(Box::new(state(WorkloadObservedState::Running)));
    client.send(&running);
    assert!(matches!(&client.received()[..], [Message::Ack { .. }]));
    let events = client.events();
    assert!(
        matches!(
            &events[..],
            [HostEvent::ExecutorState { device_name, state }]
                if device_name == "console"
                    && state.workloads[0].state == WorkloadObservedState::Running
        ),
        "{events:?}"
    );
    // A retransmission is acknowledged again but not reported again.
    client.send(&running);
    assert!(matches!(&client.received()[..], [Message::Ack { .. }]));
    assert!(client.events().is_empty());
    client.send(&Message::ExecutorState(Box::new(state(
        WorkloadObservedState::Failed,
    ))));
    client.received();
    assert_eq!(client.events().len(), 1);
}

#[test]
fn executor_state_from_a_session_without_the_role_is_ignored() {
    let mut client = Client::new(serving_executors());
    client.hello(Roles::PROVIDER, 3, 4096);
    client.received();
    client.events();
    client.send(&Message::ExecutorState(Box::new(state(
        WorkloadObservedState::Running,
    ))));
    assert!(client.received().is_empty());
    assert!(client.events().is_empty());
    assert_eq!(client.host.stats().decode_errors, 1);
}

#[test]
fn workloads_go_to_an_executor_in_parts() {
    let mut client = Client::new(serving_executors());
    client.hello(Roles::EXECUTOR, 3, 512);
    client.received();
    let workloads: Vec<WorkloadRecord> = (0..12).map(workload).collect();
    let removed = vec![WorkloadId::new("workload.gone")];
    client
        .host
        .send_workloads(true, &workloads, &removed)
        .expect("queued");
    let parts: Vec<Workloads> = client
        .received()
        .into_iter()
        .map(|message| match message {
            Message::Workloads(part) => part,
            other => panic!("unexpected {other:?}"),
        })
        .collect();
    assert!(
        parts.len() > 1,
        "12 workloads do not fit one 512-byte frame"
    );
    assert!(parts.iter().all(|part| part.bootstrap));
    assert!(parts[..parts.len() - 1].iter().all(|part| part.more));
    assert!(!parts[parts.len() - 1].more);
    let got: Vec<WorkloadRecord> = parts.iter().flat_map(|p| p.workloads.clone()).collect();
    assert_eq!(got, workloads);
    let gone: Vec<WorkloadId> = parts.iter().flat_map(|p| p.removed.clone()).collect();
    assert_eq!(gone, removed);

    // Only an executor session gets workloads.
    let mut client = Client::new(serving_executors());
    client.hello(Roles::CLIENT, 3, 4096);
    client.received();
    assert_eq!(
        client.host.send_workloads(true, &workloads, &[]),
        Err(ActionSendError::NotConnected)
    );
}

#[test]
fn record_requests_are_reported_answered_in_parts_or_refused() {
    let filter = RecordFilter {
        kind: RecordKind::Workload,
        id: None,
        node: Some("node-a".into()),
    };
    let mut client = Client::new(serving_executors());
    client.hello(Roles::CLIENT, 3, 512);
    client.received();
    client.events();
    client.send(&Message::RecordWatch(RecordRequest {
        req: 4,
        filter: filter.clone(),
    }));
    assert_eq!(
        client.events(),
        vec![HostEvent::ClientRequest {
            device_name: "console".into(),
            request: ClientRequest::RecordWatch {
                req: 4,
                filter: filter.clone()
            },
        }]
    );
    let records: Vec<Record> = (0..12).map(|i| Record::Workload(workload(i))).collect();
    client
        .host
        .send_records(4, true, &records, &[])
        .expect("queued");
    let got: Vec<Record> = client
        .received()
        .into_iter()
        .flat_map(|message| match message {
            Message::Records(part) => {
                assert_eq!(part.req, 4);
                part.updated
            }
            other => panic!("unexpected {other:?}"),
        })
        .collect();
    assert_eq!(got, records);

    // A link that does not serve records refuses them.
    let mut client = Client::new(serving());
    client.hello(Roles::CLIENT, 3, 4096);
    client.received();
    client.send(&Message::RecordQuery(RecordRequest {
        req: 5,
        filter: filter.clone(),
    }));
    assert!(matches!(
        &client.received()[..],
        [Message::RequestError(error)] if error.req == 5 && error.code == ErrorCode::UNSUPPORTED
    ));

    // A revision-2 session ignores them, as any kind of a later revision.
    let mut client = Client::new(serving_executors());
    client.hello(Roles::CLIENT, 2, 4096);
    client.received();
    client.events();
    client.send(&Message::RecordQuery(RecordRequest { req: 6, filter }));
    assert!(client.received().is_empty());
    assert!(client.events().is_empty());
}
