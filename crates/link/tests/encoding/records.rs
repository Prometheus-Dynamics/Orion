//! Revision 3 messages for the fixture: the executor role and record reads.

use orion_control_plane::{
    DesiredState, ExecutorRecord, RemoteBinding, ResourceBinding, RestartPolicy, WorkloadConfig,
    WorkloadObservedState, WorkloadRecord,
};
use orion_core::{ArtifactId, ExecutorId, NodeId, RuntimeType, WorkloadId};
use orion_link::message::{
    ExecutorState, Message, ObservedWorkload, Record, RecordFilter, RecordKey, RecordKind,
    RecordRequest, Records, TypedConfigValue, Workloads,
};

pub(super) fn executor() -> ExecutorRecord {
    ExecutorRecord::builder(
        ExecutorId::new("executor.camera-stack"),
        NodeId::new("node-a"),
    )
    .runtime_type(RuntimeType::new("graph.exec.v1"))
    .build()
}

/// A workload as the wire carries it: no requirements, no placement.
pub(super) fn workload() -> WorkloadRecord {
    let mut config = WorkloadConfig::new("camera.controller.v1");
    config
        .payload
        .insert("width".into(), TypedConfigValue::UInt(1_280));
    WorkloadRecord {
        workload_id: WorkloadId::new("workload.camera-controller"),
        runtime_type: RuntimeType::new("graph.exec.v1"),
        artifact_id: ArtifactId::new("artifact.camera-controller"),
        config: Some(config),
        desired_state: DesiredState::Running,
        observed_state: WorkloadObservedState::Assigned,
        assigned_node_id: Some(NodeId::new("node-a")),
        requirements: Vec::new(),
        resource_bindings: vec![
            ResourceBinding::new("resource.camera.raw.front", "node-a"),
            ResourceBinding {
                remote: Some(RemoteBinding {
                    endpoints: vec!["tcp://10.0.0.2:7000".into()],
                    available: true,
                }),
                ..ResourceBinding::new("resource.lidar.top", "node-b")
            },
        ],
        restart_policy: RestartPolicy::OnFailure,
        placement: None,
    }
}

pub(super) fn record_messages(
    provider: orion_link::message::ProviderRecord,
    resource: orion_link::message::ResourceRecord,
) -> Vec<(&'static str, Message)> {
    vec![
        (
            "executor_state",
            Message::ExecutorState(Box::new(ExecutorState {
                executor: executor(),
                workloads: vec![ObservedWorkload {
                    workload_id: WorkloadId::new("workload.camera-controller"),
                    state: WorkloadObservedState::Running,
                }],
                resources: Vec::new(),
            })),
        ),
        (
            "workloads",
            Message::Workloads(Workloads {
                bootstrap: true,
                more: false,
                workloads: vec![workload()],
                removed: vec![WorkloadId::new("workload.old")],
            }),
        ),
        (
            "record_query",
            Message::RecordQuery(RecordRequest {
                req: 11,
                filter: RecordFilter {
                    kind: RecordKind::Workload,
                    id: None,
                    node: Some("node-a".into()),
                },
            }),
        ),
        (
            "record_watch",
            Message::RecordWatch(RecordRequest {
                req: 12,
                filter: RecordFilter::all(RecordKind::Resource),
            }),
        ),
        (
            "records",
            Message::Records(Records {
                req: 12,
                bootstrap: false,
                more: false,
                updated: vec![
                    Record::Workload(workload()),
                    Record::Resource(resource),
                    Record::Provider(provider),
                    Record::Executor(executor()),
                ],
                removed: vec![RecordKey {
                    kind: RecordKind::Resource,
                    id: "resource.gone".into(),
                }],
            }),
        ),
    ]
}
