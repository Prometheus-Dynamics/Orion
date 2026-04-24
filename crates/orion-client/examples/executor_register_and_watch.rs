use orion_client::{LocalExecutorEvent, LocalExecutorService, LocalNodeRuntime};
use orion_control_plane::{
    ArtifactRecord, ControlMessage, DesiredState, DesiredStateMutation, ExecutorRecord,
    MutationBatch, StateSnapshot, SyncRequest, WorkloadRecord,
};
use orion_core::{ArtifactId, NodeId, Revision, RuntimeType, WorkloadId};
use orion_transport_http::{HttpClient, HttpRequestPayload, HttpResponsePayload};

#[path = "support/common.rs"]
mod common;

#[tokio::main]
async fn main() {
    common::exit_on_error(run()).await;
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let [
        http_base,
        ipc_socket,
        stream_socket,
        client_name,
        executor_id,
        node_id,
        runtime_type,
        artifact_id,
        workload_id,
    ] = common::read_exact_args::<9>()?;
    let executor = ExecutorRecord::builder(executor_id.as_str(), node_id.as_str())
        .runtime_type(runtime_type.as_str())
        .build();
    let runtime = LocalNodeRuntime::new(ipc_socket, stream_socket);
    let service = LocalExecutorService::new(runtime, client_name, executor);
    service.register().await?;
    let mut subscription = service.subscribe_workloads().await?;

    let client = HttpClient::try_new(http_base)?;
    let snapshot = fetch_snapshot(&client).await?;
    let response = client
        .send(&HttpRequestPayload::Control(Box::new(
            ControlMessage::Mutations(MutationBatch {
                base_revision: snapshot.state.desired.revision,
                mutations: vec![
                    DesiredStateMutation::PutArtifact(
                        ArtifactRecord::builder(artifact_id.as_str()).build(),
                    ),
                    DesiredStateMutation::PutWorkload(
                        WorkloadRecord::builder(
                            WorkloadId::new(workload_id.as_str()),
                            RuntimeType::new(runtime_type),
                            ArtifactId::new(artifact_id.as_str()),
                        )
                        .desired_state(DesiredState::Stopped)
                        .assigned_to(NodeId::new(node_id))
                        .build(),
                    ),
                ],
            }),
        )))
        .await?;
    if response != HttpResponsePayload::Accepted {
        return Err(format!("unexpected workload mutation response: {response:?}").into());
    }

    loop {
        match subscription.next_event().await? {
            LocalExecutorEvent::Bootstrap(_) => {}
            LocalExecutorEvent::WorkloadsChanged {
                sequence,
                workloads,
            } => {
                let ids = workloads
                    .iter()
                    .map(|workload| workload.workload_id.as_str())
                    .collect::<Vec<_>>();
                if !ids.iter().any(|id| *id == workload_id) {
                    continue;
                }
                let executor_id = service.executor().executor_id.as_str();
                println!(
                    "executor register+watch seq={} executor={} workloads={} ids={} target_workload={}",
                    sequence,
                    executor_id,
                    workloads.len(),
                    ids.join(","),
                    workload_id,
                );
                return Ok(());
            }
        }
    }
}

async fn fetch_snapshot(client: &HttpClient) -> Result<StateSnapshot, Box<dyn std::error::Error>> {
    match client
        .send(&HttpRequestPayload::Control(Box::new(
            ControlMessage::SyncRequest(SyncRequest {
                node_id: NodeId::new("example.executor"),
                desired_revision: Revision::new(u64::MAX),
                desired_fingerprint: 0,
                desired_summary: None,
                sections: Vec::new(),
                object_selectors: Vec::new(),
            }),
        )))
        .await?
    {
        HttpResponsePayload::Snapshot(snapshot) => Ok(snapshot),
        other => Err(format!("expected snapshot response, got {other:?}").into()),
    }
}
