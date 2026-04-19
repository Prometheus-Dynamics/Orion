use std::path::PathBuf;

use orion_client::{ClientIdentity, ClientRole, ExecutorEventStream, SessionConfig};
use orion_control_plane::{
    ArtifactRecord, ClientEventKind, ClientHello, ControlMessage, DesiredState,
    DesiredStateMutation, ExecutorRecord, ExecutorStateUpdate, MutationBatch, StateSnapshot,
    SyncRequest, WorkloadRecord,
};
use orion_core::{ArtifactId, NodeId, Revision, RuntimeType, WorkloadId};
use orion_transport_http::{HttpClient, HttpRequestPayload, HttpResponsePayload};
use orion_transport_ipc::{ControlEnvelope, LocalAddress, UnixControlClient};

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
    let unary = UnixControlClient::new(&ipc_socket);
    let source = LocalAddress::new(format!("{client_name}.executor"));

    let response = unary
        .send(ControlEnvelope {
            source: source.clone(),
            destination: LocalAddress::new("orion"),
            message: ControlMessage::ClientHello(ClientHello {
                client_name: client_name.clone().into(),
                role: ClientRole::Executor,
            }),
        })
        .await?;
    match response.message {
        ControlMessage::ClientWelcome(_) => {}
        other => return Err(format!("expected executor welcome, got {other:?}").into()),
    }

    let response = unary
        .send(ControlEnvelope {
            source: source.clone(),
            destination: LocalAddress::new("orion"),
            message: ControlMessage::ExecutorState(ExecutorStateUpdate {
                executor: executor.clone(),
                workloads: Vec::new(),
                resources: Vec::new(),
            }),
        })
        .await?;
    match response.message {
        ControlMessage::Accepted => {}
        other => return Err(format!("expected executor accepted, got {other:?}").into()),
    }

    let mut stream = ExecutorEventStream::connect(
        PathBuf::from(&stream_socket),
        ClientIdentity::new(client_name.clone(), ClientRole::Executor),
        SessionConfig::new(LocalAddress::new(format!("{client_name}.executor-stream"))),
    )
    .await?;
    stream.subscribe_workloads(&executor).await?;

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

    let events = stream.next_events().await?;
    let Some(event) = events.first() else {
        return Err("no executor events received".into());
    };
    match &event.event {
        ClientEventKind::ExecutorWorkloads {
            executor_id,
            workloads,
        } => {
            println!(
                "executor register+watch seq={} executor={} workloads={} ids={} target_workload={}",
                event.sequence,
                executor_id,
                workloads.len(),
                workloads
                    .iter()
                    .map(|workload| workload.workload_id.as_str())
                    .collect::<Vec<_>>()
                    .join(","),
                workload_id,
            );
            Ok(())
        }
        other => Err(format!("unexpected executor event: {other:?}").into()),
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
