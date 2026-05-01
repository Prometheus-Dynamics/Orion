use std::path::PathBuf;

use orion_client::{ClientIdentity, ClientRole, ControlPlaneEventStream, SessionConfig};
use orion_control_plane::{
    ArtifactRecord, ClientEventKind, ControlMessage, DesiredState, DesiredStateMutation,
    MutationBatch, StateSnapshot, SyncRequest, WorkloadRecord,
};
use orion_core::{ArtifactId, NodeId, Revision, RuntimeType, WorkloadId};
use orion_transport_http::{HttpClient, HttpRequestPayload, HttpResponsePayload};
use orion_transport_ipc::LocalAddress;

#[path = "support/common.rs"]
mod common;

#[tokio::main]
async fn main() {
    common::exit_on_error(run()).await;
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let [
        http_base,
        socket_path,
        client_name,
        artifact_id,
        workload_id,
        assigned_node,
        runtime_type,
    ] = common::read_exact_args::<7>()?;
    let client = HttpClient::try_new(http_base)?;
    let mut stream = ControlPlaneEventStream::connect(
        PathBuf::from(&socket_path),
        ClientIdentity::new(client_name.clone(), ClientRole::ControlPlane),
        SessionConfig::new(LocalAddress::new(format!("{client_name}.control"))),
    )
    .await?;
    stream.subscribe_state(Revision::ZERO).await?;

    let snapshot = fetch_snapshot(&client).await?;
    let response = client
        .send(&HttpRequestPayload::Control(Box::new(
            ControlMessage::Mutations(MutationBatch {
                base_revision: snapshot.state.desired.revision,
                mutations: vec![
                    DesiredStateMutation::PutArtifact(
                        ArtifactRecord::builder(ArtifactId::new(artifact_id.clone())).build(),
                    ),
                    DesiredStateMutation::PutWorkload(
                        WorkloadRecord::builder(
                            WorkloadId::new(workload_id.as_str()),
                            RuntimeType::new(runtime_type.clone()),
                            ArtifactId::new(artifact_id.clone()),
                        )
                        .desired_state(DesiredState::Stopped)
                        .assigned_to(NodeId::new(assigned_node))
                        .build(),
                    ),
                ],
            }),
        )))
        .await?;
    if response != HttpResponsePayload::Accepted {
        return Err(format!("unexpected mutation response: {response:?}").into());
    }

    let events = stream.next_events().await?;
    let Some(event) = events.first() else {
        return Err("no control-plane events received".into());
    };
    match &event.event {
        ClientEventKind::StateSnapshot(snapshot) => {
            println!(
                "control-plane write+watch seq={} desired_rev={} artifacts={} workloads={}",
                event.sequence,
                snapshot.state.desired.revision,
                snapshot.state.desired.artifacts.len(),
                snapshot.state.desired.workloads.len()
            );
            Ok(())
        }
        other => Err(format!("unexpected control-plane event: {other:?}").into()),
    }
}

async fn fetch_snapshot(client: &HttpClient) -> Result<StateSnapshot, Box<dyn std::error::Error>> {
    match client
        .send(&HttpRequestPayload::Control(Box::new(
            ControlMessage::SyncRequest(SyncRequest {
                node_id: NodeId::new("example.control-plane"),
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
