use std::path::PathBuf;

use orion_client::{ClientIdentity, ClientRole, ProviderEventStream, SessionConfig};
use orion_control_plane::{
    AvailabilityState, ClientEventKind, ClientHello, ControlMessage, DesiredStateMutation,
    LeaseRecord, LeaseState, MutationBatch, ProviderRecord, ProviderStateUpdate, ResourceRecord,
    StateSnapshot, SyncRequest,
};
use orion_core::{NodeId, Revision};
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
        provider_id,
        node_id,
        resource_type,
        resource_id,
    ] = common::read_exact_args::<8>()?;
    let provider = ProviderRecord::builder(provider_id.as_str(), node_id.as_str())
        .resource_type(resource_type.as_str())
        .build();
    let resource = ResourceRecord::builder(
        resource_id.as_str(),
        resource_type.as_str(),
        provider_id.as_str(),
    )
    .availability(AvailabilityState::Available)
    .lease_state(LeaseState::Leased)
    .build();
    let unary = UnixControlClient::new(&ipc_socket);
    let source = LocalAddress::new(format!("{client_name}.provider"));

    let response = unary
        .send(ControlEnvelope {
            source: source.clone(),
            destination: LocalAddress::new("orion"),
            message: ControlMessage::ClientHello(ClientHello {
                client_name: client_name.clone().into(),
                role: ClientRole::Provider,
            }),
        })
        .await?;
    match response.message {
        ControlMessage::ClientWelcome(_) => {}
        other => return Err(format!("expected provider welcome, got {other:?}").into()),
    }

    let response = unary
        .send(ControlEnvelope {
            source: source.clone(),
            destination: LocalAddress::new("orion"),
            message: ControlMessage::ProviderState(ProviderStateUpdate {
                provider: provider.clone(),
                resources: vec![resource.clone()],
            }),
        })
        .await?;
    match response.message {
        ControlMessage::Accepted => {}
        other => return Err(format!("expected provider accepted, got {other:?}").into()),
    }

    let mut stream = ProviderEventStream::connect(
        PathBuf::from(&stream_socket),
        ClientIdentity::new(client_name.clone(), ClientRole::Provider),
        SessionConfig::new(LocalAddress::new(format!("{client_name}.provider-stream"))),
    )
    .await?;
    stream.subscribe_leases(&provider).await?;

    let client = HttpClient::try_new(http_base)?;
    let snapshot = fetch_snapshot(&client).await?;
    let response = client
        .send(&HttpRequestPayload::Control(Box::new(
            ControlMessage::Mutations(MutationBatch {
                base_revision: snapshot.state.desired.revision,
                mutations: vec![DesiredStateMutation::PutLease(
                    LeaseRecord::builder(resource.resource_id.clone())
                        .lease_state(LeaseState::Leased)
                        .holder_node(NodeId::new(node_id))
                        .build(),
                )],
            }),
        )))
        .await?;
    if response != HttpResponsePayload::Accepted {
        return Err(format!("unexpected lease mutation response: {response:?}").into());
    }

    let events = stream.next_events().await?;
    let Some(event) = events.first() else {
        return Err("no provider events received".into());
    };
    match &event.event {
        ClientEventKind::ProviderLeases {
            provider_id,
            leases,
        } => {
            println!(
                "provider register+watch seq={} provider={} leases={} resources={} target_resource={}",
                event.sequence,
                provider_id,
                leases.len(),
                leases
                    .iter()
                    .map(|lease| lease.resource_id.as_str())
                    .collect::<Vec<_>>()
                    .join(","),
                resource_id,
            );
            Ok(())
        }
        other => Err(format!("unexpected provider event: {other:?}").into()),
    }
}

async fn fetch_snapshot(client: &HttpClient) -> Result<StateSnapshot, Box<dyn std::error::Error>> {
    match client
        .send(&HttpRequestPayload::Control(Box::new(
            ControlMessage::SyncRequest(SyncRequest {
                node_id: NodeId::new("example.provider"),
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
