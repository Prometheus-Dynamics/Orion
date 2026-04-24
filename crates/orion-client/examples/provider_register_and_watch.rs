use orion_client::{LocalNodeRuntime, LocalProviderService};
use orion_control_plane::{
    AvailabilityState, ControlMessage, DesiredStateMutation, LeaseRecord, LeaseState,
    MutationBatch, ProviderRecord, ResourceRecord, StateSnapshot, SyncRequest,
};
use orion_core::{NodeId, Revision};
use orion_transport_http::{HttpClient, HttpRequestPayload, HttpResponsePayload};
use tokio::time::{Duration, Instant, sleep};

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
    let runtime = LocalNodeRuntime::new(ipc_socket, stream_socket);
    let service = LocalProviderService::new(runtime.clone(), client_name, provider);
    service.register().await?;
    runtime
        .provider(
            service.client_name().to_string(),
            service.provider().clone(),
        )?
        .publish_resources(vec![resource.clone()])
        .await?;
    let mut subscription = service.subscribe(Revision::ZERO).await?;

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

    let _ = tokio::time::timeout(Duration::from_millis(250), subscription.next_event()).await;
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let snapshot = fetch_snapshot(&client).await?;
        if snapshot
            .state
            .desired
            .leases
            .contains_key(&resource.resource_id)
        {
            println!(
                "provider register+watch provider={} target_resource={} desired_leases={}",
                service.provider().provider_id.as_str(),
                resource_id,
                snapshot.state.desired.leases.len(),
            );
            return Ok(());
        }

        if Instant::now() >= deadline {
            return Err(format!(
                "timed out waiting for lease {resource_id} after successful mutation"
            )
            .into());
        }
        sleep(Duration::from_millis(100)).await;
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
