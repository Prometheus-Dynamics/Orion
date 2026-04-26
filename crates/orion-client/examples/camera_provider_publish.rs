use orion_client::{LocalNodeRuntime, LocalRuntimePublisher, ProviderResource};
use orion_control_plane::{
    AvailabilityState, HealthState, LeaseState, ProviderRecord, ResourceOwnershipMode,
};
use orion_core::{NodeId, ProviderId, ResourceId, ResourceType};

#[path = "support/common.rs"]
mod common;

#[tokio::main]
async fn main() {
    common::exit_on_error(run()).await;
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let [
        ipc_socket,
        client_name,
        provider_id,
        node_id,
        raw_resource_id,
    ] = common::read_exact_args::<5>()?;
    let provider = ProviderRecord::builder(
        ProviderId::new(provider_id.clone()),
        NodeId::new(node_id.clone()),
    )
    .resource_type(ResourceType::new("camera.device"))
    .build();
    let resource = ProviderResource::new(
        ResourceId::new(raw_resource_id.clone()),
        ResourceType::new("camera.device"),
        ProviderId::new(provider_id.clone()),
    )
    .ownership_mode(ResourceOwnershipMode::ExclusiveOwnerPublishesDerived)
    .health(HealthState::Healthy)
    .availability(AvailabilityState::Available)
    .lease_state(LeaseState::Unleased)
    .build();

    let runtime = LocalNodeRuntime::new(&ipc_socket, &ipc_socket);
    let publisher = LocalRuntimePublisher::builder(runtime, client_name)
        .provider(provider.clone())
        .build();
    publisher
        .publish_provider_resources(vec![resource.clone()])
        .await?;

    println!(
        "camera-provider provider={} resource={} ownership={:?}",
        provider.provider_id, resource.resource_id, resource.ownership_mode
    );
    Ok(())
}
