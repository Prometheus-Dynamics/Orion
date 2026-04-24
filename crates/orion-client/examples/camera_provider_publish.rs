use orion_client::{LocalNodeRuntime, LocalRuntimePublisher, ProviderResource};
use orion_control_plane::{
    AvailabilityState, HealthState, LeaseState, ProviderRecord, ResourceOwnershipMode,
};

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
    let provider = ProviderRecord::builder(provider_id.as_str(), node_id.as_str())
        .resource_type("camera.device")
        .build();
    let resource = ProviderResource::new(
        raw_resource_id.as_str(),
        "camera.device",
        provider_id.as_str(),
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
