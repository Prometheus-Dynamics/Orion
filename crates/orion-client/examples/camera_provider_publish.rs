use orion_client::{LocalProviderApp, ProviderResource};
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

    // This example accepts an explicit socket path so it can be wired into custom demos and tests.
    // For the default daemon layout, prefer `LocalProviderApp::connect_default(...)`.
    let app = LocalProviderApp::connect_at_with_local_address(
        &ipc_socket,
        client_name,
        format!("{provider_id}.camera-provider"),
        provider.clone(),
    )?;
    app.publish_resource(resource.clone()).await?;

    println!(
        "camera-provider provider={} resource={} ownership={:?}",
        provider.provider_id, resource.resource_id, resource.ownership_mode
    );
    Ok(())
}
