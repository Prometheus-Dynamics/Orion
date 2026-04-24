use orion_client::{LocalNodeRuntime, LocalProviderEvent, LocalProviderService};
use orion_control_plane::ProviderRecord;
use orion_core::{ResourceType, Revision};

#[path = "support/common.rs"]
mod common;

#[tokio::main]
async fn main() {
    common::exit_on_error(run()).await;
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let [
        socket_path,
        client_name,
        provider_id,
        node_id,
        resource_type,
    ] = common::read_exact_args::<5>()?;
    let provider = ProviderRecord {
        provider_id: provider_id.into(),
        node_id: node_id.into(),
        resource_types: vec![ResourceType::new(resource_type)],
    };

    let runtime = LocalNodeRuntime::new(&socket_path, &socket_path);
    let service = LocalProviderService::new(runtime, client_name, provider);
    let mut subscription = service.subscribe(Revision::ZERO).await?;

    loop {
        match subscription.next_event().await? {
            LocalProviderEvent::BootstrapLeases(leases)
            | LocalProviderEvent::LeasesChanged { leases, .. } => {
                let provider_id = service.provider().provider_id.as_str();
                println!(
                    "provider leases provider={} count={} resources={}",
                    provider_id,
                    leases.len(),
                    leases
                        .iter()
                        .map(|lease| lease.resource_id.as_str())
                        .collect::<Vec<_>>()
                        .join(",")
                );
                return Ok(());
            }
            LocalProviderEvent::BootstrapStateSnapshot(_)
            | LocalProviderEvent::StateSnapshot { .. } => {}
        }
    }
}
