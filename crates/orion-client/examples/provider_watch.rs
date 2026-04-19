use orion_client::ProviderEventStream;
use orion_control_plane::{ClientEventKind, ProviderRecord};
use orion_core::{NodeId, ProviderId, ResourceType};

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
        provider_id: ProviderId::new(provider_id),
        node_id: NodeId::new(node_id),
        resource_types: vec![ResourceType::new(resource_type)],
    };
    let mut stream = ProviderEventStream::connect_at_with_local_address(
        &socket_path,
        client_name.clone(),
        format!("{client_name}.provider"),
    )
    .await?;
    stream.subscribe_leases(&provider).await?;
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
                "provider leases seq={} provider={} count={} resources={}",
                event.sequence,
                provider_id,
                leases.len(),
                leases
                    .iter()
                    .map(|lease| lease.resource_id.as_str())
                    .collect::<Vec<_>>()
                    .join(",")
            );
            Ok(())
        }
        other => Err(format!("unexpected provider event: {other:?}").into()),
    }
}
