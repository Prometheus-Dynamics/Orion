use orion_client::{
    ClientIdentity, ClientRole, ControlPlaneEventStream, ExecutorEventStream, ProviderEventStream,
    SessionConfig,
};
use orion_control_plane::{ClientEvent, ClientEventKind, ExecutorRecord, ProviderRecord};
use orion_core::{NodeId, ProviderId, ResourceType, Revision, RuntimeType};
use orion_transport_ipc::LocalAddress;
use std::path::PathBuf;

#[path = "support/common.rs"]
mod common;

#[tokio::main]
async fn main() {
    common::exit_on_error(run()).await;
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let [
        socket_path,
        base_name,
        executor_id,
        provider_id,
        node_id,
        runtime_type,
        resource_type,
    ] = common::read_exact_args::<7>()?;
    let socket_path = PathBuf::from(socket_path);
    let executor = ExecutorRecord::builder(executor_id, NodeId::new(node_id.clone()))
        .runtime_type(RuntimeType::new(runtime_type))
        .build();
    let provider = ProviderRecord {
        provider_id: ProviderId::new(provider_id),
        node_id: NodeId::new(node_id),
        resource_types: vec![ResourceType::new(resource_type)],
    };

    let mut control = ControlPlaneEventStream::connect(
        &socket_path,
        ClientIdentity::new(format!("{base_name}-control"), ClientRole::ControlPlane),
        SessionConfig::new(LocalAddress::new(format!("{base_name}.control"))),
    )
    .await?;
    control.subscribe_state(Revision::ZERO).await?;

    let mut executor_stream = ExecutorEventStream::connect(
        &socket_path,
        ClientIdentity::new(format!("{base_name}-executor"), ClientRole::Executor),
        SessionConfig::new(LocalAddress::new(format!("{base_name}.executor"))),
    )
    .await?;
    executor_stream.subscribe_workloads(&executor).await?;

    let mut provider_stream = ProviderEventStream::connect(
        &socket_path,
        ClientIdentity::new(format!("{base_name}-provider"), ClientRole::Provider),
        SessionConfig::new(LocalAddress::new(format!("{base_name}.provider"))),
    )
    .await?;
    provider_stream.subscribe_leases(&provider).await?;

    let control_events = control.next_events().await?;
    let executor_events = executor_stream.next_events().await?;
    let provider_events = provider_stream.next_events().await?;

    let control_summary = describe(control_events.first().ok_or("missing control event")?);
    let executor_summary = describe(executor_events.first().ok_or("missing executor event")?);
    let provider_summary = describe(provider_events.first().ok_or("missing provider event")?);

    println!("multi-watch {control_summary} | {executor_summary} | {provider_summary}");
    Ok(())
}

fn describe(event: &ClientEvent) -> String {
    match &event.event {
        ClientEventKind::StateSnapshot(snapshot) => format!(
            "state:rev={} workloads={}",
            snapshot.state.desired.revision,
            snapshot.state.desired.workloads.len()
        ),
        ClientEventKind::ExecutorWorkloads {
            executor_id,
            workloads,
        } => format!("executor:{executor_id} count={}", workloads.len()),
        ClientEventKind::ProviderLeases {
            provider_id,
            leases,
        } => {
            format!("provider:{provider_id} count={}", leases.len())
        }
    }
}
