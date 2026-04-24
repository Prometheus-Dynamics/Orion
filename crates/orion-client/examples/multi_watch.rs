use orion_client::{
    LocalExecutorEvent, LocalExecutorService, LocalNodeRuntime, LocalProviderEvent,
    LocalProviderService,
};
use orion_control_plane::{ExecutorRecord, ProviderRecord};
use orion_core::{ResourceType, Revision, RuntimeType};

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
    let runtime = LocalNodeRuntime::new(&socket_path, &socket_path);
    let executor = ExecutorRecord::builder(executor_id, node_id.clone())
        .runtime_type(RuntimeType::new(runtime_type))
        .build();
    let provider = ProviderRecord {
        provider_id: provider_id.into(),
        node_id: node_id.into(),
        resource_types: vec![ResourceType::new(resource_type)],
    };

    let executor_service =
        LocalExecutorService::new(runtime.clone(), format!("{base_name}-executor"), executor);
    let provider_service =
        LocalProviderService::new(runtime, format!("{base_name}-provider"), provider);

    let mut executor_subscription = executor_service.subscribe_workloads().await?;
    let mut provider_subscription = provider_service.subscribe(Revision::ZERO).await?;

    let executor_summary = describe_executor(&executor_subscription.next_event().await?);
    let provider_summary = describe_provider(&provider_subscription.next_event().await?);

    println!("multi-watch {executor_summary} | {provider_summary}");
    Ok(())
}

fn describe_executor(event: &LocalExecutorEvent) -> String {
    match event {
        LocalExecutorEvent::Bootstrap(workloads) => {
            format!("executor:bootstrap count={}", workloads.len())
        }
        LocalExecutorEvent::WorkloadsChanged { workloads, .. } => {
            format!("executor:update count={}", workloads.len())
        }
    }
}

fn describe_provider(event: &LocalProviderEvent) -> String {
    match event {
        LocalProviderEvent::BootstrapLeases(leases) => {
            format!("provider:leases count={}", leases.len())
        }
        LocalProviderEvent::BootstrapStateSnapshot(snapshot)
        | LocalProviderEvent::StateSnapshot { snapshot, .. } => format!(
            "state:rev={} workloads={}",
            snapshot.state.desired.revision,
            snapshot.state.desired.workloads.len()
        ),
        LocalProviderEvent::LeasesChanged { leases, .. } => {
            format!("provider:leases count={}", leases.len())
        }
    }
}
