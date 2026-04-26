use orion_client::ExecutorEventStream;
use orion_control_plane::{ClientEventKind, ExecutorRecord};
use orion_core::{ExecutorId, NodeId, RuntimeType};

#[path = "support/common.rs"]
mod common;

#[tokio::main]
async fn main() {
    common::exit_on_error(run()).await;
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let [socket_path, client_name, executor_id, node_id, runtime_type] =
        common::read_exact_args::<5>()?;
    let executor = ExecutorRecord::builder(ExecutorId::new(executor_id), NodeId::new(node_id))
        .runtime_type(RuntimeType::new(runtime_type))
        .build();
    let mut stream = ExecutorEventStream::connect_at_with_local_address(
        &socket_path,
        client_name.clone(),
        format!("{client_name}.executor"),
    )
    .await?;
    stream.subscribe_workloads(&executor).await?;
    let events = stream.next_events().await?;
    let Some(event) = events.first() else {
        return Err("no executor events received".into());
    };
    match &event.event {
        ClientEventKind::ExecutorWorkloads {
            executor_id,
            workloads,
        } => {
            println!(
                "executor workloads seq={} executor={} count={} ids={}",
                event.sequence,
                executor_id,
                workloads.len(),
                workloads
                    .iter()
                    .map(|workload| workload.workload_id.as_str())
                    .collect::<Vec<_>>()
                    .join(",")
            );
            Ok(())
        }
        other => Err(format!("unexpected executor event: {other:?}").into()),
    }
}
