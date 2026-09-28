//! Watches the workloads assigned to a local executor using only `orion_client::prelude`.
//!
//! Usage: `assigned_workloads_watch <ipc-socket> <ipc-stream-socket> <client-name> <executor-id>
//! <node-id> <runtime-type> <max-updates>`

use orion_client::prelude::*;
use std::time::Duration;

#[path = "support/common.rs"]
mod common;

#[tokio::main]
async fn main() {
    common::exit_on_error(run()).await;
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let [
        socket_path,
        stream_socket_path,
        client_name,
        executor_id,
        node_id,
        runtime_type,
        max_updates,
    ] = common::read_exact_args::<7>()?;
    let max_updates: usize = max_updates.parse()?;
    let executor = ExecutorRecord::builder(ExecutorId::new(executor_id), NodeId::new(node_id))
        .runtime_type(RuntimeType::new(runtime_type))
        .build();
    let runtime = LocalNodeRuntime::new(socket_path, stream_socket_path);
    let service = LocalExecutorService::new(runtime, client_name, executor).with_retry_policy(
        LocalServiceRetryPolicy::fixed_delay(Duration::from_millis(250)).with_max_attempts(40),
    );
    service.register().await?;

    let mut watch = service.watch_assigned_workloads().await?;
    for _ in 0..max_updates {
        let update = watch.next().await?;
        println!(
            "assigned workloads seq={:?} count={}",
            update.sequence,
            update.workloads.len()
        );
        for workload in &update.workloads {
            let bound = workload
                .bound_resource_ids()
                .map(ResourceId::as_str)
                .collect::<Vec<_>>()
                .join(",");
            println!(
                "  {} runtime={} desired={:?} config_fields={} bound=[{bound}]",
                workload.workload_id(),
                workload.runtime_type(),
                workload.desired_state(),
                workload.config_payload().len(),
            );
        }
    }
    Ok(())
}
