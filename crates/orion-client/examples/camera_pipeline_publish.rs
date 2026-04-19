use orion_client::{DerivedResource, LocalExecutorApp, ResourceClaim};
use orion_control_plane::{
    AvailabilityState, DesiredState, ExecutorRecord, HealthState, ResourceOwnershipMode,
    WorkloadObservedState, WorkloadRecord,
};
use orion_core::{RuntimeType, RuntimeTypeDef};

#[path = "support/common.rs"]
mod common;

struct CameraControllerRuntime;
struct VisionConsumerRuntime;

impl RuntimeTypeDef for CameraControllerRuntime {
    const TYPE: &'static str = "camera.controller.v1";
}

impl RuntimeTypeDef for VisionConsumerRuntime {
    const TYPE: &'static str = "vision.consumer.v1";
}

#[tokio::main]
async fn main() {
    common::exit_on_error(run()).await;
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let [
        ipc_socket,
        client_name,
        executor_id,
        node_id,
        controller_workload_id,
        raw_resource_id,
        stream_resource_id,
        width,
    ] = common::read_exact_args::<8>()?;
    let executor = ExecutorRecord::builder(executor_id.as_str(), node_id.as_str())
        .runtime_type(RuntimeType::of::<CameraControllerRuntime>())
        .runtime_type(RuntimeType::of::<VisionConsumerRuntime>())
        .build();
    let controller = WorkloadRecord::builder(
        controller_workload_id.as_str(),
        RuntimeType::of::<CameraControllerRuntime>(),
        "artifact.camera-controller",
    )
    .desired_state(DesiredState::Running)
    .observed_state(WorkloadObservedState::Running)
    .assigned_to(node_id.as_str())
    .require_claim(
        ResourceClaim::new("camera.device", 1)
            .ownership_mode(ResourceOwnershipMode::ExclusiveOwnerPublishesDerived)
            .build(),
    )
    .bind_resource(raw_resource_id.as_str(), node_id.as_str())
    .build();
    let stream = DerivedResource::new(
        stream_resource_id.as_str(),
        "camera.frame_stream",
        "provider.camera",
    )
    .realized_by_executor(executor_id.as_str())
    .realized_for_workload(controller_workload_id.as_str())
    .source_resource(raw_resource_id.as_str())
    .source_workload(controller_workload_id.as_str())
    .ownership_mode(ResourceOwnershipMode::SharedRead)
    .health(HealthState::Healthy)
    .availability(AvailabilityState::Available)
    .label(format!("mode.width={}", width.parse::<u32>()?))
    .build();
    // This example accepts an explicit socket path so it can target non-default layouts.
    // For the default daemon layout, prefer `LocalExecutorApp::connect_default(...)`.
    let app = LocalExecutorApp::connect_at_with_local_address(
        &ipc_socket,
        client_name,
        format!("{executor_id}.camera-pipeline"),
        executor.clone(),
    )?;
    app.publish_snapshot(vec![controller.clone()], vec![stream.clone()])
        .await?;

    println!(
        "camera-pipeline executor={} controller={} stream={}",
        executor.executor_id, controller.workload_id, stream.resource_id
    );
    Ok(())
}
