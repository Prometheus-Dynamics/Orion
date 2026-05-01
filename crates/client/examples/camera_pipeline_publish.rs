use orion_client::{DerivedResource, LocalNodeRuntime, LocalRuntimePublisher, ResourceClaim};
use orion_control_plane::{
    AvailabilityState, DesiredState, ExecutorRecord, HealthState, ResourceOwnershipMode,
    WorkloadObservedState, WorkloadRecord,
};
use orion_core::{
    ArtifactId, ExecutorId, NodeId, ProviderId, ResourceId, ResourceType, RuntimeType,
    RuntimeTypeDef, WorkloadId,
};

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
    let executor = ExecutorRecord::builder(
        ExecutorId::new(executor_id.clone()),
        NodeId::new(node_id.clone()),
    )
    .runtime_type(RuntimeType::of::<CameraControllerRuntime>())
    .runtime_type(RuntimeType::of::<VisionConsumerRuntime>())
    .build();
    let controller = WorkloadRecord::builder(
        WorkloadId::new(controller_workload_id.clone()),
        RuntimeType::of::<CameraControllerRuntime>(),
        ArtifactId::new("artifact.camera-controller"),
    )
    .desired_state(DesiredState::Running)
    .observed_state(WorkloadObservedState::Running)
    .assigned_to(NodeId::new(node_id.clone()))
    .require_claim(
        ResourceClaim::new(ResourceType::new("camera.device"), 1)
            .ownership_mode(ResourceOwnershipMode::ExclusiveOwnerPublishesDerived)
            .build(),
    )
    .bind_resource(
        ResourceId::new(raw_resource_id.clone()),
        NodeId::new(node_id.clone()),
    )
    .build();
    let stream = DerivedResource::new(
        ResourceId::new(stream_resource_id.clone()),
        ResourceType::new("camera.frame_stream"),
        ProviderId::new("provider.camera"),
    )
    .realized_by_executor(ExecutorId::new(executor_id.clone()))
    .realized_for_workload(WorkloadId::new(controller_workload_id.clone()))
    .source_resource(ResourceId::new(raw_resource_id.clone()))
    .source_workload(WorkloadId::new(controller_workload_id.clone()))
    .ownership_mode(ResourceOwnershipMode::SharedRead)
    .health(HealthState::Healthy)
    .availability(AvailabilityState::Available)
    .label(format!("mode.width={}", width.parse::<u32>()?))
    .build();
    let runtime = LocalNodeRuntime::new(&ipc_socket, &ipc_socket);
    let publisher = LocalRuntimePublisher::builder(runtime, client_name)
        .executor(executor.clone())
        .build();
    publisher
        .publish_executor_snapshot(vec![controller.clone()], vec![stream.clone()])
        .await?;

    println!(
        "camera-pipeline executor={} controller={} stream={}",
        executor.executor_id, controller.workload_id, stream.resource_id
    );
    Ok(())
}
