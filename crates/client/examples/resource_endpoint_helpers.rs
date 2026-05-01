use orion_client::{GraphPayload, resolve_workload_graph};
use orion_control_plane::{
    AppliedClusterState, ClusterStateEnvelope, DesiredClusterState, DesiredState,
    ObservedClusterState, ResourceConfigState, ResourceRecord, ResourceState, SharedMemoryEndpoint,
    StateSnapshot, TypedConfigValue, UnixEndpoint, WorkloadConfig, WorkloadObservedState,
    WorkloadRecord,
};
use orion_core::{ArtifactId, ConfigSchemaId, RuntimeType, WorkloadId};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let resource = ResourceRecord::builder("resource.graph", "graph.payload", "provider.graph")
        .endpoint("shm://vision-graph")
        .state(
            ResourceState::new(1).with_config(
                ResourceConfigState::new()
                    .field(
                        "graph.inline",
                        TypedConfigValue::String("{\"nodes\":[]}".into()),
                    )
                    .field(
                        "graph.content_type",
                        TypedConfigValue::String("application/json".into()),
                    ),
            ),
        )
        .build();

    let shm = resource.endpoint::<SharedMemoryEndpoint>()?;
    println!("shared_memory_endpoint={}", shm.name);

    let unix_resource =
        ResourceRecord::builder("resource.socket", "unix.payload", "provider.graph")
            .endpoint("unix:///tmp/orion-resource-endpoint-helpers.sock")
            .build();
    let unix = unix_resource.endpoint::<UnixEndpoint>()?;
    println!("unix_endpoint={}", unix.path);

    let workload = WorkloadRecord::builder(
        WorkloadId::new("workload.graph"),
        RuntimeType::new("graph.exec.v1"),
        ArtifactId::new("artifact.graph"),
    )
    .desired_state(DesiredState::Running)
    .observed_state(WorkloadObservedState::Pending)
    .config(WorkloadConfig {
        schema_id: ConfigSchemaId::new("graph.workload.config.v1"),
        payload: [
            (
                "graph.kind".to_owned(),
                TypedConfigValue::String("resource".into()),
            ),
            (
                "graph.resource_id".to_owned(),
                TypedConfigValue::String("resource.graph".into()),
            ),
        ]
        .into_iter()
        .collect(),
    })
    .build();

    let mut observed = ObservedClusterState::default();
    observed.put_resource(resource);
    let snapshot = StateSnapshot {
        state: ClusterStateEnvelope {
            desired: DesiredClusterState::default(),
            observed,
            applied: AppliedClusterState::default(),
        },
    };

    let resolved = resolve_workload_graph(&workload, &snapshot, |_| None)?;
    match resolved.payload {
        GraphPayload::Text(text) => println!("resolved_graph={text}"),
        GraphPayload::Bytes(bytes) => println!("resolved_graph_bytes={}", bytes.len()),
    }

    Ok(())
}
