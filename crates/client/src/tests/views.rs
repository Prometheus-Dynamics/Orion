use super::local_services::{serve_executor_bootstrap_query, unique_socket_path, write_welcome};
use crate::prelude::*;
use orion_control_plane::{
    ClientEvent, ClientEventKind, ControlMessage, ExecutorWorkloadQuery, ResourceRecord,
};
use orion_transport_ipc::{ControlEnvelope, LocalAddress, read_control_frame, write_control_frame};
use serde::Deserialize;
use std::time::Duration;
use tokio::net::UnixListener;

#[derive(Debug, Deserialize, PartialEq, Eq)]
struct CameraConfig {
    device: String,
    rate_hz: u64,
    stream: StreamConfig,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
struct StreamConfig {
    enabled: bool,
}

#[derive(Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(default)]
struct OptionalConfig {
    verbose: bool,
}

fn engine_executor() -> ExecutorRecord {
    ExecutorRecord::builder(ExecutorId::new("executor.engine"), NodeId::new("node-a"))
        .runtime_type(RuntimeType::new("graph.exec.v1"))
        .build()
}

fn camera_workload() -> WorkloadRecord {
    WorkloadRecord::builder(
        WorkloadId::new("workload.camera"),
        RuntimeType::new("graph.exec.v1"),
        ArtifactId::new("artifact.camera"),
    )
    .desired_state(DesiredState::Running)
    .assigned_to(NodeId::new("node-a"))
    .config(
        WorkloadConfig::new("camera.config.v1")
            .field("device", TypedConfigValue::String("/dev/video0".into()))
            .field("rate_hz", TypedConfigValue::UInt(30))
            .field("stream.enabled", TypedConfigValue::Bool(true)),
    )
    .bind_resource(ResourceId::new("resource.camera"), NodeId::new("node-a"))
    .bind_resource(ResourceId::new("resource.derived"), NodeId::new("node-a"))
    .bind_resource(ResourceId::new("resource.missing"), NodeId::new("node-b"))
    .build()
}

fn snapshot() -> StateSnapshot {
    let executor = engine_executor();
    let camera = camera_workload();
    let other_node = WorkloadRecord::builder(
        WorkloadId::new("workload.remote"),
        RuntimeType::new("graph.exec.v1"),
        ArtifactId::new("artifact.remote"),
    )
    .assigned_to(NodeId::new("node-b"))
    .build();
    let other_runtime = WorkloadRecord::builder(
        WorkloadId::new("workload.wasm"),
        RuntimeType::new("wasm.v1"),
        ArtifactId::new("artifact.wasm"),
    )
    .assigned_to(NodeId::new("node-a"))
    .build();
    let unconfigured = WorkloadRecord::builder(
        WorkloadId::new("workload.plain"),
        RuntimeType::new("graph.exec.v1"),
        ArtifactId::new("artifact.plain"),
    )
    .assigned_to(NodeId::new("node-a"))
    .build();
    let camera_resource: ResourceRecord = ProviderResource::new(
        "resource.camera",
        "camera.device",
        ProviderId::new("provider.peripherals"),
    )
    .label("front")
    .endpoint("unix:///run/camera.sock")
    .endpoint("tcp://127.0.0.1:9000")
    .health(HealthState::Healthy)
    .build();
    let derived_resource: ResourceRecord = DerivedResource::new(
        "resource.derived",
        "camera.frames",
        ProviderId::new("provider.engine"),
    )
    .realized_by_executor("executor.engine")
    .endpoint("shm://frames")
    .build();

    let mut desired = DesiredClusterState {
        revision: Revision::new(9),
        ..Default::default()
    };
    desired
        .executors
        .insert(executor.executor_id.clone(), executor);
    for workload in [camera, other_node, other_runtime, unconfigured] {
        desired
            .workloads
            .insert(workload.workload_id.clone(), workload);
    }
    desired
        .resources
        .insert(camera_resource.resource_id.clone(), camera_resource);
    let mut observed = ObservedClusterState::default();
    observed
        .resources
        .insert(derived_resource.resource_id.clone(), derived_resource);

    StateSnapshot {
        state: ClusterStateEnvelope::new(desired, observed, AppliedClusterState::default()),
    }
}

#[test]
fn assigned_workloads_filters_by_node_and_runtime_type() {
    let snapshot = snapshot();
    let workloads = assigned_workloads(&snapshot, &engine_executor());
    let ids: Vec<_> = workloads
        .iter()
        .map(|workload| workload.workload_id().as_str())
        .collect();
    assert_eq!(ids, ["workload.camera", "workload.plain"]);
    assert!(
        workloads
            .iter()
            .all(|workload| workload.desired_revision() == Some(Revision::new(9)))
    );

    let by_id = assigned_workloads_for_executor(&snapshot, &ExecutorId::new("executor.engine"))
        .expect("executor should be registered");
    assert_eq!(by_id, workloads);
    assert!(
        assigned_workloads_for_executor(&snapshot, &ExecutorId::new("executor.unknown")).is_none()
    );
}

#[test]
fn assigned_workload_decodes_typed_config_and_lists_bindings() {
    let snapshot = snapshot();
    let workloads = assigned_workloads(&snapshot, &engine_executor());
    let camera = &workloads[0];

    assert_eq!(camera.runtime_type(), &RuntimeType::new("graph.exec.v1"));
    assert_eq!(camera.artifact_id(), &ArtifactId::new("artifact.camera"));
    assert_eq!(camera.desired_state(), DesiredState::Running);
    assert_eq!(
        camera.config_schema_id(),
        Some(&ConfigSchemaId::new("camera.config.v1"))
    );
    assert_eq!(
        camera
            .config::<CameraConfig>()
            .expect("config should decode"),
        CameraConfig {
            device: "/dev/video0".into(),
            rate_hz: 30,
            stream: StreamConfig { enabled: true },
        }
    );
    assert_eq!(
        camera.config_view().required_uint("rate_hz"),
        Ok(30),
        "field view should read the same payload"
    );
    let bound: Vec<_> = camera
        .bound_resource_ids()
        .map(ResourceId::as_str)
        .collect();
    assert_eq!(
        bound,
        ["resource.camera", "resource.derived", "resource.missing"]
    );

    let plain = &workloads[1];
    assert!(!plain.has_config());
    assert_eq!(
        plain.config::<OptionalConfig>(),
        Ok(OptionalConfig::default())
    );
    assert!(plain.config::<CameraConfig>().is_err());
}

#[test]
fn resources_bound_to_resolves_desired_then_observed_and_skips_missing() {
    let snapshot = snapshot();
    let resources = resources_bound_to(&snapshot, &WorkloadId::new("workload.camera"));
    let ids: Vec<_> = resources
        .iter()
        .map(|resource| resource.resource_id().as_str())
        .collect();
    assert_eq!(ids, ["resource.camera", "resource.derived"]);

    let camera = &resources[0];
    assert_eq!(camera.resource_type(), &ResourceType::new("camera.device"));
    assert_eq!(
        camera.provider_id(),
        &ProviderId::new("provider.peripherals")
    );
    assert_eq!(camera.bound_node_id(), &NodeId::new("node-a"));
    assert_eq!(camera.health(), HealthState::Healthy);
    assert!(camera.has_label("front"));
    assert_eq!(camera.endpoints().expect("endpoints parse").len(), 2);
    assert_eq!(
        camera
            .endpoint::<UnixEndpoint>()
            .expect("unix endpoint")
            .path,
        "/run/camera.sock"
    );
    assert_eq!(
        camera
            .endpoint::<TcpEndpoint>()
            .expect("tcp endpoint")
            .address,
        "127.0.0.1:9000"
    );
    assert!(camera.endpoint::<HttpEndpoint>().is_err());

    let derived = &resources[1];
    assert_eq!(
        derived.realized_by_executor_id(),
        Some(&ExecutorId::new("executor.engine"))
    );
    assert_eq!(
        derived
            .endpoint::<SharedMemoryEndpoint>()
            .expect("shm endpoint")
            .name,
        "frames"
    );

    let via_view = assigned_workloads(&snapshot, &engine_executor())[0].bound_resources(&snapshot);
    assert_eq!(via_view, resources);
    assert!(resources_bound_to(&snapshot, &WorkloadId::new("workload.unknown")).is_empty());
}

#[tokio::test]
async fn watch_assigned_workloads_dedupes_and_reconnects_with_retry_policy() {
    let unary_socket_path = unique_socket_path("assigned-watch-unary");
    let stream_socket_path = unique_socket_path("assigned-watch-stream");
    let _ = tokio::fs::remove_file(&unary_socket_path).await;
    let _ = tokio::fs::remove_file(&stream_socket_path).await;
    let unary_listener =
        UnixListener::bind(&unary_socket_path).expect("unary listener should bind");
    let stream_listener =
        UnixListener::bind(&stream_socket_path).expect("stream listener should bind");

    let executor = engine_executor();
    let initial = camera_workload();
    let changed = WorkloadRecord::builder(
        WorkloadId::new("workload.changed"),
        RuntimeType::new("graph.exec.v1"),
        ArtifactId::new("artifact.changed"),
    )
    .assigned_to(NodeId::new("node-a"))
    .build();

    let unary_task = tokio::spawn(serve_executor_bootstrap_query(
        unary_listener,
        executor.clone(),
        vec![initial.clone()],
    ));
    let stream_task = tokio::spawn(serve_flaky_watch_stream(
        stream_listener,
        executor.clone(),
        vec![initial.clone()],
        vec![changed.clone()],
    ));

    let runtime = LocalNodeRuntime::new(&unary_socket_path, &stream_socket_path);
    let service = LocalExecutorService::new(runtime, "engine", executor).with_retry_policy(
        LocalServiceRetryPolicy::fixed_delay(Duration::from_millis(10)).with_max_attempts(20),
    );
    let mut watch = service
        .watch_assigned_workloads()
        .await
        .expect("watch should connect");

    let bootstrap = watch.next().await.expect("bootstrap update");
    assert!(bootstrap.is_bootstrap());
    assert_eq!(
        bootstrap.workloads,
        assigned_workloads_from_records([initial])
    );
    assert_eq!(
        bootstrap.workloads[0]
            .config::<CameraConfig>()
            .expect("config should decode")
            .rate_hz,
        30
    );

    // The first stream repeats the bootstrap set (suppressed) and then drops; the watch must
    // reconnect and surface the change from the second stream.
    let update = watch.next().await.expect("changed update");
    assert_eq!(update.sequence, Some(2));
    assert_eq!(update.workloads, assigned_workloads_from_records([changed]));
    assert_eq!(watch.current(), Some(update.workloads.as_slice()));

    unary_task.await.expect("unary task should complete");
    stream_task.await.expect("stream task should complete");
    let _ = tokio::fs::remove_file(&unary_socket_path).await;
    let _ = tokio::fs::remove_file(&stream_socket_path).await;
}

async fn serve_flaky_watch_stream(
    listener: UnixListener,
    executor: ExecutorRecord,
    first: Vec<WorkloadRecord>,
    second: Vec<WorkloadRecord>,
) {
    for (sequence, workloads) in [(1, first), (2, second)] {
        let (mut stream, _) = listener.accept().await.expect("watch accept should work");
        let hello = read_control_frame(&mut stream)
            .await
            .expect("hello frame should decode")
            .expect("hello frame should exist");
        write_welcome(
            &mut stream,
            hello.source,
            ClientRole::Executor,
            "engine-watch",
        )
        .await;
        let request = read_control_frame(&mut stream)
            .await
            .expect("watch request should decode")
            .expect("watch request should exist");
        assert!(matches!(
            request.message,
            ControlMessage::WatchExecutorWorkloads(ExecutorWorkloadQuery { ref executor_id })
                if *executor_id == executor.executor_id
        ));
        for message in [
            ControlMessage::Accepted,
            ControlMessage::ClientEvents(vec![ClientEvent {
                sequence,
                event: ClientEventKind::ExecutorWorkloads {
                    executor_id: executor.executor_id.clone(),
                    workloads,
                },
            }]),
        ] {
            write_control_frame(
                &mut stream,
                &ControlEnvelope {
                    source: LocalAddress::new("orion"),
                    destination: request.source.clone(),
                    message,
                },
            )
            .await
            .expect("frame should send");
        }
        if sequence == 2 {
            // Keep the final stream open until the client has read the event.
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        // Dropping the stream closes the connection, forcing the client to reconnect.
    }
}
