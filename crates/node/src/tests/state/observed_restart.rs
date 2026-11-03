//! Observed state is runtime status: it is never written, and a restart starts it empty. Desired
//! state survives, and a restart with an executor still connected flaps nothing.

use super::*;
use orion::control_plane::{ExecutorRecord, ProviderRecord, WorkloadObservedState, WorkloadRecord};
use orion::runtime::ExecutorCommand;
use orion_client::{LocalNodeRuntime, LocalRuntimePublisher};

const RUNTIME: &str = "example.engine.execution.v1";
const WORKLOAD: &str = "workload.restart";
const EXECUTOR: &str = "executor.restart";

fn build_app(state_dir: &std::path::Path, socket: &std::path::Path) -> NodeApp {
    let mut config = test_node_config_for_socket_path(
        "node-a",
        socket.to_path_buf(),
        crate::PeerAuthenticationMode::Optional,
    );
    config.state_dir = Some(state_dir.to_path_buf());
    NodeApp::builder()
        .config(config)
        .try_build()
        .expect("node app should build")
}

fn workload(state: WorkloadObservedState) -> WorkloadRecord {
    let mut record = WorkloadRecord::builder(
        WorkloadId::new(WORKLOAD),
        RuntimeType::new(RUNTIME),
        ArtifactId::new("artifact.restart"),
    )
    .desired_state(DesiredState::Running)
    .assigned_to(NodeId::new("node-a"))
    .build();
    record.observed_state = state;
    record
}

fn publisher(socket: &std::path::Path, stream: &std::path::Path) -> LocalRuntimePublisher {
    let node = NodeId::new("node-a");
    LocalRuntimePublisher::builder(
        LocalNodeRuntime::new(socket.to_path_buf(), stream.to_path_buf()),
        "example-engine",
    )
    .provider(
        ProviderRecord::builder(ProviderId::new("provider.restart"), node.clone())
            .resource_type(ResourceType::new("engine.session"))
            .build(),
    )
    .executor(
        ExecutorRecord::builder(ExecutorId::new(EXECUTOR), node)
            .runtime_type(RuntimeType::new(RUNTIME))
            .build(),
    )
    .build()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn restart_with_connected_executor_rebuilds_observed_without_flapping() {
    let state_dir = temp_state_dir("observed-restart");
    let socket = temp_socket_path("observed-restart");
    let stream = temp_stream_socket_path("observed-restart-stream");

    // First life: the executor registers, takes the workload, and reports it running.
    let app = build_app(&state_dir, &socket);
    let (_path, server) = app
        .start_ipc_server(&socket)
        .await
        .expect("ipc server should start");
    let (_stream_path, stream_server) = app
        .start_ipc_stream_server(&stream)
        .await
        .expect("stream server should start");
    let mut desired = app.state_snapshot().state.desired;
    desired.put_workload(workload(WorkloadObservedState::Assigned));
    app.replace_desired(desired);
    let first = publisher(&socket, &stream);
    first
        .register_all()
        .await
        .expect("executor should register");
    first
        .publish_executor_snapshot(vec![workload(WorkloadObservedState::Running)], Vec::new())
        .await
        .expect("executor snapshot should publish");
    assert_eq!(
        app.state_snapshot()
            .state
            .observed
            .workloads
            .get(&WorkloadId::new(WORKLOAD))
            .map(|record| record.observed_state),
        Some(WorkloadObservedState::Running)
    );
    let storage = NodeStorage::new(&state_dir);
    assert!(
        !storage.snapshot_observed_path().exists(),
        "observed state must never be written"
    );
    let desired_revision = app.state_snapshot().state.desired.revision;
    server.abort();
    stream_server.abort();
    let _ = std::fs::remove_file(&socket);
    let _ = std::fs::remove_file(&stream);
    drop(first);
    drop(app);

    // Second life: same state dir. Desired state survives; observed starts empty.
    let app = build_app(&state_dir, &socket);
    let state = app.state_snapshot().state;
    assert!(
        state
            .desired
            .workloads
            .contains_key(&WorkloadId::new(WORKLOAD))
    );
    assert!(
        state
            .desired
            .executors
            .contains_key(&ExecutorId::new(EXECUTOR))
    );
    assert_eq!(state.desired.revision, desired_revision);
    assert!(
        state.observed.workloads.is_empty(),
        "observed state is pending until the executor republishes"
    );
    let (_path, server) = app
        .start_ipc_server(&socket)
        .await
        .expect("ipc server should restart");
    let (_stream_path, stream_server) = app
        .start_ipc_stream_server(&stream)
        .await
        .expect("stream server should restart");

    // Before the executor reconnects, a pass may re-derive starts (never dispatched to IPC
    // executors) but must not stop anything.
    let report = app.tick().expect("pass after restart should succeed");
    assert!(
        report
            .commands
            .iter()
            .all(|command| matches!(command, ExecutorCommand::Start(_))),
        "a restart must not stop the workload: {:?}",
        report.commands
    );

    // The executor reconnects and republishes the same state: nothing left to do.
    let second = publisher(&socket, &stream);
    second
        .register_all()
        .await
        .expect("executor should re-register");
    second
        .publish_executor_snapshot(vec![workload(WorkloadObservedState::Running)], Vec::new())
        .await
        .expect("executor snapshot should publish");
    let report = app.tick().expect("pass after republish should succeed");
    assert!(
        report.commands.is_empty(),
        "republished state matches desired state: {:?}",
        report.commands
    );
    let state = app.state_snapshot().state;
    assert_eq!(
        state.desired.revision, desired_revision,
        "the restart makes no desired commit"
    );
    assert_eq!(
        state
            .observed
            .workloads
            .get(&WorkloadId::new(WORKLOAD))
            .map(|record| record.observed_state),
        Some(WorkloadObservedState::Running)
    );

    server.abort();
    stream_server.abort();
    let _ = std::fs::remove_dir_all(state_dir);
}
