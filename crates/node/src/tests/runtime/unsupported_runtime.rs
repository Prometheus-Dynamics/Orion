//! Workloads whose runtime type no local executor declares: a pass skips them without failing,
//! logs them once, and an executor published through `LocalRuntimePublisher` counts as local.

use super::*;
use orion::control_plane::{ExecutorRecord, ProviderRecord, WorkloadRecord};
use orion_client::{LocalNodeRuntime, LocalRuntimePublisher};
use std::sync::{Arc, Mutex};

const EXTERNAL_TYPE: &str = "helios.engine.execution.v1";
const NO_EXECUTOR: &str = "no local executor realizes this workload";

#[derive(Clone, Default)]
struct LogBuffer(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for LogBuffer {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for LogBuffer {
    type Writer = LogBuffer;
    fn make_writer(&'a self) -> LogBuffer {
        self.clone()
    }
}

impl LogBuffer {
    fn count(&self, needle: &str, level: &str) -> usize {
        let text = String::from_utf8_lossy(&self.0.lock().unwrap()).to_string();
        text.lines()
            .filter(|line| line.contains(level) && line.contains(needle))
            .count()
    }
}

fn capture_logs() -> (LogBuffer, tracing::subscriber::DefaultGuard) {
    let buffer = LogBuffer::default();
    let subscriber = tracing_subscriber::fmt()
        .with_writer(buffer.clone())
        .with_max_level(tracing::Level::DEBUG)
        .with_ansi(false)
        .finish();
    (buffer, tracing::subscriber::set_default(subscriber))
}

fn ipc_app(name: &str) -> NodeApp {
    NodeApp::builder()
        .config(test_node_config_for_socket_path(
            "node-a",
            temp_socket_path(name),
            crate::PeerAuthenticationMode::Optional,
        ))
        .try_build()
        .expect("node app should build")
}

fn running_workload(id: &str, runtime_type: &str, node: &str) -> WorkloadRecord {
    WorkloadRecord::builder(
        WorkloadId::new(id),
        RuntimeType::new(runtime_type),
        ArtifactId::new(format!("artifact.{id}")),
    )
    .desired_state(DesiredState::Running)
    .assigned_to(NodeId::new(node))
    .build()
}

fn put_desired_workload(app: &NodeApp, workload: WorkloadRecord) {
    let mut desired = app.state_snapshot().state.desired;
    desired.put_workload(workload);
    app.replace_desired(desired);
}

/// The engine-style publisher: a provider and an executor that both live on `node`.
fn engine_publisher(
    app: &NodeApp,
    socket: &std::path::Path,
    stream: &std::path::Path,
    executor_node: &str,
) -> LocalRuntimePublisher {
    let runtime = LocalNodeRuntime::new(socket.to_path_buf(), stream.to_path_buf());
    let node = app.config.node_id.clone();
    let provider = ProviderRecord::builder(ProviderId::new("provider.engine"), node.clone())
        .resource_type(ResourceType::new("engine.session"))
        .build();
    let executor = ExecutorRecord::builder(
        ExecutorId::new("executor.engine"),
        NodeId::new(executor_node),
    )
    .runtime_type(RuntimeType::new(EXTERNAL_TYPE))
    .build();
    LocalRuntimePublisher::builder(runtime, "helios-engine")
        .provider(provider)
        .executor(executor)
        .build()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn workload_realized_by_published_external_executor_reconciles_silently() {
    let (logs, _guard) = capture_logs();
    let app = ipc_app("unsupported-external");
    let socket = app.config.ipc_socket_path.clone();
    let stream = temp_stream_socket_path("unsupported-external-stream");
    let (_path, server) = app
        .start_ipc_server(&socket)
        .await
        .expect("ipc server should start");
    let (_stream_path, stream_server) = app
        .start_ipc_stream_server(&stream)
        .await
        .expect("stream server should start");
    put_desired_workload(
        &app,
        running_workload("workload.engine", EXTERNAL_TYPE, "node-a"),
    );

    let publisher = engine_publisher(&app, &socket, &stream, "node-a");
    publisher
        .register_all()
        .await
        .expect("executor should register over IPC");
    let observed = running_workload("workload.engine", EXTERNAL_TYPE, "node-a");
    publisher
        .publish_executor_snapshot(vec![observed], Vec::new())
        .await
        .expect("executor snapshot should publish");

    for _ in 0..50 {
        app.tick()
            .expect("a pass with a registered executor must not fail");
    }
    assert_eq!(logs.count(NO_EXECUTOR, "WARN"), 0);
    assert_eq!(logs.count(NO_EXECUTOR, "DEBUG"), 0);

    server.abort();
    stream_server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn workload_waiting_for_its_executor_warns_once_then_starts_when_it_registers() {
    let (logs, _guard) = capture_logs();
    let app = ipc_app("unsupported-race");
    let socket = app.config.ipc_socket_path.clone();
    let stream = temp_stream_socket_path("unsupported-race-stream");
    let (_path, server) = app
        .start_ipc_server(&socket)
        .await
        .expect("ipc server should start");
    let (_stream_path, stream_server) = app
        .start_ipc_stream_server(&stream)
        .await
        .expect("stream server should start");
    put_desired_workload(
        &app,
        running_workload("workload.engine", EXTERNAL_TYPE, "node-a"),
    );

    for _ in 0..20 {
        app.tick()
            .expect("pass before the executor registers must not fail");
    }
    assert_eq!(
        logs.count(NO_EXECUTOR, "WARN"),
        1,
        "one warning per workload and runtime type"
    );

    let publisher = engine_publisher(&app, &socket, &stream, "node-a");
    publisher
        .register_all()
        .await
        .expect("executor should register over IPC");
    let report = app.tick().expect("pass after registration must not fail");
    assert_eq!(
        report.commands.len(),
        1,
        "the workload starts once its executor exists"
    );
    assert_eq!(logs.count(NO_EXECUTOR, "WARN"), 1);

    server.abort();
    stream_server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn truly_unsupported_type_is_reported_once_and_does_not_block_others() {
    let (logs, _guard) = capture_logs();
    let app = ipc_app("unsupported-type");
    let socket = app.config.ipc_socket_path.clone();
    let stream = temp_stream_socket_path("unsupported-type-stream");
    let (_path, server) = app
        .start_ipc_server(&socket)
        .await
        .expect("ipc server should start");
    let (_stream_path, stream_server) = app
        .start_ipc_stream_server(&stream)
        .await
        .expect("stream server should start");
    let publisher = engine_publisher(&app, &socket, &stream, "node-a");
    publisher
        .register_all()
        .await
        .expect("executor should register over IPC");

    put_desired_workload(
        &app,
        running_workload("workload.nobody", "nobody.exec.v1", "node-a"),
    );
    put_desired_workload(
        &app,
        running_workload("workload.engine", EXTERNAL_TYPE, "node-a"),
    );
    let mut commands = 0;
    for _ in 0..20 {
        commands += app
            .tick()
            .expect("an unsupported workload must not fail the pass")
            .commands
            .len();
    }
    assert!(
        commands > 0,
        "the supported workload is still planned on every pass"
    );
    assert_eq!(logs.count("nobody.exec.v1", "WARN"), 1);
    assert_eq!(
        logs.count("nobody.exec.v1", "DEBUG"),
        0,
        "debug follows only after back-off"
    );

    server.abort();
    stream_server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn executor_declaring_another_node_is_refused_and_never_persisted() {
    let app = ipc_app("unsupported-foreign");
    let socket = app.config.ipc_socket_path.clone();
    let stream = temp_stream_socket_path("unsupported-foreign-stream");
    let (_path, server) = app
        .start_ipc_server(&socket)
        .await
        .expect("ipc server should start");
    let (_stream_path, stream_server) = app
        .start_ipc_stream_server(&stream)
        .await
        .expect("stream server should start");

    let publisher = engine_publisher(&app, &socket, &stream, "node-b");
    assert!(
        publisher.register_all().await.is_err(),
        "an executor for another node must be refused"
    );
    assert!(
        !app.state_snapshot()
            .state
            .desired
            .executors
            .contains_key(&ExecutorId::new("executor.engine")),
        "a refused executor must not be written to desired state"
    );

    server.abort();
    stream_server.abort();
}
