#![allow(dead_code)]

use std::{path::PathBuf, process::Command, time::Duration};

use orion::{
    ArtifactId, NodeId, WorkloadId,
    client::LocalNodeRuntime,
    control_plane::{
        AvailabilityState, ControlMessage, DesiredState, ExecutorRecord, HealthState, LeaseState,
        ProviderRecord, ResourceOwnershipMode, ResourceRecord, SyncRequest, WorkloadObservedState,
        WorkloadRecord,
    },
    transport::http::{HttpClient, HttpRequestPayload, HttpResponsePayload},
};
use orion_node::{NodeApp, NodeConfig};

pub(crate) struct TestHarness {
    pub(crate) _app: NodeApp,
    pub(crate) _http_task: tokio::task::JoinHandle<()>,
    pub(crate) _ipc_task: tokio::task::JoinHandle<()>,
    pub(crate) _ipc_stream_task: tokio::task::JoinHandle<()>,
    pub(crate) http_addr: std::net::SocketAddr,
    pub(crate) client: HttpClient,
    pub(crate) ipc_socket: PathBuf,
    pub(crate) ipc_stream_socket: PathBuf,
}

impl TestHarness {
    pub(crate) async fn start(node_id: &str) -> Self {
        let config = NodeConfig::for_local_node(NodeId::new(node_id))
            .with_http_bind_addr("127.0.0.1:0".parse().expect("socket address should parse"))
            .with_ipc_socket_path(NodeConfig::default_ipc_socket_path_for(node_id))
            .with_reconcile_interval(Duration::from_millis(10))
            .with_peer_authentication(orion_node::PeerAuthenticationMode::Disabled)
            .with_peer_sync_execution(
                NodeConfig::try_peer_sync_execution_from_env()
                    .expect("peer sync execution defaults should parse"),
            )
            .with_runtime_tuning(
                NodeConfig::try_runtime_tuning_from_env()
                    .expect("runtime tuning defaults should parse"),
            );
        let ipc_socket = config.ipc_socket_path.clone();
        let ipc_stream_socket = NodeConfig::default_ipc_stream_socket_path_for(node_id);
        let app = NodeApp::try_new(config.clone()).expect("node app should build");
        let (http_addr, http_server) = app
            .start_http_server_graceful(config.http_bind_addr)
            .await
            .expect("http server should start");
        let (_, ipc_server) = app
            .start_ipc_server_graceful(&config.ipc_socket_path)
            .await
            .expect("ipc server should start");
        let (_, ipc_stream_server) = app
            .start_ipc_stream_server_graceful(&ipc_stream_socket)
            .await
            .expect("ipc stream server should start");
        tokio::time::sleep(Duration::from_millis(50)).await;

        Self {
            _app: app,
            _http_task: tokio::spawn(async move {
                std::future::pending::<()>().await;
                let _ = http_server.shutdown().await;
            }),
            _ipc_task: tokio::spawn(async move {
                std::future::pending::<()>().await;
                let _ = ipc_server.shutdown().await;
            }),
            _ipc_stream_task: tokio::spawn(async move {
                std::future::pending::<()>().await;
                let _ = ipc_stream_server.shutdown().await;
            }),
            http_addr,
            client: HttpClient::try_new(format!("http://{http_addr}"))
                .expect("HTTP client should build"),
            ipc_socket,
            ipc_stream_socket,
        }
    }

    pub(crate) fn http_base(&self) -> String {
        format!("http://{}", self.http_addr)
    }

    pub(crate) async fn snapshot(&self) -> orion::control_plane::StateSnapshot {
        match self
            .client
            .send(&HttpRequestPayload::Control(Box::new(
                ControlMessage::SyncRequest(SyncRequest {
                    node_id: NodeId::new("orionctl.test.peer"),
                    desired_revision: orion::Revision::new(u64::MAX),
                    desired_fingerprint: 0,
                    desired_summary: None,
                    sections: Vec::new(),
                    object_selectors: Vec::new(),
                }),
            )))
            .await
            .expect("snapshot request should succeed")
        {
            HttpResponsePayload::Snapshot(snapshot) => snapshot,
            other => panic!("expected snapshot response, got {other:?}"),
        }
    }
}

pub(crate) fn run_orionctl<const N: usize>(args: [&str; N]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_orionctl"))
        .args(args)
        .output()
        .expect("orionctl should run")
}

pub(crate) fn run_orionctl_with_env<const N: usize>(
    args: [&str; N],
    envs: &[(&str, &str)],
) -> std::process::Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_orionctl"));
    command.args(args);
    for (key, value) in envs {
        command.env(key, value);
    }
    command.output().expect("orionctl should run")
}

pub(crate) fn output_text(output: &std::process::Output) -> String {
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    format!("stdout:\n{stdout}\nstderr:\n{stderr}")
}

pub(crate) fn temp_spec_path(name: &str) -> PathBuf {
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time should be after unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("{stamp}-{name}"))
}

pub(crate) async fn publish_observed_updater_state(harness: &TestHarness) {
    let mut desired = harness._app.state_snapshot().state.desired;
    desired.put_provider(
        ProviderRecord::builder("provider.observed", harness._app.config.node_id.clone())
            .resource_type("helios.updater.runtime.v1")
            .build(),
    );
    desired.put_executor(
        ExecutorRecord::builder("executor.observed", harness._app.config.node_id.clone())
            .runtime_type("helios.updater.v1")
            .build(),
    );
    desired.put_workload(
        WorkloadRecord::builder(
            WorkloadId::new("workload.update"),
            "helios.updater.v1",
            ArtifactId::new("artifact.update"),
        )
        .assigned_to(harness._app.config.node_id.clone())
        .desired_state(DesiredState::Running)
        .observed_state(WorkloadObservedState::Pending)
        .build(),
    );
    harness._app.replace_desired(desired);

    let runtime = LocalNodeRuntime::new(&harness.ipc_socket, &harness.ipc_stream_socket);
    runtime
        .provider(
            "orionctl.test.provider",
            ProviderRecord::builder("provider.observed", harness._app.config.node_id.clone())
                .resource_type("helios.updater.runtime.v1")
                .build(),
        )
        .expect("provider client should connect")
        .publish_resources(vec![
            ResourceRecord::builder(
                "resource.updater.runtime",
                "helios.updater.runtime.v1",
                "provider.observed",
            )
            .ownership_mode(ResourceOwnershipMode::Exclusive)
            .health(HealthState::Healthy)
            .availability(AvailabilityState::Available)
            .lease_state(LeaseState::Unleased)
            .build(),
        ])
        .await
        .expect("provider snapshot should publish");

    runtime
        .executor(
            "orionctl.test.executor",
            ExecutorRecord::builder("executor.observed", harness._app.config.node_id.clone())
                .runtime_type("helios.updater.v1")
                .build(),
        )
        .expect("executor client should connect")
        .publish_snapshot(
            vec![
                WorkloadRecord::builder(
                    WorkloadId::new("workload.update"),
                    "helios.updater.v1",
                    ArtifactId::new("artifact.update"),
                )
                .assigned_to(harness._app.config.node_id.clone())
                .desired_state(DesiredState::Running)
                .observed_state(WorkloadObservedState::Running)
                .build(),
            ],
            vec![
                ResourceRecord::builder(
                    "resource.update.execution",
                    "helios.updater.execution.v1",
                    "provider.observed",
                )
                .realized_by_executor("executor.observed")
                .realized_for_workload("workload.update")
                .source_workload("workload.update")
                .ownership_mode(ResourceOwnershipMode::Exclusive)
                .health(HealthState::Healthy)
                .availability(AvailabilityState::Available)
                .lease_state(LeaseState::Unleased)
                .build(),
            ],
        )
        .await
        .expect("executor snapshot should publish");
}
