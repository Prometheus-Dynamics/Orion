use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    net::TcpListener,
    path::{Path, PathBuf},
    process::{Command, Output},
    sync::OnceLock,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use orion::{
    ArtifactId, NodeId, Revision, RuntimeType, WorkloadId,
    control_plane::{
        ArtifactRecord, ControlMessage, DesiredState, DesiredStateMutation, MutationBatch,
        StateSnapshot, WorkloadRecord,
    },
    transport::http::{
        ControlRoute, HttpClient, HttpCodec, HttpRequestPayload, HttpResponse, HttpResponsePayload,
    },
};

pub const DOCKER_RECONCILE_MS: u64 = 50;
pub const DOCKER_WAIT_POLL_MS: u64 = 100;
const DOCKER_STARTUP_RETRIES: usize = 3;
const DOCKER_MUTATION_RETRIES: usize = 10;

pub fn stopped_workload(workload_id: &str, assigned_node: &str) -> WorkloadRecord {
    WorkloadRecord::builder(
        WorkloadId::new(workload_id),
        RuntimeType::new("graph.exec.v1"),
        ArtifactId::new(format!("artifact.{workload_id}")),
    )
    .desired_state(DesiredState::Stopped)
    .assigned_to(NodeId::new(assigned_node))
    .build()
}

pub struct DockerCluster {
    repo_root: PathBuf,
    compose_file: PathBuf,
    project_name: String,
    ports: ClusterPorts,
    nodes: Vec<String>,
}

impl DockerCluster {
    pub async fn start(name: &str) -> Self {
        Self::start_with_options(name, ClusterOptions::default()).await
    }

    pub async fn start_with_options(name: &str, options: ClusterOptions) -> Self {
        let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("crate directory should have parent")
            .parent()
            .expect("workspace root should exist")
            .to_path_buf();
        let node_ids = options.effective_node_ids();
        let image = ensure_test_image(&repo_root);
        let mut last_error = None;

        for attempt in 0..DOCKER_STARTUP_RETRIES {
            let ports = ClusterPorts::allocate(&node_ids);
            let project_name = format!(
                "orion-{}-{}-{}-{}",
                name,
                std::process::id(),
                attempt,
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .expect("system time should be after epoch")
                    .as_nanos()
            );
            let compose_file = write_compose_file(&project_name, &ports, &options, &image);

            let cluster = Self {
                repo_root: repo_root.clone(),
                compose_file,
                project_name,
                ports,
                nodes: node_ids.clone(),
            };

            match cluster.compose_result(["up", "-d", "--build"]) {
                Ok(()) => return cluster,
                Err(error) => {
                    last_error = Some(error.clone());
                    cluster.cleanup_resources();
                    if !docker_error_is_retryable(&error) || attempt + 1 == DOCKER_STARTUP_RETRIES {
                        panic!("{error}");
                    }
                }
            }
        }

        panic!(
            "{}",
            last_error.unwrap_or_else(|| "docker cluster startup failed".to_owned())
        );
    }

    pub fn nodes(&self) -> &[String] {
        &self.nodes
    }

    pub async fn wait_for_all_http(&self) {
        for node in &self.nodes {
            self.wait_for_http(node).await;
        }
    }

    pub async fn wait_for_http(&self, node: &str) {
        let client = self.client(node);
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        loop {
            let response = client.get_route(ControlRoute::Health).await;

            if matches!(response, Ok(HttpResponsePayload::Health(_))) {
                return;
            }

            let last_result = match &response {
                Ok(other) => format!("unexpected response: {other:?}"),
                Err(err) => format!("request error: {err}"),
            };

            if std::time::Instant::now() >= deadline {
                panic!(
                    "node {node} did not become healthy in time\nlast_result: {}\n{}",
                    last_result,
                    self.compose_logs()
                );
            }

            tokio::time::sleep(Duration::from_millis(DOCKER_WAIT_POLL_MS)).await;
        }
    }

    pub async fn snapshot(&self, node: &str) -> StateSnapshot {
        match self
            .client(node)
            .send(&HttpRequestPayload::Control(Box::new(
                ControlMessage::SyncRequest(orion::control_plane::SyncRequest {
                    node_id: NodeId::new("test.peer"),
                    desired_revision: Revision::new(u64::MAX),
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

    pub async fn put_workload(&self, node: &str, workload: WorkloadRecord) {
        let codec = HttpCodec;
        for attempt in 0..DOCKER_MUTATION_RETRIES {
            let snapshot = self.snapshot(node).await;
            let artifact = ArtifactRecord::builder(workload.artifact_id.clone()).build();
            let batch = MutationBatch {
                base_revision: snapshot.state.desired.revision,
                mutations: vec![
                    DesiredStateMutation::PutArtifact(artifact),
                    DesiredStateMutation::PutWorkload(workload.clone()),
                ],
            };

            let request = codec
                .encode_request(&HttpRequestPayload::Control(Box::new(
                    ControlMessage::Mutations(batch),
                )))
                .expect("mutation request should encode");
            let response = self.raw_post(node, &request.path, &request.body).await;
            let status = response.status().as_u16();
            let body = response
                .bytes()
                .await
                .expect("mutation response body should be readable")
                .to_vec();
            let decoded = codec.decode_response(&HttpResponse { status, body });

            match decoded {
                Ok(HttpResponsePayload::Accepted) => return,
                Ok(_) | Err(_) if attempt + 1 < DOCKER_MUTATION_RETRIES => {
                    tokio::time::sleep(Duration::from_millis(DOCKER_WAIT_POLL_MS)).await;
                }
                Ok(other) => panic!(
                    "expected accepted mutation response, got {other:?}\n{}",
                    self.compose_logs()
                ),
                Err(err) => panic!(
                    "mutation request should succeed: {err:?}\n{}",
                    self.compose_logs()
                ),
            }
        }

        unreachable!("mutation retries should either return or panic");
    }

    pub async fn wait_for_workload(&self, node: &str, workload_id: &str, timeout: Duration) {
        let workload_id = WorkloadId::new(workload_id);
        let deadline = std::time::Instant::now() + timeout;
        loop {
            let snapshot = self.snapshot(node).await;
            if snapshot.state.desired.workloads.contains_key(&workload_id) {
                return;
            }

            if std::time::Instant::now() >= deadline {
                panic!(
                    "node {node} did not receive workload {} in time\n{}",
                    workload_id,
                    self.compose_logs()
                );
            }

            tokio::time::sleep(Duration::from_millis(DOCKER_WAIT_POLL_MS)).await;
        }
    }

    pub async fn wait_for_workloads<I, S>(&self, node: &str, workload_ids: I, timeout: Duration)
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let workload_ids = workload_ids
            .into_iter()
            .map(|workload_id| WorkloadId::new(workload_id.as_ref()))
            .collect::<BTreeSet<_>>();
        let deadline = std::time::Instant::now() + timeout;

        loop {
            let snapshot = self.snapshot(node).await;
            let present = snapshot
                .state
                .desired
                .workloads
                .keys()
                .cloned()
                .collect::<BTreeSet<_>>();
            let missing = workload_ids
                .difference(&present)
                .cloned()
                .collect::<Vec<_>>();
            if missing.is_empty() {
                return;
            }

            if std::time::Instant::now() >= deadline {
                panic!(
                    "node {node} did not receive workloads {:?} in time\n{}",
                    missing,
                    self.compose_logs()
                );
            }

            tokio::time::sleep(Duration::from_millis(DOCKER_WAIT_POLL_MS)).await;
        }
    }

    pub async fn wait_for_workload_absent(&self, node: &str, workload_id: &str, timeout: Duration) {
        let workload_id = WorkloadId::new(workload_id);
        let deadline = std::time::Instant::now() + timeout;
        loop {
            let snapshot = self.snapshot(node).await;
            if !snapshot.state.desired.workloads.contains_key(&workload_id) {
                return;
            }

            if std::time::Instant::now() >= deadline {
                panic!(
                    "node {node} still has workload {} after timeout\n{}",
                    workload_id,
                    self.compose_logs()
                );
            }

            tokio::time::sleep(Duration::from_millis(DOCKER_WAIT_POLL_MS)).await;
        }
    }

    pub async fn delete_workload(&self, node: &str, workload_id: &str) {
        let snapshot = self.snapshot(node).await;
        let batch = MutationBatch {
            base_revision: snapshot.state.desired.revision,
            mutations: vec![DesiredStateMutation::RemoveWorkload(WorkloadId::new(
                workload_id,
            ))],
        };

        let response = self
            .client(node)
            .send(&HttpRequestPayload::Control(Box::new(
                ControlMessage::Mutations(batch),
            )))
            .await
            .expect("delete mutation request should succeed");
        assert_eq!(response, HttpResponsePayload::Accepted);
    }

    pub async fn raw_post(&self, node: &str, path: &str, body: &[u8]) -> reqwest::Response {
        let port = self.ports.port_for(node);
        reqwest::Client::new()
            .post(format!("http://127.0.0.1:{port}{path}"))
            .body(body.to_vec())
            .send()
            .await
            .expect("raw HTTP request should succeed")
    }

    pub fn restart_service(&self, service: &str) {
        self.compose(["restart", service]);
    }

    pub fn stop_service(&self, service: &str) {
        self.compose(["stop", service]);
    }

    pub fn start_service(&self, service: &str) {
        self.compose(["start", service]);
    }

    pub fn exec_service<I, S>(&self, service: &str, args: I)
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let output = self.exec_service_output(service, args);
        if !output.status.success() {
            panic!(
                "docker compose exec failed\nstdout:\n{}\nstderr:\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        }
    }

    pub fn exec_service_output<I, S>(&self, service: &str, args: I) -> Output
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut command = self.compose_command();
        command.arg("exec").arg("-T").arg(service);
        for arg in args {
            command.arg(arg.as_ref());
        }
        command.output().expect("docker compose exec should run")
    }

    pub fn compose_logs(&self) -> String {
        let output = self
            .compose_command()
            .arg("logs")
            .output()
            .expect("docker compose logs should run");
        format!(
            "stdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        )
    }

    pub fn client(&self, node: &str) -> HttpClient {
        let port = self.ports.port_for(node);
        HttpClient::try_new(format!("http://127.0.0.1:{port}")).expect("HTTP client should build")
    }

    fn compose<const N: usize>(&self, args: [&str; N]) {
        if let Err(error) = self.compose_result(args) {
            panic!("{error}");
        }
    }

    fn compose_result<const N: usize>(&self, args: [&str; N]) -> Result<(), String> {
        let output = self
            .compose_command()
            .args(args)
            .output()
            .expect("docker compose command should run");
        if output.status.success() {
            Ok(())
        } else {
            Err(format!(
                "docker compose command failed: {:?}\nstdout:\n{}\nstderr:\n{}",
                args,
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            ))
        }
    }

    fn cleanup_resources(&self) {
        let _ = self
            .compose_command()
            .args(["down", "-v", "--remove-orphans"])
            .output();
        let _ = fs::remove_file(&self.compose_file);
    }

    fn compose_command(&self) -> Command {
        let mut command = Command::new("docker");
        command
            .current_dir(&self.repo_root)
            .arg("compose")
            .arg("-p")
            .arg(&self.project_name)
            .arg("-f")
            .arg(&self.compose_file);
        command
    }
}

impl Drop for DockerCluster {
    fn drop(&mut self) {
        self.cleanup_resources();
    }
}

fn docker_error_is_retryable(error: &str) -> bool {
    error.contains("address already in use")
        || error.contains("failed to bind host port")
        || error.contains("port is already allocated")
}

pub struct ClusterPorts {
    by_node: BTreeMap<String, u16>,
}

#[derive(Default)]
pub struct ClusterOptions {
    pub node_ids: Vec<String>,
    pub extra_peers: BTreeMap<String, Vec<String>>,
    pub explicit_peers: BTreeMap<String, Vec<String>>,
    pub reconcile_ms_by_node: BTreeMap<String, u64>,
    pub peer_sync_mode_by_node: BTreeMap<String, String>,
    pub peer_sync_max_in_flight_by_node: BTreeMap<String, usize>,
}

impl ClusterPorts {
    fn allocate(node_ids: &[String]) -> Self {
        let by_node = node_ids
            .iter()
            .cloned()
            .map(|node_id| (node_id, ephemeral_port()))
            .collect();
        Self { by_node }
    }

    fn port_for(&self, node: &str) -> u16 {
        *self
            .by_node
            .get(node)
            .unwrap_or_else(|| panic!("unknown node {node}"))
    }
}

impl ClusterOptions {
    pub fn with_node_count(node_count: usize) -> Self {
        let node_ids = (0..node_count)
            .map(|index| format!("node-{}", (b'a' + index as u8) as char))
            .collect();
        Self {
            node_ids,
            extra_peers: BTreeMap::new(),
            explicit_peers: BTreeMap::new(),
            reconcile_ms_by_node: BTreeMap::new(),
            peer_sync_mode_by_node: BTreeMap::new(),
            peer_sync_max_in_flight_by_node: BTreeMap::new(),
        }
    }

    pub fn effective_node_ids(&self) -> Vec<String> {
        if self.node_ids.is_empty() {
            vec![
                "node-a".to_owned(),
                "node-b".to_owned(),
                "node-c".to_owned(),
            ]
        } else {
            self.node_ids.clone()
        }
    }

    fn peers_for(&self, node_id: &str) -> Vec<String> {
        if let Some(explicit) = self.explicit_peers.get(node_id) {
            return explicit.clone();
        }
        let node_ids = self.effective_node_ids();
        let mut peers = node_ids
            .into_iter()
            .filter(|peer_id| peer_id != node_id)
            .map(|peer_id| format!("{peer_id}=http://{peer_id}:9100"))
            .collect::<Vec<_>>();
        if let Some(extra) = self.extra_peers.get(node_id) {
            peers.extend(extra.clone());
        }
        peers
    }
}

fn write_compose_file(
    project_name: &str,
    ports: &ClusterPorts,
    options: &ClusterOptions,
    image: &str,
) -> PathBuf {
    let node_ids = options.effective_node_ids();
    let mut compose = String::from("services:\n");
    let image = image.to_owned();

    for node_id in &node_ids {
        let peers = options.peers_for(node_id).join(",");
        let port = ports.port_for(node_id);
        let reconcile_ms = options
            .reconcile_ms_by_node
            .get(node_id)
            .copied()
            .unwrap_or(DOCKER_RECONCILE_MS);
        let peer_sync_mode = options
            .peer_sync_mode_by_node
            .get(node_id)
            .cloned()
            .unwrap_or_else(|| "parallel".to_owned());
        let peer_sync_max_in_flight = options
            .peer_sync_max_in_flight_by_node
            .get(node_id)
            .copied()
            .unwrap_or(4);
        compose.push_str(&format!(
            r#"  {node_id}:
    image: {image}
    environment:
      ORION_NODE_ID: {node_id}
      ORION_NODE_HTTP_ADDR: 0.0.0.0:9100
      ORION_NODE_PEER_AUTH: disabled
      ORION_NODE_RECONCILE_MS: {reconcile_ms}
      ORION_NODE_STATE_DIR: /var/lib/orion
      ORION_NODE_PEERS: {peers}
      ORION_NODE_PEER_SYNC_MODE: {peer_sync_mode}
      ORION_NODE_PEER_SYNC_MAX_IN_FLIGHT: {peer_sync_max_in_flight}
    ports:
      - "{port}:9100"
    volumes:
      - {node_id}-data:/var/lib/orion

"#,
        ));
    }

    compose.push_str("volumes:\n");
    for node_id in &node_ids {
        compose.push_str(&format!("  {node_id}-data:\n"));
    }

    let path = std::env::temp_dir().join(format!("{project_name}.compose.yml"));
    fs::write(&path, compose).expect("compose file should be written");
    path
}

fn ephemeral_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("ephemeral port bind should succeed")
        .local_addr()
        .expect("ephemeral port address should be readable")
        .port()
}

fn ensure_test_image(repo_root: &Path) -> String {
    static IMAGE: OnceLock<String> = OnceLock::new();
    if let Some(tag) = IMAGE.get() {
        return tag.clone();
    }

    let tag =
        std::env::var("ORION_TEST_IMAGE").unwrap_or_else(|_| "orion-node-test:dev".to_owned());
    let skip_build = matches!(
        std::env::var("ORION_SKIP_DOCKER_BUILD").as_deref(),
        Ok("1") | Ok("true") | Ok("yes")
    );
    if skip_build {
        if !docker_image_exists(&tag) {
            panic!(
                "ORION_SKIP_DOCKER_BUILD is set but docker image '{}' does not exist",
                tag
            );
        }
        let _ = IMAGE.set(tag.clone());
        return tag;
    }

    let dockerfile = repo_root.join("testing/docker/orion-node.Dockerfile");
    let status = Command::new("docker")
        .current_dir(repo_root)
        .arg("build")
        .arg("--pull=false")
        .arg("-f")
        .arg(&dockerfile)
        .arg("-t")
        .arg(&tag)
        .arg(".")
        .status()
        .expect("docker build should run");
    if !status.success() {
        panic!("docker build failed for test image '{tag}'");
    }

    let _ = IMAGE.set(tag.clone());
    tag
}

fn docker_image_exists(tag: &str) -> bool {
    Command::new("docker")
        .arg("image")
        .arg("inspect")
        .arg(tag)
        .output()
        .map(|output| output.status.success())
        .unwrap_or(false)
}
