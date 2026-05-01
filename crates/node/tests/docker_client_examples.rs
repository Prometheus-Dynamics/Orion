mod support;

use orion::{
    ConfigSchemaId,
    control_plane::{
        AvailabilityState, ControlMessage, DesiredState, DesiredStateMutation, ExecutorRecord,
        HealthState, LeaseRecord, LeaseState, MutationBatch, ProviderRecord, ResourceOwnershipMode,
        ResourceRecord, TypedConfigValue, WorkloadConfig, WorkloadRecord,
    },
    transport::http::{HttpRequestPayload, HttpResponsePayload},
};
use support::docker_cluster::{ClusterOptions, DockerCluster, stopped_workload};

fn stream_socket(node_id: &str) -> String {
    format!("/tmp/orion-{node_id}-control-stream.sock")
}

fn output_text(output: &std::process::Output) -> String {
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    format!("stdout:\n{stdout}\nstderr:\n{stderr}")
}

async fn wait_for_snapshot_condition<F>(
    cluster: &DockerCluster,
    node: &str,
    timeout: std::time::Duration,
    predicate: F,
) where
    F: Fn(&orion::control_plane::StateSnapshot) -> bool,
{
    let deadline = std::time::Instant::now() + timeout;
    loop {
        let snapshot = cluster.snapshot(node).await;
        if predicate(&snapshot) {
            return;
        }

        if std::time::Instant::now() >= deadline {
            panic!(
                "node {node} did not satisfy snapshot condition in time\n{}",
                cluster.compose_logs()
            );
        }

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

async fn apply_mutations_with_retries(
    cluster: &DockerCluster,
    node: &str,
    batch: MutationBatch,
) -> HttpResponsePayload {
    for _ in 0..10 {
        match cluster
            .client(node)
            .send(&HttpRequestPayload::Control(Box::new(
                ControlMessage::Mutations(batch.clone()),
            )))
            .await
        {
            Ok(HttpResponsePayload::Accepted) => return HttpResponsePayload::Accepted,
            Ok(_) | Err(_) => tokio::time::sleep(std::time::Duration::from_millis(100)).await,
        }
    }

    cluster
        .client(node)
        .send(&HttpRequestPayload::Control(Box::new(
            ControlMessage::Mutations(batch),
        )))
        .await
        .expect("mutation request should succeed after retries")
}

#[tokio::test]
#[ignore = "requires docker compose"]
async fn docker_client_example_control_plane_write_watch_receives_state() {
    let cluster = DockerCluster::start("client-example-control-write").await;
    cluster.wait_for_all_http().await;

    let output = cluster.exec_service_output(
        "node-a",
        [
            "control_plane_write_watch",
            "http://node-a:9100",
            &stream_socket("node-a"),
            "example-control",
            "artifact.demo",
            "workload.demo",
            "node-a",
            "graph.exec.v1",
        ],
    );
    assert!(
        output.status.success(),
        "control-plane example failed\n{}",
        output_text(&output)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("control-plane write+watch"));
    assert!(stdout.contains("artifacts=1"));
    assert!(stdout.contains("workloads=1"));
}

#[tokio::test]
#[ignore = "requires docker compose"]
async fn docker_client_example_executor_register_and_watch_receives_assignments() {
    let cluster = DockerCluster::start("client-example-executor-register").await;
    cluster.wait_for_all_http().await;

    let output = cluster.exec_service_output(
        "node-a",
        [
            "executor_register_and_watch",
            "http://node-a:9100",
            "/tmp/orion-node-a-control.sock",
            &stream_socket("node-a"),
            "example-executor-client",
            "executor.engine",
            "node-a",
            "graph.exec.v1",
            "artifact.exec",
            "workload.exec",
        ],
    );
    assert!(
        output.status.success(),
        "executor example failed\n{}",
        output_text(&output)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("executor register+watch"));
    assert!(stdout.contains("executor.engine"));
    assert!(stdout.contains("workload.exec"));
}

#[tokio::test]
#[ignore = "requires docker compose"]
async fn docker_client_example_provider_register_and_watch_receives_leases() {
    let cluster = DockerCluster::start("client-example-provider-register").await;
    cluster.wait_for_all_http().await;

    let output = cluster.exec_service_output(
        "node-a",
        [
            "provider_register_and_watch",
            "http://node-a:9100",
            "/tmp/orion-node-a-control.sock",
            &stream_socket("node-a"),
            "example-provider-client",
            "provider.camera",
            "node-a",
            "camera.device",
            "resource.camera-01",
        ],
    );
    assert!(
        output.status.success(),
        "provider example failed\n{}",
        output_text(&output)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("provider register+watch"));
    assert!(stdout.contains("provider.camera"));
    assert!(stdout.contains("resource.camera-01"));
}

#[tokio::test]
#[ignore = "requires docker compose"]
async fn docker_client_example_multi_watch_receives_all_roles() {
    let cluster = DockerCluster::start("client-example-multi").await;
    cluster.wait_for_all_http().await;

    let provider = ProviderRecord::builder("provider.multi", "node-a")
        .resource_type("imu.sample")
        .build();
    let resource = ResourceRecord::builder("resource.imu-01", "imu.sample", "provider.multi")
        .health(HealthState::Healthy)
        .availability(AvailabilityState::Available)
        .lease_state(LeaseState::Leased)
        .build();
    let snapshot = cluster.snapshot("node-a").await;
    let batch = MutationBatch {
        base_revision: snapshot.state.desired.revision,
        mutations: vec![
            DesiredStateMutation::PutExecutor(
                ExecutorRecord::builder("executor.engine", "node-a")
                    .runtime_type("graph.exec.v1")
                    .build(),
            ),
            DesiredStateMutation::PutWorkload(stopped_workload("workload.multi", "node-a")),
            DesiredStateMutation::PutProvider(provider.clone()),
            DesiredStateMutation::PutResource(resource.clone()),
            DesiredStateMutation::PutLease(
                LeaseRecord::builder(resource.resource_id.clone())
                    .lease_state(LeaseState::Leased)
                    .holder_node("node-a")
                    .build(),
            ),
        ],
    };
    let response = apply_mutations_with_retries(&cluster, "node-a", batch).await;
    assert_eq!(response, HttpResponsePayload::Accepted);

    let output = cluster.exec_service_output(
        "node-a",
        [
            "multi_watch",
            "/tmp/orion-node-a-control.sock",
            &stream_socket("node-a"),
            "example-multi",
            "executor.engine",
            "provider.multi",
            "node-a",
            "graph.exec.v1",
            "imu.sample",
        ],
    );
    assert!(
        output.status.success(),
        "multi example failed\n{}",
        output_text(&output)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("multi-watch"));
    assert!(stdout.contains("state:rev="));
    assert!(stdout.contains("executor:executor.engine"));
    assert!(stdout.contains("provider:provider.multi"));
}

#[tokio::test]
#[ignore = "requires docker compose"]
async fn docker_client_example_reports_expected_errors_for_invalid_usage() {
    let cluster = DockerCluster::start("client-example-errors").await;
    cluster.wait_for_all_http().await;

    let bad_args = cluster.exec_service_output("node-a", ["control_plane_write_watch"]);
    assert!(
        !bad_args.status.success(),
        "control-plane example should fail with missing args\n{}",
        output_text(&bad_args)
    );
    let stderr = String::from_utf8_lossy(&bad_args.stderr);
    assert!(stderr.contains("expected 7 args"));

    let rejected = cluster.exec_service_output("node-a", ["camera_pipeline_publish"]);
    assert!(
        !rejected.status.success(),
        "camera pipeline example should fail with missing args\n{}",
        output_text(&rejected)
    );
    let stderr = String::from_utf8_lossy(&rejected.stderr);
    assert!(stderr.contains("expected 8 args"));
}

#[tokio::test]
#[ignore = "requires docker compose"]
async fn docker_client_example_camera_controller_and_consumers_flow() {
    let cluster = DockerCluster::start_with_options(
        "client-example-camera-flow",
        ClusterOptions::with_node_count(1),
    )
    .await;
    cluster.wait_for_all_http().await;

    let provider = cluster.exec_service_output(
        "node-a",
        [
            "camera_provider_publish",
            "/tmp/orion-node-a-control.sock",
            "example-camera-provider",
            "provider.camera",
            "node-a",
            "resource.camera.raw.front",
        ],
    );
    assert!(
        provider.status.success(),
        "camera provider publish failed\n{}",
        output_text(&provider)
    );

    let pipeline = cluster.exec_service_output(
        "node-a",
        [
            "camera_pipeline_publish",
            "/tmp/orion-node-a-control.sock",
            "example-camera-pipeline",
            "executor.camera-stack",
            "node-a",
            "workload.camera-controller",
            "resource.camera.raw.front",
            "resource.camera.stream.front",
            "1280",
        ],
    );
    assert!(
        pipeline.status.success(),
        "camera pipeline publish failed\n{}",
        output_text(&pipeline)
    );

    wait_for_snapshot_condition(
        &cluster,
        "node-a",
        std::time::Duration::from_secs(10),
        |snapshot| {
            snapshot
                .state
                .observed
                .resources
                .get(&orion::ResourceId::new("resource.camera.stream.front"))
                .map(|resource| {
                    resource
                        .labels
                        .iter()
                        .any(|label| label == "mode.width=1280")
                })
                .unwrap_or(false)
        },
    )
    .await;

    let snapshot = cluster.snapshot("node-a").await;
    let batch = MutationBatch {
        base_revision: snapshot.state.desired.revision,
        mutations: vec![
            DesiredStateMutation::PutWorkload(
                WorkloadRecord::builder(
                    "workload.camera-controller",
                    "camera.controller.v1",
                    "artifact.camera-controller",
                )
                .config(
                    WorkloadConfig::new(ConfigSchemaId::new("camera.controller.config.v1"))
                        .field("width", TypedConfigValue::UInt(1280)),
                )
                .desired_state(DesiredState::Running)
                .assigned_to("node-a")
                .require_resource_with_ownership(
                    "camera.device",
                    1,
                    ResourceOwnershipMode::ExclusiveOwnerPublishesDerived,
                )
                .build(),
            ),
            DesiredStateMutation::PutWorkload(
                WorkloadRecord::builder(
                    "workload.vision-a",
                    "vision.consumer.v1",
                    "artifact.vision-a",
                )
                .desired_state(DesiredState::Running)
                .assigned_to("node-a")
                .require_resource_with_ownership(
                    "camera.frame_stream",
                    1,
                    ResourceOwnershipMode::SharedRead,
                )
                .build(),
            ),
            DesiredStateMutation::PutWorkload(
                WorkloadRecord::builder(
                    "workload.vision-b",
                    "vision.consumer.v1",
                    "artifact.vision-b",
                )
                .desired_state(DesiredState::Running)
                .assigned_to("node-a")
                .require_resource_with_ownership(
                    "camera.frame_stream",
                    1,
                    ResourceOwnershipMode::SharedRead,
                )
                .build(),
            ),
        ],
    };
    let response = cluster
        .client("node-a")
        .send(&HttpRequestPayload::Control(Box::new(
            ControlMessage::Mutations(batch),
        )))
        .await
        .expect("camera desired mutation should apply");
    assert_eq!(response, HttpResponsePayload::Accepted);

    cluster
        .wait_for_workloads(
            "node-a",
            [
                "workload.camera-controller",
                "workload.vision-a",
                "workload.vision-b",
            ],
            std::time::Duration::from_secs(10),
        )
        .await;

    let snapshot = cluster.snapshot("node-a").await;
    let reconfigured = MutationBatch {
        base_revision: snapshot.state.desired.revision,
        mutations: vec![DesiredStateMutation::PutWorkload(
            WorkloadRecord::builder(
                "workload.camera-controller",
                "camera.controller.v1",
                "artifact.camera-controller",
            )
            .config(
                WorkloadConfig::new(ConfigSchemaId::new("camera.controller.config.v1"))
                    .field("width", TypedConfigValue::UInt(1920)),
            )
            .desired_state(DesiredState::Running)
            .assigned_to("node-a")
            .require_resource_with_ownership(
                "camera.device",
                1,
                ResourceOwnershipMode::ExclusiveOwnerPublishesDerived,
            )
            .build(),
        )],
    };
    let response = cluster
        .client("node-a")
        .send(&HttpRequestPayload::Control(Box::new(
            ControlMessage::Mutations(reconfigured),
        )))
        .await
        .expect("camera reconfiguration should apply");
    assert_eq!(response, HttpResponsePayload::Accepted);

    let republished = cluster.exec_service_output(
        "node-a",
        [
            "camera_pipeline_publish",
            "/tmp/orion-node-a-control.sock",
            "example-camera-pipeline",
            "executor.camera-stack",
            "node-a",
            "workload.camera-controller",
            "resource.camera.raw.front",
            "resource.camera.stream.front",
            "1920",
        ],
    );
    assert!(
        republished.status.success(),
        "camera pipeline republish failed\n{}",
        output_text(&republished)
    );

    wait_for_snapshot_condition(
        &cluster,
        "node-a",
        std::time::Duration::from_secs(10),
        |snapshot| {
            snapshot
                .state
                .observed
                .resources
                .get(&orion::ResourceId::new("resource.camera.stream.front"))
                .map(|resource| {
                    resource
                        .labels
                        .iter()
                        .any(|label| label == "mode.width=1920")
                })
                .unwrap_or(false)
        },
    )
    .await;

    let snapshot = cluster.snapshot("node-a").await;
    assert!(
        snapshot
            .state
            .desired
            .workloads
            .contains_key(&orion::WorkloadId::new("workload.vision-a"))
    );
    assert!(
        snapshot
            .state
            .desired
            .workloads
            .contains_key(&orion::WorkloadId::new("workload.vision-b"))
    );

    let codec = orion::transport::http::HttpCodec;
    let invalid_reconfiguration = MutationBatch {
        base_revision: snapshot.state.desired.revision,
        mutations: vec![DesiredStateMutation::PutWorkload(
            WorkloadRecord::builder(
                "workload.camera-controller",
                "camera.controller.v1",
                "artifact.camera-controller",
            )
            .config(
                WorkloadConfig::new(ConfigSchemaId::new("camera.controller.config.v1"))
                    .field("width", TypedConfigValue::UInt(1920)),
            )
            .desired_state(DesiredState::Running)
            .assigned_to("node-a")
            .require_resource_with_ownership(
                "camera.device",
                2,
                ResourceOwnershipMode::ExclusiveOwnerPublishesDerived,
            )
            .build(),
        )],
    };
    let invalid_request = codec
        .encode_request(&HttpRequestPayload::Control(Box::new(
            ControlMessage::Mutations(invalid_reconfiguration),
        )))
        .expect("invalid reconfiguration should encode");
    let invalid_raw = cluster
        .raw_post("node-a", &invalid_request.path, &invalid_request.body)
        .await;
    assert_eq!(
        invalid_raw.status(),
        reqwest::StatusCode::INTERNAL_SERVER_ERROR
    );
    let invalid_body = invalid_raw
        .text()
        .await
        .expect("invalid reconfiguration body should read");
    assert!(invalid_body.contains("does not allow additional consumers"));

    let snapshot = cluster.snapshot("node-a").await;
    let conflicting = MutationBatch {
        base_revision: snapshot.state.desired.revision,
        mutations: vec![DesiredStateMutation::PutWorkload(
            WorkloadRecord::builder(
                "workload.camera-controller-b",
                "camera.controller.v1",
                "artifact.camera-controller-b",
            )
            .config(
                WorkloadConfig::new(ConfigSchemaId::new("camera.controller.config.v1"))
                    .field("width", TypedConfigValue::UInt(640)),
            )
            .desired_state(DesiredState::Running)
            .assigned_to("node-a")
            .require_resource_with_ownership(
                "camera.device",
                1,
                ResourceOwnershipMode::ExclusiveOwnerPublishesDerived,
            )
            .build(),
        )],
    };
    let request = codec
        .encode_request(&HttpRequestPayload::Control(Box::new(
            ControlMessage::Mutations(conflicting),
        )))
        .expect("conflicting mutation request should encode");
    let raw = cluster
        .raw_post("node-a", &request.path, &request.body)
        .await;
    assert_eq!(raw.status(), reqwest::StatusCode::INTERNAL_SERVER_ERROR);
    let body = raw.text().await.expect("error body should read");
    assert!(body.contains("does not allow additional consumers"));
}
