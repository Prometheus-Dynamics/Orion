use std::time::Duration;

use orion::{
    ArtifactId, NodeId, ResourceType, WorkloadId,
    control_plane::{
        AvailabilityState, ControlMessage, DesiredState, DesiredStateMutation, ExecutorRecord,
        HealthState, LeaseRecord, LeaseState, MutationBatch, ProviderRecord, ResourceRecord,
        WorkloadRecord,
    },
    transport::http::{HttpRequestPayload, HttpResponsePayload},
};

use super::wait_for_snapshot_condition;
use crate::support::docker_cluster::DockerCluster;

#[tokio::test]
#[ignore = "requires docker compose"]
async fn docker_cluster_propagates_provider_resources_and_cross_node_leases() {
    let cluster = DockerCluster::start("resource-lease-propagation").await;

    cluster.wait_for_http("node-a").await;
    cluster.wait_for_http("node-b").await;
    cluster.wait_for_http("node-c").await;

    let snapshot = cluster.snapshot("node-a").await;
    let batch = MutationBatch {
        base_revision: snapshot.state.desired.revision,
        mutations: vec![
            DesiredStateMutation::PutProvider(
                ProviderRecord::builder("provider.camera", "node-a")
                    .resource_type("camera.device")
                    .build(),
            ),
            DesiredStateMutation::PutResource(
                ResourceRecord::builder("resource.camera-01", "camera.device", "provider.camera")
                    .health(HealthState::Healthy)
                    .availability(AvailabilityState::Available)
                    .lease_state(LeaseState::Leased)
                    .endpoint("channel://camera-01")
                    .build(),
            ),
            DesiredStateMutation::PutLease(
                LeaseRecord::builder("resource.camera-01")
                    .lease_state(LeaseState::Leased)
                    .holder_node("node-b")
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
        .expect("resource mutations should apply");
    assert_eq!(response, HttpResponsePayload::Accepted);

    for node in ["node-a", "node-b", "node-c"] {
        wait_for_snapshot_condition(&cluster, node, Duration::from_secs(20), |snapshot| {
            snapshot
                .state
                .desired
                .providers
                .contains_key(&orion::ProviderId::new("provider.camera"))
                && snapshot
                    .state
                    .desired
                    .resources
                    .contains_key(&orion::ResourceId::new("resource.camera-01"))
                && snapshot
                    .state
                    .desired
                    .leases
                    .get(&orion::ResourceId::new("resource.camera-01"))
                    .and_then(|lease| lease.holder_node_id.as_ref())
                    == Some(&NodeId::new("node-b"))
        })
        .await;
    }
}

#[tokio::test]
#[ignore = "requires docker compose"]
async fn docker_cluster_workload_can_use_remote_resource_across_nodes() {
    let cluster = DockerCluster::start("remote-resource-binding").await;

    cluster.wait_for_http("node-a").await;
    cluster.wait_for_http("node-b").await;
    cluster.wait_for_http("node-c").await;

    let snapshot = cluster.snapshot("node-a").await;
    let workload = WorkloadRecord::builder(
        WorkloadId::new("workload.remote-camera"),
        "graph.exec.v1",
        ArtifactId::new("artifact.workload.remote-camera"),
    )
    .desired_state(DesiredState::Running)
    .assigned_to("node-b")
    .require_resource(ResourceType::new("camera.device"), 1)
    .bind_resource("resource.camera-01", "node-a")
    .build();
    let batch = MutationBatch {
        base_revision: snapshot.state.desired.revision,
        mutations: vec![
            DesiredStateMutation::PutProvider(
                ProviderRecord::builder("provider.camera", "node-a")
                    .resource_type("camera.device")
                    .build(),
            ),
            DesiredStateMutation::PutResource(
                ResourceRecord::builder("resource.camera-01", "camera.device", "provider.camera")
                    .health(HealthState::Healthy)
                    .availability(AvailabilityState::Available)
                    .lease_state(LeaseState::Leased)
                    .endpoint("channel://camera-01")
                    .build(),
            ),
            DesiredStateMutation::PutLease(
                LeaseRecord::builder("resource.camera-01")
                    .lease_state(LeaseState::Leased)
                    .holder_node("node-b")
                    .holder_workload("workload.remote-camera")
                    .build(),
            ),
            DesiredStateMutation::PutArtifact(
                orion::control_plane::ArtifactRecord::builder("artifact.workload.remote-camera")
                    .build(),
            ),
            DesiredStateMutation::PutWorkload(workload),
        ],
    };
    let response = cluster
        .client("node-a")
        .send(&HttpRequestPayload::Control(Box::new(
            ControlMessage::Mutations(batch),
        )))
        .await
        .expect("remote-resource workload mutations should apply");
    assert_eq!(response, HttpResponsePayload::Accepted);

    for node in ["node-a", "node-b", "node-c"] {
        wait_for_snapshot_condition(&cluster, node, Duration::from_secs(20), |snapshot| {
            snapshot
                .state
                .desired
                .workloads
                .get(&WorkloadId::new("workload.remote-camera"))
                .map(|workload| {
                    workload.assigned_node_id.as_ref() == Some(&NodeId::new("node-b"))
                        && workload.resource_bindings.len() == 1
                        && workload.resource_bindings[0].resource_id.as_str()
                            == "resource.camera-01"
                        && workload.resource_bindings[0].node_id.as_str() == "node-a"
                })
                .unwrap_or(false)
        })
        .await;
    }
}

#[tokio::test]
#[ignore = "requires docker compose"]
async fn docker_cluster_workload_move_updates_assignment_and_resource_binding() {
    let cluster = DockerCluster::start("workload-move-binding").await;

    cluster.wait_for_http("node-a").await;
    cluster.wait_for_http("node-b").await;
    cluster.wait_for_http("node-c").await;

    let snapshot = cluster.snapshot("node-a").await;
    let initial = MutationBatch {
        base_revision: snapshot.state.desired.revision,
        mutations: vec![
            DesiredStateMutation::PutProvider(
                ProviderRecord::builder("provider.camera-a", "node-a")
                    .resource_type("camera.device")
                    .build(),
            ),
            DesiredStateMutation::PutProvider(
                ProviderRecord::builder("provider.camera-c", "node-c")
                    .resource_type("camera.device")
                    .build(),
            ),
            DesiredStateMutation::PutExecutor(
                ExecutorRecord::builder("executor.engine-a", "node-a")
                    .runtime_type("graph.exec.v1")
                    .build(),
            ),
            DesiredStateMutation::PutExecutor(
                ExecutorRecord::builder("executor.engine-c", "node-c")
                    .runtime_type("graph.exec.v1")
                    .build(),
            ),
            DesiredStateMutation::PutResource(
                ResourceRecord::builder("resource.camera-a", "camera.device", "provider.camera-a")
                    .health(HealthState::Healthy)
                    .availability(AvailabilityState::Available)
                    .lease_state(LeaseState::Leased)
                    .build(),
            ),
            DesiredStateMutation::PutResource(
                ResourceRecord::builder("resource.camera-c", "camera.device", "provider.camera-c")
                    .health(HealthState::Healthy)
                    .availability(AvailabilityState::Available)
                    .build(),
            ),
            DesiredStateMutation::PutLease(
                LeaseRecord::builder("resource.camera-a")
                    .lease_state(LeaseState::Leased)
                    .holder_node("node-a")
                    .holder_workload("workload.move")
                    .build(),
            ),
            DesiredStateMutation::PutArtifact(
                orion::control_plane::ArtifactRecord::builder("artifact.workload.move").build(),
            ),
            DesiredStateMutation::PutWorkload(
                WorkloadRecord::builder(
                    "workload.move",
                    "graph.exec.v1",
                    ArtifactId::new("artifact.workload.move"),
                )
                .desired_state(DesiredState::Running)
                .assigned_to("node-a")
                .require_resource("camera.device", 1)
                .bind_resource("resource.camera-a", "node-a")
                .build(),
            ),
        ],
    };
    let response = cluster
        .client("node-a")
        .send(&HttpRequestPayload::Control(Box::new(
            ControlMessage::Mutations(initial),
        )))
        .await
        .expect("initial move setup should apply");
    assert_eq!(response, HttpResponsePayload::Accepted);

    for node in ["node-a", "node-b", "node-c"] {
        wait_for_snapshot_condition(&cluster, node, Duration::from_secs(20), |snapshot| {
            snapshot
                .state
                .desired
                .workloads
                .get(&WorkloadId::new("workload.move"))
                .map(|workload| {
                    workload.assigned_node_id.as_ref() == Some(&NodeId::new("node-a"))
                        && workload.resource_bindings[0].resource_id.as_str() == "resource.camera-a"
                        && workload.resource_bindings[0].node_id.as_str() == "node-a"
                })
                .unwrap_or(false)
        })
        .await;
    }

    let snapshot = cluster.snapshot("node-b").await;
    let moved = MutationBatch {
        base_revision: snapshot.state.desired.revision,
        mutations: vec![
            DesiredStateMutation::PutResource(
                ResourceRecord::builder("resource.camera-c", "camera.device", "provider.camera-c")
                    .health(HealthState::Healthy)
                    .availability(AvailabilityState::Available)
                    .lease_state(LeaseState::Leased)
                    .build(),
            ),
            DesiredStateMutation::PutLease(
                LeaseRecord::builder("resource.camera-c")
                    .lease_state(LeaseState::Leased)
                    .holder_node("node-c")
                    .holder_workload("workload.move")
                    .build(),
            ),
            DesiredStateMutation::RemoveLease(orion::ResourceId::new("resource.camera-a")),
            DesiredStateMutation::PutWorkload(
                WorkloadRecord::builder(
                    "workload.move",
                    "graph.exec.v1",
                    ArtifactId::new("artifact.workload.move"),
                )
                .desired_state(DesiredState::Running)
                .assigned_to("node-c")
                .require_resource("camera.device", 1)
                .bind_resource("resource.camera-c", "node-c")
                .build(),
            ),
        ],
    };
    let response = cluster
        .client("node-b")
        .send(&HttpRequestPayload::Control(Box::new(
            ControlMessage::Mutations(moved),
        )))
        .await
        .expect("workload move should apply");
    assert_eq!(response, HttpResponsePayload::Accepted);

    for node in ["node-a", "node-b", "node-c"] {
        wait_for_snapshot_condition(&cluster, node, Duration::from_secs(20), |snapshot| {
            snapshot
                .state
                .desired
                .workloads
                .get(&WorkloadId::new("workload.move"))
                .map(|workload| {
                    workload.assigned_node_id.as_ref() == Some(&NodeId::new("node-c"))
                        && workload.resource_bindings.len() == 1
                        && workload.resource_bindings[0].resource_id.as_str() == "resource.camera-c"
                        && workload.resource_bindings[0].node_id.as_str() == "node-c"
                })
                .unwrap_or(false)
        })
        .await;
    }
}
