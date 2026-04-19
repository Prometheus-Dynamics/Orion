mod support;

use orion::{
    ArtifactId, NodeId, Revision, RuntimeType, WorkloadId,
    control_plane::{ControlMessage, DesiredStateMutation, MutationBatch, SyncRequest},
    encode_to_vec,
    transport::http::{HttpRequestPayload, HttpResponsePayload},
};
use reqwest::StatusCode;
use std::time::Duration;

use support::docker_cluster::{ClusterOptions, DockerCluster, stopped_workload};

#[tokio::test]
#[ignore = "requires docker compose"]
async fn docker_cluster_returns_bad_request_for_malformed_json_and_stays_healthy() {
    let cluster = DockerCluster::start("bad-json").await;

    cluster.wait_for_http("node-a").await;

    let response = cluster
        .raw_post("node-a", "/v1/control/mutations", b"{not-json")
        .await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    cluster.wait_for_http("node-a").await;
}

#[tokio::test]
#[ignore = "requires docker compose"]
async fn docker_cluster_returns_not_found_for_unknown_route_and_stays_healthy() {
    let cluster = DockerCluster::start("unknown-route").await;

    cluster.wait_for_http("node-a").await;

    let response = cluster
        .raw_post("node-a", "/v1/control/unknown", b"{}")
        .await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    cluster.wait_for_http("node-a").await;
}

#[tokio::test]
#[ignore = "requires docker compose"]
async fn docker_cluster_five_nodes_bad_routes_and_bad_payloads_do_not_break_cluster() {
    let cluster = DockerCluster::start_with_options(
        "five-node-bad-inputs",
        ClusterOptions::with_node_count(5),
    )
    .await;

    cluster.wait_for_all_http().await;

    let bad_json = cluster
        .raw_post("node-b", "/v1/control/mutations", b"{broken")
        .await;
    assert_eq!(bad_json.status(), StatusCode::BAD_REQUEST);

    let unknown = cluster
        .raw_post("node-d", "/v1/control/not-real", b"{}")
        .await;
    assert_eq!(unknown.status(), StatusCode::NOT_FOUND);

    cluster
        .put_workload(
            "node-a",
            stopped_workload("workload.after-bad-inputs", "node-a"),
        )
        .await;

    for node in cluster.nodes().iter().map(String::as_str) {
        cluster
            .wait_for_workload(node, "workload.after-bad-inputs", Duration::from_secs(30))
            .await;
    }
}

#[tokio::test]
#[ignore = "requires docker compose"]
async fn docker_cluster_returns_error_for_valid_json_with_invalid_mutation_semantics_and_stays_healthy()
 {
    let cluster = DockerCluster::start("invalid-semantics").await;

    cluster.wait_for_http("node-a").await;

    let valid_but_invalid = encode_to_vec(&ControlMessage::Mutations(MutationBatch {
        base_revision: Revision::new(999999),
        mutations: vec![DesiredStateMutation::PutWorkload(stopped_workload(
            "workload.invalid.semantic",
            "node-a",
        ))],
    }))
    .expect("mutation batch should encode");

    let response = cluster
        .raw_post("node-a", "/v1/control/mutations", &valid_but_invalid)
        .await;
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);

    cluster.wait_for_http("node-a").await;
}

#[tokio::test]
#[ignore = "requires docker compose"]
async fn docker_cluster_returns_bad_request_for_valid_json_with_wrong_control_shape() {
    let cluster = DockerCluster::start("wrong-control-shape").await;

    cluster.wait_for_http("node-a").await;

    let wrong_shape = encode_to_vec(&orion::control_plane::ObservedStateUpdate {
        observed: orion::control_plane::ObservedClusterState::default(),
        applied: orion::control_plane::AppliedClusterState::default(),
    })
    .expect("observed update should encode");

    let response = cluster
        .raw_post("node-a", "/v1/control/mutations", &wrong_shape)
        .await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    cluster.wait_for_http("node-a").await;
}

#[tokio::test]
#[ignore = "requires docker compose"]
async fn docker_cluster_accepts_duplicate_workload_ids_in_single_batch_and_stays_healthy() {
    let cluster = DockerCluster::start("duplicate-workload-ids").await;

    cluster.wait_for_http("node-a").await;

    let snapshot = cluster.snapshot("node-a").await;
    let batch = MutationBatch {
        base_revision: snapshot.state.desired.revision,
        mutations: vec![
            DesiredStateMutation::PutWorkload(stopped_workload(
                "workload.duplicate.batch",
                "node-a",
            )),
            DesiredStateMutation::PutWorkload(stopped_workload(
                "workload.duplicate.batch",
                "node-b",
            )),
        ],
    };

    let response = cluster
        .client("node-a")
        .send(&HttpRequestPayload::Control(Box::new(
            ControlMessage::Mutations(batch),
        )))
        .await
        .expect("duplicate id batch request should return");
    assert_eq!(response, HttpResponsePayload::Accepted);

    cluster.wait_for_http("node-a").await;
    cluster
        .wait_for_workload(
            "node-a",
            "workload.duplicate.batch",
            Duration::from_secs(20),
        )
        .await;
}

#[tokio::test]
#[ignore = "requires docker compose"]
async fn docker_cluster_fake_peer_hello_does_not_break_node_health() {
    let cluster = DockerCluster::start("fake-peer-hello").await;

    cluster.wait_for_http("node-a").await;

    let response = cluster
        .client("node-a")
        .send(&HttpRequestPayload::Control(Box::new(
            ControlMessage::Hello(orion::control_plane::PeerHello {
                node_id: NodeId::new("totally.fake.peer"),
                desired_revision: Revision::new(999),
                desired_fingerprint: 0,
                desired_section_fingerprints:
                    orion::control_plane::DesiredStateSectionFingerprints {
                        nodes: 0,
                        artifacts: 0,
                        workloads: 0,
                        resources: 0,
                        providers: 0,
                        executors: 0,
                        leases: 0,
                    },
                observed_revision: Revision::new(888),
                applied_revision: Revision::new(777),
                transport_binding_version: None,
                transport_binding_public_key: None,
                transport_tls_cert_pem: None,
                transport_binding_signature: None,
            }),
        )))
        .await
        .expect("hello should respond");
    assert!(matches!(response, HttpResponsePayload::Hello(_)));

    cluster.wait_for_http("node-a").await;
}

#[tokio::test]
#[ignore = "requires docker compose"]
async fn docker_cluster_large_mutation_batch_stays_healthy_and_propagates() {
    let cluster = DockerCluster::start("large-batch").await;

    cluster.wait_for_http("node-a").await;
    cluster.wait_for_http("node-b").await;
    cluster.wait_for_http("node-c").await;

    let snapshot = cluster.snapshot("node-a").await;
    let mut mutations = Vec::new();
    for index in 0..64 {
        let workload_id = format!("workload.large.{index}");
        let artifact_id = ArtifactId::new(format!("artifact.large.{index}"));
        mutations.push(DesiredStateMutation::PutArtifact(
            orion::control_plane::ArtifactRecord::builder(artifact_id.clone()).build(),
        ));
        mutations.push(DesiredStateMutation::PutWorkload(
            orion::control_plane::WorkloadRecord::builder(
                WorkloadId::new(&workload_id),
                RuntimeType::new("graph.exec.v1"),
                artifact_id,
            )
            .desired_state(orion::control_plane::DesiredState::Stopped)
            .assigned_to(NodeId::new("node-a"))
            .build(),
        ));
    }
    let batch = MutationBatch {
        base_revision: snapshot.state.desired.revision,
        mutations,
    };

    let response = cluster
        .client("node-a")
        .send(&HttpRequestPayload::Control(Box::new(
            ControlMessage::Mutations(batch),
        )))
        .await
        .expect("large batch request should return");
    assert_eq!(response, HttpResponsePayload::Accepted);

    for node in ["node-b", "node-c"] {
        for workload_id in ["workload.large.0", "workload.large.31", "workload.large.63"] {
            cluster
                .wait_for_workload(node, workload_id, Duration::from_secs(30))
                .await;
        }
    }
}

#[tokio::test]
#[ignore = "requires docker compose"]
async fn docker_cluster_impossible_sync_revision_request_falls_back_without_breaking_health() {
    let cluster = DockerCluster::start("impossible-sync-revision").await;

    cluster.wait_for_http("node-a").await;

    let response = cluster
        .client("node-a")
        .send(&HttpRequestPayload::Control(Box::new(
            ControlMessage::SyncRequest(SyncRequest {
                node_id: NodeId::new("totally.fake.peer"),
                desired_revision: Revision::new(u64::MAX),
                desired_fingerprint: 0,
                desired_summary: None,
                sections: Vec::new(),
                object_selectors: Vec::new(),
            }),
        )))
        .await
        .expect("sync request should respond");

    assert!(matches!(
        response,
        HttpResponsePayload::Snapshot(_) | HttpResponsePayload::Accepted
    ));
    cluster.wait_for_http("node-a").await;
}

#[tokio::test]
#[ignore = "requires docker compose"]
async fn docker_cluster_impossible_mutation_revision_is_rejected_without_breaking_health() {
    let cluster = DockerCluster::start("impossible-mutation-revision").await;

    cluster.wait_for_http("node-a").await;

    let impossible_revision = encode_to_vec(&ControlMessage::Mutations(MutationBatch {
        base_revision: Revision::new(u64::MAX),
        mutations: Vec::new(),
    }))
    .expect("impossible revision payload should encode");

    let response = cluster
        .raw_post("node-a", "/v1/control/mutations", &impossible_revision)
        .await;
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);

    cluster.wait_for_http("node-a").await;
}

#[tokio::test]
#[ignore = "requires docker compose"]
async fn docker_cluster_returns_bad_request_for_control_payload_sent_to_observed_route() {
    let cluster = DockerCluster::start("observed-wrong-shape").await;

    cluster.wait_for_http("node-a").await;

    let wrong_payload = encode_to_vec(&ControlMessage::Hello(orion::control_plane::PeerHello {
        node_id: NodeId::new("fake-peer"),
        desired_revision: Revision::ZERO,
        desired_fingerprint: 0,
        desired_section_fingerprints: orion::control_plane::DesiredStateSectionFingerprints {
            nodes: 0,
            artifacts: 0,
            workloads: 0,
            resources: 0,
            providers: 0,
            executors: 0,
            leases: 0,
        },
        observed_revision: Revision::ZERO,
        applied_revision: Revision::ZERO,
        transport_binding_version: None,
        transport_binding_public_key: None,
        transport_tls_cert_pem: None,
        transport_binding_signature: None,
    }))
    .expect("hello payload should encode");

    let response = cluster
        .raw_post("node-a", "/v1/control/observed", &wrong_payload)
        .await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    cluster.wait_for_http("node-a").await;
}

#[tokio::test]
#[ignore = "requires docker compose"]
async fn docker_cluster_returns_bad_request_for_observed_route_with_garbage_but_cluster_stays_healthy()
 {
    let cluster = DockerCluster::start_with_options(
        "observed-garbage-five",
        ClusterOptions::with_node_count(5),
    )
    .await;

    cluster.wait_for_all_http().await;

    let response = cluster
        .raw_post(
            "node-c",
            "/v1/control/observed",
            br#"{"definitely":"wrong"}"#,
        )
        .await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    cluster
        .put_workload(
            "node-a",
            stopped_workload("workload.after-observed-garbage", "node-a"),
        )
        .await;

    for node in cluster.nodes().iter().map(String::as_str) {
        cluster
            .wait_for_workload(
                node,
                "workload.after-observed-garbage",
                Duration::from_secs(30),
            )
            .await;
    }
}
